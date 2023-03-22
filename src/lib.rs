use anyhow::{Context, Error, Result};
use futures::future::FutureExt;
use futures::{SinkExt as _, StreamExt as _};
use proto::Stmt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;

pub mod handshake;
pub mod proto;

type ClientReceiver = tokio::sync::mpsc::Receiver<proto::ClientMsg>;
type ServerSender = tokio::sync::mpsc::Sender<proto::ServerMsg>;

pub struct Client {
    client_receiver_handles: Arc<Mutex<HashMap<i32, ClientReceiver>>>,
    server_sender_handles: Arc<Mutex<HashMap<i32, ServerSender>>>,
    _reader_handle: futures::future::RemoteHandle<Result<()>>,
    _writer_handle: futures::future::RemoteHandle<Result<()>>,
}

impl Client {
    pub async fn open(url: impl Into<String>, jwt: impl Into<String>) -> Result<Self, Error> {
        let url: String = url.into();
        let jwt = jwt.into();
        tracing::debug!("Ignored jwt: <{jwt}>");
        let tcp_stream = tokio::net::TcpStream::connect(url.clone()).await?;
        let ws = handshake::handshake_tcp("ws://".to_string() + &url, tcp_stream).await?;
        let (mut write_half, mut read_half) = ws.split();
        tracing::debug!("Handshake done");
        write_half
            .send(Self::serialize_msg(&proto::ClientMsg::Hello { jwt: None })?)
            .await?;
        // FIXME: do not receive here, allow pipelining after Hello is sent
        read_half
            .next()
            .await
            .context("Receiving HelloOk failed")??;
        let client_receiver_handles = Arc::new(Mutex::new(HashMap::<i32, ClientReceiver>::new()));
        let server_sender_handles = Arc::new(Mutex::new(HashMap::<i32, ServerSender>::new()));
        let client_receiver_handles_ = client_receiver_handles.clone();
        let server_sender_handles_ = server_sender_handles.clone();
        let request_id_to_stream_for_reader =
            Arc::new(std::sync::Mutex::new(HashMap::<i32, i32>::new()));
        let request_id_to_stream_for_writer = request_id_to_stream_for_reader.clone();

        // The reader fiber
        let (reader_task, _reader_handle) = async move {
            let mut write_half = write_half;
            loop {
                // The lock is expected not to be contended, except for open_stream(), which is supposed to be rare
                let mut receiver_handles = client_receiver_handles_.lock().await;
                for (stream_id, receiver_handle) in receiver_handles.iter_mut() {
                    // FIXME: select! instead of try_recv, or wait on a condition variable until *any* message arrives
                    if let Ok(msg) = receiver_handle.try_recv() {
                        tracing::debug!("Sending message: {msg:?}");
                        let request_id = if let proto::ClientMsg::Request { request_id, .. } = &msg
                        {
                            request_id
                        } else {
                            anyhow::bail!("Unexpected message: RequestMsg expected")
                        };
                        tracing::debug!("Sending message from stream {stream_id}: {request_id}");
                        {
                            let mut request_id_to_stream =
                                request_id_to_stream_for_reader.lock().unwrap();
                            request_id_to_stream.insert(*request_id, *stream_id);
                        }
                        // Send the message to the server
                        write_half.send(Self::serialize_msg(&msg)?).await?;
                    }
                }
            }
        }
        .remote_handle();
        tokio::spawn(reader_task);

        // The writer fiber
        let (writer_task, _writer_handle) = async move {
            let mut read_half = read_half;
            loop {
                let resp = read_half.next().await.context("Receiving failed")??;
                let resp = match resp {
                    tungstenite::Message::Text(text) => {
                        serde_json::from_str::<proto::ServerMsg>(&text)
                            .context("Could not parse message")
                    }

                    _ => Err(anyhow::anyhow!("Unexpected message")),
                }?;
                tracing::debug!("Received message: {resp:?}");
                let mut sender_handles = server_sender_handles_.lock().await;
                let request_id = match &resp {
                    proto::ServerMsg::ResponseOk { request_id, .. } => request_id,
                    proto::ServerMsg::ResponseError { request_id, .. } => request_id,
                    _ => anyhow::bail!("Unexpected message: ResponseMsg expected"),
                };
                let stream_id = {
                    let mut request_id_to_stream = request_id_to_stream_for_writer.lock().unwrap();
                    request_id_to_stream.remove(request_id).unwrap()
                };
                tracing::debug!("Sending message to stream {stream_id}: {request_id}");
                let sender_handle = sender_handles.get_mut(&stream_id).unwrap();
                sender_handle.send(resp).await?;
            }
        }
        .remote_handle();
        tokio::spawn(writer_task);

        let client = Self {
            client_receiver_handles,
            server_sender_handles,
            _reader_handle,
            _writer_handle,
        };
        Ok(client)
    }

    pub async fn open_stream(&mut self) -> Result<Stream, Error> {
        let (to_server_sender, to_server_receiver) = tokio::sync::mpsc::channel(1024);
        let (from_server_sender, mut from_server_receiver) = tokio::sync::mpsc::channel(1024);

        // FIXME: if we assume that open_stream is rare, we can use an RCU-like scheme
        let stream_id = {
            let mut client_receiver_handles = self.client_receiver_handles.lock().await;
            let mut server_sender_handles = self.server_sender_handles.lock().await;
            let stream_id = client_receiver_handles.len() as i32;
            client_receiver_handles.insert(stream_id, to_server_receiver);
            server_sender_handles.insert(stream_id, from_server_sender);
            stream_id
        };

        let req = proto::ClientMsg::Request {
            request_id: 0,
            request: proto::Request::OpenStream(proto::OpenStreamReq { stream_id }),
        };
        to_server_sender.send(req).await?;
        // FIXME: do not receive here, allow pipelining after OpenStream is sent
        let resp = from_server_receiver
            .recv()
            .await
            .context("Receiving failed")?;
        tracing::debug!("Response: {resp:?}");
        Ok(Stream::new(
            stream_id,
            to_server_sender,
            from_server_receiver,
        ))
    }

    fn serialize_msg(msg: &proto::ClientMsg) -> Result<tungstenite::Message> {
        let msg = serde_json::to_string(&msg).context("Could not serialize response message")?;
        Ok(tungstenite::Message::Text(msg))
    }

    pub async fn close(&mut self) -> Result<()> {
        // TODO
        Ok(())
    }
}

// Arc-Mutex makes *very* little sense here, it will all be rewritten to a reactor
// that runs I/O operations in a separate fiber and communicates via channels.
pub struct Stream {
    id: i32,
    next_request_id: i32,
    to_server_sender: tokio::sync::mpsc::Sender<proto::ClientMsg>,
    from_server_receiver: tokio::sync::mpsc::Receiver<proto::ServerMsg>,
}

impl Stream {
    fn new(
        stream_id: i32,
        to_server_sender: tokio::sync::mpsc::Sender<proto::ClientMsg>,
        from_server_receiver: tokio::sync::mpsc::Receiver<proto::ServerMsg>,
    ) -> Self {
        Self {
            id: stream_id,
            next_request_id: 1,
            to_server_sender,
            from_server_receiver,
        }
    }

    fn next_request_id(&mut self) -> i32 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    pub async fn execute(&mut self, stmt: impl Into<String>) -> Result<proto::StmtResult> {
        let stmt = stmt.into();
        let req = proto::ClientMsg::Request {
            request_id: self.next_request_id(),
            request: proto::Request::Execute(proto::ExecuteReq {
                stream_id: self.id,
                stmt: Stmt {
                    sql: stmt,
                    args: vec![],
                    named_args: vec![],
                    want_rows: false,
                },
            }),
        };
        self.to_server_sender.send(req).await?;
        let resp = self
            .from_server_receiver
            .recv()
            .await
            .context("Receiving failed")?;
        match resp {
            proto::ServerMsg::ResponseOk {
                request_id: _,
                response,
            } => match response {
                proto::Response::Execute(proto::ExecuteResp { result }) => Ok(result),
                _ => Err(anyhow::anyhow!("Unexpected response")),
            },
            _ => Err(anyhow::anyhow!("Unexpected message")),
        }
    }
}
