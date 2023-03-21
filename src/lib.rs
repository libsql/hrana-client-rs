use anyhow::{Context, Error, Result};
use futures::future::FutureExt;
use handshake::WebSocket;
use proto::Stmt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;

mod handshake;
mod proto;

pub struct StreamHandle {
    to_server_receiver: tokio::sync::mpsc::Receiver<proto::ClientMsg>,
    from_server_sender: tokio::sync::mpsc::Sender<proto::ServerMsg>,
}

pub struct Client {
    // FIXME: split to two maps: one for senders, one for receivers
    stream_handles: Arc<Mutex<HashMap<i32, StreamHandle>>>,
    _worker_handle: futures::future::RemoteHandle<Result<()>>,
}

impl Client {
    pub async fn open(url: impl Into<String>, jwt: impl Into<String>) -> Result<Self, Error> {
        let url: String = url.into();
        let jwt = jwt.into();
        println!("Ignored jwt: <{jwt}>");
        let tcp_stream = tokio::net::TcpStream::connect(url.clone()).await?;
        let mut ws = handshake::handshake_tcp("ws://".to_string() + &url, tcp_stream).await?;
        println!("Handshake done");
        Self::send_msg(&mut ws, &proto::ClientMsg::Hello { jwt: None })
            .await
            .context("Could not send Hello message")?;
        Self::recv_msg(&mut ws)
            .await
            .context("Receiving HelloOk failed")??;
        let stream_handles = Arc::new(Mutex::new(HashMap::<i32, StreamHandle>::new()));
        let stream_handles_for_worker = stream_handles.clone();
        let (worker_task, _worker_handle) = async move {
            let mut ws = ws;
            loop {
                let mut handles = stream_handles_for_worker.lock().await;
                for handle in handles.values_mut() {
                    // FIXME: select! instead of try_recv, or wait on a condition variable until *any* message arrives
                    if let Ok(msg) = handle.to_server_receiver.try_recv() {
                        println!("Sending message: {msg:?}");
                        // Send the message to the server
                        Self::send_msg(&mut ws, &msg).await?;
                        let resp = Self::recv_msg(&mut ws)
                            .await
                            .ok_or(anyhow::anyhow!("Receiving failed"))??;
                        println!("Sent message: {resp:?}");
                        let resp = match resp {
                            tungstenite::Message::Text(text) => {
                                serde_json::from_str::<proto::ServerMsg>(&text)
                                    .context("Could not parse message")
                            }

                            _ => Err(anyhow::anyhow!("Unexpected message")),
                        }?;
                        handle.from_server_sender.send(resp).await?;
                    }
                }
            }
        }
        .remote_handle();
        tokio::spawn(worker_task);
        let client = Self {
            stream_handles,
            _worker_handle,
        };
        Ok(client)
    }

    pub async fn open_stream(&mut self) -> Result<Stream, Error> {
        let (to_server_sender, to_server_receiver) = tokio::sync::mpsc::channel(1024);
        let (from_server_sender, mut from_server_receiver) = tokio::sync::mpsc::channel(1024);

        // FIXME: if we assume that open_stream is rare, we can use an RCU-like scheme
        let mut handles = self.stream_handles.lock().await;
        let stream_id = handles.len() as i32;
        handles.insert(
            stream_id,
            StreamHandle {
                to_server_receiver,
                from_server_sender,
            },
        );
        drop(handles);

        let req = proto::ClientMsg::Request {
            request_id: 0,
            request: proto::Request::OpenStream(proto::OpenStreamReq { stream_id: 0 }),
        };
        to_server_sender.send(req).await?;
        let resp = from_server_receiver
            .recv()
            .await
            .context("Receiving failed")?;
        println!("Response: {resp:?}");
        Ok(Stream {
            id: 0,
            to_server_sender,
            from_server_receiver,
        })
    }

    pub async fn send_msg(ws: &mut WebSocket, msg: &proto::ClientMsg) -> Result<()> {
        let msg = serde_json::to_string(&msg).context("Could not serialize response message")?;
        let msg = tungstenite::Message::Text(msg);
        ws.send(msg)
            .await
            .context("Could not send response to the WebSocket")
    }

    pub async fn recv_msg(ws: &mut WebSocket) -> Option<tungstenite::Result<tungstenite::Message>> {
        ws.recv().await
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
    to_server_sender: tokio::sync::mpsc::Sender<proto::ClientMsg>,
    from_server_receiver: tokio::sync::mpsc::Receiver<proto::ServerMsg>,
}

impl Stream {
    pub async fn execute(&mut self, stmt: impl Into<String>) -> Result<proto::StmtResult> {
        let stmt = stmt.into();
        let req = proto::ClientMsg::Request {
            request_id: 0,
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
