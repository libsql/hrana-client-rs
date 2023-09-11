use std::collections::HashMap;
use std::future::poll_fn;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use futures::{Future, FutureExt, SinkExt, StreamExt};
use hyper::Uri;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{client_async_tls_with_config, MaybeTlsStream};
use tower::make::MakeConnection;

use crate::error::{Error, Result};
use crate::id_alloc::IdAllocator;
use crate::op::Op;
use crate::proto::{
    self, ClientMsg, CloseStreamReq, OpenStreamReq, Response, ServerMsg, Stmt, StmtResult,
};
use crate::Stream;

type WebSocketStream<S> = tokio_tungstenite::WebSocketStream<MaybeTlsStream<S>>;
type HandleResponseCallback =
    Box<dyn FnOnce(&mut ConnState, Result<proto::Response>) -> Result<()> + Sync + Send>;

enum StreamState {
    Open,
    Closed,
    /// stream is waiting to open.
    Opening {
        waiters: Vec<oneshot::Sender<Result<()>>>,
    },
}

impl StreamState {
    fn is_closed(&self) -> bool {
        matches!(self, StreamState::Closed)
    }
}

#[derive(Default)]
struct ConnState {
    streams: HashMap<i32, StreamState>,
    requests: HashMap<i32, HandleResponseCallback>,
    id_allocator: IdAllocator,
}

pub struct ConnFut(JoinHandle<Result<()>>);

impl Future for ConnFut {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.0.poll_unpin(cx)) {
            Ok(Ok(r)) => Poll::Ready(Ok(r)),
            Ok(Err(e)) => Poll::Ready(Err(e)),
            Err(e) => Poll::Ready(Err(Error::Internal(e.to_string()))),
        }
    }
}

pub(crate) async fn spawn_hrana_conn<M>(
    url: &str,
    jwt: Option<String>,
    mut make_conn: M,
) -> Result<(mpsc::UnboundedSender<Op>, ConnFut)>
where
    M: MakeConnection<Uri>,
    M::Connection: Send + 'static + Unpin,
    M::Error: std::error::Error + Sync + Send + 'static,
{
    poll_fn(|cx| make_conn.poll_ready(cx))
        .await
        .map_err(|e| Error::Connect(Arc::new(e)))?;
    let stream = make_conn
        .make_connection(Uri::from_str(url).map_err(Arc::new)?)
        .await
        .map_err(|e| Error::Connect(Arc::new(e)))?;
    let mut conn = HranaConn::connect(stream, url, jwt).await?;
    let sender = conn.sender();
    let handle = tokio::spawn(async move {
        match conn.run().await {
            Ok(_) => conn.shutdown(Error::Shutdown),
            Err(e) => conn.shutdown(e),
        }

        conn.close().await
    });

    Ok((sender, ConnFut(handle)))
}

struct HranaConn<S> {
    ws: WebSocketStream<S>,
    state: ConnState,
    receiver: mpsc::UnboundedReceiver<Op>,
    sender: mpsc::UnboundedSender<Op>,
}

fn get_ws_config() -> tungstenite::protocol::WebSocketConfig {
    tungstenite::protocol::WebSocketConfig {
        max_send_queue: Some(1 << 20),
        ..Default::default()
    }
}

impl<S> HranaConn<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub(crate) async fn connect(stream: S, url: &str, jwt: Option<String>) -> Result<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut request = url.into_client_request()?;
        request
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("hrana1"));

        let (ws, _) =
            client_async_tls_with_config(request, stream, Some(get_ws_config()), None).await?;
        let (sender, receiver) = mpsc::unbounded_channel();

        let mut this = Self {
            ws,
            state: ConnState::default(),
            receiver,
            sender,
        };

        this.send_message(&ClientMsg::Hello { jwt }).await?;

        Ok(this)
    }

    fn sender(&self) -> mpsc::UnboundedSender<Op> {
        self.sender.clone()
    }

    async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                Some(op) = self.receiver.recv() => {
                    self.handle_op(op).await?;
                },
                Some(msg) = self.ws.next() => {
                    self.handle_socket_msg(msg?).await?;
                },
                else => break,
            }
        }

        Ok(())
    }

    async fn send_message(&mut self, req: &ClientMsg) -> Result<()> {
        let msg_data = serde_json::to_string(req).unwrap();
        self.ws.send(tungstenite::Message::Text(msg_data)).await?;

        Ok(())
    }

    async fn send_request(
        &mut self,
        request: proto::Request,
        cb: HandleResponseCallback,
    ) -> Result<()> {
        let request_id = self.state.id_allocator.allocate();
        let msg = ClientMsg::Request {
            request_id,
            request,
        };
        self.send_message(&msg).await?;

        self.state.requests.insert(request_id, cb);

        Ok(())
    }

    async fn handle_op_create_stream(&mut self, ret: oneshot::Sender<Stream>) -> Result<()> {
        let stream_id = self.state.id_allocator.allocate();

        let cb: HandleResponseCallback = Box::new(move |state, resp| {
            let Some(state) = state.streams.get_mut(&stream_id) else { return Err(Error::StreamDoesNotExist)};
            *state = match state {
                StreamState::Opening { waiters } => match resp {
                    Ok(proto::Response::OpenStream(_)) => {
                        for waiter in waiters.drain(..) {
                            let _ = waiter.send(Ok(()));
                        }
                        StreamState::Open
                    }
                    Ok(_) => {
                        for waiter in waiters.drain(..) {
                            let _ = waiter.send(Err(Error::BadResponse));
                        }
                        StreamState::Closed
                    }
                    Err(e) => {
                        for waiter in waiters.drain(..) {
                            let _ = waiter.send(Err(e.clone()));
                        }
                        StreamState::Closed
                    }
                },
                StreamState::Closed | StreamState::Open => return Err(Error::InvalidState),
            };

            Ok(())
        });

        self.send_request(proto::Request::OpenStream(OpenStreamReq { stream_id }), cb)
            .await?;

        let stream = Stream {
            stream_id,
            conn_sender: self.sender(),
        };
        let _ = ret.send(stream);

        self.state.streams.insert(
            stream_id,
            StreamState::Opening {
                waiters: Vec::new(),
            },
        );

        Ok(())
    }

    async fn handle_op_wait_stream_open(
        &mut self,
        ret: oneshot::Sender<Result<()>>,
        stream_id: i32,
    ) {
        let res = match self.state.streams.get_mut(&stream_id) {
            Some(state) => match state {
                StreamState::Open => Ok(()),
                StreamState::Closed => Err(Error::StreamClosed),
                StreamState::Opening { ref mut waiters } => {
                    waiters.push(ret);
                    return;
                }
            },
            None => Err(Error::StreamDoesNotExist),
        };

        let _ = ret.send(res);
    }

    async fn handle_op(&mut self, op: Op) -> Result<()> {
        match op {
            Op::OpenStream { ret } => self.handle_op_create_stream(ret).await?,
            Op::Execute {
                stmt,
                ret,
                stream_id,
            } => self.handle_op_execute(ret, stream_id, stmt).await?,
            Op::WaitStreamOpen { ret, stream_id } => {
                self.handle_op_wait_stream_open(ret, stream_id).await
            }
            Op::CloseStream { stream_id, ret } => {
                self.handle_op_close_stream(ret, stream_id).await?
            }
            Op::ExecuteBatch {
                batch,
                ret,
                stream_id,
            } => self.handle_op_execute_batch(ret, stream_id, batch).await?,
            Op::Shutdown { ret } => {
                let _ = ret.send(());
                return Err(Error::Shutdown);
            }
        }

        Ok(())
    }

    fn handle_response(&mut self, request_id: i32, response: Result<Response>) -> Result<()> {
        let ret = self
            .state
            .requests
            .remove(&request_id)
            .ok_or(Error::RequestDoesNotExist)?;

        (ret)(&mut self.state, response)
    }

    async fn handle_socket_msg(&mut self, socket_msg: Message) -> Result<()> {
        match socket_msg {
            Message::Text(json) => {
                let server_msg: ServerMsg =
                    serde_json::from_str(&json).map_err(|_| Error::InvalidServerMessage)?;
                match server_msg {
                    ServerMsg::HelloOk {} => (),
                    ServerMsg::HelloError { error } => {
                        return Err(error.into());
                    }
                    ServerMsg::ResponseOk {
                        request_id,
                        response,
                    } => self.handle_response(request_id, Ok(response))?,
                    ServerMsg::ResponseError { request_id, error } => {
                        self.handle_response(request_id, Err(error.into()))?
                    }
                }
            }
            Message::Close(_) => {
                return Err(Error::Shutdown);
            }
            Message::Ping(_) => {
                self.ws.send(Message::Pong(Vec::new())).await?;
            }
            Message::Pong(_) => (),
            _ => return Err(Error::InvalidServerMessage),
        }

        Ok(())
    }

    fn shutdown(&mut self, error: Error) {
        for (_, state) in self.state.streams.iter_mut() {
            *state = StreamState::Closed;
        }

        let requests = std::mem::take(&mut self.state.requests);
        for (_, resp) in requests.into_iter() {
            // ignore the error, at this point we're already shutting down...
            let _ = (resp)(&mut self.state, Err(error.clone()));
        }
    }

    async fn handle_op_execute(
        &mut self,
        ret: oneshot::Sender<std::result::Result<StmtResult, Error>>,
        stream_id: i32,
        stmt: Stmt,
    ) -> Result<()> {
        if let Err(e) = self.get_stream_mut(stream_id) {
            let _ = ret.send(Err(e));
            return Ok(());
        }

        let cb: HandleResponseCallback = Box::new(move |_, resp| {
            let res = match resp {
                Ok(r) => match r {
                    proto::Response::Execute(e) => Ok(e.result),
                    _ => Err(Error::BadResponse),
                },
                Err(e) => Err(e),
            };

            let _ = ret.send(res);

            Ok(())
        });

        self.send_request(
            proto::Request::Execute(proto::ExecuteReq { stream_id, stmt }),
            cb,
        )
        .await?;

        Ok(())
    }

    async fn handle_op_close_stream(
        &mut self,
        ret: oneshot::Sender<Result<()>>,
        stream_id: i32,
    ) -> Result<()> {
        match self.get_stream_mut(stream_id) {
            Err(e) => {
                let _ = ret.send(Err(e));
                return Ok(());
            }
            Ok(s) => {
                *s = StreamState::Closed;
            }
        }

        let cb: HandleResponseCallback = Box::new(move |state, resp| {
            state.streams.remove(&stream_id);
            state.id_allocator.free(stream_id);
            let res = resp.map(|_| ());
            let _ = ret.send(res);

            Ok(())
        });

        self.send_request(
            proto::Request::CloseStream(CloseStreamReq { stream_id }),
            cb,
        )
        .await?;

        Ok(())
    }

    async fn close(mut self) -> Result<()> {
        self.ws.close(None).await?;
        Ok(())
    }

    fn get_stream_mut(&mut self, stream_id: i32) -> Result<&mut StreamState> {
        match self.state.streams.get_mut(&stream_id) {
            Some(s) if s.is_closed() => Err(Error::StreamClosed),
            Some(s) => Ok(s),
            None => Err(Error::StreamDoesNotExist),
        }
    }

    async fn handle_op_execute_batch(
        &mut self,
        ret: oneshot::Sender<std::result::Result<proto::BatchResult, Error>>,
        stream_id: i32,
        batch: proto::Batch,
    ) -> Result<()> {
        if let Err(e) = self.get_stream_mut(stream_id) {
            let _ = ret.send(Err(e));
            return Ok(());
        }

        let cb: HandleResponseCallback = Box::new(move |_, resp| {
            let res = match resp {
                Ok(r) => match r {
                    proto::Response::Batch(e) => Ok(e.result),
                    _ => Err(Error::BadResponse),
                },
                Err(e) => Err(e),
            };

            let _ = ret.send(res);

            Ok(())
        });

        self.send_request(
            proto::Request::Batch(proto::BatchReq { stream_id, batch }),
            cb,
        )
        .await?;

        Ok(())
    }
}
