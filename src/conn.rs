use std::collections::HashMap;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures::stream::{SplitSink, SplitStream};
use futures::{Future, FutureExt, SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{connect_async_with_config, MaybeTlsStream};

use crate::error::{Error, Result};
use crate::id_alloc::IdAllocator;
use crate::op::Op;
use crate::proto::{
    self, ClientMsg, CloseStreamReq, OpenStreamReq, Response, ServerMsg, Stmt, StmtResult,
};

type WebSocketStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WebSocketStream, tungstenite::Message>;
type WsStream = SplitStream<WebSocketStream>;
type HandleResponseCallback =
    Box<dyn FnOnce(&mut HranaConnState, Result<proto::Response>) + Sync + Send>;

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
struct HranaConnState {
    streams: HashMap<i32, StreamState>,
    requests: HashMap<i32, HandleResponseCallback>,
    id_allocator: IdAllocator,
}

pub struct HranaConnFut(JoinHandle<Result<()>>);

impl Future for HranaConnFut {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.0.poll_unpin(cx)) {
            Ok(Ok(r)) => Poll::Ready(Ok(r)),
            Ok(Err(e)) => Poll::Ready(Err(e)),
            Err(e) => Poll::Ready(Err(Error::Internal(e.to_string()))),
        }
    }
}

pub(crate) async fn spawn_hrana_conn(
    url: &str,
    jwt: Option<String>,
) -> Result<(mpsc::UnboundedSender<Op>, HranaConnFut)> {
    let (mut conn, sender) = HranaConn::connect(url, jwt).await?;
    let handle = tokio::spawn(async move {
        match conn.run().await {
            Ok(_) => conn.shutdown(Error::Shutdown),
            Err(e) => conn.shutdown(e),
        }

        conn.close().await
    });

    Ok((sender, HranaConnFut(handle)))
}

struct HranaConn {
    sink: WsSink,
    stream: WsStream,
    state: HranaConnState,
    receiver: mpsc::UnboundedReceiver<Op>,
}

fn get_ws_config() -> tungstenite::protocol::WebSocketConfig {
    tungstenite::protocol::WebSocketConfig {
        max_send_queue: Some(1 << 20),
        ..Default::default()
    }
}

impl HranaConn {
    pub(crate) async fn connect(
        url: &str,
        jwt: Option<String>,
    ) -> Result<(Self, mpsc::UnboundedSender<Op>)> {
        let mut request = url.into_client_request()?;
        request
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("hrana1"));

        let (ws, _) = connect_async_with_config(request, Some(get_ws_config())).await?;
        let (sink, stream) = ws.split();
        let (sender, receiver) = mpsc::unbounded_channel();

        let mut this = Self {
            sink,
            stream,
            state: HranaConnState::default(),
            receiver,
        };

        this.send_client_message(&ClientMsg::Hello { jwt }).await?;

        Ok((this, sender))
    }

    async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                Some(op) = self.receiver.recv() => {
                    self.handle_op(op).await?;
                },
                Some(msg) = self.stream.next() => {
                    self.handle_socket_msg(msg?)?;
                },
                else => break,
            }
        }

        Ok(())
    }

    async fn send_client_message(&mut self, req: &ClientMsg) -> Result<()> {
        let msg_data = serde_json::to_string(req).unwrap();
        self.sink.send(tungstenite::Message::Text(msg_data)).await?;

        Ok(())
    }

    async fn handle_op_create_stream(&mut self, ret: oneshot::Sender<i32>) -> Result<()> {
        // TODO: better id allocation
        let stream_id = self.state.id_allocator.allocate();
        let request_id = self.state.id_allocator.allocate();

        let msg = ClientMsg::Request {
            request_id,
            request: proto::Request::OpenStream(OpenStreamReq { stream_id }),
        };

        self.send_client_message(&msg).await?;

        if ret.send(stream_id).is_ok() {
            self.state.streams.insert(
                stream_id,
                StreamState::Opening {
                    waiters: Vec::new(),
                },
            );

            self.state.requests.insert(
                request_id,
                Box::new(move |state, resp| {
                    if let Some(state) = state.streams.get_mut(&stream_id) {
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
                            StreamState::Closed | StreamState::Open => {
                                unreachable!("invalid state")
                            }
                        }
                    }
                }),
            );
        } else {
            self.state.id_allocator.free(stream_id);
            self.state.id_allocator.free(request_id);
        }

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

        self.state.id_allocator.free(request_id);
        (ret)(&mut self.state, response);

        Ok(())
    }

    fn handle_socket_msg(&mut self, socket_msg: Message) -> Result<()> {
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
            (resp)(&mut self.state, Err(error.clone()))
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

        let request_id = self.state.id_allocator.allocate();
        let req = ClientMsg::Request {
            request_id,
            request: proto::Request::Execute(proto::ExecuteReq { stream_id, stmt }),
        };

        self.send_client_message(&req).await?;

        self.state.requests.insert(
            request_id,
            Box::new(move |_, resp| {
                let res = match resp {
                    Ok(r) => match r {
                        proto::Response::Execute(e) => Ok(e.result),
                        _ => Err(Error::BadResponse),
                    },
                    Err(e) => Err(e),
                };

                let _ = ret.send(res);
            }),
        );

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

        let request_id = self.state.id_allocator.allocate();
        let req = ClientMsg::Request {
            request_id,
            request: proto::Request::CloseStream(CloseStreamReq { stream_id }),
        };

        self.send_client_message(&req).await?;

        self.state.requests.insert(
            request_id,
            Box::new(move |state, resp| {
                state.streams.remove(&stream_id);
                state.id_allocator.free(stream_id);
                let res = resp.map(|_| ());
                let _ = ret.send(res);
            }),
        );

        Ok(())
    }

    async fn close(self) -> Result<()> {
        let mut ws = self.sink.reunite(self.stream).unwrap();
        ws.close(None).await?;
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

        let request_id = self.state.id_allocator.allocate();
        let req = ClientMsg::Request {
            request_id,
            request: proto::Request::Batch(proto::BatchReq { stream_id, batch }),
        };

        self.send_client_message(&req).await?;

        self.state.requests.insert(
            request_id,
            Box::new(move |_, resp| {
                let res = match resp {
                    Ok(r) => match r {
                        proto::Response::Batch(e) => Ok(e.result),
                        _ => Err(Error::BadResponse),
                    },
                    Err(e) => Err(e),
                };

                let _ = ret.send(res);
            }),
        );

        Ok(())
    }
}
