use anyhow::{Context, Error, Result};
use proto::Stmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;

mod handshake;
mod proto;

pub struct Client {
    ws: handshake::WebSocket,
}

impl Client {
    pub async fn open(url: impl Into<String>, jwt: impl Into<String>) -> Result<Self, Error> {
        let url: String = url.into();
        let jwt = jwt.into();
        println!("Ignored jwt: <{jwt}>");
        let tcp_stream = tokio::net::TcpStream::connect(url.clone()).await?;
        let ws = handshake::handshake_tcp("ws://".to_string() + &url, tcp_stream).await?;
        println!("Handshake done");
        Ok(Self { ws })
    }

    pub async fn open_stream(self_: Arc<Mutex<Self>>) -> Result<Stream, Error> {
        //let (tx, rx) = oneshot::channel(); # FIXME: we'll get there
        let req = proto::ClientMsg::Request {
            request_id: 0,
            request: proto::Request::OpenStream(proto::OpenStreamReq { stream_id: 0 }),
        };
        let mut client = self_.lock().await;
        client.send_msg(&req).await?;
        let resp = client
            .recv_msg()
            .await
            .ok_or(anyhow::anyhow!("Receiving failed"))?;
        drop(client);
        println!("Response: {resp:?}");
        Ok(Stream {
            id: 42,
            client: self_,
        })
    }

    pub async fn send_msg(&mut self, msg: &proto::ClientMsg) -> Result<()> {
        let msg = serde_json::to_string(&msg).context("Could not serialize response message")?;
        let msg = tungstenite::Message::Text(msg);
        self.ws
            .send(msg)
            .await
            .context("Could not send response to the WebSocket")
    }

    pub async fn recv_msg(&mut self) -> Option<tungstenite::Result<tungstenite::Message>> {
        self.ws.recv().await
    }

    pub async fn close(&mut self) -> Result<()> {
        // TODO
        Ok(())
    }
}

// Arc-Mutex makes no sense here, it will all be rewritten to a reactor
// that runs I/O operations in a separate fiber and communicates
// via channels.
pub struct Stream {
    id: i32,
    client: Arc<Mutex<Client>>,
}

//TODO: actually implement this
#[derive(Debug)]
pub struct Row {
    columns: Vec<String>,
    values: Vec<String>,
}

impl Stream {
    pub async fn execute(&mut self, stmt: impl Into<String>) -> Result<Vec<Row>> {
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
        let mut client = self.client.lock().await;
        client.send_msg(&req).await?;
        let resp = client
            .recv_msg()
            .await
            .ok_or(anyhow::anyhow!("Receiving failed"))?;
        println!("Response: {resp:?}");
        Ok(vec![])
    }

    pub async fn query_value(&mut self, stmt: impl Into<String>) -> Result<Vec<Row>> {
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
        let mut client = self.client.lock().await;
        client.send_msg(&req).await?;
        let resp = client
            .recv_msg()
            .await
            .ok_or(anyhow::anyhow!("Receiving failed"))?;
        println!("Response: {resp:?}");
        Ok(vec![])
    }

    pub async fn query_row(&mut self, stmt: impl Into<String>) -> Result<Vec<Row>> {
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
        let mut client = self.client.lock().await;
        client.send_msg(&req).await?;
        let resp = client
            .recv_msg()
            .await
            .ok_or(anyhow::anyhow!("Receiving failed"))?;
        println!("Response: {resp:?}");
        Ok(vec![])
    }

    pub async fn query(&mut self, stmt: impl Into<String>) -> Result<Vec<Row>> {
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
        let mut client = self.client.lock().await;
        client.send_msg(&req).await?;
        let resp = client
            .recv_msg()
            .await
            .ok_or(anyhow::anyhow!("Receiving failed"))?;
        println!("Response: {resp:?}");
        Ok(vec![])
    }
}
