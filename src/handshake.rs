use anyhow::Result;
use futures::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite;

#[derive(Debug)]
pub struct WebSocket {
    stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
}

pub async fn handshake_tcp(url: String, socket: tokio::net::TcpStream) -> Result<WebSocket> {
    let ws_config = Some(get_ws_config());
    let (stream, _) = tokio_tungstenite::client_async_with_config(url, socket, ws_config).await?;
    Ok(WebSocket { stream })
}

fn get_ws_config() -> tungstenite::protocol::WebSocketConfig {
    tungstenite::protocol::WebSocketConfig {
        max_send_queue: Some(1 << 20),
        ..Default::default()
    }
}

impl WebSocket {
    pub async fn recv(&mut self) -> Option<tungstenite::Result<tungstenite::Message>> {
        self.stream.next().await
    }

    pub async fn send(&mut self, msg: tungstenite::Message) -> tungstenite::Result<()> {
        self.stream.send(msg).await
    }
}
