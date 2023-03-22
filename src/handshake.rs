use anyhow::Result;
use tokio_tungstenite::tungstenite;

pub type WebSocketStream = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

pub async fn handshake_tcp(url: String, socket: tokio::net::TcpStream) -> Result<WebSocketStream> {
    let ws_config = Some(get_ws_config());
    let (stream, _) = tokio_tungstenite::client_async_with_config(url, socket, ws_config).await?;
    Ok(stream)
}

fn get_ws_config() -> tungstenite::protocol::WebSocketConfig {
    tungstenite::protocol::WebSocketConfig {
        max_send_queue: Some(1 << 20),
        ..Default::default()
    }
}
