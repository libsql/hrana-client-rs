use tokio_tungstenite::tungstenite;

use crate::proto;

#[derive(Debug, thiserror::Error, Clone)]
pub enum Error {
    #[error("URL is missing host")]
    MissingHost,
    #[error("stream closed")]
    StreamClosed,
    #[error("stream doesn't exist")]
    StreamDoesNotExist,
    #[error(transparent)]
    HranaError(#[from] proto::Error),
    #[error("received bad response from server")]
    BadResponse,
    #[error("client shutdown")]
    Shutdown,
    #[error("websocket error: {0}")]
    WebSocket(String),
    #[error("received invalid message from server")]
    InvalidServerMessage,
    #[error("internal error: {0}")]
    Internal(String),
    #[error("{0}")]
    InvalidUrl(String),
    #[error("received response does not match any request")]
    RequestDoesNotExist,
}

impl From<tungstenite::Error> for Error {
    fn from(error: tungstenite::Error) -> Self {
        Self::WebSocket(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
