use std::sync::Arc;

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
    #[error(transparent)]
    WebSocket(Arc<tungstenite::Error>),
    #[error("received invalid message from server")]
    InvalidServerMessage,
    #[error("internal error: {0}")]
    Internal(String),
    #[error("{0}")]
    InvalidUrl(String),
    #[error("received response does not match any request")]
    RequestDoesNotExist,
    #[error("connection state is invalid")]
    InvalidState,
    #[error("invalid URI: {0}")]
    InvalidUri(#[from] Arc<hyper::http::uri::InvalidUri>),
    #[error("connect error: {0}")]
    Connect(Arc<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<tungstenite::Error> for Error {
    fn from(error: tungstenite::Error) -> Self {
        Self::WebSocket(Arc::new(error))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
