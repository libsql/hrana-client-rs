use tokio::sync::{mpsc, oneshot};

use crate::conn::{spawn_hrana_conn, HranaConnFut};
use crate::error::{Error, Result};
use crate::op::Op;
use crate::Stream;

#[derive(Clone)]
pub struct Client {
    conn_sender: mpsc::UnboundedSender<Op>,
}

impl Client {
    /// Connects to the remote hrana server.
    ///
    /// Returns a `Client` handle, along with a `HranaConnFut` that drives the socket connection,
    /// and needs to be awaited
    ///
    /// # Example:
    ///
    /// ```no_run
    /// let (client, fut) = Client::connect("ws://localhost:8080", None).await?;
    /// let handle = tokio::task::spawn(fut);
    /// let stream = client.open_stream().await?;
    /// // do things with stream...
    ///
    /// // collect errors
    /// handle.await??
    /// ```
    pub async fn connect(url: &str, jwt: Option<String>) -> Result<(Self, HranaConnFut)> {
        let (conn_sender, handle) = spawn_hrana_conn(url, jwt).await?;
        Ok((Self { conn_sender }, handle))
    }

    /// Open a new stream on this client
    pub async fn open_stream(&self) -> Result<Stream> {
        let (ret, recv) = oneshot::channel();
        let op = Op::OpenStream { ret };
        self.conn_sender.send(op).map_err(|_| Error::Shutdown)?;
        let stream_id = recv.await.map_err(|_| Error::Shutdown)?;

        Ok(Stream {
            stream_id,
            conn_sender: self.conn_sender.clone(),
        })
    }

    /// Client shutdown.
    /// Causes all inflight request abort with a `Shutdown` error.
    pub async fn shutdown(&self) -> Result<()> {
        let (ret, recv) = oneshot::channel();
        let op = Op::Shutdown { ret };
        self.conn_sender.send(op).map_err(|_| Error::Shutdown)?;
        recv.await.map_err(|_| Error::Shutdown)?;

        Ok(())
    }
}
