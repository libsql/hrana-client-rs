use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};
use crate::op::Op;
use crate::proto::{Batch, BatchResult, Stmt, StmtResult};

pub struct Stream {
    pub(crate) stream_id: i32,
    pub(crate) conn_sender: mpsc::UnboundedSender<Op>,
}

impl Stream {
    /// Waits for the stream opening to be acknowledged by the server
    pub async fn wait_for_open(&self) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let _ = self.conn_sender.send(Op::WaitStreamOpen {
            ret,
            stream_id: self.stream_id,
        });

        receiver.await.map_err(|_| Error::Shutdown)?
    }

    /// Execute a simple statement on this stream
    pub async fn execute(&self, stmt: Stmt) -> Result<StmtResult> {
        let (ret, receiver) = oneshot::channel();
        let _ = self.conn_sender.send(Op::Execute {
            stmt,
            ret,
            stream_id: self.stream_id,
        });

        receiver.await.map_err(|_| Error::StreamClosed)?
    }

    /// Execute a batch on this stream
    pub async fn execute_batch(&self, batch: Batch) -> Result<BatchResult> {
        let (ret, receiver) = oneshot::channel();
        let _ = self.conn_sender.send(Op::ExecuteBatch {
            batch,
            ret,
            stream_id: self.stream_id,
        });

        receiver.await.map_err(|_| Error::StreamClosed)?
    }

    /// Close the stream.
    pub async fn close(self) -> Result<()> {
        let (ret, receiver) = oneshot::channel();
        let _ = self.conn_sender.send(Op::CloseStream {
            stream_id: self.stream_id,
            ret,
        });

        receiver.await.map_err(|_| Error::StreamClosed)?
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        let (ret, _) = oneshot::channel();
        let _ = self.conn_sender.send(Op::CloseStream {
            stream_id: self.stream_id,
            ret,
        });
    }
}
