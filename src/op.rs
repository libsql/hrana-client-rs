use tokio::sync::oneshot;

use crate::error::Result;
use crate::proto::{Batch, BatchResult, Stmt, StmtResult};
use crate::Stream;

pub(crate) enum Op {
    OpenStream {
        ret: oneshot::Sender<Stream>,
    },
    Execute {
        ret: oneshot::Sender<Result<StmtResult>>,
        stream_id: i32,
        stmt: Stmt,
    },
    ExecuteBatch {
        ret: oneshot::Sender<Result<BatchResult>>,
        stream_id: i32,
        batch: Batch,
    },
    WaitStreamOpen {
        ret: oneshot::Sender<Result<()>>,
        stream_id: i32,
    },
    CloseStream {
        ret: oneshot::Sender<Result<()>>,
        stream_id: i32,
    },
    Shutdown {
        ret: oneshot::Sender<()>,
    },
}
