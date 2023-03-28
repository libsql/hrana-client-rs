use tokio::sync::oneshot;

use crate::error::Result;
use crate::proto::{Batch, BatchResult, Stmt, StmtResult};
use crate::Stream;

pub(crate) enum Op {
    OpenStream {
        ret: oneshot::Sender<Stream>,
    },
    Execute {
        stmt: Stmt,
        ret: oneshot::Sender<Result<StmtResult>>,
        stream_id: i32,
    },
    ExecuteBatch {
        batch: Batch,
        ret: oneshot::Sender<Result<BatchResult>>,
        stream_id: i32,
    },
    WaitStreamOpen {
        ret: oneshot::Sender<Result<()>>,
        stream_id: i32,
    },
    CloseStream {
        stream_id: i32,
        ret: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        ret: oneshot::Sender<()>,
    },
}
