use serde::{Deserialize, Serialize};

use crate::batch::{BatchReq, BatchResp};
use crate::stmt::StmtResult;
use crate::{Error, Stmt};

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    Hello { jwt: Option<String> },
    Request { request_id: i32, request: Request },
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    HelloOk {},
    HelloError { error: Error },
    ResponseOk { request_id: i32, response: Response },
    ResponseError { request_id: i32, error: Error },
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    OpenStream(OpenStreamReq),
    CloseStream(CloseStreamReq),
    Execute(ExecuteReq),
    Batch(BatchReq),
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    OpenStream(OpenStreamResp),
    CloseStream(CloseStreamResp),
    Execute(ExecuteResp),
    Batch(BatchResp),
}

#[derive(Serialize, Debug)]
pub struct OpenStreamReq {
    pub stream_id: i32,
}

#[derive(Deserialize, Debug)]
pub struct OpenStreamResp {}

#[derive(Serialize, Debug)]
pub struct CloseStreamReq {
    pub stream_id: i32,
}

#[derive(Deserialize, Debug)]
pub struct CloseStreamResp {}

#[derive(Serialize, Debug)]
pub struct ExecuteReq {
    pub stream_id: i32,
    pub stmt: Stmt,
}

#[derive(Deserialize, Debug)]
pub struct ExecuteResp {
    pub result: StmtResult,
}
