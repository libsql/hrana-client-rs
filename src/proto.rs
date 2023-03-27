//! Messages in the Hrana protocol.
//!
//! Please consult the Hrana specification in the `docs/` directory for more information.
use std::fmt;

use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Debug)]
pub struct Stmt {
    pub sql: String,
    #[serde(default)]
    pub args: Vec<Value>,
    #[serde(default)]
    pub named_args: Vec<NamedArg>,
    pub want_rows: bool,
}

impl Stmt {
    pub fn new(sql: String, want_rows: bool) -> Self {
        Self {
            sql,
            want_rows,
            named_args: Vec::new(),
            args: Vec::new(),
        }
    }

    pub fn bind(&mut self, val: Value) {
        self.args.push(val);
    }

    pub fn bind_named(&mut self, name: String, value: Value) {
        self.named_args.push(NamedArg { name, value });
    }
}

#[derive(Serialize, Debug)]
pub struct NamedArg {
    pub name: String,
    pub value: Value,
}

#[derive(Deserialize, Debug)]
pub struct StmtResult {
    pub cols: Vec<Col>,
    pub rows: Vec<Vec<Value>>,
    pub affected_row_count: u64,
    #[serde(with = "option_i64_as_str")]
    pub last_insert_rowid: Option<i64>,
}

#[derive(Deserialize, Debug)]
pub struct Col {
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Value {
    Null,
    Integer {
        #[serde(with = "i64_as_str")]
        value: i64,
    },
    Float {
        value: f64,
    },
    Text {
        value: String,
    },
    Blob {
        #[serde(with = "bytes_as_base64", rename = "base64")]
        value: Vec<u8>,
    },
}

#[derive(Serialize, Debug)]
pub struct BatchReq {
    pub stream_id: i32,
    pub batch: Batch,
}

#[derive(Serialize, Debug, Default)]
pub struct Batch {
    steps: Vec<BatchStep>,
}

impl Batch {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub fn step(&mut self, condition: Option<BatchCond>, stmt: Stmt) {
        self.steps.push(BatchStep { condition, stmt });
    }
}

#[derive(Serialize, Debug)]
pub struct BatchStep {
    condition: Option<BatchCond>,
    stmt: Stmt,
}

#[derive(Serialize, Debug)]
pub enum BatchCond {
    Ok { step: i32 },
    Error { step: i32 },
    Not { cond: Box<BatchCond> },
    And { conds: Vec<BatchCond> },
    Or { conds: Vec<BatchCond> },
}

#[derive(Deserialize, Debug)]
pub struct BatchResp {
    pub result: BatchResult,
}

#[derive(Deserialize, Debug)]
pub struct BatchResult {
    pub step_results: Vec<Option<StmtResult>>,
    pub step_errors: Vec<Option<Error>>,
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            None => Self::Null,
            Some(t) => t.into(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Error {
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

mod i64_as_str {
    use serde::{de, ser};
    use serde::{de::Error as _, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &i64, ser: S) -> Result<S::Ok, S::Error> {
        value.to_string().serialize(ser)
    }

    pub fn deserialize<'de, D: de::Deserializer<'de>>(de: D) -> Result<i64, D::Error> {
        let str_value = <&'de str as de::Deserialize>::deserialize(de)?;
        str_value.parse().map_err(|_| {
            D::Error::invalid_value(
                de::Unexpected::Str(str_value),
                &"decimal integer as a string",
            )
        })
    }
}

mod option_i64_as_str {
    use serde::{de, de::Error as _};

    pub fn deserialize<'de, D: de::Deserializer<'de>>(de: D) -> Result<Option<i64>, D::Error> {
        let str_value = <Option<&'de str> as de::Deserialize>::deserialize(de)?;
        str_value
            .map(|s| {
                s.parse().map_err(|_| {
                    D::Error::invalid_value(de::Unexpected::Str(s), &"decimal integer as a string")
                })
            })
            .transpose()
    }
}

mod bytes_as_base64 {
    use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
    use serde::{de, ser};
    use serde::{de::Error as _, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &Vec<u8>, ser: S) -> Result<S::Ok, S::Error> {
        STANDARD_NO_PAD.encode(value).serialize(ser)
    }

    pub fn deserialize<'de, D: de::Deserializer<'de>>(de: D) -> Result<Vec<u8>, D::Error> {
        let str_value = <&'de str as de::Deserialize>::deserialize(de)?;
        STANDARD_NO_PAD
            .decode(str_value.trim_end_matches('='))
            .map_err(|_| {
                D::Error::invalid_value(
                    de::Unexpected::Str(str_value),
                    &"binary data encoded as base64",
                )
            })
    }
}
