///! # hrana protocol
///! This crate defines the hrana protocal types. The hrana protocol is documented [here](https://github.com/libsql/sqld/blob/main/docs/HRANA_SPEC.md).
use std::fmt;

use serde::Deserialize;

mod batch;
mod message;
mod serde_utils;
mod stmt;
mod value;

pub use batch::*;
pub use message::*;
pub use stmt::*;
pub use value::Value;

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
