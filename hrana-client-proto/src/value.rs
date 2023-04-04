use serde::{Deserialize, Serialize};

use crate::serde_utils::{bytes_as_base64, i64_as_str};

/// A libsql Value type
#[derive(Serialize, Deserialize, Clone, Debug)]
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

impl From<()> for Value {
    fn from(_: ()) -> Value {
        Value::Null
    }
}
macro_rules! impl_from_value {
    ($typename: ty, $variant: ident) => {
        impl From<$typename> for Value {
            fn from(t: $typename) -> Value {
                Value::$variant { value: t.into() }
            }
        }
    };
}

impl_from_value!(String, Text);
impl_from_value!(&String, Text);
impl_from_value!(&str, Text);

impl_from_value!(i8, Integer);
impl_from_value!(i16, Integer);
impl_from_value!(i32, Integer);
impl_from_value!(i64, Integer);

impl_from_value!(u8, Integer);
impl_from_value!(u16, Integer);
impl_from_value!(u32, Integer);

impl_from_value!(f32, Float);
impl_from_value!(f64, Float);

impl_from_value!(Vec<u8>, Blob);

macro_rules! impl_value_try_from {
    ($variant: ident, $typename: ty) => {
        impl TryFrom<Value> for $typename {
            type Error = String;
            fn try_from(v: Value) -> Result<$typename, Self::Error> {
                match v {
                    Value::$variant { value: v } => v.try_into().map_err(|e| format!("{e}")),
                    other => Err(format!(
                        "cannot transform {other:?} to {}",
                        stringify!($variant)
                    )),
                }
            }
        }
    };
}

impl_value_try_from!(Text, String);

impl_value_try_from!(Integer, i8);
impl_value_try_from!(Integer, i16);
impl_value_try_from!(Integer, i32);
impl_value_try_from!(Integer, i64);
impl_value_try_from!(Integer, u8);
impl_value_try_from!(Integer, u16);
impl_value_try_from!(Integer, u32);
impl_value_try_from!(Integer, u64);

impl_value_try_from!(Float, f64);

impl_value_try_from!(Blob, Vec<u8>);

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Integer { value: n } => write!(f, "{n}"),
            Value::Float { value: d } => write!(f, "{d}"),
            Value::Text { value: s } => write!(f, "{}", serde_json::json!(s)),
            Value::Blob { value: b } => {
                use base64::{prelude::BASE64_STANDARD_NO_PAD, Engine};
                let b = BASE64_STANDARD_NO_PAD.encode(b);
                write!(f, "{{\"base64\": {b}}}")
            }
        }
    }
}
