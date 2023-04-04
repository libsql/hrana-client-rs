use serde::{Deserialize, Serialize};

use crate::serde_utils::option_i64_as_str;
use crate::Value;

/// Represents a SQL statement to be executed over the hrana protocol
#[derive(Serialize, Debug)]
pub struct Stmt {
    sql: String,
    #[serde(default)]
    args: Vec<Value>,
    #[serde(default)]
    named_args: Vec<NamedArg>,
    want_rows: bool,
}

impl Stmt {
    /// Creates a new statement from a SQL string.
    /// `want_rows` determines whether the reponse to this statement should return rows.
    pub fn new(sql: impl Into<String>, want_rows: bool) -> Self {
        let sql = sql.into();
        Self {
            sql,
            want_rows,
            named_args: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Bind the next positional parameter to this statement.
    ///
    /// ## Example:
    ///
    /// ```ignore
    /// let mut statement = Statement::new("SELECT * FROM users WHERE username=?", true);
    /// statement.bind("adhoc");
    /// ```
    pub fn bind(&mut self, val: impl Into<Value>) {
        self.args.push(val.into());
    }

    /// Bind a named parameter to this statement.
    ///
    /// ## Example:
    ///
    /// ```ignore
    /// let mut statement = Statement::new("SELECT * FROM users WHERE username=$username", true);
    /// statement.bind("$username", "adhoc" });
    /// ```
    pub fn bind_named(&mut self, name: impl Into<String>, value: impl Into<Value>) {
        self.named_args.push(NamedArg {
            name: name.into(),
            value: value.into(),
        });
    }
}

#[derive(Serialize, Debug)]
struct NamedArg {
    name: String,
    value: Value,
}

/// Result type for the successful execution of a `Stmt`
#[derive(Deserialize, Clone, Debug)]
pub struct StmtResult {
    /// List of column descriptors
    pub cols: Vec<Col>,
    /// List of row values
    pub rows: Vec<Vec<Value>>,
    /// Number of rows affected by the query
    pub affected_row_count: u64,
    #[serde(with = "option_i64_as_str")]
    pub last_insert_rowid: Option<i64>,
}

/// A column description
#[derive(Deserialize, Clone, Debug)]
pub struct Col {
    /// Name of the column
    pub name: Option<String>,
}
