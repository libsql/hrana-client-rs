use serde::{Deserialize, Serialize};

use crate::{stmt::StmtResult, Error, Stmt};

/// The request type for a `Batch`
#[derive(Serialize, Debug)]
pub struct BatchReq {
    pub stream_id: i32,
    pub batch: Batch,
}

/// A `Batch` allows to group multiple `Stmt` to be executed together. Execution of the steps in
/// the batch is controlled with the `BatchCond`.
#[derive(Serialize, Debug, Default)]
pub struct Batch {
    steps: Vec<BatchStep>,
}

impl Batch {
    /// Creates a new, empty batch
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Adds a step to the batch, with an optional condition.
    ///
    /// The `condition` specifies whether of not this batch step should be executed, based on the
    /// execution of previous steps.
    /// If `condition` is `None`, then the step is executed unconditionally.
    ///
    /// ## Example:
    /// ```ignore
    /// let mut batch = Batch::new();
    /// // A step that is executed unconditionally
    /// batch.step(None, Stmt::new("SELECT * FROM user", true));
    /// // A step that is executed only if the first step was an error
    /// batch.step(Some(BatchCond::Error { step: 0 }), Stmt::new("ROLLBACK", false));
    /// ```
    pub fn step(&mut self, condition: Option<BatchCond>, stmt: Stmt) {
        self.steps.push(BatchStep { condition, stmt });
    }
}

/// An execution step in a `Batch`
#[derive(Serialize, Debug)]
pub struct BatchStep {
    condition: Option<BatchCond>,
    stmt: Stmt,
}

/// Represents a condition determining whether a batch step should be executed.
#[derive(Serialize, Debug)]
pub enum BatchCond {
    /// Evaluated to true is step `step` was a success
    Ok { step: i32 },
    /// Evaluated to true is step `step` was a error
    Error { step: i32 },
    /// Evaluates to the negation of `cond`
    Not { cond: Box<BatchCond> },
    /// Evaluates to the conjunction of `conds`
    And { conds: Vec<BatchCond> },
    /// Evaluates to the disjunction of `conds`
    Or { conds: Vec<BatchCond> },
}

/// The response type for a `BatchReq` request
#[derive(Deserialize, Debug)]
pub struct BatchResp {
    pub result: BatchResult,
}

/// The result of the execution of a batch.
/// For a given step `i`,  is possible for both `step_results[i]` and `step_errors[i]` to be
/// `None`, if that step was skipped because of a negative condition.
/// For a given step `i`, it is not possible for `step_results[i]` and `step_errors[i]` to be `Some` at the same time
#[derive(Deserialize, Debug)]
pub struct BatchResult {
    /// The success result for all steps in the batch
    pub step_results: Vec<Option<StmtResult>>,
    /// The error result for all steps in the batch
    pub step_errors: Vec<Option<Error>>,
}
