use datatypes::{ConcreteDatatype, Value};

use crate::expr::func::{BinaryFunc, EvalError, UnaryFunc};

/// A scalar expression, which can be evaluated to a value.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarExpr {
    /// A column reference by index
    Column(usize),
    /// A literal value with its type
    Literal(Value, ConcreteDatatype),
    /// A unary function call
    CallUnary {
        func: UnaryFunc,
        expr: Box<ScalarExpr>,
    },
    /// A binary function call
    CallBinary {
        func: BinaryFunc,
        expr1: Box<ScalarExpr>,
        expr2: Box<ScalarExpr>,
    },
}

impl ScalarExpr {
    /// Evaluate this expression with the given row values.
    ///
    /// # Arguments
    ///
    /// * `values` - A slice of values representing the row, where each value corresponds
    ///   to a column at the same index.
    ///
    /// # Returns
    ///
    /// Returns the evaluated value, or an error if evaluation fails.
    pub fn eval(&self, values: &[Value]) -> Result<Value, EvalError> {
        match self {
            ScalarExpr::Column(index) => {
                values
                    .get(*index)
                    .cloned()
                    .ok_or(EvalError::IndexOutOfBounds {
                        index: *index,
                        length: values.len(),
                    })
            }
            ScalarExpr::Literal(val, _) => Ok(val.clone()),
            ScalarExpr::CallUnary { func, expr } => func.eval(values, expr),
            ScalarExpr::CallBinary { func, expr1, expr2 } => func.eval(values, expr1, expr2),
        }
    }

    /// Create a column reference expression
    pub fn column(index: usize) -> Self {
        ScalarExpr::Column(index)
    }

    /// Create a literal expression
    pub fn literal(value: Value, typ: ConcreteDatatype) -> Self {
        ScalarExpr::Literal(value, typ)
    }

    /// Create a unary function call expression
    pub fn call_unary(self, func: UnaryFunc) -> Self {
        ScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    /// Create a binary function call expression
    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        ScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    /// Check if this expression is a column reference
    pub fn is_column(&self) -> bool {
        matches!(self, ScalarExpr::Column(_))
    }

    /// Get the column index if this is a column reference
    pub fn as_column(&self) -> Option<usize> {
        if let ScalarExpr::Column(index) = self {
            Some(*index)
        } else {
            None
        }
    }

    /// Check if this expression is a literal
    pub fn is_literal(&self) -> bool {
        matches!(self, ScalarExpr::Literal(..))
    }

    /// Get the literal value if this is a literal expression
    pub fn as_literal(&self) -> Option<&Value> {
        if let ScalarExpr::Literal(val, _) = self {
            Some(val)
        } else {
            None
        }
    }
}
