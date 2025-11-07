use sqlparser::ast::Expr;
use std::collections::HashMap;

/// Represents a SELECT statement with its fields, optional HAVING clause, and aggregate mappings
#[derive(Debug, Clone)]
pub struct SelectStmt {
    /// The select fields/expressions
    pub select_fields: Vec<SelectField>,
    /// Optional HAVING clause expression
    pub having: Option<Expr>,
    /// Aggregate function mappings: column name -> original aggregate expression
    pub aggregate_mappings: HashMap<String, Expr>,
}

/// Represents a single select field/expression
#[derive(Debug, Clone)]
pub struct SelectField {
    /// The expression for this field (from sqlparser AST)
    pub expr: Expr,
    /// Optional alias for this field
    pub alias: Option<String>,
}

impl SelectStmt {
    /// Create a new SelectStmt with empty fields and no HAVING clause
    pub fn new() -> Self {
        Self {
            select_fields: Vec::new(),
            having: None,
            aggregate_mappings: HashMap::new(),
        }
    }

    /// Create a new SelectStmt with given fields and no HAVING clause
    pub fn with_fields(select_fields: Vec<SelectField>) -> Self {
        Self { 
            select_fields, 
            having: None,
            aggregate_mappings: HashMap::new(),
        }
    }

    /// Create a new SelectStmt with given fields and HAVING clause
    pub fn with_fields_and_having(select_fields: Vec<SelectField>, having: Option<Expr>) -> Self {
        Self { 
            select_fields, 
            having,
            aggregate_mappings: HashMap::new(),
        }
    }
}

impl Default for SelectStmt {
    fn default() -> Self {
        Self::new()
    }
}

impl SelectField {
    /// Create a new SelectField
    pub fn new(expr: Expr, alias: Option<String>) -> Self {
        Self { expr, alias }
    }
}
