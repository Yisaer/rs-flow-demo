use crate::datatypes::ConcreteDatatype;

/// Schema of a column
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnSchema {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: ConcreteDatatype,
}

impl ColumnSchema {
    /// Create a new column schema
    pub fn new(name: String, data_type: ConcreteDatatype) -> Self {
        ColumnSchema { name, data_type }
    }
}
