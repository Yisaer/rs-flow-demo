use crate::datatypes::DataType;
use crate::value::Value;

/// String type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StringType;

impl DataType for StringType {
    fn name(&self) -> String {
        "String".to_string()
    }

    fn default_value(&self) -> Value {
        Value::String(String::new())
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::String(s) => Some(Value::String(s)),
            Value::Int64(v) => Some(Value::String(v.to_string())),
            Value::Float64(v) => Some(Value::String(v.to_string())),
            Value::Bool(v) => Some(Value::String(v.to_string())),
            _ => None,
        }
    }
}
