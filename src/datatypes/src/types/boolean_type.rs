use crate::datatypes::DataType;
use crate::value::Value;

/// Boolean type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BooleanType;

impl DataType for BooleanType {
    fn name(&self) -> String {
        "Boolean".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Bool(false)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Bool(v) => Some(Value::Bool(v)),
            Value::Int64(v) => Some(Value::Bool(v != 0)),
            Value::Float64(v) => Some(Value::Bool(v != 0.0)),
            Value::String(s) => {
                let s_lower = s.to_lowercase();
                match s_lower.as_str() {
                    "true" | "1" | "yes" | "on" => Some(Value::Bool(true)),
                    "false" | "0" | "no" | "off" => Some(Value::Bool(false)),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}
