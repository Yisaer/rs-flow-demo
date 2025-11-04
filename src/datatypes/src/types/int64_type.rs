use crate::datatypes::DataType;
use crate::value::Value;

/// 64-bit signed integer type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Int64Type;

impl DataType for Int64Type {
    fn name(&self) -> String {
        "Int64".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Int64(0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Int64(v) => Some(Value::Int64(v)),
            Value::Float64(v) => Some(Value::Int64(v as i64)),
            Value::Bool(v) => Some(Value::Int64(if v { 1 } else { 0 })),
            Value::String(s) => s.parse::<i64>().ok().map(Value::Int64),
            _ => None,
        }
    }
}
