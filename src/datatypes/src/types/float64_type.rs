use crate::datatypes::DataType;
use crate::value::Value;

/// 64-bit floating point number type
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Float64Type;

impl DataType for Float64Type {
    fn name(&self) -> String {
        "Float64".to_string()
    }

    fn default_value(&self) -> Value {
        Value::Float64(0.0)
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Float64(v) => Some(Value::Float64(v)),
            Value::Int64(v) => Some(Value::Float64(v as f64)),
            Value::Bool(v) => Some(Value::Float64(if v { 1.0 } else { 0.0 })),
            Value::String(s) => s.parse::<f64>().ok().map(Value::Float64),
            _ => None,
        }
    }
}
