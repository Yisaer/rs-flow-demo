use datatypes::{ConcreteDatatype, DataType, Schema, Value};
use serde_json::Value as JsonValue;

use crate::row::Row;

/// Tuple represents a row of data with its associated schema.
///
/// A Tuple combines a Schema (which defines the structure and types of columns)
/// with a Row (which contains the actual data values).
#[derive(Clone, Debug, PartialEq)]
pub struct Tuple {
    /// The schema defining the structure of this tuple
    schema: Schema,
    /// The row containing the actual data values
    row: Row,
}

impl Tuple {
    /// Create a new tuple with the given schema and row
    ///
    /// # Panics
    ///
    /// Panics if the number of values in the row does not match the number of columns in the schema
    pub fn new(schema: Schema, row: Row) -> Self {
        assert_eq!(
            schema.column_schemas().len(),
            row.len(),
            "Row length must match schema column count"
        );
        Self { schema, row }
    }

    /// Create a new tuple from a schema and a vector of values
    pub fn from_values(schema: Schema, values: Vec<Value>) -> Self {
        let row = Row::new(values);
        Self::new(schema, row)
    }

    /// Get a reference to the schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get a reference to the row
    pub fn row(&self) -> &Row {
        &self.row
    }

    /// Get a mutable reference to the row
    pub fn row_mut(&mut self) -> &mut Row {
        &mut self.row
    }

    /// Get the value at the given column index
    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.row.get(idx)
    }

    /// Get the value for a column by name
    pub fn get_by_name(&self, name: &str) -> Option<&Value> {
        self.schema
            .column_schema_by_name(name)
            .and_then(|col_schema| {
                self.schema
                    .column_schemas()
                    .iter()
                    .position(|cs| cs.name == col_schema.name)
                    .and_then(|idx| self.row.get(idx))
            })
    }

    /// Get the number of columns in this tuple
    pub fn len(&self) -> usize {
        self.row.len()
    }

    /// Returns true if the tuple contains no columns
    pub fn is_empty(&self) -> bool {
        self.row.is_empty()
    }

    /// Consume the tuple and return the schema and row
    pub fn into_parts(self) -> (Schema, Row) {
        (self.schema, self.row)
    }

    /// Create a new tuple from a schema and JSON bytes
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema defining the structure of the tuple
    /// * `json_bytes` - A byte slice containing a JSON object
    ///
    /// # Returns
    ///
    /// Returns a `Result` with the created `Tuple` or an error if JSON parsing fails
    /// or if the JSON doesn't match the schema.
    pub fn new_from_json(schema: Schema, json_bytes: &[u8]) -> Result<Self, JsonError> {
        let json_value: JsonValue = serde_json::from_slice(json_bytes)
            .map_err(|e| JsonError::ParseError(e.to_string()))?;

        let json_obj = json_value
            .as_object()
            .ok_or_else(|| JsonError::InvalidFormat("Expected JSON object".to_string()))?;

        let mut values = Vec::new();
        for col_schema in schema.column_schemas() {
            let json_val = json_obj.get(&col_schema.name);
            let value = match json_val {
                Some(val) => json_value_to_value(val, &col_schema.data_type)?,
                None => get_default_value(&col_schema.data_type),
            };
            values.push(value);
        }

        let row = Row::new(values);
        Ok(Self::new(schema, row))
    }
}

/// Convert a JSON value to a Value based on the expected data type
fn json_value_to_value(json_val: &JsonValue, data_type: &ConcreteDatatype) -> Result<Value, JsonError> {
    match (json_val, data_type) {
        (JsonValue::Null, _) => Ok(get_default_value(data_type)),
        (JsonValue::Bool(b), ConcreteDatatype::Bool(_)) => Ok(Value::Bool(*b)),
        (JsonValue::Number(n), ConcreteDatatype::Int8(_)) => {
            n.as_i64()
                .and_then(|i| if i >= i8::MIN as i64 && i <= i8::MAX as i64 { Some(i as i8) } else { None })
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Int8".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Int8)
        }
        (JsonValue::Number(n), ConcreteDatatype::Int16(_)) => {
            n.as_i64()
                .and_then(|i| if i >= i16::MIN as i64 && i <= i16::MAX as i64 { Some(i as i16) } else { None })
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Int16".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Int16)
        }
        (JsonValue::Number(n), ConcreteDatatype::Int32(_)) => {
            n.as_i64()
                .and_then(|i| if i >= i32::MIN as i64 && i <= i32::MAX as i64 { Some(i as i32) } else { None })
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Int32".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Int32)
        }
        (JsonValue::Number(n), ConcreteDatatype::Int64(_)) => {
            n.as_i64()
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Int64".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Int64)
        }
        (JsonValue::Number(n), ConcreteDatatype::Float32(_)) => {
            n.as_f64()
                .and_then(|f| if f >= f32::MIN as f64 && f <= f32::MAX as f64 { Some(f as f32) } else { None })
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Float32".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Float32)
        }
        (JsonValue::Number(n), ConcreteDatatype::Float64(_)) => {
            n.as_f64()
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Float64".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Float64)
        }
        (JsonValue::Number(n), ConcreteDatatype::Uint8(_)) => {
            n.as_u64()
                .and_then(|u| if u <= u8::MAX as u64 { Some(u as u8) } else { None })
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Uint8".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Uint8)
        }
        (JsonValue::Number(n), ConcreteDatatype::Uint16(_)) => {
            n.as_u64()
                .and_then(|u| if u <= u16::MAX as u64 { Some(u as u16) } else { None })
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Uint16".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Uint16)
        }
        (JsonValue::Number(n), ConcreteDatatype::Uint32(_)) => {
            n.as_u64()
                .and_then(|u| if u <= u32::MAX as u64 { Some(u as u32) } else { None })
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Uint32".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Uint32)
        }
        (JsonValue::Number(n), ConcreteDatatype::Uint64(_)) => {
            n.as_u64()
                .ok_or_else(|| JsonError::TypeMismatch {
                    expected: "Uint64".to_string(),
                    actual: format!("{}", n),
                })
                .map(Value::Uint64)
        }
        (JsonValue::String(s), ConcreteDatatype::String(_)) => Ok(Value::String(s.clone())),
        (JsonValue::Number(n), dt) => {
            // Try to convert number to the expected type
            if let Some(i) = n.as_i64() {
                match dt {
                    ConcreteDatatype::Int8(_) => {
                        if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                            Ok(Value::Int8(i as i8))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Int8".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Int16(_) => {
                        if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                            Ok(Value::Int16(i as i16))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Int16".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Int32(_) => {
                        if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                            Ok(Value::Int32(i as i32))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Int32".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Int64(_) => Ok(Value::Int64(i)),
                    ConcreteDatatype::Float32(_) => Ok(Value::Float32(i as f32)),
                    ConcreteDatatype::Float64(_) => Ok(Value::Float64(i as f64)),
                    ConcreteDatatype::Uint8(_) => {
                        if i >= 0 && i <= u8::MAX as i64 {
                            Ok(Value::Uint8(i as u8))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Uint8".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Uint16(_) => {
                        if i >= 0 && i <= u16::MAX as i64 {
                            Ok(Value::Uint16(i as u16))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Uint16".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Uint32(_) => {
                        if i >= 0 && i <= u32::MAX as i64 {
                            Ok(Value::Uint32(i as u32))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Uint32".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Uint64(_) => {
                        if i >= 0 {
                            Ok(Value::Uint64(i as u64))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Uint64".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::String(_) => Ok(Value::String(i.to_string())),
                    _ => Err(JsonError::TypeMismatch {
                        expected: format!("{:?}", dt),
                        actual: format!("{}", n),
                    }),
                }
            } else if let Some(f) = n.as_f64() {
                match dt {
                    ConcreteDatatype::Int8(_) => {
                        if f >= i8::MIN as f64 && f <= i8::MAX as f64 && f.fract() == 0.0 {
                            Ok(Value::Int8(f as i8))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Int8".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Int16(_) => {
                        if f >= i16::MIN as f64 && f <= i16::MAX as f64 && f.fract() == 0.0 {
                            Ok(Value::Int16(f as i16))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Int16".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Int32(_) => {
                        if f >= i32::MIN as f64 && f <= i32::MAX as f64 && f.fract() == 0.0 {
                            Ok(Value::Int32(f as i32))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Int32".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Int64(_) => Ok(Value::Int64(f as i64)),
                    ConcreteDatatype::Float32(_) => {
                        if f >= f32::MIN as f64 && f <= f32::MAX as f64 {
                            Ok(Value::Float32(f as f32))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Float32".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Float64(_) => Ok(Value::Float64(f)),
                    ConcreteDatatype::Uint8(_) => {
                        if f >= 0.0 && f <= u8::MAX as f64 && f.fract() == 0.0 {
                            Ok(Value::Uint8(f as u8))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Uint8".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Uint16(_) => {
                        if f >= 0.0 && f <= u16::MAX as f64 && f.fract() == 0.0 {
                            Ok(Value::Uint16(f as u16))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Uint16".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Uint32(_) => {
                        if f >= 0.0 && f <= u32::MAX as f64 && f.fract() == 0.0 {
                            Ok(Value::Uint32(f as u32))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Uint32".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Uint64(_) => {
                        if f >= 0.0 && f <= u64::MAX as f64 && f.fract() == 0.0 {
                            Ok(Value::Uint64(f as u64))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Uint64".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::String(_) => Ok(Value::String(f.to_string())),
                    _ => Err(JsonError::TypeMismatch {
                        expected: format!("{:?}", dt),
                        actual: format!("{}", n),
                    }),
                }
            } else if let Some(u) = n.as_u64() {
                match dt {
                    ConcreteDatatype::Int8(_) => {
                        if u <= i8::MAX as u64 {
                            Ok(Value::Int8(u as i8))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Int8".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Int16(_) => {
                        if u <= i16::MAX as u64 {
                            Ok(Value::Int16(u as i16))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Int16".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Int32(_) => {
                        if u <= i32::MAX as u64 {
                            Ok(Value::Int32(u as i32))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Int32".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Int64(_) => Ok(Value::Int64(u as i64)),
                    ConcreteDatatype::Float32(_) => Ok(Value::Float32(u as f32)),
                    ConcreteDatatype::Float64(_) => Ok(Value::Float64(u as f64)),
                    ConcreteDatatype::Uint8(_) => {
                        if u <= u8::MAX as u64 {
                            Ok(Value::Uint8(u as u8))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Uint8".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Uint16(_) => {
                        if u <= u16::MAX as u64 {
                            Ok(Value::Uint16(u as u16))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Uint16".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Uint32(_) => {
                        if u <= u32::MAX as u64 {
                            Ok(Value::Uint32(u as u32))
                        } else {
                            Err(JsonError::TypeMismatch {
                                expected: "Uint32".to_string(),
                                actual: format!("{}", n),
                            })
                        }
                    }
                    ConcreteDatatype::Uint64(_) => Ok(Value::Uint64(u)),
                    ConcreteDatatype::String(_) => Ok(Value::String(u.to_string())),
                    _ => Err(JsonError::TypeMismatch {
                        expected: format!("{:?}", dt),
                        actual: format!("{}", n),
                    }),
                }
            } else {
                Err(JsonError::TypeMismatch {
                    expected: format!("{:?}", dt),
                    actual: format!("{}", n),
                })
            }
        }
        (JsonValue::Bool(b), dt) => {
            match dt {
                ConcreteDatatype::Int8(_) => Ok(Value::Int8(if *b { 1 } else { 0 })),
                ConcreteDatatype::Int16(_) => Ok(Value::Int16(if *b { 1 } else { 0 })),
                ConcreteDatatype::Int32(_) => Ok(Value::Int32(if *b { 1 } else { 0 })),
                ConcreteDatatype::Int64(_) => Ok(Value::Int64(if *b { 1 } else { 0 })),
                ConcreteDatatype::Float32(_) => Ok(Value::Float32(if *b { 1.0 } else { 0.0 })),
                ConcreteDatatype::Float64(_) => Ok(Value::Float64(if *b { 1.0 } else { 0.0 })),
                ConcreteDatatype::Uint8(_) => Ok(Value::Uint8(if *b { 1 } else { 0 })),
                ConcreteDatatype::Uint16(_) => Ok(Value::Uint16(if *b { 1 } else { 0 })),
                ConcreteDatatype::Uint32(_) => Ok(Value::Uint32(if *b { 1 } else { 0 })),
                ConcreteDatatype::Uint64(_) => Ok(Value::Uint64(if *b { 1 } else { 0 })),
                ConcreteDatatype::String(_) => Ok(Value::String(b.to_string())),
                _ => Err(JsonError::TypeMismatch {
                    expected: format!("{:?}", dt),
                    actual: format!("{}", b),
                }),
            }
        }
        (JsonValue::String(s), dt) => {
            match dt {
                ConcreteDatatype::Int8(_) => {
                    s.parse::<i8>()
                        .map(Value::Int8)
                        .map_err(|_| JsonError::TypeMismatch {
                            expected: "Int8".to_string(),
                            actual: s.clone(),
                        })
                }
                ConcreteDatatype::Int16(_) => {
                    s.parse::<i16>()
                        .map(Value::Int16)
                        .map_err(|_| JsonError::TypeMismatch {
                            expected: "Int16".to_string(),
                            actual: s.clone(),
                        })
                }
                ConcreteDatatype::Int32(_) => {
                    s.parse::<i32>()
                        .map(Value::Int32)
                        .map_err(|_| JsonError::TypeMismatch {
                            expected: "Int32".to_string(),
                            actual: s.clone(),
                        })
                }
                ConcreteDatatype::Int64(_) => {
                    s.parse::<i64>()
                        .map(Value::Int64)
                        .map_err(|_| JsonError::TypeMismatch {
                            expected: "Int64".to_string(),
                            actual: s.clone(),
                        })
                }
                ConcreteDatatype::Float32(_) => {
                    s.parse::<f32>()
                        .map(Value::Float32)
                        .map_err(|_| JsonError::TypeMismatch {
                            expected: "Float32".to_string(),
                            actual: s.clone(),
                        })
                }
                ConcreteDatatype::Float64(_) => {
                    s.parse::<f64>()
                        .map(Value::Float64)
                        .map_err(|_| JsonError::TypeMismatch {
                            expected: "Float64".to_string(),
                            actual: s.clone(),
                        })
                }
                ConcreteDatatype::Uint8(_) => {
                    s.parse::<u8>()
                        .map(Value::Uint8)
                        .map_err(|_| JsonError::TypeMismatch {
                            expected: "Uint8".to_string(),
                            actual: s.clone(),
                        })
                }
                ConcreteDatatype::Uint16(_) => {
                    s.parse::<u16>()
                        .map(Value::Uint16)
                        .map_err(|_| JsonError::TypeMismatch {
                            expected: "Uint16".to_string(),
                            actual: s.clone(),
                        })
                }
                ConcreteDatatype::Uint32(_) => {
                    s.parse::<u32>()
                        .map(Value::Uint32)
                        .map_err(|_| JsonError::TypeMismatch {
                            expected: "Uint32".to_string(),
                            actual: s.clone(),
                        })
                }
                ConcreteDatatype::Uint64(_) => {
                    s.parse::<u64>()
                        .map(Value::Uint64)
                        .map_err(|_| JsonError::TypeMismatch {
                            expected: "Uint64".to_string(),
                            actual: s.clone(),
                        })
                }
                ConcreteDatatype::Bool(_) => {
                    match s.to_lowercase().as_str() {
                        "true" | "1" | "yes" | "on" => Ok(Value::Bool(true)),
                        "false" | "0" | "no" | "off" => Ok(Value::Bool(false)),
                        _ => Err(JsonError::TypeMismatch {
                            expected: "Bool".to_string(),
                            actual: s.clone(),
                        }),
                    }
                }
                _ => Err(JsonError::TypeMismatch {
                    expected: format!("{:?}", dt),
                    actual: s.clone(),
                }),
            }
        }
        _ => Err(JsonError::TypeMismatch {
            expected: format!("{:?}", data_type),
            actual: format!("{:?}", json_val),
        }),
    }
}

/// Get default value for a ConcreteDatatype
fn get_default_value(data_type: &ConcreteDatatype) -> Value {
    match data_type {
        ConcreteDatatype::Int8(t) => t.default_value(),
        ConcreteDatatype::Int16(t) => t.default_value(),
        ConcreteDatatype::Int32(t) => t.default_value(),
        ConcreteDatatype::Int64(t) => t.default_value(),
        ConcreteDatatype::Float32(t) => t.default_value(),
        ConcreteDatatype::Float64(t) => t.default_value(),
        ConcreteDatatype::Uint8(t) => t.default_value(),
        ConcreteDatatype::Uint16(t) => t.default_value(),
        ConcreteDatatype::Uint32(t) => t.default_value(),
        ConcreteDatatype::Uint64(t) => t.default_value(),
        ConcreteDatatype::String(t) => t.default_value(),
        ConcreteDatatype::Bool(t) => t.default_value(),
        ConcreteDatatype::Struct(t) => t.default_value(),
        ConcreteDatatype::List(t) => t.default_value(),
    }
}

/// Error type for JSON parsing and conversion
#[derive(Debug, Clone, PartialEq)]
pub enum JsonError {
    /// JSON parsing error
    ParseError(String),
    /// Invalid JSON format
    InvalidFormat(String),
    /// Type mismatch error
    TypeMismatch {
        expected: String,
        actual: String,
    },
}

impl std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonError::ParseError(msg) => write!(f, "JSON parse error: {}", msg),
            JsonError::InvalidFormat(msg) => write!(f, "Invalid JSON format: {}", msg),
            JsonError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, actual)
            }
        }
    }
}

impl std::error::Error for JsonError {}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::{ColumnSchema, Int64Type, StringType, Float64Type, BooleanType};

    #[test]
    fn test_new_from_json() {
        let schema = Schema::new(vec![
            ColumnSchema::new("id".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("name".to_string(), ConcreteDatatype::String(StringType)),
            ColumnSchema::new("age".to_string(), ConcreteDatatype::Int64(Int64Type)),
            ColumnSchema::new("score".to_string(), ConcreteDatatype::Float64(Float64Type)),
            ColumnSchema::new("active".to_string(), ConcreteDatatype::Bool(BooleanType)),
        ]);

        let json = br#"{"id": 1, "name": "Alice", "age": 25, "score": 98.5, "active": true}"#;
        let tuple = Tuple::new_from_json(schema.clone(), json).unwrap();

        assert_eq!(tuple.len(), 5);
        assert_eq!(tuple.get(0), Some(&Value::Int64(1)));
        assert_eq!(tuple.get(1), Some(&Value::String("Alice".to_string())));
        assert_eq!(tuple.get(2), Some(&Value::Int64(25)));
        assert_eq!(tuple.get(3), Some(&Value::Float64(98.5)));
        assert_eq!(tuple.get(4), Some(&Value::Bool(true)));

        // Test with missing fields (should use default values)
        let json2 = br#"{"id": 2, "name": "Bob"}"#;
        let tuple2 = Tuple::new_from_json(schema.clone(), json2).unwrap();
        
        assert_eq!(tuple2.get(0), Some(&Value::Int64(2)));
        assert_eq!(tuple2.get(1), Some(&Value::String("Bob".to_string())));
        assert_eq!(tuple2.get(2), Some(&Value::Int64(0))); // default for Int64
        assert_eq!(tuple2.get(3), Some(&Value::Float64(0.0))); // default for Float64
        assert_eq!(tuple2.get(4), Some(&Value::Bool(false))); // default for Bool

        // Test with null values (should use default values)
        let json3 = br#"{"id": 3, "name": null, "age": 30, "score": null, "active": null}"#;
        let tuple3 = Tuple::new_from_json(schema, json3).unwrap();
        
        assert_eq!(tuple3.get(0), Some(&Value::Int64(3)));
        assert_eq!(tuple3.get(1), Some(&Value::String(String::new()))); // default for String
        assert_eq!(tuple3.get(2), Some(&Value::Int64(30)));
        assert_eq!(tuple3.get(3), Some(&Value::Float64(0.0))); // default for Float64
        assert_eq!(tuple3.get(4), Some(&Value::Bool(false))); // default for Bool
    }
}
