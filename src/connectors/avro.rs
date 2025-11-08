use std::path::PathBuf;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::collections::HashMap;
use async_trait::async_trait;
use apache_avro::{
    Reader, Writer, Schema as AvroSchema, from_value, to_value,
    types::Value as AvroValue, to_avro_datum,
};
use serde_json::{Value as JsonValue, json};

use crate::{
    Result, TinyEtlError,
    schema::{Schema, Row, Value, Column, DataType, SchemaInferer},
    connectors::{Source, Target},
};

pub struct AvroSource {
    file_path: PathBuf,
    current_position: usize,
    total_records: Option<usize>,
    has_more: bool,
}

impl AvroSource {
    pub fn new(file_path: &str) -> Result<Self> {
        Ok(Self {
            file_path: PathBuf::from(file_path),
            current_position: 0,
            total_records: None,
            has_more: true,
        })
    }

    fn avro_type_to_schema_type(avro_type: &JsonValue) -> DataType {
        match avro_type {
            JsonValue::String(type_name) => match type_name.as_str() {
                "string" => DataType::String,
                "int" | "long" => DataType::Integer,
                "float" | "double" => DataType::Float,
                "boolean" => DataType::Boolean,
                "date" => DataType::Date,
                "timestamp-millis" | "timestamp-micros" => DataType::DateTime,
                _ => DataType::String, // Default to string
            },
            JsonValue::Array(union_types) => {
                // Handle union types (e.g., ["null", "string"])
                for union_type in union_types {
                    if let JsonValue::String(type_str) = union_type {
                        if type_str != "null" {
                            return Self::avro_type_to_schema_type(union_type);
                        }
                    }
                }
                DataType::String // Default
            },
            JsonValue::Object(obj) => {
                if let Some(logical_type) = obj.get("logicalType") {
                    match logical_type.as_str().unwrap_or("") {
                        "date" => DataType::Date,
                        "timestamp-millis" | "timestamp-micros" => DataType::DateTime,
                        _ => DataType::String,
                    }
                } else if let Some(type_val) = obj.get("type") {
                    Self::avro_type_to_schema_type(type_val)
                } else {
                    DataType::String
                }
            },
            _ => DataType::String,
        }
    }

    fn is_nullable(avro_type: &JsonValue) -> bool {
        match avro_type {
            JsonValue::Array(union_types) => {
                union_types.iter().any(|t| {
                    if let JsonValue::String(type_str) = t {
                        type_str == "null"
                    } else {
                        false
                    }
                })
            },
            _ => false,
        }
    }

    fn avro_value_to_value(avro_value: &AvroValue) -> Result<Value> {
        match avro_value {
            AvroValue::Null => Ok(Value::Null),
            AvroValue::Boolean(b) => Ok(Value::Boolean(*b)),
            AvroValue::Int(i) => Ok(Value::Integer(*i as i64)),
            AvroValue::Long(l) => Ok(Value::Integer(*l)),
            AvroValue::Float(f) => Ok(Value::Float(*f as f64)),
            AvroValue::Double(d) => Ok(Value::Float(*d)),
            AvroValue::Bytes(b) => Ok(Value::String(format!("{:?}", b))),
            AvroValue::String(s) => Ok(Value::String(s.clone())),
            AvroValue::Fixed(_, bytes) => Ok(Value::String(format!("{:?}", bytes))),
            AvroValue::Enum(_, symbol) => Ok(Value::String(symbol.clone())),
            AvroValue::Union(_, boxed_value) => Self::avro_value_to_value(boxed_value),
            AvroValue::Array(values) => {
                // Convert array to JSON string for now
                let json_values: std::result::Result<Vec<JsonValue>, _> = values.iter()
                    .map(|v| Self::avro_value_to_json_value(v))
                    .collect();
                match json_values {
                    Ok(vals) => Ok(Value::String(serde_json::to_string(&vals).unwrap_or_default())),
                    Err(_) => Ok(Value::String("[]".to_string())),
                }
            },
            AvroValue::Map(map) => {
                // Convert map to JSON string
                let mut json_map = serde_json::Map::new();
                for (k, v) in map {
                    if let Ok(json_val) = Self::avro_value_to_json_value(v) {
                        json_map.insert(k.clone(), json_val);
                    }
                }
                Ok(Value::String(serde_json::to_string(&json_map).unwrap_or_default()))
            },
            AvroValue::Record(fields) => {
                // Convert record to JSON string
                let mut json_map = serde_json::Map::new();
                for (field_name, field_value) in fields {
                    if let Ok(json_val) = Self::avro_value_to_json_value(field_value) {
                        json_map.insert(field_name.clone(), json_val);
                    }
                }
                Ok(Value::String(serde_json::to_string(&json_map).unwrap_or_default()))
            },
            AvroValue::Date(days) => {
                // Convert days since epoch to date
                let base_date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                    .ok_or_else(|| TinyEtlError::DataTransfer("Invalid base date".to_string()))?;
                let target_date = base_date + chrono::Duration::days(*days as i64);
                let datetime = target_date.and_hms_opt(0, 0, 0)
                    .ok_or_else(|| TinyEtlError::DataTransfer("Invalid time conversion".to_string()))?
                    .and_utc();
                Ok(Value::Date(datetime))
            },
            AvroValue::TimeMillis(millis) => {
                // Time in milliseconds since midnight
                Ok(Value::String(format!("{}ms", millis)))
            },
            AvroValue::TimeMicros(micros) => {
                // Time in microseconds since midnight
                Ok(Value::String(format!("{}μs", micros)))
            },
            AvroValue::TimestampMillis(millis) => {
                // Timestamp in milliseconds since epoch
                let datetime = chrono::DateTime::from_timestamp(*millis / 1000, ((*millis % 1000) * 1_000_000) as u32)
                    .ok_or_else(|| TinyEtlError::DataTransfer("Invalid timestamp".to_string()))?;
                Ok(Value::Date(datetime))
            },
            AvroValue::TimestampMicros(micros) => {
                // Timestamp in microseconds since epoch
                let datetime = chrono::DateTime::from_timestamp(*micros / 1_000_000, ((*micros % 1_000_000) * 1000) as u32)
                    .ok_or_else(|| TinyEtlError::DataTransfer("Invalid timestamp".to_string()))?;
                Ok(Value::Date(datetime))
            },
            AvroValue::LocalTimestampMillis(millis) => {
                // Local timestamp in milliseconds since epoch
                let datetime = chrono::DateTime::from_timestamp(*millis / 1000, ((*millis % 1000) * 1_000_000) as u32)
                    .ok_or_else(|| TinyEtlError::DataTransfer("Invalid local timestamp".to_string()))?;
                Ok(Value::Date(datetime))
            },
            AvroValue::LocalTimestampMicros(micros) => {
                // Local timestamp in microseconds since epoch
                let datetime = chrono::DateTime::from_timestamp(*micros / 1_000_000, ((*micros % 1_000_000) * 1000) as u32)
                    .ok_or_else(|| TinyEtlError::DataTransfer("Invalid local timestamp".to_string()))?;
                Ok(Value::Date(datetime))
            },
            AvroValue::Decimal(decimal) => {
                // Convert decimal to string representation
                Ok(Value::String(format!("{:?}", decimal)))
            },
            AvroValue::Uuid(uuid) => {
                Ok(Value::String(uuid.to_string()))
            },
            AvroValue::Duration(duration) => {
                // Convert duration to string representation
                Ok(Value::String(format!("{:?}", duration)))
            },
        }
    }

    fn avro_value_to_json_value(avro_value: &AvroValue) -> Result<JsonValue> {
        match avro_value {
            AvroValue::Null => Ok(JsonValue::Null),
            AvroValue::Boolean(b) => Ok(JsonValue::Bool(*b)),
            AvroValue::Int(i) => Ok(JsonValue::Number(serde_json::Number::from(*i))),
            AvroValue::Long(l) => Ok(JsonValue::Number(serde_json::Number::from(*l))),
            AvroValue::Float(f) => Ok(JsonValue::Number(
                serde_json::Number::from_f64(*f as f64).unwrap_or(serde_json::Number::from(0))
            )),
            AvroValue::Double(d) => Ok(JsonValue::Number(
                serde_json::Number::from_f64(*d).unwrap_or(serde_json::Number::from(0))
            )),
            AvroValue::String(s) => Ok(JsonValue::String(s.clone())),
            AvroValue::Union(_, boxed_value) => Self::avro_value_to_json_value(boxed_value),
            _ => Ok(JsonValue::String(format!("{:?}", avro_value))),
        }
    }
}

#[async_trait]
impl Source for AvroSource {
    async fn connect(&mut self) -> Result<()> {
        if !self.file_path.exists() {
            return Err(TinyEtlError::Connection(
                format!("Avro file not found: {}", self.file_path.display())
            ));
        }
        
        // Just verify the file can be opened and is a valid Avro file
        let file = File::open(&self.file_path)?;
        let buf_reader = BufReader::new(file);
        
        let _reader = Reader::new(buf_reader)
            .map_err(|e| TinyEtlError::DataTransfer(format!("Invalid Avro file: {}", e)))?;
        
        self.current_position = 0;
        self.has_more = true;
        
        Ok(())
    }

    async fn infer_schema(&mut self, _sample_size: usize) -> Result<Schema> {
        if !self.file_path.exists() {
            return Err(TinyEtlError::Connection(
                format!("Avro file not found: {}", self.file_path.display())
            ));
        }

        // Create a new reader for schema inference
        let file = File::open(&self.file_path)?;
        let buf_reader = BufReader::new(file);
        
        let reader = Reader::new(buf_reader)
            .map_err(|e| TinyEtlError::DataTransfer(format!("Failed to create Avro reader: {}", e)))?;

        let avro_schema = reader.writer_schema();
        
        // Parse the Avro schema to extract field information
        let schema_json: JsonValue = serde_json::from_str(&avro_schema.canonical_form())
            .map_err(|e| TinyEtlError::DataTransfer(format!("Failed to parse Avro schema: {}", e)))?;

        let mut columns = Vec::new();
        
        if let JsonValue::Object(obj) = &schema_json {
            if let Some(JsonValue::Array(fields)) = obj.get("fields") {
                for field in fields {
                    if let JsonValue::Object(field_obj) = field {
                        let name = field_obj.get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        
                        let default_type = JsonValue::String("string".to_string());
                        let field_type = field_obj.get("type").unwrap_or(&default_type);
                        let data_type = Self::avro_type_to_schema_type(field_type);
                        let nullable = Self::is_nullable(field_type);
                        
                        columns.push(Column {
                            name,
                            data_type,
                            nullable,
                        });
                    }
                }
            }
        }

        Ok(Schema {
            columns,
            estimated_rows: self.total_records,
            primary_key_candidate: None,
        })
    }

    async fn read_batch(&mut self, batch_size: usize) -> Result<Vec<Row>> {
        if !self.has_more {
            return Ok(vec![]);
        }

        // Create a new reader each time (not ideal for performance, but works around lifetime issues)
        let file = File::open(&self.file_path)?;
        let buf_reader = BufReader::new(file);
        
        let reader = Reader::new(buf_reader)
            .map_err(|e| TinyEtlError::DataTransfer(format!("Failed to create Avro reader: {}", e)))?;

        let mut rows = Vec::new();
        let mut count = 0;
        let mut current_index = 0;

        for value_result in reader {
            // Skip records we've already read
            if current_index < self.current_position {
                current_index += 1;
                continue;
            }

            if count >= batch_size {
                break;
            }

            match value_result {
                Ok(avro_value) => {
                    if let AvroValue::Record(fields) = avro_value {
                        let mut row = HashMap::new();
                        for (field_name, field_value) in fields {
                            let converted_value = Self::avro_value_to_value(&field_value)?;
                            row.insert(field_name, converted_value);
                        }
                        rows.push(row);
                        count += 1;
                        self.current_position += 1;
                        current_index += 1;
                    }
                }
                Err(e) => {
                    return Err(TinyEtlError::DataTransfer(format!("Failed to read Avro record: {}", e)));
                }
            }
        }

        if rows.is_empty() {
            self.has_more = false;
        }

        Ok(rows)
    }

    async fn estimated_row_count(&self) -> Result<Option<usize>> {
        Ok(self.total_records)
    }

    async fn reset(&mut self) -> Result<()> {
        self.current_position = 0;
        self.has_more = true;
        self.connect().await
    }

    fn has_more(&self) -> bool {
        self.has_more
    }
}

pub struct AvroTarget {
    file_path: PathBuf,
    schema: Option<AvroSchema>,
    buffer: Vec<Row>,
}

impl AvroTarget {
    pub fn new(file_path: &str) -> Result<Self> {
        Ok(Self {
            file_path: PathBuf::from(file_path),
            schema: None,
            buffer: Vec::new(),
        })
    }

    fn schema_to_avro_schema(schema: &Schema) -> Result<AvroSchema> {
        let mut fields = Vec::new();
        
        for column in &schema.columns {
            let field_type = match column.data_type {
                DataType::String => {
                    if column.nullable {
                        json!(["null", "string"])
                    } else {
                        json!("string")
                    }
                },
                DataType::Integer => {
                    if column.nullable {
                        json!(["null", "long"])
                    } else {
                        json!("long")
                    }
                },
                DataType::Float => {
                    if column.nullable {
                        json!(["null", "double"])
                    } else {
                        json!("double")
                    }
                },
                DataType::Boolean => {
                    if column.nullable {
                        json!(["null", "boolean"])
                    } else {
                        json!("boolean")
                    }
                },
                DataType::Date => {
                    if column.nullable {
                        json!(["null", {"type": "int", "logicalType": "date"}])
                    } else {
                        json!({"type": "int", "logicalType": "date"})
                    }
                },
                DataType::DateTime => {
                    if column.nullable {
                        json!(["null", {"type": "long", "logicalType": "timestamp-millis"}])
                    } else {
                        json!({"type": "long", "logicalType": "timestamp-millis"})
                    }
                },
                DataType::Null => json!(["null", "string"]),
            };

            fields.push(json!({
                "name": column.name,
                "type": field_type
            }));
        }

        let avro_schema_json = json!({
            "type": "record",
            "name": "Record",
            "fields": fields
        });

        AvroSchema::parse(&avro_schema_json)
            .map_err(|e| TinyEtlError::Configuration(format!("Failed to create Avro schema: {}", e)))
    }

    fn value_to_avro_value(value: &Value, data_type: &DataType) -> Result<AvroValue> {
        match (value, data_type) {
            (Value::Null, _) => Ok(AvroValue::Union(0, Box::new(AvroValue::Null))),
            (Value::String(s), DataType::String) => Ok(AvroValue::String(s.clone())),
            (Value::Integer(i), DataType::Integer) => Ok(AvroValue::Long(*i)),
            (Value::Float(f), DataType::Float) => Ok(AvroValue::Double(*f)),
            (Value::Boolean(b), DataType::Boolean) => Ok(AvroValue::Boolean(*b)),
            (Value::Date(dt), DataType::Date) => {
                let days_since_epoch = (dt.timestamp() / 86400) as i32;
                Ok(AvroValue::Date(days_since_epoch))
            },
            (Value::Date(dt), DataType::DateTime) => {
                Ok(AvroValue::TimestampMillis(dt.timestamp_millis()))
            },
            // Type conversion fallbacks
            (Value::String(s), DataType::Integer) => {
                let parsed = s.parse::<i64>()
                    .map_err(|_| TinyEtlError::DataTransfer(format!("Cannot convert '{}' to integer", s)))?;
                Ok(AvroValue::Long(parsed))
            },
            (Value::String(s), DataType::Float) => {
                let parsed = s.parse::<f64>()
                    .map_err(|_| TinyEtlError::DataTransfer(format!("Cannot convert '{}' to float", s)))?;
                Ok(AvroValue::Double(parsed))
            },
            (Value::String(s), DataType::Boolean) => {
                let parsed = s.parse::<bool>()
                    .map_err(|_| TinyEtlError::DataTransfer(format!("Cannot convert '{}' to boolean", s)))?;
                Ok(AvroValue::Boolean(parsed))
            },
            (Value::Integer(i), DataType::String) => Ok(AvroValue::String(i.to_string())),
            (Value::Float(f), DataType::String) => Ok(AvroValue::String(f.to_string())),
            (Value::Boolean(b), DataType::String) => Ok(AvroValue::String(b.to_string())),
            _ => Err(TinyEtlError::DataTransfer(format!(
                "Cannot convert value {:?} to Avro type {:?}", value, data_type
            ))),
        }
    }
}

#[async_trait]
impl Target for AvroTarget {
    async fn connect(&mut self) -> Result<()> {
        // Connection will be established when create_table is called
        Ok(())
    }

    async fn create_table(&mut self, _table_name: &str, schema: &Schema) -> Result<()> {
        let avro_schema = Self::schema_to_avro_schema(schema)?;
        self.schema = Some(avro_schema);
        Ok(())
    }

    async fn write_batch(&mut self, rows: &[Row]) -> Result<usize> {
        if self.schema.is_none() {
            return Err(TinyEtlError::Configuration("Schema not initialized. Call create_table() first.".to_string()));
        }

        // Buffer the rows for writing during finalization
        self.buffer.extend_from_slice(rows);
        Ok(rows.len())
    }

    async fn finalize(&mut self) -> Result<()> {
        if self.schema.is_none() {
            return Ok(());
        }

        let schema = self.schema.as_ref().unwrap();
        
        // Create writer and write all buffered data
        let file = File::create(&self.file_path)?;
        let buf_writer = BufWriter::new(file);
        
        let mut writer = Writer::new(schema, buf_writer);

        for row in &self.buffer {
            let mut record_fields = Vec::new();
            
            // Extract fields from schema to maintain order
            let schema_json: JsonValue = serde_json::from_str(&schema.canonical_form())
                .map_err(|e| TinyEtlError::DataTransfer(format!("Failed to parse schema: {}", e)))?;
            
            if let JsonValue::Object(obj) = &schema_json {
                if let Some(JsonValue::Array(fields)) = obj.get("fields") {
                    for field in fields {
                        if let JsonValue::Object(field_obj) = field {
                            let field_name = field_obj.get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown");
                            
                            let default_type = JsonValue::String("string".to_string());
                            let field_type = field_obj.get("type").unwrap_or(&default_type);
                            let data_type = AvroSource::avro_type_to_schema_type(field_type);
                            
                            let value = row.get(field_name).unwrap_or(&Value::Null);
                            let avro_value = Self::value_to_avro_value(value, &data_type)?;
                            
                            record_fields.push((field_name.to_string(), avro_value));
                        }
                    }
                }
            }

            let record = AvroValue::Record(record_fields);
            writer.append(record)
                .map_err(|e| TinyEtlError::DataTransfer(format!("Failed to write Avro record: {}", e)))?;
        }

        writer.flush()
            .map_err(|e| TinyEtlError::DataTransfer(format!("Failed to flush Avro writer: {}", e)))?;
            
        self.buffer.clear();
        Ok(())
    }

    async fn exists(&self, _table_name: &str) -> Result<bool> {
        Ok(self.file_path.exists())
    }
}
