use std::collections::HashMap;
use std::path::Path;
use mlua::{Lua, Table, Value as LuaValue, Function};
use tracing::{debug, warn};

use crate::{
    Result, TinyEtlError,
    schema::{Row, Value, DataType, Schema, SchemaInferer, Column},
};

/// Configuration for data transformation
#[derive(Debug, Clone)]
pub enum TransformConfig {
    /// Path to a Lua file containing a transform function
    File(String),
    /// Semicolon-separated inline expressions like "col1=row.old_col * 2; col2=row.name .. '_suffix'"
    Inline(String),
    /// No transformation
    None,
}

impl Default for TransformConfig {
    fn default() -> Self {
        TransformConfig::None
    }
}

/// Row transformer using Lua scripting
pub struct Transformer {
    lua: Lua,
    has_transform: bool,
    inferred_schema: Option<Schema>,
}

impl Transformer {
    /// Create a new transformer with the given configuration
    pub fn new(config: &TransformConfig) -> Result<Self> {
        let lua = Lua::new();
        let mut transformer = Self {
            lua,
            has_transform: false,
            inferred_schema: None,
        };

        match config {
            TransformConfig::File(path) => {
                transformer.load_from_file(path)?;
            }
            TransformConfig::Inline(expressions) => {
                transformer.load_from_expressions(expressions)?;
            }
            TransformConfig::None => {
                // No transformation needed
            }
        }

        Ok(transformer)
    }

    /// Load transformation from a Lua file
    fn load_from_file(&mut self, path: &str) -> Result<()> {
        if !Path::new(path).exists() {
            return Err(TinyEtlError::Configuration(format!("Transform file not found: {}", path)));
        }

        let lua_code = std::fs::read_to_string(path)
            .map_err(|e| TinyEtlError::Configuration(format!("Failed to read transform file {}: {}", path, e)))?;

        self.lua.load(&lua_code).exec()
            .map_err(|e| TinyEtlError::Configuration(format!("Failed to execute Lua file {}: {}", path, e)))?;

        let globals = self.lua.globals();
        let _transform_fn: Function = globals.get("transform")
            .map_err(|_| TinyEtlError::Configuration("Lua file must contain a 'transform' function".to_string()))?;

        self.has_transform = true;
        
        debug!("Loaded transform function from file: {}", path);
        Ok(())
    }

    /// Load transformation from inline expressions
    fn load_from_expressions(&mut self, expressions: &str) -> Result<()> {
        let lua_code = self.build_transform_function(expressions)?;
        
        self.lua.load(&lua_code).exec()
            .map_err(|e| TinyEtlError::Configuration(format!("Failed to execute inline expressions: {}", e)))?;

        let globals = self.lua.globals();
        let _transform_fn: Function = globals.get("transform")
            .map_err(|_| TinyEtlError::Configuration("Failed to create transform function from expressions".to_string()))?;

        self.has_transform = true;
        
        debug!("Created transform function from expressions: {}", expressions);
        Ok(())
    }

    /// Build a Lua transform function from semicolon-separated expressions
    fn build_transform_function(&self, expressions: &str) -> Result<String> {
        let assignments: Vec<&str> = expressions
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        if assignments.is_empty() {
            return Err(TinyEtlError::Configuration("No valid expressions provided".to_string()));
        }

        let mut lua_code = String::new();
        lua_code.push_str("function transform(row)\n");
        lua_code.push_str("  local result = {}\n");
        lua_code.push_str("  -- Copy all existing columns first\n");
        lua_code.push_str("  for k, v in pairs(row) do\n");
        lua_code.push_str("    result[k] = v\n");
        lua_code.push_str("  end\n");
        lua_code.push_str("  -- Apply transformations (can override existing columns)\n");

        for assignment in assignments {
            if let Some(equals_pos) = assignment.find('=') {
                let col_name = assignment[..equals_pos].trim();
                let expression = assignment[equals_pos + 1..].trim();
                
                // Validate column name
                if col_name.is_empty() || !col_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    return Err(TinyEtlError::Configuration(format!("Invalid column name: {}", col_name)));
                }

                lua_code.push_str(&format!("  result['{}'] = {}\n", col_name, expression));
            } else {
                return Err(TinyEtlError::Configuration(format!("Invalid expression format (missing '='): {}", assignment)));
            }
        }

        lua_code.push_str("  return result\n");
        lua_code.push_str("end\n");

        debug!("Generated Lua code:\n{}", lua_code);
        Ok(lua_code)
    }

    /// Check if this transformer has any transformation logic
    pub fn is_enabled(&self) -> bool {
        self.has_transform
    }

    /// Transform a batch of rows and infer schema from the first transformed row
    pub fn transform_batch(&mut self, rows: &[Row]) -> Result<Vec<Row>> {
        if !self.is_enabled() {
            return Ok(rows.to_vec());
        }

        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let mut transformed_rows = Vec::with_capacity(rows.len());

        for (i, row) in rows.iter().enumerate() {
            let transformed_row = self.transform_row(row)?;
            
            // Infer schema from the first transformed row
            if i == 0 {
                self.infer_schema_from_first_row(&transformed_row)?;
            }

            // Validate the transformed row against the inferred schema
            let validated_row = self.validate_and_filter_row(transformed_row)?;
            transformed_rows.push(validated_row);
        }

        Ok(transformed_rows)
    }

    /// Transform a single row using the Lua function
    fn transform_row(&self, row: &Row) -> Result<Row> {
        if !self.has_transform {
            return Err(TinyEtlError::Configuration("No transform function loaded".to_string()));
        }

        let globals = self.lua.globals();
        let transform_fn: Function = globals.get("transform")
            .map_err(|e| TinyEtlError::Transform(format!("Failed to get transform function: {}", e)))?;

        // Convert Row to Lua table
        let lua_row = self.row_to_lua_table(row)?;
        
        // Call the transform function
        let result: Table = transform_fn.call(lua_row)
            .map_err(|e| TinyEtlError::Transform(format!("Lua transform function failed: {}", e)))?;

        // Convert result back to Row
        self.lua_table_to_row(result)
    }

    /// Convert a Row to a Lua table
    fn row_to_lua_table(&self, row: &Row) -> Result<Table> {
        let table = self.lua.create_table()
            .map_err(|e| TinyEtlError::Transform(format!("Failed to create Lua table: {}", e)))?;

        for (key, value) in row {
            let lua_value = match value {
                Value::String(s) => LuaValue::String(self.lua.create_string(s)?),
                Value::Integer(i) => LuaValue::Integer(*i),
                Value::Float(f) => LuaValue::Number(*f),
                Value::Boolean(b) => LuaValue::Boolean(*b),
                Value::Date(dt) => LuaValue::String(self.lua.create_string(&dt.to_rfc3339())?),
                Value::Null => LuaValue::Nil,
            };
            table.set(key.as_str(), lua_value)?;
        }

        Ok(table)
    }

    /// Convert a Lua table back to a Row
    fn lua_table_to_row(&self, table: Table) -> Result<Row> {
        let mut row = Row::new();

        for pair in table.pairs::<String, LuaValue>() {
            let (key, lua_value) = pair
                .map_err(|e| TinyEtlError::Transform(format!("Failed to iterate Lua table: {}", e)))?;

            let value = match lua_value {
                LuaValue::String(s) => {
                    let str_val = s.to_str()
                        .map_err(|e| TinyEtlError::Transform(format!("Failed to convert Lua string: {}", e)))?;
                    Value::String(str_val.to_string())
                }
                LuaValue::Integer(i) => Value::Integer(i),
                LuaValue::Number(f) => Value::Float(f),
                LuaValue::Boolean(b) => Value::Boolean(b),
                LuaValue::Nil => Value::Null,
                _ => {
                    warn!("Unsupported Lua value type for column '{}', converting to string", key);
                    Value::String(format!("{:?}", lua_value))
                }
            };

            row.insert(key, value);
        }

        Ok(row)
    }

    /// Infer schema from the first transformed row
    fn infer_schema_from_first_row(&mut self, row: &Row) -> Result<()> {
        let columns: Vec<Column> = row
            .iter()
            .map(|(name, value)| {
                let data_type = SchemaInferer::infer_type(value);
                Column {
                    name: name.clone(),
                    data_type,
                    nullable: matches!(value, Value::Null),
                }
            })
            .collect();

        self.inferred_schema = Some(Schema {
            columns,
            estimated_rows: None,
            primary_key_candidate: None,
        });

        debug!("Inferred schema from first transformed row: {} columns", self.inferred_schema.as_ref().unwrap().columns.len());
        Ok(())
    }

    /// Validate and filter a row based on the inferred schema
    fn validate_and_filter_row(&self, mut row: Row) -> Result<Row> {
        let schema = self.inferred_schema.as_ref()
            .ok_or_else(|| TinyEtlError::Transform("Schema not inferred yet".to_string()))?;

        let expected_columns: std::collections::HashSet<_> = schema.columns.iter()
            .map(|c| c.name.clone())
            .collect();

        // Remove columns not in the schema (dropped from first row)
        row.retain(|col_name, _| expected_columns.contains(col_name));

        // Add missing columns as null
        for column in &schema.columns {
            if !row.contains_key(&column.name) {
                row.insert(column.name.clone(), Value::Null);
            }
        }

        // Type validation could be added here if needed
        // For now, we trust Lua to return consistent types

        Ok(row)
    }

    /// Get the inferred schema from transformed data
    pub fn get_inferred_schema(&self) -> Option<&Schema> {
        self.inferred_schema.as_ref()
    }

    /// Update the base schema with transformations (to be called after processing first batch)
    pub fn merge_with_base_schema(&self, base_schema: &Schema) -> Result<Schema> {
        match &self.inferred_schema {
            Some(transform_schema) => {
                // If we have transformations, use the transformed schema
                Ok(transform_schema.clone())
            }
            None => {
                // No transformations, use base schema as-is
                Ok(base_schema.clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Value;
    use std::collections::HashMap;

    #[test]
    fn test_no_transformation() {
        let config = TransformConfig::None;
        let transformer = Transformer::new(&config).unwrap();
        assert!(!transformer.is_enabled());
    }

    #[test]
    fn test_inline_expressions() {
        let config = TransformConfig::Inline("full_name=row.first_name .. ' ' .. row.last_name; age_next_year=row.age + 1".to_string());
        let mut transformer = Transformer::new(&config).unwrap();
        assert!(transformer.is_enabled());

        let mut row = HashMap::new();
        row.insert("first_name".to_string(), Value::String("John".to_string()));
        row.insert("last_name".to_string(), Value::String("Doe".to_string()));
        row.insert("age".to_string(), Value::Integer(30));

        let result = transformer.transform_batch(&[row]).unwrap();
        assert_eq!(result.len(), 1);
        
        let transformed = &result[0];
        // Check new columns
        assert_eq!(transformed.get("full_name"), Some(&Value::String("John Doe".to_string())));
        assert_eq!(transformed.get("age_next_year"), Some(&Value::Integer(31)));
        // Check original columns are preserved
        assert_eq!(transformed.get("first_name"), Some(&Value::String("John".to_string())));
        assert_eq!(transformed.get("last_name"), Some(&Value::String("Doe".to_string())));
        assert_eq!(transformed.get("age"), Some(&Value::Integer(30)));
    }

    #[test]
    fn test_invalid_expression() {
        let config = TransformConfig::Inline("invalid_syntax".to_string());
        let result = Transformer::new(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_inference() {
        let config = TransformConfig::Inline("new_col=42; str_col='test'".to_string());
        let mut transformer = Transformer::new(&config).unwrap();

        let row = HashMap::new();
        let result = transformer.transform_batch(&[row]).unwrap();
        
        let schema = transformer.get_inferred_schema().unwrap();
        assert_eq!(schema.columns.len(), 2);
        
        let col_names: Vec<_> = schema.columns.iter().map(|c| &c.name).collect();
        assert!(col_names.contains(&&"new_col".to_string()));
        assert!(col_names.contains(&&"str_col".to_string()));
    }
}
