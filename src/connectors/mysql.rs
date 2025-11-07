use std::collections::HashMap;
use async_trait::async_trait;
use sqlx::{MySqlPool, Row as SqlxRow, Column as SqlxColumn};
use url::Url;

use crate::{
    Result, TinyEtlError,
    schema::{Schema, Row, Value, Column, DataType, SchemaInferer},
    connectors::Target,
};

pub struct MysqlTarget {
    connection_string: String,
    database_url: String,
    table_name: String,
    pool: Option<MySqlPool>,
}

impl MysqlTarget {
    pub fn new(connection_string: &str) -> Result<Self> {
        let (db_url, table_name) = Self::parse_connection_string(connection_string)?;
        
        Ok(Self {
            connection_string: connection_string.to_string(),
            database_url: db_url,
            table_name,
            pool: None,
        })
    }

    fn parse_connection_string(connection_string: &str) -> Result<(String, String)> {
        if let Some((db_part, table_part)) = connection_string.split_once('#') {
            Ok((db_part.to_string(), table_part.to_string()))
        } else {
            // Extract database name from mysql://user:pass@host:port/dbname
            let url = Url::parse(connection_string).map_err(|e| {
                TinyEtlError::Configuration(format!("Invalid MySQL URL: {}", e))
            })?;
            let db_name = url.path().trim_start_matches('/');
            let default_table = if db_name.is_empty() { "data" } else { "data" };
            Ok((connection_string.to_string(), default_table.to_string()))
        }
    }

    async fn get_pool(&self) -> Result<&MySqlPool> {
        self.pool.as_ref().ok_or_else(|| {
            TinyEtlError::Connection("MySQL connection not established".to_string())
        })
    }

    fn map_data_type_to_mysql(&self, data_type: &DataType) -> &'static str {
        match data_type {
            DataType::Integer => "BIGINT",
            DataType::Float => "DOUBLE",
            DataType::String => "TEXT",
            DataType::Boolean => "BOOLEAN",
            DataType::Date => "DATE",
            DataType::DateTime => "DATETIME",
            DataType::Null => "TEXT",
        }
    }
}

#[async_trait]
impl Target for MysqlTarget {
    async fn connect(&mut self) -> Result<()> {
        let pool = MySqlPool::connect(&self.database_url)
            .await
            .map_err(|e| TinyEtlError::Connection(format!(
                "Failed to connect to MySQL database: {}", e
            )))?;
        
        self.pool = Some(pool);
        Ok(())
    }

    async fn create_table(&mut self, table_name: &str, schema: &Schema) -> Result<()> {
        // Determine the actual table name to use
        let actual_table_name = if table_name.is_empty() {
            self.table_name.clone()
        } else {
            table_name.to_string()
        };

        // Update internal table name
        self.table_name = actual_table_name.clone();
        
        // Get pool after updating table name to avoid borrowing conflicts
        let pool = self.get_pool().await?;
        
        let mut columns = Vec::new();
        for column in &schema.columns {
            let mysql_type = self.map_data_type_to_mysql(&column.data_type);
            let nullable = if column.nullable { "" } else { " NOT NULL" };
            columns.push(format!("`{}` {}{}", column.name, mysql_type, nullable));
        }
        
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}` ({})",
            actual_table_name,
            columns.join(", ")
        );
        
        sqlx::query(&create_sql)
            .execute(pool)
            .await
            .map_err(|e| TinyEtlError::Connection(format!(
                "Failed to create MySQL table '{}': {}", actual_table_name, e
            )))?;
        
        Ok(())
    }

    async fn write_batch(&mut self, rows: &[Row]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let pool = self.get_pool().await?;
        
        // Build INSERT statement
        let columns: Vec<String> = rows[0].keys().cloned().collect();
        let placeholders = (0..columns.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
        
        let insert_sql = format!(
            "INSERT INTO `{}` ({}) VALUES ({})",
            self.table_name,
            columns.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", "),
            placeholders
        );

        let mut affected_rows = 0;
        let default_value = Value::String("".to_string());
        
        for row in rows {
            let mut query = sqlx::query(&insert_sql);
            
            for column in &columns {
                let value = row.get(column).unwrap_or(&default_value);
                query = match value {
                    Value::Integer(i) => query.bind(i),
                    Value::Float(f) => query.bind(f),
                    Value::String(s) => query.bind(s),
                    Value::Boolean(b) => query.bind(b),
                    Value::Date(d) => query.bind(d.to_rfc3339()),
                    Value::Null => query.bind(None::<String>),
                };
            }
            
            query.execute(pool).await.map_err(|e| {
                TinyEtlError::Connection(format!("Failed to insert row into MySQL: {}", e))
            })?;
            affected_rows += 1;
        }
        
        Ok(affected_rows)
    }

    async fn finalize(&mut self) -> Result<()> {
        Ok(())
    }

    async fn exists(&self, table_name: &str) -> Result<bool> {
        let pool = self.pool.as_ref().ok_or_else(|| {
            TinyEtlError::Connection("MySQL connection not established".to_string())
        })?;
        
        let actual_table_name = if table_name.is_empty() {
            &self.table_name
        } else {
            table_name
        };
        
        let result = sqlx::query("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = DATABASE()")
            .bind(actual_table_name)
            .fetch_one(pool)
            .await;
            
        match result {
            Ok(row) => Ok(row.get::<i64, _>(0) > 0),
            Err(_) => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_connection_string_with_table() {
        let target = MysqlTarget::new("mysql://user:pass@localhost:3306/testdb#employees");
        assert!(target.is_ok());
        let target = target.unwrap();
        assert_eq!(target.database_url, "mysql://user:pass@localhost:3306/testdb");
        assert_eq!(target.table_name, "employees");
    }

    #[test]
    fn test_parse_connection_string_without_table() {
        let target = MysqlTarget::new("mysql://user:pass@localhost:3306/testdb");
        assert!(target.is_ok());
        let target = target.unwrap();
        assert_eq!(target.database_url, "mysql://user:pass@localhost:3306/testdb");
        assert_eq!(target.table_name, "data");
    }

    #[test]
    fn test_invalid_connection_string() {
        let target = MysqlTarget::new("invalid-url");
        assert!(target.is_err());
    }
}
