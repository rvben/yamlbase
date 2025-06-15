use bytes::{BufMut, BytesMut};
use std::collections::HashMap;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::debug;

use crate::database::Value;
use crate::sql::{parse_sql, QueryExecutor};
use crate::sql::executor::QueryResult;
use crate::yaml::schema::SqlType;
use crate::YamlBaseError;

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub name: String,
    pub query: String,
    pub parameter_types: Vec<SqlType>,
    pub parsed_statements: Vec<sqlparser::ast::Statement>,
}

#[derive(Debug, Clone)]
pub struct Portal {
    pub name: String,
    pub statement: PreparedStatement,
    pub parameters: Vec<Value>,
}

pub struct ExtendedProtocol {
    pub prepared_statements: HashMap<String, PreparedStatement>,
    pub portals: HashMap<String, Portal>,
}

impl ExtendedProtocol {
    pub fn new() -> Self {
        Self {
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    pub async fn handle_parse(
        &mut self,
        stream: &mut TcpStream,
        data: &[u8],
    ) -> crate::Result<()> {
        debug!("Handling Parse message");
        
        let mut pos = 0;
        
        // Read statement name
        let name_end = data[pos..].iter().position(|&b| b == 0).unwrap_or(data.len() - pos);
        let name = std::str::from_utf8(&data[pos..pos + name_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in statement name".to_string()))?
            .to_string();
        pos += name_end + 1;
        
        // Read query
        let query_end = data[pos..].iter().position(|&b| b == 0).unwrap_or(data.len() - pos);
        let query = std::str::from_utf8(&data[pos..pos + query_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in query".to_string()))?
            .to_string();
        pos += query_end + 1;
        
        // Read parameter type count
        if pos + 2 > data.len() {
            return Err(YamlBaseError::Protocol("Incomplete parse message".to_string()));
        }
        let param_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        
        // Read parameter types
        let mut parameter_types = Vec::new();
        for _ in 0..param_count {
            if pos + 4 > data.len() {
                return Err(YamlBaseError::Protocol("Incomplete parameter types".to_string()));
            }
            let oid = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            parameter_types.push(oid_to_sql_type(oid));
            pos += 4;
        }
        
        // Parse the SQL
        let parsed_statements = parse_sql(&query)?;
        
        // Store prepared statement
        let stmt = PreparedStatement {
            name: name.clone(),
            query,
            parameter_types,
            parsed_statements,
        };
        
        self.prepared_statements.insert(name, stmt);
        
        // Send ParseComplete
        let mut buf = BytesMut::new();
        buf.put_u8(b'1');
        buf.put_u32(4);
        stream.write_all(&buf).await?;
        
        Ok(())
    }

    pub async fn handle_bind(
        &mut self,
        stream: &mut TcpStream,
        data: &[u8],
    ) -> crate::Result<()> {
        debug!("Handling Bind message");
        
        let mut pos = 0;
        
        // Read portal name
        let portal_name_end = data[pos..].iter().position(|&b| b == 0).unwrap_or(data.len() - pos);
        let portal_name = std::str::from_utf8(&data[pos..pos + portal_name_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in portal name".to_string()))?
            .to_string();
        pos += portal_name_end + 1;
        
        // Read statement name
        let stmt_name_end = data[pos..].iter().position(|&b| b == 0).unwrap_or(data.len() - pos);
        let stmt_name = std::str::from_utf8(&data[pos..pos + stmt_name_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in statement name".to_string()))?
            .to_string();
        pos += stmt_name_end + 1;
        
        // Get the prepared statement
        let statement = self.prepared_statements.get(&stmt_name)
            .ok_or_else(|| YamlBaseError::Protocol(format!("Unknown prepared statement: {}", stmt_name)))?
            .clone();
        
        // Read parameter format codes
        if pos + 2 > data.len() {
            return Err(YamlBaseError::Protocol("Incomplete bind message".to_string()));
        }
        let format_code_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        
        let mut _format_codes = Vec::new();
        for _ in 0..format_code_count {
            if pos + 2 > data.len() {
                return Err(YamlBaseError::Protocol("Incomplete format codes".to_string()));
            }
            let format = u16::from_be_bytes([data[pos], data[pos + 1]]);
            _format_codes.push(format);
            pos += 2;
        }
        
        // Read parameter values
        if pos + 2 > data.len() {
            return Err(YamlBaseError::Protocol("Incomplete parameter count".to_string()));
        }
        let param_value_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        
        let mut parameters = Vec::new();
        for i in 0..param_value_count {
            if pos + 4 > data.len() {
                return Err(YamlBaseError::Protocol("Incomplete parameter value".to_string()));
            }
            let length = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;
            
            if length == -1 {
                parameters.push(Value::Null);
            } else {
                let length = length as usize;
                if pos + length > data.len() {
                    return Err(YamlBaseError::Protocol("Incomplete parameter data".to_string()));
                }
                let value_data = &data[pos..pos + length];
                pos += length;
                
                // Convert based on parameter type
                let sql_type = statement.parameter_types.get(i).unwrap_or(&SqlType::Text);
                let value = parse_parameter_value(value_data, sql_type)?;
                parameters.push(value);
            }
        }
        
        // Store portal
        let portal = Portal {
            name: portal_name.clone(),
            statement,
            parameters,
        };
        
        self.portals.insert(portal_name, portal);
        
        // Send BindComplete
        let mut buf = BytesMut::new();
        buf.put_u8(b'2');
        buf.put_u32(4);
        stream.write_all(&buf).await?;
        
        Ok(())
    }

    pub async fn handle_describe(
        &self,
        stream: &mut TcpStream,
        data: &[u8],
        executor: &QueryExecutor,
    ) -> crate::Result<()> {
        debug!("Handling Describe message");
        
        if data.is_empty() {
            return Err(YamlBaseError::Protocol("Empty describe message".to_string()));
        }
        
        let describe_type = data[0];
        let name_end = data[1..].iter().position(|&b| b == 0).unwrap_or(data.len() - 1);
        let name = std::str::from_utf8(&data[1..1 + name_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in describe name".to_string()))?;
        
        match describe_type {
            b'S' => {
                // Describe statement
                if let Some(stmt) = self.prepared_statements.get(name) {
                    // Send ParameterDescription
                    let mut buf = BytesMut::new();
                    buf.put_u8(b't');
                    buf.put_u32(4 + 2 + stmt.parameter_types.len() as u32 * 4);
                    buf.put_u16(stmt.parameter_types.len() as u16);
                    for param_type in &stmt.parameter_types {
                        buf.put_u32(sql_type_to_oid(param_type));
                    }
                    stream.write_all(&buf).await?;
                    
                    // For SELECT queries, we need to describe the result
                    if !stmt.parsed_statements.is_empty() {
                        if let sqlparser::ast::Statement::Query(_) = &stmt.parsed_statements[0] {
                            // Execute with empty parameters to get column info
                            match executor.execute(&stmt.parsed_statements[0]).await {
                                Ok(result) => {
                                    send_row_description(stream, &result).await?;
                                }
                                Err(_) => {
                                    // Send NoData if we can't determine columns
                                    buf.clear();
                                    buf.put_u8(b'n');
                                    buf.put_u32(4);
                                    stream.write_all(&buf).await?;
                                }
                            }
                        } else {
                            // Non-SELECT statements don't return data
                            buf.clear();
                            buf.put_u8(b'n');
                            buf.put_u32(4);
                            stream.write_all(&buf).await?;
                        }
                    }
                } else {
                    return Err(YamlBaseError::Protocol(format!("Unknown statement: {}", name)));
                }
            }
            b'P' => {
                // Describe portal
                if let Some(portal) = self.portals.get(name) {
                    // For SELECT queries, describe the result
                    if !portal.statement.parsed_statements.is_empty() {
                        if let sqlparser::ast::Statement::Query(_) = &portal.statement.parsed_statements[0] {
                            match executor.execute(&portal.statement.parsed_statements[0]).await {
                                Ok(result) => {
                                    send_row_description(stream, &result).await?;
                                }
                                Err(_) => {
                                    // Send NoData
                                    let mut buf = BytesMut::new();
                                    buf.put_u8(b'n');
                                    buf.put_u32(4);
                                    stream.write_all(&buf).await?;
                                }
                            }
                        } else {
                            // Send NoData
                            let mut buf = BytesMut::new();
                            buf.put_u8(b'n');
                            buf.put_u32(4);
                            stream.write_all(&buf).await?;
                        }
                    }
                } else {
                    return Err(YamlBaseError::Protocol(format!("Unknown portal: {}", name)));
                }
            }
            _ => {
                return Err(YamlBaseError::Protocol(format!("Unknown describe type: {}", describe_type as char)));
            }
        }
        
        Ok(())
    }

    pub async fn handle_execute(
        &self,
        stream: &mut TcpStream,
        data: &[u8],
        executor: &QueryExecutor,
    ) -> crate::Result<()> {
        debug!("Handling Execute message");
        
        let mut pos = 0;
        
        // Read portal name
        let name_end = data[pos..].iter().position(|&b| b == 0).unwrap_or(data.len() - pos);
        let portal_name = std::str::from_utf8(&data[pos..pos + name_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in portal name".to_string()))?;
        pos += name_end + 1;
        
        // Read row limit
        if pos + 4 > data.len() {
            return Err(YamlBaseError::Protocol("Incomplete execute message".to_string()));
        }
        let _row_limit = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        
        // Get portal
        let portal = self.portals.get(portal_name)
            .ok_or_else(|| YamlBaseError::Protocol(format!("Unknown portal: {}", portal_name)))?;
        
        // Execute the statement
        // TODO: Handle parameters properly by substituting them into the query
        if !portal.statement.parsed_statements.is_empty() {
            match executor.execute(&portal.statement.parsed_statements[0]).await {
                Ok(result) => {
                    send_data_rows(stream, &result).await?;
                    
                    // Send CommandComplete
                    let mut buf = BytesMut::new();
                    buf.put_u8(b'C');
                    let tag = format!("SELECT {}", result.rows.len());
                    buf.put_u32(4 + tag.len() as u32 + 1);
                    buf.put_slice(tag.as_bytes());
                    buf.put_u8(0);
                    stream.write_all(&buf).await?;
                }
                Err(e) => {
                    send_error_response(stream, "XX000", &e.to_string()).await?;
                }
            }
        }
        
        Ok(())
    }

    pub async fn handle_sync(&self, stream: &mut TcpStream) -> crate::Result<()> {
        debug!("Handling Sync message");
        
        // Send ReadyForQuery
        let mut buf = BytesMut::new();
        buf.put_u8(b'Z');
        buf.put_u32(5);
        buf.put_u8(b'I'); // Idle
        stream.write_all(&buf).await?;
        
        Ok(())
    }

    pub fn close_statement(&mut self, name: &str) {
        self.prepared_statements.remove(name);
    }

    pub fn close_portal(&mut self, name: &str) {
        self.portals.remove(name);
    }
}

async fn send_row_description(stream: &mut TcpStream, result: &QueryResult) -> crate::Result<()> {
    let mut buf = BytesMut::new();
    buf.put_u8(b'T');
    
    // Calculate length
    let mut length = 6; // 4 bytes for length + 2 bytes for field count
    for col in &result.columns {
        length += col.len() + 1 + 18; // name + null + field info
    }
    buf.put_u32(length as u32);
    buf.put_u16(result.columns.len() as u16);
    
    // Send field descriptions
    for (i, col) in result.columns.iter().enumerate() {
        buf.put_slice(col.as_bytes());
        buf.put_u8(0); // Null terminator
        buf.put_u32(0); // Table OID
        buf.put_u16(i as u16); // Column number
        buf.put_u32(25); // Type OID (text) - TODO: Proper type mapping
        buf.put_i16(-1); // Type size
        buf.put_i32(-1); // Type modifier
        buf.put_i16(0); // Format code (text)
    }
    
    stream.write_all(&buf).await?;
    Ok(())
}

async fn send_data_rows(stream: &mut TcpStream, result: &QueryResult) -> crate::Result<()> {
    for row in &result.rows {
        let mut buf = BytesMut::new();
        buf.put_u8(b'D');
        
        // Calculate row length
        let mut row_length = 6; // 4 bytes for length + 2 bytes for field count
        for val in row {
            let val_str = val.to_string();
            row_length += 4 + val_str.len(); // 4 bytes for value length + value
        }
        
        buf.put_u32(row_length as u32);
        buf.put_u16(row.len() as u16);
        
        // Send field values
        for val in row {
            if matches!(val, Value::Null) {
                buf.put_i32(-1); // NULL
            } else {
                let val_str = val.to_string();
                buf.put_i32(val_str.len() as i32);
                buf.put_slice(val_str.as_bytes());
            }
        }
        
        stream.write_all(&buf).await?;
    }
    Ok(())
}

async fn send_error_response(stream: &mut TcpStream, code: &str, message: &str) -> crate::Result<()> {
    let mut buf = BytesMut::new();
    buf.put_u8(b'E');
    
    let error_fields = vec![
        (b'S', "ERROR"),
        (b'C', code),
        (b'M', message),
    ];
    
    let mut length = 4; // Length field
    for (_, val) in &error_fields {
        length += 1 + val.len() + 1; // Field type + value + null
    }
    length += 1; // Final null
    
    buf.put_u32(length as u32);
    
    for (field_type, val) in error_fields {
        buf.put_u8(field_type);
        buf.put_slice(val.as_bytes());
        buf.put_u8(0);
    }
    buf.put_u8(0); // End of fields
    
    stream.write_all(&buf).await?;
    Ok(())
}

fn oid_to_sql_type(oid: u32) -> SqlType {
    match oid {
        16 => SqlType::Boolean,      // bool
        20 => SqlType::Integer,      // int8
        21 => SqlType::Integer,      // int2
        23 => SqlType::Integer,      // int4
        25 => SqlType::Text,         // text
        700 => SqlType::Float,       // float4
        701 => SqlType::Double,      // float8
        1043 => SqlType::Varchar(255), // varchar
        1082 => SqlType::Date,       // date
        1083 => SqlType::Time,       // time
        1114 => SqlType::Timestamp,  // timestamp
        1700 => SqlType::Decimal(38, 0), // numeric
        2950 => SqlType::Uuid,       // uuid
        3802 => SqlType::Json,       // jsonb
        _ => SqlType::Text,          // Default to text
    }
}

fn sql_type_to_oid(sql_type: &SqlType) -> u32 {
    match sql_type {
        SqlType::Boolean => 16,
        SqlType::Integer => 23,
        SqlType::Float => 700,
        SqlType::Double => 701,
        SqlType::Decimal(_, _) => 1700,
        SqlType::Varchar(_) => 1043,
        SqlType::Text => 25,
        SqlType::Date => 1082,
        SqlType::Time => 1083,
        SqlType::Timestamp => 1114,
        SqlType::Uuid => 2950,
        SqlType::Json => 3802,
    }
}

fn parse_parameter_value(data: &[u8], sql_type: &SqlType) -> crate::Result<Value> {
    match sql_type {
        SqlType::Integer => {
            if data.len() == 8 {
                let val = i64::from_be_bytes(data.try_into().unwrap());
                Ok(Value::Integer(val))
            } else if data.len() == 4 {
                let val = i32::from_be_bytes(data.try_into().unwrap()) as i64;
                Ok(Value::Integer(val))
            } else if data.len() == 2 {
                let val = i16::from_be_bytes(data.try_into().unwrap()) as i64;
                Ok(Value::Integer(val))
            } else {
                Err(YamlBaseError::Protocol("Invalid integer size".to_string()))
            }
        }
        SqlType::Float => {
            if data.len() == 4 {
                let val = f32::from_be_bytes(data.try_into().unwrap());
                Ok(Value::Float(val))
            } else {
                Err(YamlBaseError::Protocol("Invalid float size".to_string()))
            }
        }
        SqlType::Double => {
            if data.len() == 8 {
                let val = f64::from_be_bytes(data.try_into().unwrap());
                Ok(Value::Double(val))
            } else {
                Err(YamlBaseError::Protocol("Invalid double size".to_string()))
            }
        }
        SqlType::Boolean => {
            if data.len() == 1 {
                Ok(Value::Boolean(data[0] != 0))
            } else {
                Err(YamlBaseError::Protocol("Invalid boolean size".to_string()))
            }
        }
        _ => {
            // For text types, assume UTF-8 encoding
            let text = std::str::from_utf8(data)
                .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in parameter".to_string()))?;
            Ok(Value::Text(text.to_string()))
        }
    }
}