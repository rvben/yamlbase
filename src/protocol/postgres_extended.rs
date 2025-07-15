use bytes::{BufMut, BytesMut};
use std::collections::HashMap;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::debug;

use crate::YamlBaseError;
use crate::database::Value;
use crate::sql::executor::QueryResult;
use crate::sql::{QueryExecutor, parse_sql};
use crate::yaml::schema::SqlType;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, Statement, Value as SqlValue,
};

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
    pub result_formats: Vec<u16>,
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
}

impl Default for ExtendedProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl ExtendedProtocol {
    pub async fn handle_parse(&mut self, stream: &mut TcpStream, data: &[u8]) -> crate::Result<()> {
        debug!("Handling Parse message");

        let mut pos = 0;

        // Read statement name
        let name_end = data[pos..]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(data.len() - pos);
        let name = std::str::from_utf8(&data[pos..pos + name_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in statement name".to_string()))?
            .to_string();
        pos += name_end + 1;

        // Read query
        let query_end = data[pos..]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(data.len() - pos);
        let query = std::str::from_utf8(&data[pos..pos + query_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in query".to_string()))?
            .to_string();
        pos += query_end + 1;

        // Read parameter type count
        if pos + 2 > data.len() {
            return Err(YamlBaseError::Protocol(
                "Incomplete parse message".to_string(),
            ));
        }
        let param_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        // Read parameter types
        let mut parameter_types = Vec::new();
        for _ in 0..param_count {
            if pos + 4 > data.len() {
                return Err(YamlBaseError::Protocol(
                    "Incomplete parameter types".to_string(),
                ));
            }
            let oid = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            parameter_types.push(oid_to_sql_type(oid));
            pos += 4;
        }

        // Parse the SQL
        let parsed_statements = parse_sql(&query)?;

        // If no parameter types were provided, we need to infer them from the query
        if parameter_types.is_empty() && !parsed_statements.is_empty() {
            if let Statement::Query(query_ref) = &parsed_statements[0] {
                let inferred_types = infer_parameter_types(query_ref);
                debug!("Inferred {} parameters from query", inferred_types.len());
                parameter_types = inferred_types;
            }
        }

        debug!(
            "PreparedStatement '{}' has {} parameter types",
            name,
            parameter_types.len()
        );

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

    pub async fn handle_bind(&mut self, stream: &mut TcpStream, data: &[u8]) -> crate::Result<()> {
        debug!("Handling Bind message");

        let mut pos = 0;

        // Read portal name
        let portal_name_end = data[pos..]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(data.len() - pos);
        let portal_name = std::str::from_utf8(&data[pos..pos + portal_name_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in portal name".to_string()))?
            .to_string();
        pos += portal_name_end + 1;

        // Read statement name
        let stmt_name_end = data[pos..]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(data.len() - pos);
        let stmt_name = std::str::from_utf8(&data[pos..pos + stmt_name_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in statement name".to_string()))?
            .to_string();
        pos += stmt_name_end + 1;

        // Get the prepared statement
        let statement = self
            .prepared_statements
            .get(&stmt_name)
            .ok_or_else(|| {
                YamlBaseError::Protocol(format!("Unknown prepared statement: {}", stmt_name))
            })?
            .clone();

        // Read parameter format codes
        if pos + 2 > data.len() {
            return Err(YamlBaseError::Protocol(
                "Incomplete bind message".to_string(),
            ));
        }
        let format_code_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        let mut _format_codes = Vec::new();
        for _ in 0..format_code_count {
            if pos + 2 > data.len() {
                return Err(YamlBaseError::Protocol(
                    "Incomplete format codes".to_string(),
                ));
            }
            let format = u16::from_be_bytes([data[pos], data[pos + 1]]);
            _format_codes.push(format);
            pos += 2;
        }

        // Read parameter values
        if pos + 2 > data.len() {
            return Err(YamlBaseError::Protocol(
                "Incomplete parameter count".to_string(),
            ));
        }
        let param_value_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        let mut parameters = Vec::new();
        for i in 0..param_value_count {
            if pos + 4 > data.len() {
                return Err(YamlBaseError::Protocol(
                    "Incomplete parameter value".to_string(),
                ));
            }
            let length =
                i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;

            if length == -1 {
                parameters.push(Value::Null);
            } else {
                let length = length as usize;
                if pos + length > data.len() {
                    return Err(YamlBaseError::Protocol(
                        "Incomplete parameter data".to_string(),
                    ));
                }
                let value_data = &data[pos..pos + length];
                pos += length;

                // Convert based on parameter type
                let sql_type = statement.parameter_types.get(i).unwrap_or(&SqlType::Text);
                let value = parse_parameter_value(value_data, sql_type)?;
                parameters.push(value);
            }
        }

        // Read result format codes
        if pos + 2 > data.len() {
            return Err(YamlBaseError::Protocol(
                "Incomplete result format count".to_string(),
            ));
        }
        let result_format_count = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        let mut result_formats = Vec::new();
        for _ in 0..result_format_count {
            if pos + 2 > data.len() {
                return Err(YamlBaseError::Protocol(
                    "Incomplete result format codes".to_string(),
                ));
            }
            let format = u16::from_be_bytes([data[pos], data[pos + 1]]);
            result_formats.push(format);
            pos += 2;
        }

        // Store portal
        let portal = Portal {
            name: portal_name.clone(),
            statement,
            parameters,
            result_formats,
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
        debug!("Handling Describe message with {} bytes", data.len());

        if data.is_empty() {
            return Err(YamlBaseError::Protocol(
                "Empty describe message".to_string(),
            ));
        }

        let describe_type = data[0];
        let name_end = data[1..]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(data.len() - 1);
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
                        if let sqlparser::ast::Statement::Query(query) = &stmt.parsed_statements[0]
                        {
                            // Try to extract column information from the query
                            if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
                                let (columns, types) =
                                    extract_columns_and_types_from_select(select, executor);
                                send_row_description_for_columns_with_types(
                                    stream, &columns, &types,
                                )
                                .await?;
                            } else {
                                // Send NoData if we can't determine columns
                                buf.clear();
                                buf.put_u8(b'n');
                                buf.put_u32(4);
                                stream.write_all(&buf).await?;
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
                    return Err(YamlBaseError::Protocol(format!(
                        "Unknown statement: {}",
                        name
                    )));
                }
            }
            b'P' => {
                // Describe portal
                if let Some(portal) = self.portals.get(name) {
                    // For SELECT queries, describe the result
                    if !portal.statement.parsed_statements.is_empty() {
                        if let sqlparser::ast::Statement::Query(_) =
                            &portal.statement.parsed_statements[0]
                        {
                            match executor
                                .execute(&portal.statement.parsed_statements[0])
                                .await
                            {
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
                return Err(YamlBaseError::Protocol(format!(
                    "Unknown describe type: {}",
                    describe_type as char
                )));
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
        let name_end = data[pos..]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(data.len() - pos);
        let portal_name = std::str::from_utf8(&data[pos..pos + name_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in portal name".to_string()))?;
        pos += name_end + 1;

        // Read row limit
        if pos + 4 > data.len() {
            return Err(YamlBaseError::Protocol(
                "Incomplete execute message".to_string(),
            ));
        }
        let _row_limit =
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);

        // Get portal
        let portal = self
            .portals
            .get(portal_name)
            .ok_or_else(|| YamlBaseError::Protocol(format!("Unknown portal: {}", portal_name)))?;

        // Execute the statement with parameter substitution
        if !portal.statement.parsed_statements.is_empty() {
            // Clone the statement and substitute parameters
            let mut statement = portal.statement.parsed_statements[0].clone();
            substitute_parameters(&mut statement, &portal.parameters)?;

            match executor.execute(&statement).await {
                Ok(result) => {
                    debug!(
                        "Execute result: {} rows, {} columns: {:?}",
                        result.rows.len(),
                        result.columns.len(),
                        result.columns
                    );
                    if !result.rows.is_empty() {
                        debug!("First row: {:?}", result.rows[0]);
                    }

                    // Pass the result formats from the portal
                    send_data_rows(stream, &result, &portal.result_formats).await?;

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

        // Get the type OID from column_types if available
        let type_oid = if i < result.column_types.len() {
            sql_type_to_oid(&result.column_types[i])
        } else {
            25 // Default to text
        };
        buf.put_u32(type_oid);

        buf.put_i16(-1); // Type size
        buf.put_i32(-1); // Type modifier
        buf.put_i16(0); // Format code (text)
    }

    stream.write_all(&buf).await?;
    Ok(())
}

async fn send_row_description_for_columns_with_types(
    stream: &mut TcpStream,
    columns: &[String],
    types: &[SqlType],
) -> crate::Result<()> {
    let mut buf = BytesMut::new();
    buf.put_u8(b'T');

    // Calculate length
    let mut length = 6; // 4 bytes for length + 2 bytes for field count
    for col in columns {
        length += col.len() + 1 + 18; // name + null + field info
    }
    buf.put_u32(length as u32);
    buf.put_u16(columns.len() as u16);

    // Send field descriptions
    for (i, col) in columns.iter().enumerate() {
        buf.put_slice(col.as_bytes());
        buf.put_u8(0); // Null terminator
        buf.put_u32(0); // Table OID
        buf.put_u16(i as u16); // Column number

        // Get the type OID from types if available
        let type_oid = if i < types.len() {
            sql_type_to_oid(&types[i])
        } else {
            25 // Default to text
        };
        buf.put_u32(type_oid);

        buf.put_i16(-1); // Type size
        buf.put_i32(-1); // Type modifier
        buf.put_i16(0); // Format code (text)
    }

    stream.write_all(&buf).await?;
    Ok(())
}

fn extract_columns_and_types_from_select(
    select: &sqlparser::ast::Select,
    executor: &QueryExecutor,
) -> (Vec<String>, Vec<SqlType>) {
    let mut columns = Vec::new();
    let mut types = Vec::new();

    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                match expr {
                    Expr::Identifier(ident) => {
                        columns.push(ident.value.clone());
                        // Try to infer type from column name in WHERE clause context
                        types.push(infer_type_from_column_name(&ident.value));
                    }
                    Expr::Function(func) => {
                        let func_name = func
                            .name
                            .0
                            .first()
                            .map(|ident| ident.value.to_uppercase())
                            .unwrap_or_default();

                        // For aggregate functions, we know the result type
                        match func_name.as_str() {
                            "COUNT" => {
                                columns.push(func_name.clone());
                                types.push(SqlType::BigInt); // COUNT returns i64
                            }
                            "SUM" => {
                                columns.push(func_name.clone());
                                types.push(SqlType::Text); // SUM returns formatted text for monetary values
                            }
                            "AVG" => {
                                columns.push(func_name.clone());
                                types.push(SqlType::Double);
                            }
                            _ => {
                                columns.push(func_name.clone());
                                types.push(SqlType::Text);
                            }
                        }
                    }
                    _ => {
                        // For complex expressions, use a generic name and text type
                        columns.push(format!("column{}", columns.len()));
                        types.push(SqlType::Text);
                    }
                }
            }
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                columns.push(alias.value.clone());
                // Try to determine type from expression
                match expr {
                    Expr::Function(func) => {
                        let func_name = func
                            .name
                            .0
                            .first()
                            .map(|ident| ident.value.to_uppercase())
                            .unwrap_or_default();

                        match func_name.as_str() {
                            "COUNT" => types.push(SqlType::BigInt), // COUNT returns i64
                            "SUM" => types.push(SqlType::Text),
                            "AVG" => types.push(SqlType::Double),
                            _ => types.push(SqlType::Text),
                        }
                    }
                    _ => types.push(SqlType::Text),
                }
            }
            sqlparser::ast::SelectItem::Wildcard(_) => {
                // For SELECT *, we need to get all columns from the table
                if let Some(table) = select.from.first() {
                    if let Some(table_name) = get_table_name_from_relation(&table.relation) {
                        if let Ok(db) = executor.storage().database().try_read() {
                            if let Some(table) = db.get_table(&table_name) {
                                for col in &table.columns {
                                    columns.push(col.name.clone());
                                    types.push(col.sql_type.clone());
                                }
                            }
                        }
                    }
                }
            }
            _ => {
                // For other types, use a generic name
                columns.push(format!("column{}", columns.len()));
                types.push(SqlType::Text);
            }
        }
    }

    (columns, types)
}

fn infer_type_from_column_name(name: &str) -> SqlType {
    match name.to_lowercase().as_str() {
        "age" | "id" | "count" | "quantity" => SqlType::Integer,
        "price" | "amount" | "total" => SqlType::Double,
        "active" | "enabled" | "deleted" | "is_active" | "in_stock" => SqlType::Boolean,
        "created_at" | "updated_at" => SqlType::Timestamp,
        "created_date" => SqlType::Date,
        _ => SqlType::Text,
    }
}

fn get_table_name_from_relation(relation: &sqlparser::ast::TableFactor) -> Option<String> {
    match relation {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            name.0.first().map(|ident| ident.value.clone())
        }
        _ => None,
    }
}

async fn send_data_rows(
    stream: &mut TcpStream,
    result: &QueryResult,
    result_formats: &[u16],
) -> crate::Result<()> {
    for row in &result.rows {
        let mut buf = BytesMut::new();
        buf.put_u8(b'D');

        // First pass: calculate row length
        let mut row_length = 6; // 4 bytes for length + 2 bytes for field count
        for (col_idx, val) in row.iter().enumerate() {
            if matches!(val, Value::Null) {
                row_length += 4; // 4 bytes for -1 (NULL indicator)
            } else {
                // Check the format for this column
                let format = if result_formats.is_empty() {
                    0 // Default to text
                } else if result_formats.len() == 1 {
                    result_formats[0] // Use the single format for all columns
                } else if col_idx < result_formats.len() {
                    result_formats[col_idx] // Use the specific format for this column
                } else {
                    0 // Default to text if not specified
                };

                if format == 1 {
                    // Binary format
                    match val {
                        Value::Integer(_) => {
                            // Check the column type to determine size
                            let col_type = result.column_types.get(col_idx);
                            match col_type {
                                Some(SqlType::BigInt) => row_length += 4 + 8, // int8 (i64)
                                Some(SqlType::Integer) => row_length += 4 + 4, // int4 (i32)
                                _ => row_length += 4 + 4, // Default to int4 for compatibility
                            }
                        }
                        Value::Boolean(_) => row_length += 4 + 1, // 4 bytes for length + 1 byte for bool
                        Value::Float(_) => row_length += 4 + 4, // 4 bytes for length + 4 bytes for f32
                        Value::Double(_) => row_length += 4 + 8, // 4 bytes for length + 8 bytes for f64
                        _ => {
                            // For other types, fall back to text
                            let val_str = val.to_string();
                            row_length += 4 + val_str.len();
                        }
                    }
                } else {
                    // Text format
                    let val_str = val.to_string();
                    row_length += 4 + val_str.len();
                }
            }
        }

        buf.put_u32(row_length as u32);
        buf.put_u16(row.len() as u16);

        // Second pass: send field values
        for (col_idx, val) in row.iter().enumerate() {
            if matches!(val, Value::Null) {
                buf.put_i32(-1); // NULL
            } else {
                // Check the format for this column
                let format = if result_formats.is_empty() {
                    0 // Default to text
                } else if result_formats.len() == 1 {
                    result_formats[0] // Use the single format for all columns
                } else if col_idx < result_formats.len() {
                    result_formats[col_idx] // Use the specific format for this column
                } else {
                    0 // Default to text if not specified
                };

                if format == 1 {
                    // Binary format
                    match val {
                        Value::Integer(i) => {
                            // Check the column type to determine size
                            let col_type = result.column_types.get(col_idx);
                            match col_type {
                                Some(SqlType::BigInt) => {
                                    buf.put_i32(8); // Length of i64
                                    buf.put_i64(*i); // Send as 8-byte big-endian integer
                                }
                                Some(SqlType::Integer) => {
                                    buf.put_i32(4); // Length of i32
                                    buf.put_i32(*i as i32); // Send as 4-byte big-endian integer
                                }
                                _ => {
                                    // Default to int4 for compatibility
                                    buf.put_i32(4); // Length of i32
                                    buf.put_i32(*i as i32); // Send as 4-byte big-endian integer
                                }
                            }
                        }
                        Value::Boolean(b) => {
                            buf.put_i32(1); // Length of bool
                            buf.put_u8(if *b { 1 } else { 0 });
                        }
                        Value::Float(f) => {
                            buf.put_i32(4); // Length of f32
                            buf.put_f32(*f);
                        }
                        Value::Double(d) => {
                            buf.put_i32(8); // Length of f64
                            buf.put_f64(*d);
                        }
                        _ => {
                            // For other types, fall back to text
                            let val_str = val.to_string();
                            buf.put_i32(val_str.len() as i32);
                            buf.put_slice(val_str.as_bytes());
                        }
                    }
                } else {
                    // Text format
                    let val_str = val.to_string();
                    buf.put_i32(val_str.len() as i32);
                    buf.put_slice(val_str.as_bytes());
                }
            }
        }

        stream.write_all(&buf).await?;
    }
    Ok(())
}

async fn send_error_response(
    stream: &mut TcpStream,
    code: &str,
    message: &str,
) -> crate::Result<()> {
    let mut buf = BytesMut::new();
    buf.put_u8(b'E');

    let error_fields = vec![(b'S', "ERROR"), (b'C', code), (b'M', message)];

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
        16 => SqlType::Boolean,          // bool
        20 => SqlType::BigInt,           // int8
        21 => SqlType::Integer,          // int2
        23 => SqlType::Integer,          // int4
        25 => SqlType::Text,             // text
        700 => SqlType::Float,           // float4
        701 => SqlType::Double,          // float8
        1043 => SqlType::Varchar(255),   // varchar
        1082 => SqlType::Date,           // date
        1083 => SqlType::Time,           // time
        1114 => SqlType::Timestamp,      // timestamp
        1700 => SqlType::Decimal(38, 0), // numeric
        2950 => SqlType::Uuid,           // uuid
        3802 => SqlType::Json,           // jsonb
        _ => SqlType::Text,              // Default to text
    }
}

fn sql_type_to_oid(sql_type: &SqlType) -> u32 {
    match sql_type {
        SqlType::Boolean => 16,
        SqlType::Integer => 23, // int4 - PostgreSQL INTEGER type
        SqlType::BigInt => 20,  // int8 - PostgreSQL BIGINT type
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

fn substitute_parameters(statement: &mut Statement, parameters: &[Value]) -> crate::Result<()> {
    match statement {
        Statement::Query(query) => {
            substitute_parameters_in_query(query, parameters)?;
        }
        _ => {
            return Err(YamlBaseError::Protocol(
                "Parameter substitution only supported for queries".to_string(),
            ));
        }
    }
    Ok(())
}

fn substitute_parameters_in_query(
    query: &mut sqlparser::ast::Query,
    parameters: &[Value],
) -> crate::Result<()> {
    if let sqlparser::ast::SetExpr::Select(select) = &mut *query.body {
        if let Some(selection) = &mut select.selection {
            substitute_parameters_in_expr(selection, parameters)?;
        }
    }
    Ok(())
}

fn substitute_parameters_in_expr(expr: &mut Expr, parameters: &[Value]) -> crate::Result<()> {
    match expr {
        Expr::Value(SqlValue::Placeholder(s)) => {
            // Parse placeholder like "$1", "$2", etc.
            if let Some(num_str) = s.strip_prefix('$') {
                if let Ok(param_idx) = num_str.parse::<usize>() {
                    if param_idx > 0 && param_idx <= parameters.len() {
                        let param_value = &parameters[param_idx - 1];
                        *expr = value_to_sql_expr(param_value);
                    } else {
                        return Err(YamlBaseError::Protocol(format!(
                            "Invalid parameter index: ${}",
                            param_idx
                        )));
                    }
                } else {
                    return Err(YamlBaseError::Protocol(format!(
                        "Invalid placeholder format: {}",
                        s
                    )));
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            substitute_parameters_in_expr(left, parameters)?;
            substitute_parameters_in_expr(right, parameters)?;
        }
        Expr::UnaryOp { expr, .. } => {
            substitute_parameters_in_expr(expr, parameters)?;
        }
        Expr::InList { expr, list, .. } => {
            substitute_parameters_in_expr(expr, parameters)?;
            for item in list {
                substitute_parameters_in_expr(item, parameters)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            substitute_parameters_in_expr(expr, parameters)?;
            substitute_parameters_in_expr(low, parameters)?;
            substitute_parameters_in_expr(high, parameters)?;
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                substitute_parameters_in_expr(op, parameters)?;
            }
            for cond in conditions {
                substitute_parameters_in_expr(cond, parameters)?;
            }
            for res in results {
                substitute_parameters_in_expr(res, parameters)?;
            }
            if let Some(else_res) = else_result {
                substitute_parameters_in_expr(else_res, parameters)?;
            }
        }
        Expr::Nested(inner) => {
            substitute_parameters_in_expr(inner, parameters)?;
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            substitute_parameters_in_expr(inner, parameters)?;
        }
        Expr::Like { expr, pattern, .. } => {
            substitute_parameters_in_expr(expr, parameters)?;
            substitute_parameters_in_expr(pattern, parameters)?;
        }
        _ => {}
    }
    Ok(())
}

fn value_to_sql_expr(value: &Value) -> Expr {
    match value {
        Value::Null => Expr::Value(SqlValue::Null),
        Value::Boolean(b) => Expr::Value(SqlValue::Boolean(*b)),
        Value::Integer(i) => Expr::Value(SqlValue::Number(i.to_string(), false)),
        Value::Float(f) => Expr::Value(SqlValue::Number(f.to_string(), false)),
        Value::Double(d) => Expr::Value(SqlValue::Number(d.to_string(), false)),
        Value::Text(s) => Expr::Value(SqlValue::SingleQuotedString(s.clone())),
        Value::Date(d) => Expr::Value(SqlValue::SingleQuotedString(d.to_string())),
        Value::Time(t) => Expr::Value(SqlValue::SingleQuotedString(t.to_string())),
        Value::Timestamp(ts) => Expr::Value(SqlValue::SingleQuotedString(ts.to_string())),
        Value::Uuid(u) => Expr::Value(SqlValue::SingleQuotedString(u.to_string())),
        Value::Json(j) => Expr::Value(SqlValue::SingleQuotedString(j.to_string())),
        Value::Decimal(d) => Expr::Value(SqlValue::Number(d.to_string(), false)),
    }
}

fn infer_parameter_types(query: &sqlparser::ast::Query) -> Vec<SqlType> {
    let mut parameter_types = std::collections::HashMap::new();

    if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
        if let Some(selection) = &select.selection {
            infer_types_in_expr(selection, &mut parameter_types);
        }

        // Also check projection for parameters in aggregate functions
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    infer_types_in_projection_expr(expr, &mut parameter_types);
                }
                _ => {}
            }
        }
    }

    // Convert HashMap to Vec, using the parameter index as the key
    let max_param = parameter_types.keys().max().copied().unwrap_or(0);
    let mut result = Vec::new();
    for i in 1..=max_param {
        result.push(parameter_types.get(&i).cloned().unwrap_or(SqlType::Text));
    }
    result
}

fn infer_types_in_expr(
    expr: &Expr,
    parameter_types: &mut std::collections::HashMap<usize, SqlType>,
) {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // For comparison operators, try to infer parameter type from the other side
            match op {
                sqlparser::ast::BinaryOperator::Eq
                | sqlparser::ast::BinaryOperator::NotEq
                | sqlparser::ast::BinaryOperator::Lt
                | sqlparser::ast::BinaryOperator::LtEq
                | sqlparser::ast::BinaryOperator::Gt
                | sqlparser::ast::BinaryOperator::GtEq => {
                    // If one side is a parameter and the other is a column, infer type
                    if let Expr::Value(SqlValue::Placeholder(s)) = &**left {
                        if let Some(num_str) = s.strip_prefix('$') {
                            if let Ok(param_num) = num_str.parse::<usize>() {
                                if let Some(inferred_type) = infer_type_from_expr(right) {
                                    parameter_types.insert(param_num, inferred_type);
                                }
                            }
                        }
                    }
                    if let Expr::Value(SqlValue::Placeholder(s)) = &**right {
                        if let Some(num_str) = s.strip_prefix('$') {
                            if let Ok(param_num) = num_str.parse::<usize>() {
                                if let Some(inferred_type) = infer_type_from_expr(left) {
                                    parameter_types.insert(param_num, inferred_type);
                                }
                            }
                        }
                    }
                }
                sqlparser::ast::BinaryOperator::And | sqlparser::ast::BinaryOperator::Or => {
                    // For AND/OR, recurse into both sides
                    infer_types_in_expr(left, parameter_types);
                    infer_types_in_expr(right, parameter_types);
                }
                _ => {}
            }
        }
        Expr::UnaryOp { expr, .. } => {
            infer_types_in_expr(expr, parameter_types);
        }
        Expr::InList { expr, list, .. } => {
            infer_types_in_expr(expr, parameter_types);
            for item in list {
                infer_types_in_expr(item, parameter_types);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            infer_types_in_expr(expr, parameter_types);
            infer_types_in_expr(low, parameter_types);
            infer_types_in_expr(high, parameter_types);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                infer_types_in_expr(op, parameter_types);
            }
            for cond in conditions {
                infer_types_in_expr(cond, parameter_types);
            }
            for res in results {
                infer_types_in_expr(res, parameter_types);
            }
            if let Some(else_res) = else_result {
                infer_types_in_expr(else_res, parameter_types);
            }
        }
        Expr::Nested(inner) => {
            infer_types_in_expr(inner, parameter_types);
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            infer_types_in_expr(inner, parameter_types);
        }
        Expr::Like { expr, pattern, .. } => {
            // For LIKE expressions, both sides should be text
            infer_types_in_expr(expr, parameter_types);

            // If the pattern is a parameter, mark it as text
            if let Expr::Value(SqlValue::Placeholder(s)) = &**pattern {
                if let Some(num_str) = s.strip_prefix('$') {
                    if let Ok(param_num) = num_str.parse::<usize>() {
                        parameter_types.insert(param_num, SqlType::Text);
                    }
                }
            } else {
                infer_types_in_expr(pattern, parameter_types);
            }
        }
        _ => {}
    }
}

fn infer_type_from_expr(expr: &Expr) -> Option<SqlType> {
    match expr {
        Expr::Identifier(ident) => {
            // Try to infer type from column name
            match ident.value.to_lowercase().as_str() {
                "age" | "id" | "count" | "quantity" | "value" => Some(SqlType::Integer),
                "price" | "amount" | "total" => Some(SqlType::Double),
                "active" | "enabled" | "deleted" | "is_active" | "in_stock" => {
                    Some(SqlType::Boolean)
                }
                "name" | "username" | "email" | "description" | "status" | "customer_name" => {
                    Some(SqlType::Text)
                }
                "created_at" | "updated_at" => Some(SqlType::Timestamp),
                "created_date" => Some(SqlType::Date),
                _ => None,
            }
        }
        Expr::Value(SqlValue::Boolean(_)) => Some(SqlType::Boolean),
        Expr::Value(SqlValue::Number(_, _)) => Some(SqlType::Integer),
        Expr::Value(SqlValue::SingleQuotedString(_)) => Some(SqlType::Text),
        _ => None,
    }
}

fn infer_types_in_projection_expr(
    expr: &Expr,
    parameter_types: &mut std::collections::HashMap<usize, SqlType>,
) {
    match expr {
        Expr::Function(func) => {
            // Check function arguments for parameters
            if let FunctionArguments::List(args) = &func.args {
                for arg in &args.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = arg {
                        infer_types_in_expr(arg_expr, parameter_types);
                    }
                }
            }
        }
        _ => {
            // For non-function expressions in projection, just use regular inference
            infer_types_in_expr(expr, parameter_types);
        }
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
        SqlType::BigInt => {
            if data.len() == 8 {
                let val = i64::from_be_bytes(data.try_into().unwrap());
                Ok(Value::Integer(val))
            } else {
                Err(YamlBaseError::Protocol("Invalid bigint size".to_string()))
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
