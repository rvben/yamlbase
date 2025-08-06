use std::sync::Arc;
use anyhow::bail;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::database::{Storage, Value};
use crate::sql::QueryExecutor;

use super::auth::TeradataAuth;
use super::parcels::{Message, Parcel, ParcelKind};
use super::sql_translator::TeradataSqlTranslator;

pub struct TeradataProtocol {
    _config: Arc<Config>,
    storage: Arc<Storage>,
    auth: TeradataAuth,
    translator: TeradataSqlTranslator,
    authenticated: bool,
}

impl TeradataProtocol {
    pub async fn new(config: Arc<Config>, storage: Arc<Storage>) -> crate::Result<Self> {
        let auth = TeradataAuth::new(
            config.username.clone(),
            config.password.clone(),
        );
        
        Ok(Self {
            _config: config,
            storage,
            auth,
            translator: TeradataSqlTranslator::new(),
            authenticated: false,
        })
    }
    
    pub async fn handle_connection(&mut self, mut stream: TcpStream) -> crate::Result<()> {
        info!("New Teradata connection");
        
        let mut buffer = BytesMut::with_capacity(4096);
        
        loop {
            // Read data from stream
            let n = stream.read_buf(&mut buffer).await?;
            if n == 0 {
                info!("Client disconnected");
                break;
            }
            
            // Try to decode a message
            match Message::decode(&mut buffer) {
                Ok(Some(message)) => {
                    match self.handle_message(message).await {
                        Ok(response) => {
                            let encoded = response.encode();
                            stream.write_all(&encoded).await
                                .map_err(crate::YamlBaseError::Io)?;
                        }
                        Err(e) => {
                            error!("Failed to handle message: {}", e);
                            let error_response = Message::single(
                                Parcel::error_parcel(5001, &format!("Message handling error: {}", e))
                            );
                            stream.write_all(&error_response.encode()).await
                                .map_err(crate::YamlBaseError::Io)?;
                        }
                    }
                }
                Ok(None) => {
                    // Not enough data yet, continue reading
                    continue;
                }
                Err(e) => {
                    error!("Failed to decode message: {}", e);
                    let error_response = Message::single(
                        Parcel::error_parcel(5000, &format!("Protocol error: {}", e))
                    );
                    stream.write_all(&error_response.encode()).await
                        .map_err(crate::YamlBaseError::Io)?;
                }
            }
        }
        
        Ok(())
    }
    
    async fn handle_message(&mut self, message: Message) -> anyhow::Result<Message> {
        let mut response_parcels = Vec::new();
        
        for parcel in message.parcels {
            debug!("Handling parcel: {:?}", parcel.kind);
            
            match parcel.kind {
                ParcelKind::LogonRequest => {
                    let response = self.auth.handle_logon(&parcel)?;
                    self.authenticated = response.parcels.iter()
                        .any(|p| p.kind == ParcelKind::AuthenticationOk);
                    return Ok(response);
                }
                
                ParcelKind::LogoffRequest => {
                    self.authenticated = false;
                    return Ok(self.auth.handle_logoff());
                }
                
                ParcelKind::RunRequest => {
                    if !self.authenticated {
                        return Ok(Message::single(
                            Parcel::error_parcel(4001, "Not authenticated")
                        ));
                    }
                    
                    let sql = self.parse_run_request(&parcel)?;
                    let parcels = self.execute_query(&sql).await?;
                    response_parcels.extend(parcels);
                }
                
                _ => {
                    warn!("Unhandled parcel kind: {:?}", parcel.kind);
                    response_parcels.push(
                        Parcel::error_parcel(6000, &format!("Unsupported parcel type: {:?}", parcel.kind))
                    );
                }
            }
        }
        
        if response_parcels.is_empty() {
            response_parcels.push(Parcel::success_parcel(0));
        }
        
        response_parcels.push(Parcel::end_statement_parcel());
        response_parcels.push(Parcel::end_request_parcel());
        
        Ok(Message::new(response_parcels))
    }
    
    fn parse_run_request(&self, parcel: &Parcel) -> anyhow::Result<String> {
        let data = &parcel.data;
        
        if data.len() < 8 {
            bail!("Invalid run request");
        }
        
        // Skip statement info (4 bytes)
        let sql_length = u32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;
        
        if data.len() < 8 + sql_length {
            bail!("Invalid SQL length");
        }
        
        let sql = String::from_utf8_lossy(&data[8..8 + sql_length]).to_string();
        Ok(sql)
    }
    
    async fn execute_query(&self, sql: &str) -> anyhow::Result<Vec<Parcel>> {
        info!("Executing Teradata query: {}", sql);
        
        // Check if it's a system query
        if self.translator.is_teradata_system_query(sql) {
            if let Some(translated) = self.translator.handle_system_query(sql) {
                return self.execute_translated_query(&translated).await;
            }
        }
        
        // Translate Teradata SQL to PostgreSQL SQL
        let translated_sql = self.translator.translate(sql);
        debug!("Translated SQL: {}", translated_sql);
        
        self.execute_translated_query(&translated_sql).await
    }
    
    async fn execute_translated_query(&self, sql: &str) -> anyhow::Result<Vec<Parcel>> {
        use crate::sql::parser::parse_sql;
        use bytes::BufMut;
        
        let mut parcels = Vec::new();
        
        // Parse the SQL statement
        let statements = parse_sql(sql).map_err(|e| anyhow::anyhow!("SQL parse error: {}", e))?;
        
        if statements.is_empty() {
            return Ok(vec![Parcel::error_parcel(8000, "No SQL statement found")]);
        }
        
        // Execute the first statement (Teradata typically processes one statement at a time)
        let statement = &statements[0];
        
        // Execute the query using the QueryExecutor
        let executor = QueryExecutor::new(self.storage.clone()).await
            .map_err(|e| anyhow::anyhow!("Failed to create executor: {}", e))?;
        
        match executor.execute(statement).await {
            Ok(result) => {
                // Send data info parcel with column metadata
                let column_info: Vec<(String, String)> = result.columns.iter()
                    .map(|c| (c.clone(), "VARCHAR".to_string()))
                    .collect();
                parcels.push(Parcel::data_info_parcel(
                    result.columns.len() as u16,
                    &column_info
                ));
                
                // Send record parcels for each row
                let row_count = result.rows.len();
                for row in result.rows {
                    let mut row_data = BytesMut::new();
                    for value in row {
                        let value_str = match value {
                            Value::Null => None,
                            Value::Text(s) => Some(s),
                            Value::Integer(i) => Some(i.to_string()),
                            Value::Float(f) => Some(f.to_string()),
                            Value::Double(d) => Some(d.to_string()),
                            Value::Decimal(dec) => Some(dec.to_string()),
                            Value::Boolean(b) => Some(b.to_string()),
                            Value::Date(d) => Some(d.to_string()),
                            Value::Time(t) => Some(t.to_string()),
                            Value::Timestamp(dt) => Some(dt.to_string()),
                            Value::Uuid(u) => Some(u.to_string()),
                            Value::Json(j) => Some(j.to_string()),
                        };
                        
                        match value_str {
                            Some(v) => {
                                row_data.put_u16(v.len() as u16);
                                row_data.put_slice(v.as_bytes());
                            }
                            None => {
                                row_data.put_u16(0xFFFF); // NULL indicator
                            }
                        }
                    }
                    parcels.push(Parcel::record_parcel(&row_data));
                }
                
                parcels.push(Parcel::success_parcel(row_count as u32));
            }
            Err(e) => {
                error!("Query execution error: {}", e);
                parcels.push(Parcel::error_parcel(7000, &e.to_string()));
            }
        }
        
        Ok(parcels)
    }
}