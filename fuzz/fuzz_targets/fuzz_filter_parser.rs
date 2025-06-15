#![no_main]
use libfuzzer_sys::fuzz_target;
use yamlbase::sql::executor::QueryExecutor;
use yamlbase::yaml::{Database, Table, Column};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use tokio::sync::RwLock;

fuzz_target!(|data: &[u8]| {
    // Only fuzz valid UTF-8 strings
    if let Ok(filter_str) = std::str::from_utf8(data) {
        // Create a test table for context
        let table = Table {
            name: "test".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    primary_key: Some(true),
                    nullable: Some(false),
                },
                Column {
                    name: "name".to_string(),
                    data_type: "VARCHAR(255)".to_string(),
                    primary_key: None,
                    nullable: Some(true),
                },
                Column {
                    name: "age".to_string(),
                    data_type: "INTEGER".to_string(),
                    primary_key: None,
                    nullable: Some(true),
                },
            ],
            data: vec![
                vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
                vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
            ],
        };
        
        let db = Database {
            tables: vec![table.clone()],
        };
        
        let db_arc = Arc::new(RwLock::new(db));
        let executor = QueryExecutor::new(db_arc);
        
        // Try to parse as a WHERE clause expression
        let query = format!("SELECT * FROM test WHERE {}", filter_str);
        
        // First try to parse with sqlparser
        let dialect = GenericDialect {};
        if let Ok(ast) = Parser::parse_sql(&dialect, &query) {
            // If it parses, try to execute it
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            
            runtime.block_on(async {
                // We don't care about the result, just that it doesn't panic
                let _ = executor.execute(&query).await;
            });
        }
    }
});