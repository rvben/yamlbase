use std::time::Instant;
use yamlbase::Database;
use yamlbase::database::{Table, Column, Value, Storage};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_primary_key_index_performance() {
    // Create a database with many rows
    let mut db = Database::new("test_db".to_string());
    
    let columns = vec![
        Column {
            name: "id".to_string(),
            sql_type: SqlType::Integer,
            primary_key: true,
            nullable: false,
            unique: true,
            default: None,
            references: None,
        },
        Column {
            name: "data".to_string(),
            sql_type: SqlType::Text,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];
    
    let mut table = Table::new("test_table".to_string(), columns);
    
    // Insert many rows
    const NUM_ROWS: i64 = 10000;
    for i in 1..=NUM_ROWS {
        let row = vec![
            Value::Integer(i),
            Value::Text(format!("Data for row {}", i)),
        ];
        table.insert_row(row).unwrap();
    }
    
    db.add_table(table).unwrap();
    
    // Create storage and executor
    use yamlbase::sql::{parse_sql, QueryExecutor};
    use std::sync::Arc;
    
    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone());
    
    // Wait for indexes to be built
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    // Test 1: Primary key lookup (should use index)
    let pk_query = "SELECT * FROM test_table WHERE id = 5000";
    let pk_statements = parse_sql(pk_query).unwrap();
    
    let start = Instant::now();
    let pk_result = executor.execute(&pk_statements[0]).await.unwrap();
    let pk_duration = start.elapsed();
    
    assert_eq!(pk_result.rows.len(), 1);
    assert_eq!(pk_result.rows[0][0], Value::Integer(5000));
    
    // Test 2: Non-indexed lookup (full table scan)
    let scan_query = "SELECT * FROM test_table WHERE data = 'Data for row 5000'";
    let scan_statements = parse_sql(scan_query).unwrap();
    
    let start = Instant::now();
    let scan_result = executor.execute(&scan_statements[0]).await.unwrap();
    let scan_duration = start.elapsed();
    
    assert_eq!(scan_result.rows.len(), 1);
    
    // The indexed lookup should be significantly faster
    println!("Primary key lookup: {:?}", pk_duration);
    println!("Full table scan: {:?}", scan_duration);
    
    // The index lookup should be significantly faster
    // In debug mode, we're seeing about 5-6x improvement, in release it should be much more
    assert!(
        pk_duration.as_micros() * 3 < scan_duration.as_micros(),
        "Index lookup should be much faster than table scan. PK: {:?}, Scan: {:?}",
        pk_duration,
        scan_duration
    );
}

#[tokio::test]
async fn test_index_with_different_types() {
    let mut db = Database::new("test_db".to_string());
    
    // Test with string primary key
    let columns = vec![
        Column {
            name: "username".to_string(),
            sql_type: SqlType::Varchar(50),
            primary_key: true,
            nullable: false,
            unique: true,
            default: None,
            references: None,
        },
        Column {
            name: "email".to_string(),
            sql_type: SqlType::Text,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];
    
    let mut table = Table::new("users".to_string(), columns);
    
    // Insert test data
    for i in 1..=100 {
        let row = vec![
            Value::Text(format!("user{}", i)),
            Value::Text(format!("user{}@example.com", i)),
        ];
        table.insert_row(row).unwrap();
    }
    
    db.add_table(table).unwrap();
    
    use yamlbase::sql::{parse_sql, QueryExecutor};
    use std::sync::Arc;
    
    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone());
    
    // Wait for indexes
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    // Test string primary key lookup
    let query = "SELECT * FROM users WHERE username = 'user50'";
    let statements = parse_sql(query).unwrap();
    
    let result = executor.execute(&statements[0]).await.unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Text("user50".to_string()));
}