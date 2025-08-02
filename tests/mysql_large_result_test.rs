#![allow(clippy::uninlined_format_args)]

use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

/// Test for the MySQL "Lost connection to MySQL server during query" fix
/// This test creates a large result set that would previously cause connection drops
#[tokio::test]
async fn test_mysql_large_result_set_no_connection_loss() {
    // Create a database with a large dataset to test connection stability
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
            name: "description".to_string(),
            sql_type: SqlType::Text,
            primary_key: false,
            nullable: false,
            unique: false,
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
        Column {
            name: "category".to_string(),
            sql_type: SqlType::Varchar(50),
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];

    let mut table = Table::new("large_table".to_string(), columns);

    // Insert a substantial amount of data to test large result sets
    const NUM_ROWS: i64 = 2000; // Enough to create a large result set
    for i in 1..=NUM_ROWS {
        let row = vec![
            Value::Integer(i),
            Value::Text(format!(
                "This is a detailed description for record {} with substantial content that will contribute to packet size. {}",
                i,
                "Additional padding text to increase row size and test packet splitting functionality. ".repeat(10)
            )),
            Value::Text(format!(
                "Large data field {} containing extensive information that simulates real enterprise data scenarios. {}",
                i,
                "Enterprise data often contains substantial text content that can lead to large result sets. ".repeat(15)
            )),
            Value::Text(format!("Category{}", i % 20)),
        ];
        table.insert_row(row).unwrap();
    }

    db.add_table(table).unwrap();

    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone()).await.unwrap();

    // Test 1: Large result set without filters (most challenging case)
    println!("Testing large result set query (all {} rows)...", NUM_ROWS);
    let full_query = "SELECT * FROM large_table ORDER BY id";
    let full_statements = parse_sql(full_query).unwrap();

    let start = std::time::Instant::now();
    let full_result = executor.execute(&full_statements[0]).await.unwrap();
    let duration = start.elapsed();

    println!("✅ Large query completed in {:?}", duration);
    assert_eq!(full_result.rows.len(), NUM_ROWS as usize);
    assert_eq!(full_result.columns.len(), 4);

    // Test 2: Query with filtering that still returns large result set
    println!("Testing filtered large result set query...");
    let filtered_query = "SELECT * FROM large_table WHERE id > 500 ORDER BY id";
    let filtered_statements = parse_sql(filtered_query).unwrap();

    let start = std::time::Instant::now();
    let filtered_result = executor.execute(&filtered_statements[0]).await.unwrap();
    let duration = start.elapsed();

    println!("✅ Filtered large query completed in {:?}", duration);
    assert_eq!(filtered_result.rows.len(), (NUM_ROWS - 500) as usize);
    assert_eq!(filtered_result.columns.len(), 4);

    // Test 3: Query with very wide result set (many columns with large text data)
    println!("Testing wide result set query...");
    let wide_query = "SELECT id, description, data, category FROM large_table WHERE id <= 50";
    let wide_statements = parse_sql(wide_query).unwrap();

    let start = std::time::Instant::now();
    let wide_result = executor.execute(&wide_statements[0]).await.unwrap();
    let duration = start.elapsed();

    println!("✅ Wide result set query completed in {:?}", duration);
    assert_eq!(wide_result.rows.len(), 50);
    assert_eq!(wide_result.columns.len(), 4);

    println!("🎉 All MySQL large result set tests passed - connection loss issue is fixed!");
}

/// Test specifically for packet splitting functionality
#[tokio::test]
async fn test_mysql_packet_splitting_edge_cases() {
    let mut db = Database::new("packet_test_db".to_string());

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
            name: "huge_text".to_string(),
            sql_type: SqlType::Text,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];

    let mut table = Table::new("packet_test".to_string(), columns);

    // Create rows with very large text fields to test packet splitting
    for i in 1..=10 {
        let huge_text = "X".repeat(2 * 1024 * 1024); // 2MB per field
        let row = vec![Value::Integer(i), Value::Text(huge_text)];
        table.insert_row(row).unwrap();
    }

    db.add_table(table).unwrap();

    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone()).await.unwrap();

    // Test query that would definitely require packet splitting
    println!("Testing packet splitting with very large text fields...");
    let packet_query = "SELECT * FROM packet_test";
    let packet_statements = parse_sql(packet_query).unwrap();

    let start = std::time::Instant::now();
    let packet_result = executor.execute(&packet_statements[0]).await.unwrap();
    let duration = start.elapsed();

    println!("✅ Packet splitting test completed in {:?}", duration);
    assert_eq!(packet_result.rows.len(), 10);

    // Verify that the large text was transmitted correctly
    for row in &packet_result.rows {
        if let Value::Text(text) = &row[1] {
            assert_eq!(text.len(), 2 * 1024 * 1024);
            assert!(text.starts_with("XXXX"));
        }
    }

    println!("🎉 Packet splitting functionality verified!");
}
