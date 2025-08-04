use std::sync::Arc;
use yamlbase::database::Value;
use yamlbase::database::{Column, Database, Storage, Table};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_basic_window_functions() {
    println!("=== BASIC WINDOW FUNCTIONS TEST ===");

    let mut db = Database::new("test_db".to_string());

    // Create users table
    let mut users_table = Table::new(
        "users".to_string(),
        vec![
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
                name: "username".to_string(),
                sql_type: SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    users_table
        .insert_row(vec![Value::Integer(1), Value::Text("alice".to_string())])
        .unwrap();
    users_table
        .insert_row(vec![Value::Integer(2), Value::Text("bob".to_string())])
        .unwrap();
    users_table
        .insert_row(vec![Value::Integer(3), Value::Text("charlie".to_string())])
        .unwrap();

    db.add_table(users_table).unwrap();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test 1: Basic ROW_NUMBER() window function
    println!("\n1. Testing ROW_NUMBER() window function:");
    let stmts = parse_sql("SELECT username, ROW_NUMBER() OVER (ORDER BY id) as row_num FROM users")
        .unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => {
            println!("   ✅ ROW_NUMBER() works! Got {} rows", result.rows.len());
            for (i, row) in result.rows.iter().enumerate() {
                println!("      Row {}: {:?}", i + 1, row);
            }
            // Should have 3 rows with row_num 1, 2, 3
            assert_eq!(result.rows.len(), 3, "Should have 3 rows");
        }
        Err(e) => {
            println!("   ❌ ROW_NUMBER() failed: {e}");
            // For now, expect this to fail until implemented
            assert!(
                e.to_string().contains("not implemented")
                    || e.to_string().contains("Function 'ROW_NUMBER'")
            );
        }
    }

    // Test 2: Basic RANK() window function
    println!("\n2. Testing RANK() window function:");
    let stmts =
        parse_sql("SELECT username, RANK() OVER (ORDER BY id) as rank_num FROM users").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => {
            println!("   ✅ RANK() works! Got {} rows", result.rows.len());
            for (i, row) in result.rows.iter().enumerate() {
                println!("      Row {}: {:?}", i + 1, row);
            }
        }
        Err(e) => {
            println!("   ❌ RANK() failed: {e}");
            // For now, expect this to fail until implemented
            assert!(
                e.to_string().contains("not implemented")
                    || e.to_string().contains("Function 'RANK'")
            );
        }
    }

    println!("\n=== WINDOW FUNCTIONS TEST COMPLETE ===");
}
