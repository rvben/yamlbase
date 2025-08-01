use yamlbase::database::Value;
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::database::{Storage, Database, Table, Column};
use yamlbase::yaml::schema::SqlType;
use std::sync::Arc;

#[tokio::test]
async fn test_subquery_gap_analysis() {
    // Create minimal test setup
    let mut db = Database::new("test_db".to_string());
    
    // Create simple test table
    let mut test_table = Table::new(
        "test".to_string(),
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
                name: "value".to_string(),
                sql_type: SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );
    
    test_table.insert_row(vec![Value::Integer(1), Value::Integer(10)]).unwrap();
    test_table.insert_row(vec![Value::Integer(2), Value::Integer(20)]).unwrap();
    test_table.insert_row(vec![Value::Integer(3), Value::Integer(30)]).unwrap();
    
    db.add_table(test_table).unwrap();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    println!("=== SUBQUERY GAP ANALYSIS ===");
    
    // Test 1: EXISTS subquery - should fail with NotImplemented
    println!("\n1. Testing EXISTS subquery:");
    let stmts = parse_sql("SELECT * FROM test WHERE EXISTS (SELECT 1 FROM test WHERE id = 1)").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => println!("   ✅ EXISTS works! Got {} rows", result.rows.len()),
        Err(e) => println!("   ❌ EXISTS failed: {}", e),
    }
    
    // Test 2: IN subquery - should fail with NotImplemented  
    println!("\n2. Testing IN subquery:");
    let stmts = parse_sql("SELECT * FROM test WHERE id IN (SELECT id FROM test WHERE value > 15)").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => println!("   ✅ IN subquery works! Got {} rows", result.rows.len()),
        Err(e) => println!("   ❌ IN subquery failed: {}", e),
    }
    
    // Test 3: Scalar subquery in SELECT - should fail with NotImplemented
    println!("\n3. Testing scalar subquery in SELECT:");
    let stmts = parse_sql("SELECT id, (SELECT MAX(value) FROM test) as max_val FROM test WHERE id = 1").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => println!("   ✅ Scalar subquery works! Got {} rows", result.rows.len()),
        Err(e) => println!("   ❌ Scalar subquery failed: {}", e),
    }
    
    // Test 4: Scalar subquery in WHERE - should fail with NotImplemented
    println!("\n4. Testing scalar subquery in WHERE:");
    let stmts = parse_sql("SELECT * FROM test WHERE value > (SELECT AVG(value) FROM test)").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => println!("   ✅ Scalar WHERE subquery works! Got {} rows", result.rows.len()),
        Err(e) => println!("   ❌ Scalar WHERE subquery failed: {}", e),
    }
    
    // Test 5: NOT EXISTS - should fail with NotImplemented
    println!("\n5. Testing NOT EXISTS:");
    let stmts = parse_sql("SELECT * FROM test WHERE NOT EXISTS (SELECT 1 FROM test WHERE id = 999)").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => println!("   ✅ NOT EXISTS works! Got {} rows", result.rows.len()),
        Err(e) => println!("   ❌ NOT EXISTS failed: {}", e),
    }

    // Control test: Basic query should work
    println!("\n6. Control test - basic query:");
    let stmts = parse_sql("SELECT * FROM test WHERE id = 1").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => println!("   ✅ Basic query works! Got {} rows", result.rows.len()),
        Err(e) => println!("   ❌ Basic query failed: {}", e),
    }
    
    println!("\n=== GAP ANALYSIS COMPLETE ===");
    println!("This test documents the current subquery support gaps.");
    println!("All subquery tests should show NotImplemented errors until implemented.");
}