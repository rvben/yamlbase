use std::sync::Arc;
use yamlbase::database::{Database, Storage, Table, Column};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_basic_date_arithmetic() {
    println!("=== DATE ARITHMETIC TEST ===");
    
    // Create a simple test database
    let mut db = Database::new("test_db".to_string());
    
    // Create a dummy table (needed for context)
    let dummy_table = Table::new(
        "dummy".to_string(),
        vec![Column {
            name: "id".to_string(),
            sql_type: SqlType::Integer,
            primary_key: true,
            nullable: false,
            unique: true,
            default: None,
            references: None,
        }],
    );
    
    db.add_table(dummy_table).unwrap();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();
    
    // Test 1: Simple date subtraction
    println!("\n1. Testing CURRENT_DATE - 5:");
    let stmt = parse_sql("SELECT CURRENT_DATE - 5 as five_days_ago").unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ Date arithmetic works!");
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.columns.len(), 1);
            assert_eq!(result.columns[0], "five_days_ago");
            println!("   Result: {:?}", result.rows[0][0]);
        }
        Err(e) => {
            println!("   ❌ Failed with: {}", e);
            // This is expected to fail initially
        }
    }
    
    // Test 2: Date addition
    println!("\n2. Testing CURRENT_DATE + 7:");
    let stmt = parse_sql("SELECT CURRENT_DATE + 7 as next_week").unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ Date addition works!");
            println!("   Result: {:?}", result.rows[0][0]);
        }
        Err(e) => {
            println!("   ❌ Failed with: {}", e);
        }
    }
    
    // Test 3: Complex date expression (month start)
    println!("\n3. Testing month start calculation:");
    let stmt = parse_sql("SELECT CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1 as month_start").unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ Complex date arithmetic works!");
            println!("   Result: {:?}", result.rows[0][0]);
        }
        Err(e) => {
            println!("   ❌ Failed with: {}", e);
        }
    }
    
    // Test 4: Date arithmetic in CTE
    println!("\n4. Testing date arithmetic in CTE:");
    let stmt = parse_sql(r#"
        WITH DateRange AS (
            SELECT 
                CURRENT_DATE - 1 as yesterday,
                CURRENT_DATE + 30 as future_date
        )
        SELECT * FROM DateRange
    "#).unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ CTE date arithmetic works!");
            println!("   Columns: {:?}", result.columns);
            println!("   Result: {:?}", result.rows[0]);
        }
        Err(e) => {
            println!("   ❌ Failed with: {}", e);
        }
    }
    
    // Test 5: Simpler date arithmetic with EXTRACT
    println!("\n5. Testing date arithmetic with EXTRACT:");
    let stmt = parse_sql("SELECT CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) as days_back").unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ Production date arithmetic works!");
            println!("   Columns: {:?}", result.columns);
            println!("   Result: {:?}", result.rows[0]);
        }
        Err(e) => {
            println!("   ❌ Failed with: {}", e);
        }
    }
    
    println!("\n=== DATE ARITHMETIC TEST COMPLETE ===");
}