use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_not_in_cte_simple() {
    println!("=== NOT IN CTE TEST ===");

    // Create test database
    let mut db = Database::new("test_db".to_string());

    // Create SF_PROJECT_ALLOCATIONS table
    let mut allocations_table = Table::new(
        "sf_project_allocations".to_string(),
        vec![
            Column {
                name: "sap_project_id".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "project_status_code".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "wbi_id".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "planned_effort_hours".to_string(),
                sql_type: SqlType::Decimal(10, 2),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Add data
    allocations_table.insert_row(vec![
        Value::Text("123001".to_string()),
        Value::Text("Active".to_string()),
        Value::Text("emp001".to_string()),
        Value::Decimal(rust_decimal::Decimal::new(400, 1)), // 40.0
    ]).unwrap();
    allocations_table.insert_row(vec![
        Value::Text("123002".to_string()),
        Value::Text("Cancelled".to_string()),
        Value::Text("emp002".to_string()),
        Value::Decimal(rust_decimal::Decimal::new(200, 1)), // 20.0
    ]).unwrap();
    allocations_table.insert_row(vec![
        Value::Text("123003".to_string()),
        Value::Text("Closed".to_string()),
        Value::Text("emp003".to_string()),
        Value::Decimal(rust_decimal::Decimal::new(300, 1)), // 30.0
    ]).unwrap();
    allocations_table.insert_row(vec![
        Value::Text("123004".to_string()),
        Value::Text("In Progress".to_string()),
        Value::Text("emp004".to_string()),
        Value::Decimal(rust_decimal::Decimal::new(500, 1)), // 50.0
    ]).unwrap();

    db.add_table(allocations_table).unwrap();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test 1: Simple NOT IN
    println!("\n1. Testing NOT IN with CTE:");
    let query = r#"
        WITH FilteredAllocations AS (
            SELECT a.SAP_PROJECT_ID, a.PROJECT_STATUS_CODE
            FROM SF_PROJECT_ALLOCATIONS a
            WHERE a.PROJECT_STATUS_CODE NOT IN ('Cancelled', 'Closed')
        )
        SELECT COUNT(*) AS count FROM FilteredAllocations
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ NOT IN in CTE works!");
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], Value::Integer(2)); // Active and In Progress
            println!("   Count of non-cancelled/closed allocations: {:?}", result.rows[0][0]);
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 2: NOT IN combined with AND conditions
    println!("\n2. Testing NOT IN with AND conditions:");
    let query = r#"
        WITH FilteredAllocations AS (
            SELECT a.SAP_PROJECT_ID, a.PLANNED_EFFORT_HOURS
            FROM SF_PROJECT_ALLOCATIONS a
            WHERE a.PROJECT_STATUS_CODE NOT IN ('Cancelled', 'Closed')
                AND a.PLANNED_EFFORT_HOURS > 30
        )
        SELECT COUNT(*) AS count FROM FilteredAllocations
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ NOT IN with AND conditions works!");
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], Value::Integer(2)); // 123001 (40.0) and 123004 (50.0)
            println!("   Count of active allocations > 30 hours: {:?}", result.rows[0][0]);
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 3: NOT IN with multiple values in list
    println!("\n3. Testing NOT IN with multiple values:");
    let query = r#"
        WITH FilteredAllocations AS (
            SELECT a.SAP_PROJECT_ID
            FROM SF_PROJECT_ALLOCATIONS a
            WHERE a.PROJECT_STATUS_CODE NOT IN ('Active', 'Cancelled', 'Closed')
        )
        SELECT COUNT(*) AS count FROM FilteredAllocations
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ NOT IN with multiple values works!");
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], Value::Integer(1)); // Only "In Progress"
            println!("   Count of 'In Progress' allocations: {:?}", result.rows[0][0]);
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 4: NOT IN used in an expression context (returns boolean)
    println!("\n4. Testing NOT IN as boolean expression:");
    let query = r#"
        WITH FilteredAllocations AS (
            SELECT 
                a.SAP_PROJECT_ID,
                a.PROJECT_STATUS_CODE NOT IN ('Cancelled', 'Closed') AS is_active
            FROM SF_PROJECT_ALLOCATIONS a
        )
        SELECT SAP_PROJECT_ID, is_active FROM FilteredAllocations ORDER BY SAP_PROJECT_ID
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ NOT IN as boolean expression works!");
            assert_eq!(result.rows.len(), 4);
            
            // Check results
            assert_eq!(result.rows[0][1], Value::Boolean(true));  // 123001 - Active
            assert_eq!(result.rows[1][1], Value::Boolean(false)); // 123002 - Cancelled
            assert_eq!(result.rows[2][1], Value::Boolean(false)); // 123003 - Closed
            assert_eq!(result.rows[3][1], Value::Boolean(true));  // 123004 - In Progress
            
            println!("   Boolean results for each project:");
            for row in &result.rows {
                println!("     {} is_active: {:?}", row[0], row[1]);
            }
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    println!("\n✅ All NOT IN CTE tests passed!");
}