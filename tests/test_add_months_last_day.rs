use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_add_months_and_last_day() {
    println!("=== ADD_MONTHS AND LAST_DAY TEST ===");

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

    // Test 1: ADD_MONTHS basic
    println!("\n1. Testing ADD_MONTHS(CURRENT_DATE, 3):");
    let stmt = parse_sql("SELECT ADD_MONTHS(CURRENT_DATE, 3) as three_months_later").unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ ADD_MONTHS works!");
            println!("   Result: {:?}", result.rows[0][0]);
        }
        Err(e) => {
            println!("   ❌ Failed with: {e}");
        }
    }

    // Test 2: ADD_MONTHS negative
    println!("\n2. Testing ADD_MONTHS(CURRENT_DATE, -2):");
    let stmt = parse_sql("SELECT ADD_MONTHS(CURRENT_DATE, -2) as two_months_ago").unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ ADD_MONTHS with negative works!");
            println!("   Result: {:?}", result.rows[0][0]);
        }
        Err(e) => {
            println!("   ❌ Failed with: {e}");
        }
    }

    // Test 3: LAST_DAY
    println!("\n3. Testing LAST_DAY(CURRENT_DATE):");
    let stmt = parse_sql("SELECT LAST_DAY(CURRENT_DATE) as month_end").unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ LAST_DAY works!");
            println!("   Result: {:?}", result.rows[0][0]);
        }
        Err(e) => {
            println!("   ❌ Failed with: {e}");
        }
    }

    // Test 4: Customer's production query (full version)
    println!("\n4. Testing customer's full production query:");
    let stmt = parse_sql(
        r#"
        WITH DateRange AS (
            SELECT
                ADD_MONTHS(CURRENT_DATE, 0) - EXTRACT(DAY FROM CURRENT_DATE) + 1 AS START_DATE,
                LAST_DAY(ADD_MONTHS(CURRENT_DATE, 1)) AS END_DATE
        )
        SELECT * FROM DateRange
    "#,
    )
    .unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ Production query works!");
            println!("   Columns: {:?}", result.columns);
            println!("   Result: {:?}", result.rows[0]);
        }
        Err(e) => {
            println!("   ❌ Failed with: {e}");
        }
    }

    // Test 5: Date arithmetic with ADD_MONTHS result
    println!("\n5. Testing date arithmetic with ADD_MONTHS:");
    let stmt = parse_sql("SELECT ADD_MONTHS(CURRENT_DATE, 1) - 5 as complex_date").unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ Date arithmetic with ADD_MONTHS works!");
            println!("   Result: {:?}", result.rows[0][0]);
        }
        Err(e) => {
            println!("   ❌ Failed with: {e}");
        }
    }

    println!("\n=== ADD_MONTHS AND LAST_DAY TEST COMPLETE ===");
}
