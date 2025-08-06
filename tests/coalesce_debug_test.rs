use chrono::NaiveDate;
use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_coalesce_diagnostics() -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple test database
    let mut db = Database::new("test_db".to_string());

    // Create a simple table
    let mut test_table = Table::new(
        "project_table".to_string(),
        vec![
            Column {
                name: "project_id".to_string(),
                sql_type: SqlType::Varchar(255),
                primary_key: true,
                nullable: false,
                unique: true,
                default: None,
                references: None,
            },
            Column {
                name: "project_name".to_string(),
                sql_type: SqlType::Varchar(255),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "status_code".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "end_date".to_string(),
                sql_type: SqlType::Date,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "parent_id".to_string(),
                sql_type: SqlType::Varchar(255),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Insert some test data
    test_table.insert_row(vec![
        Value::Text("P001".to_string()),
        Value::Text("Main Project".to_string()),
        Value::Text("Active".to_string()),
        Value::Date(NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()),
        Value::Null,
    ])?;

    test_table.insert_row(vec![
        Value::Text("P002".to_string()),
        Value::Text("Sub Project".to_string()),
        Value::Null, // NULL status
        Value::Null, // NULL end date
        Value::Text("P001".to_string()),
    ])?;

    db.add_table(test_table)?;
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await?;

    println!("Testing basic COALESCE in SELECT without CTE...");
    let query = parse_sql("SELECT COALESCE(NULL, 'test')").unwrap();
    match executor.execute(&query[0]).await {
        Ok(result) => println!("✅ Basic COALESCE: {:?}", result.rows),
        Err(e) => println!("❌ Basic COALESCE failed: {e}"),
    }

    println!("Testing COALESCE with table data...");
    let query = parse_sql("SELECT project_id, COALESCE(status_code, 'Default') FROM project_table")
        .unwrap();
    match executor.execute(&query[0]).await {
        Ok(result) => println!("✅ COALESCE with table: {:?}", result.rows),
        Err(e) => println!("❌ COALESCE with table failed: {e}"),
    }

    println!("Testing simple CTE...");
    let query = parse_sql(
        r#"
        WITH TestCTE AS (
            SELECT project_id, status_code FROM project_table
        )
        SELECT * FROM TestCTE
    "#,
    )
    .unwrap();
    match executor.execute(&query[0]).await {
        Ok(result) => println!("✅ Simple CTE: {} rows", result.rows.len()),
        Err(e) => println!("❌ Simple CTE failed: {e}"),
    }

    println!("Testing CTE with COALESCE in SELECT...");
    let query = parse_sql(
        r#"
        WITH TestCTE AS (
            SELECT 
                project_id,
                COALESCE(status_code, 'Default') as status
            FROM project_table
        )
        SELECT * FROM TestCTE
    "#,
    )
    .unwrap();
    match executor.execute(&query[0]).await {
        Ok(result) => println!("✅ CTE with COALESCE in SELECT: {} rows", result.rows.len()),
        Err(e) => println!("❌ CTE with COALESCE in SELECT failed: {e}"),
    }

    println!("Testing problematic CTE with JOIN and COALESCE...");
    let query = parse_sql(
        r#"
        WITH NullHandlingCTE AS (
            SELECT 
                p1.project_id,
                COALESCE(p2.project_name, 'No Parent') as parent_name
            FROM project_table p1
            LEFT JOIN project_table p2
                ON p1.parent_id = p2.project_id
                AND COALESCE(p2.status_code, 'Active') = 'Active'
        )
        SELECT * FROM NullHandlingCTE
    "#,
    )
    .unwrap();
    match executor.execute(&query[0]).await {
        Ok(result) => println!("✅ Problematic CTE: {} rows", result.rows.len()),
        Err(e) => println!("❌ Problematic CTE failed: {e}"),
    }

    Ok(())
}
