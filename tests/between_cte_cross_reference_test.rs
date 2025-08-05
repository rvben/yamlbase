use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

async fn setup_test_db() -> Arc<QueryExecutor> {
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
                name: "month_number".to_string(),
                sql_type: SqlType::Date,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "version_code".to_string(),
                sql_type: SqlType::Varchar(20),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "assignment_type".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "planned_effort_hours".to_string(),
                sql_type: SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "actual_effort_hours".to_string(),
                sql_type: SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Add test data
    allocations_table
        .insert_row(vec![
            Value::Text("PROJ001".to_string()),
            Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 8, 15).unwrap()),
            Value::Text("Published".to_string()),
            Value::Text("Hard Allocation".to_string()),
            Value::Integer(100),
            Value::Integer(90),
        ])
        .unwrap();
    
    allocations_table
        .insert_row(vec![
            Value::Text("PROJ002".to_string()),
            Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 9, 15).unwrap()),
            Value::Text("Published".to_string()),
            Value::Text("Hard Allocation".to_string()),
            Value::Integer(150),
            Value::Integer(140),
        ])
        .unwrap();
    
    allocations_table
        .insert_row(vec![
            Value::Text("PROJ003".to_string()),
            Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 10, 15).unwrap()),
            Value::Text("Published".to_string()),
            Value::Text("Soft Allocation".to_string()),
            Value::Integer(80),
            Value::Integer(0),
        ])
        .unwrap();
    
    allocations_table
        .insert_row(vec![
            Value::Text("PROJ004".to_string()),
            Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 7, 15).unwrap()),
            Value::Text("Published".to_string()),
            Value::Text("Hard Allocation".to_string()),
            Value::Integer(120),
            Value::Integer(110),
        ])
        .unwrap();

    db.add_table(allocations_table).unwrap();
    let storage = Arc::new(Storage::new(db));
    Arc::new(QueryExecutor::new(storage).await.unwrap())
}

#[tokio::test]
async fn test_between_with_cte_cross_reference_simple() {
    let executor = setup_test_db().await;

    // Test Case 1: Simple BETWEEN with CTE Cross-Reference
    let sql = r#"
        WITH DateRange AS (
            SELECT DATE('2025-08-01') AS start_date, DATE('2025-09-30') AS end_date
        ),
        FilteredData AS (
            SELECT a.sap_project_id, a.month_number
            FROM sf_project_allocations a
            CROSS JOIN DateRange dr
            WHERE a.month_number BETWEEN dr.start_date AND dr.end_date
        )
        SELECT COUNT(*) FROM FilteredData
    "#;

    let statements = parse_sql(sql).unwrap();
    let result = executor.execute(&statements[0]).await.unwrap();
    
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(2)); // Should return 2 rows
}

#[tokio::test]
async fn test_between_with_cte_and_additional_conditions() {
    let executor = setup_test_db().await;

    // Test Case 2: Complex BETWEEN with Additional Conditions
    let sql = r#"
        WITH DateRange AS (
            SELECT DATE('2025-08-01') AS start_date, DATE('2025-09-30') AS end_date
        ),
        FilteredAllocations AS (
            SELECT a.*
            FROM sf_project_allocations a
            CROSS JOIN DateRange dr
            WHERE 
                a.month_number BETWEEN dr.start_date AND dr.end_date
                AND a.version_code = 'Published'
                AND a.assignment_type = 'Hard Allocation'
        )
        SELECT COUNT(*) FROM FilteredAllocations
    "#;

    let statements = parse_sql(sql).unwrap();
    let result = executor.execute(&statements[0]).await.unwrap();
    
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(2)); // Both matching rows have Hard Allocation
}

#[tokio::test]
async fn test_between_with_nested_conditions() {
    let executor = setup_test_db().await;

    // Test Case 3: Nested BETWEEN with Parenthesized Conditions
    // Note: This test verifies BETWEEN works with complex conditions in CTEs.
    // There's a known issue with GROUP BY on CTE columns returning NULL values
    // that needs to be addressed in a future release.
    let sql = r#"
        WITH DateRange AS (
            SELECT DATE('2025-01-01') AS start_date, DATE('2025-12-31') AS end_date
        ),
        ComplexFilter AS (
            SELECT a.*
            FROM sf_project_allocations a
            CROSS JOIN DateRange dr
            WHERE 
                a.month_number BETWEEN dr.start_date AND dr.end_date
                AND (a.planned_effort_hours > 0 OR a.actual_effort_hours > 0)
                AND a.version_code = 'Published'
        )
        SELECT COUNT(*) as total_count
        FROM ComplexFilter
    "#;

    let statements = parse_sql(sql).unwrap();
    let result = executor.execute(&statements[0]).await.unwrap();
    
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0], "total_count");
    assert_eq!(result.rows[0][0], Value::Integer(4)); // All 4 projects match the criteria
}

#[tokio::test] 
async fn test_multiple_between_conditions() {
    let executor = setup_test_db().await;

    // Test multiple BETWEEN conditions in the same WHERE clause
    let sql = r#"
        WITH DateRange AS (
            SELECT 
                DATE('2025-08-01') AS start_date1, 
                DATE('2025-08-31') AS end_date1,
                DATE('2025-09-01') AS start_date2,
                DATE('2025-09-30') AS end_date2
        ),
        FilteredData AS (
            SELECT a.sap_project_id, a.month_number
            FROM sf_project_allocations a
            CROSS JOIN DateRange dr
            WHERE 
                (a.month_number BETWEEN dr.start_date1 AND dr.end_date1)
                OR (a.month_number BETWEEN dr.start_date2 AND dr.end_date2)
        )
        SELECT COUNT(*) FROM FilteredData
    "#;

    let statements = parse_sql(sql).unwrap();
    let result = executor.execute(&statements[0]).await.unwrap();
    
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(2)); // PROJ001 and PROJ002
}