//! Enterprise SQL compatibility test suite
//!
//! Tests complex SQL patterns commonly found in enterprise applications
//! including project management systems, resource allocation systems,
//! and enterprise resource planning (ERP) systems.

use chrono::NaiveDate;
use rust_decimal::prelude::ToPrimitive;
use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

fn create_test_database() -> Database {
    let mut db = Database::new("enterprise_db".to_string());

    // Create PROJECT_MASTER table (similar to enterprise project management systems)
    let mut project_table = Table::new(
        "PROJECT_MASTER".to_string(),
        vec![
            Column {
                name: "PROJECT_ID".to_string(),
                sql_type: SqlType::Varchar(255),
                primary_key: true,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "PROJECT_NAME".to_string(),
                sql_type: SqlType::Varchar(255),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "DESCRIPTION".to_string(),
                sql_type: SqlType::Text,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "STATUS_CODE".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "ACTIVE_FLAG".to_string(),
                sql_type: SqlType::Varchar(1),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "PROJECT_STRUCTURE".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "START_DATE".to_string(),
                sql_type: SqlType::Date,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "MANAGER_ID".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "HIERARCHY_PARENT_ID".to_string(),
                sql_type: SqlType::Varchar(255),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Insert test data
    project_table
        .insert_row(vec![
            Value::Text("PRJ001".to_string()),
            Value::Text("Enterprise Platform Alpha".to_string()),
            Value::Text("Core platform development project".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("Project".to_string()),
            Value::Date(NaiveDate::from_ymd_opt(2025, 2, 1).unwrap()),
            Value::Text("MGR001".to_string()),
            Value::Null,
        ])
        .unwrap();

    project_table
        .insert_row(vec![
            Value::Text("PRJ002".to_string()),
            Value::Text("Research Initiative Beta".to_string()),
            Value::Text("Technology research and development".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("Project".to_string()),
            Value::Date(NaiveDate::from_ymd_opt(2025, 3, 1).unwrap()),
            Value::Text("MGR002".to_string()),
            Value::Null,
        ])
        .unwrap();

    project_table
        .insert_row(vec![
            Value::Text("WP001".to_string()),
            Value::Text("Alpha Work Package 1".to_string()),
            Value::Text("Development work package under Alpha".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("Work Package".to_string()),
            Value::Date(NaiveDate::from_ymd_opt(2025, 2, 15).unwrap()),
            Value::Text("MGR001".to_string()),
            Value::Text("PRJ001".to_string()),
        ])
        .unwrap();

    // Create RESOURCE_ALLOCATION table
    let mut allocation_table = Table::new(
        "RESOURCE_ALLOCATION".to_string(),
        vec![
            Column {
                name: "PROJECT_ID".to_string(),
                sql_type: SqlType::Varchar(255),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: Some(("PROJECT_MASTER".to_string(), "PROJECT_ID".to_string())),
            },
            Column {
                name: "RESOURCE_ID".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "ALLOCATION_PERCENT".to_string(),
                sql_type: SqlType::Decimal(5, 2),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "ALLOCATION_HOURS".to_string(),
                sql_type: SqlType::Decimal(10, 2),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "MONTH_PERIOD".to_string(),
                sql_type: SqlType::Date,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Insert allocation test data
    allocation_table
        .insert_row(vec![
            Value::Text("PRJ001".to_string()),
            Value::Text("RES001".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(10000, 2)), // 100.00%
            Value::Decimal(rust_decimal::Decimal::new(4000, 2)),  // 40.00 hours
            Value::Date(NaiveDate::from_ymd_opt(2025, 8, 1).unwrap()),
        ])
        .unwrap();

    allocation_table
        .insert_row(vec![
            Value::Text("WP001".to_string()),
            Value::Text("RES002".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(7500, 2)), // 75.00%
            Value::Decimal(rust_decimal::Decimal::new(3000, 2)), // 30.00 hours
            Value::Date(NaiveDate::from_ymd_opt(2025, 8, 1).unwrap()),
        ])
        .unwrap();

    allocation_table
        .insert_row(vec![
            Value::Text("PRJ002".to_string()),
            Value::Text("RES003".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(5000, 2)), // 50.00%
            Value::Decimal(rust_decimal::Decimal::new(2000, 2)), // 20.00 hours
            Value::Date(NaiveDate::from_ymd_opt(2025, 8, 1).unwrap()),
        ])
        .unwrap();

    // Create RESOURCE_MASTER table
    let mut resource_table = Table::new(
        "RESOURCE_MASTER".to_string(),
        vec![
            Column {
                name: "RESOURCE_ID".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: true,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "FIRST_NAME".to_string(),
                sql_type: SqlType::Varchar(100),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "LAST_NAME".to_string(),
                sql_type: SqlType::Varchar(100),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "EMAIL".to_string(),
                sql_type: SqlType::Varchar(255),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "ROLE".to_string(),
                sql_type: SqlType::Varchar(100),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Insert resource test data
    resource_table
        .insert_row(vec![
            Value::Text("RES001".to_string()),
            Value::Text("Alice".to_string()),
            Value::Text("Johnson".to_string()),
            Value::Text("alice.johnson@company.com".to_string()),
            Value::Text("Senior Engineer".to_string()),
        ])
        .unwrap();

    resource_table
        .insert_row(vec![
            Value::Text("RES002".to_string()),
            Value::Text("Bob".to_string()),
            Value::Text("Smith".to_string()),
            Value::Text("bob.smith@company.com".to_string()),
            Value::Text("Project Manager".to_string()),
        ])
        .unwrap();

    resource_table
        .insert_row(vec![
            Value::Text("RES003".to_string()),
            Value::Text("Carol".to_string()),
            Value::Text("Davis".to_string()),
            Value::Text("carol.davis@company.com".to_string()),
            Value::Text("Research Scientist".to_string()),
        ])
        .unwrap();

    db.add_table(project_table).unwrap();
    db.add_table(allocation_table).unwrap();
    db.add_table(resource_table).unwrap();

    db
}

#[tokio::test]
async fn test_recursive_cte_project_hierarchy() {
    let db = create_test_database();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test RECURSIVE CTE for project hierarchy - common in enterprise systems
    let query = r#"
        WITH RECURSIVE ProjectHierarchy AS (
            -- Base case: top-level projects
            SELECT 
                PROJECT_ID,
                PROJECT_NAME,
                HIERARCHY_PARENT_ID,
                1 as LEVEL
            FROM PROJECT_MASTER 
            WHERE HIERARCHY_PARENT_ID IS NULL
            
            UNION ALL
            
            -- Recursive case: child projects
            SELECT 
                p.PROJECT_ID,
                p.PROJECT_NAME,
                p.HIERARCHY_PARENT_ID,
                ph.LEVEL + 1 as LEVEL
            FROM PROJECT_MASTER p
            INNER JOIN ProjectHierarchy ph ON p.HIERARCHY_PARENT_ID = ph.PROJECT_ID
        )
        SELECT PROJECT_ID, PROJECT_NAME, LEVEL
        FROM ProjectHierarchy
        ORDER BY LEVEL, PROJECT_ID
    "#;

    let query = parse_sql(query).unwrap();
    let result = executor.execute(&query[0]).await.unwrap();
    assert_eq!(result.rows.len(), 3); // Should include all projects in hierarchy

    // Verify hierarchy levels
    let first_row = &result.rows[0];
    assert_eq!(first_row[2], Value::Integer(1)); // Top-level project should have LEVEL 1

    let last_row = &result.rows[2];
    assert_eq!(last_row[2], Value::Integer(2)); // Work package should have LEVEL 2
}

#[tokio::test]
async fn test_complex_cte_with_aggregation_and_date_functions() {
    let db = create_test_database();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test complex CTE with date arithmetic and aggregation - typical in enterprise reporting
    let query = r#"
        WITH DateRange AS (
            SELECT 
                ADD_MONTHS(CURRENT_DATE, 0) - EXTRACT(DAY FROM CURRENT_DATE) + 1 AS START_DATE,
                LAST_DAY(ADD_MONTHS(CURRENT_DATE, 1)) AS END_DATE
        ),
        ProjectSummary AS (
            SELECT
                p.PROJECT_ID,
                p.PROJECT_NAME,
                COUNT(DISTINCT a.RESOURCE_ID) AS RESOURCE_COUNT,
                SUM(a.ALLOCATION_HOURS) AS TOTAL_HOURS
            FROM PROJECT_MASTER p
            INNER JOIN RESOURCE_ALLOCATION a ON p.PROJECT_ID = a.PROJECT_ID
            CROSS JOIN DateRange dr
            WHERE a.MONTH_PERIOD BETWEEN dr.START_DATE AND dr.END_DATE
            GROUP BY p.PROJECT_ID, p.PROJECT_NAME
        )
        SELECT 
            PROJECT_ID,
            PROJECT_NAME,
            RESOURCE_COUNT,
            TOTAL_HOURS,
            COALESCE(TOTAL_HOURS / NULLIF(RESOURCE_COUNT, 0), 0) AS AVG_HOURS_PER_RESOURCE
        FROM ProjectSummary
        ORDER BY TOTAL_HOURS DESC
    "#;

    let query = parse_sql(query).unwrap();
    let result = executor.execute(&query[0]).await.unwrap();
    assert!(result.rows.len() > 0);

    // Verify COALESCE works in complex expressions
    for row in &result.rows {
        let avg_hours = &row[4];
        assert!(!matches!(avg_hours, Value::Null)); // COALESCE should prevent NULL
    }
}

#[tokio::test]
async fn test_union_all_with_complex_filtering() {
    let db = create_test_database();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test UNION ALL with complex filtering - common in enterprise data consolidation
    let query = r#"
        WITH ActiveProjects AS (
            SELECT PROJECT_ID, 'Direct Project' as PROJECT_TYPE
            FROM PROJECT_MASTER
            WHERE PROJECT_STRUCTURE = 'Project' 
            AND ACTIVE_FLAG = 'Y'
            AND STATUS_CODE = 'Active'
            
            UNION ALL
            
            SELECT 
                parent.PROJECT_ID as PROJECT_ID,
                'Project with Work Packages' as PROJECT_TYPE
            FROM PROJECT_MASTER parent
            INNER JOIN PROJECT_MASTER child ON parent.PROJECT_ID = child.HIERARCHY_PARENT_ID
            WHERE parent.PROJECT_STRUCTURE = 'Project'
            AND parent.ACTIVE_FLAG = 'Y' 
            AND parent.STATUS_CODE = 'Active'
            AND child.PROJECT_STRUCTURE = 'Work Package'
        )
        SELECT DISTINCT PROJECT_ID, PROJECT_TYPE
        FROM ActiveProjects
        ORDER BY PROJECT_ID
    "#;

    let query = parse_sql(query).unwrap();
    let result = executor.execute(&query[0]).await.unwrap();
    assert!(result.rows.len() >= 2); // Should find both direct and hierarchical projects
}

#[tokio::test]
async fn test_case_expressions_in_joins() {
    let db = create_test_database();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test CASE expressions in JOIN conditions - used in conditional business logic
    let query = r#"
        SELECT 
            p.PROJECT_ID,
            p.PROJECT_NAME,
            r.FIRST_NAME,
            r.LAST_NAME,
            CASE 
                WHEN a.ALLOCATION_PERCENT >= 100 THEN 'Full Time'
                WHEN a.ALLOCATION_PERCENT >= 50 THEN 'Part Time'
                ELSE 'Minimal'
            END as ALLOCATION_TYPE
        FROM PROJECT_MASTER p
        INNER JOIN RESOURCE_ALLOCATION a ON p.PROJECT_ID = a.PROJECT_ID
        INNER JOIN RESOURCE_MASTER r ON a.RESOURCE_ID = r.RESOURCE_ID
        WHERE CASE 
            WHEN p.PROJECT_STRUCTURE = 'Project' THEN a.ALLOCATION_PERCENT >= 25
            WHEN p.PROJECT_STRUCTURE = 'Work Package' THEN a.ALLOCATION_PERCENT >= 50
            ELSE FALSE
        END
        ORDER BY p.PROJECT_ID, a.ALLOCATION_PERCENT DESC
    "#;

    let query = parse_sql(query).unwrap();
    let result = executor.execute(&query[0]).await.unwrap();
    assert!(result.rows.len() > 0);

    // Verify CASE expression results
    for row in &result.rows {
        let allocation_type = &row[4];
        assert!(matches!(allocation_type, Value::Text(_)));
    }
}

#[tokio::test]
async fn test_complex_nested_conditions() {
    let db = create_test_database();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test complex nested AND/OR/NOT conditions - typical in enterprise filtering
    let query = r#"
        SELECT 
            p.PROJECT_ID,
            p.PROJECT_NAME,
            a.RESOURCE_ID,
            a.ALLOCATION_PERCENT
        FROM PROJECT_MASTER p
        INNER JOIN RESOURCE_ALLOCATION a ON p.PROJECT_ID = a.PROJECT_ID
        WHERE (
            (p.PROJECT_STRUCTURE = 'Project' AND a.ALLOCATION_PERCENT >= 75)
            OR 
            (p.PROJECT_STRUCTURE = 'Work Package' AND a.ALLOCATION_PERCENT >= 50)
        )
        AND NOT (
            p.STATUS_CODE = 'Cancelled' 
            OR p.ACTIVE_FLAG = 'N'
            OR a.ALLOCATION_HOURS = 0
        )
        AND COALESCE(p.START_DATE, CURRENT_DATE) >= DATE '2025-01-01'
        ORDER BY p.PROJECT_ID, a.ALLOCATION_PERCENT DESC
    "#;

    let query = parse_sql(query).unwrap();
    let result = executor.execute(&query[0]).await.unwrap();
    assert!(result.rows.len() > 0);

    // Verify all returned rows meet the complex conditions
    for row in &result.rows {
        let allocation_percent = match &row[3] {
            Value::Decimal(d) => d.to_f64().unwrap_or(0.0),
            _ => 0.0,
        };
        assert!(allocation_percent >= 50.0); // Should meet minimum threshold
    }
}

#[tokio::test]
async fn test_aggregate_functions_with_distinct() {
    let db = create_test_database();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test aggregate functions with DISTINCT - common in enterprise reporting
    let query = r#"
        SELECT 
            COUNT(*) as TOTAL_ALLOCATIONS,
            COUNT(DISTINCT PROJECT_ID) as UNIQUE_PROJECTS,
            COUNT(DISTINCT RESOURCE_ID) as UNIQUE_RESOURCES,
            SUM(ALLOCATION_HOURS) as TOTAL_HOURS,
            AVG(ALLOCATION_PERCENT) as AVG_PERCENT,
            MIN(ALLOCATION_PERCENT) as MIN_PERCENT,
            MAX(ALLOCATION_PERCENT) as MAX_PERCENT
        FROM RESOURCE_ALLOCATION
        WHERE ALLOCATION_HOURS > 0
    "#;

    let query = parse_sql(query).unwrap();
    let result = executor.execute(&query[0]).await.unwrap();
    assert_eq!(result.rows.len(), 1);

    let row = &result.rows[0];
    // Verify all aggregate functions return valid results
    assert!(matches!(row[0], Value::Integer(_))); // COUNT(*)
    assert!(matches!(row[1], Value::Integer(_))); // COUNT(DISTINCT PROJECT_ID) 
    assert!(matches!(row[2], Value::Integer(_))); // COUNT(DISTINCT RESOURCE_ID)
    assert!(!matches!(row[3], Value::Null)); // SUM should not be NULL
    assert!(!matches!(row[4], Value::Null)); // AVG should not be NULL
    assert!(!matches!(row[5], Value::Null)); // MIN should not be NULL
    assert!(!matches!(row[6], Value::Null)); // MAX should not be NULL
}

#[tokio::test]
async fn test_date_arithmetic_functions() {
    let db = create_test_database();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test date arithmetic functions commonly used in enterprise systems
    let query = r#"
        SELECT 
            PROJECT_ID,
            PROJECT_NAME,
            START_DATE,
            ADD_MONTHS(START_DATE, 3) as QUARTER_END,
            EXTRACT(YEAR FROM START_DATE) as START_YEAR,
            EXTRACT(MONTH FROM START_DATE) as START_MONTH,
            LAST_DAY(START_DATE) as MONTH_END,
            CURRENT_DATE as TODAY
        FROM PROJECT_MASTER
        WHERE START_DATE IS NOT NULL
        ORDER BY START_DATE
    "#;

    let query = parse_sql(query).unwrap();
    let result = executor.execute(&query[0]).await.unwrap();
    assert!(result.rows.len() > 0);

    // Verify date functions work correctly
    for row in &result.rows {
        assert!(!matches!(row[3], Value::Null)); // ADD_MONTHS result
        assert!(!matches!(row[4], Value::Null)); // EXTRACT YEAR result
        assert!(!matches!(row[5], Value::Null)); // EXTRACT MONTH result
        assert!(!matches!(row[6], Value::Null)); // LAST_DAY result
        assert!(!matches!(row[7], Value::Null)); // CURRENT_DATE result
    }
}

#[tokio::test]
async fn test_compound_identifier_support() {
    let db = create_test_database();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test compound identifiers (table.column) - essential for enterprise SQL
    let query = r#"
        SELECT 
            p.PROJECT_ID,
            p.PROJECT_NAME,
            r.FIRST_NAME,
            r.LAST_NAME,
            a.ALLOCATION_PERCENT
        FROM PROJECT_MASTER p
        INNER JOIN RESOURCE_ALLOCATION a ON p.PROJECT_ID = a.PROJECT_ID  
        INNER JOIN RESOURCE_MASTER r ON a.RESOURCE_ID = r.RESOURCE_ID
        WHERE p.ACTIVE_FLAG = 'Y'
        AND p.STATUS_CODE = 'Active'
        AND a.ALLOCATION_HOURS > 0
        ORDER BY p.PROJECT_ID, r.LAST_NAME, r.FIRST_NAME
    "#;

    let query = parse_sql(query).unwrap();
    let result = executor.execute(&query[0]).await.unwrap();
    assert!(result.rows.len() > 0);

    // Verify compound identifiers resolve correctly
    for row in &result.rows {
        assert!(matches!(row[0], Value::Text(_))); // p.PROJECT_ID
        assert!(matches!(row[1], Value::Text(_))); // p.PROJECT_NAME
        // Names might be NULL in test data, so check they exist as columns
        assert!(row.len() == 5); // All 5 selected columns present
    }
}
