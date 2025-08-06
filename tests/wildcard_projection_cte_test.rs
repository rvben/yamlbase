use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_wildcard_projection_cte() {
    println!("=== WILDCARD PROJECTION CTE TEST ===");

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
                name: "version_code".to_string(),
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
    allocations_table
        .insert_row(vec![
            Value::Text("123001".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("emp001".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(400, 1)), // 40.0
        ])
        .unwrap();
    allocations_table
        .insert_row(vec![
            Value::Text("123002".to_string()),
            Value::Text("Draft".to_string()),
            Value::Text("emp002".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(200, 1)), // 20.0
        ])
        .unwrap();
    allocations_table
        .insert_row(vec![
            Value::Text("123003".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("emp003".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(300, 1)), // 30.0
        ])
        .unwrap();

    db.add_table(allocations_table).unwrap();

    // Create SF_PROJECT_V2 table for testing qualified wildcards
    let mut projects_table = Table::new(
        "sf_project_v2".to_string(),
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
                name: "project_name".to_string(),
                sql_type: SqlType::Varchar(100),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    projects_table
        .insert_row(vec![
            Value::Text("123001".to_string()),
            Value::Text("Project Alpha".to_string()),
        ])
        .unwrap();
    projects_table
        .insert_row(vec![
            Value::Text("123002".to_string()),
            Value::Text("Project Beta".to_string()),
        ])
        .unwrap();
    projects_table
        .insert_row(vec![
            Value::Text("123003".to_string()),
            Value::Text("Project Gamma".to_string()),
        ])
        .unwrap();

    db.add_table(projects_table).unwrap();

    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test 1: Simple wildcard projection SELECT *
    println!("\n1. Testing SELECT * in CTE:");
    let query = r#"
        WITH AllData AS (
            SELECT a.*
            FROM SF_PROJECT_ALLOCATIONS a
            WHERE a.VERSION_CODE = 'Published'
        )
        SELECT COUNT(*) FROM AllData
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ SELECT * in CTE works!");
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], Value::Integer(2)); // Two published allocations
            println!("   Count of published allocations: {:?}", result.rows[0][0]);
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 2: Wildcard with additional filter
    println!("\n2. Testing SELECT * with additional filter:");
    let query = r#"
        WITH FilteredData AS (
            SELECT a.*
            FROM SF_PROJECT_ALLOCATIONS a
            WHERE a.VERSION_CODE = 'Published' AND a.PLANNED_EFFORT_HOURS > 25
        )
        SELECT SAP_PROJECT_ID, PLANNED_EFFORT_HOURS FROM FilteredData ORDER BY SAP_PROJECT_ID
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ SELECT * with specific column retrieval works!");
            assert_eq!(result.rows.len(), 2);
            assert_eq!(result.rows[0][0], Value::Text("123001".to_string()));
            assert_eq!(
                result.rows[0][1],
                Value::Decimal(rust_decimal::Decimal::new(400, 1))
            );
            assert_eq!(result.rows[1][0], Value::Text("123003".to_string()));
            assert_eq!(
                result.rows[1][1],
                Value::Decimal(rust_decimal::Decimal::new(300, 1))
            );
            println!("   Retrieved specific columns from wildcard CTE");
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 3: Qualified wildcard projection (table.*)
    println!("\n3. Testing qualified wildcard (table.*) in CTE:");
    let query = r#"
        WITH ProjectsAndAllocations AS (
            SELECT p.*, a.WBI_ID, a.PLANNED_EFFORT_HOURS
            FROM SF_PROJECT_V2 p
            INNER JOIN SF_PROJECT_ALLOCATIONS a ON p.SAP_PROJECT_ID = a.SAP_PROJECT_ID
            WHERE a.VERSION_CODE = 'Published'
        )
        SELECT COUNT(*) FROM ProjectsAndAllocations
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ Qualified wildcard (table.*) in CTE works!");
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], Value::Integer(2)); // Two published allocations joined with projects
            println!(
                "   Count of joined project-allocation records: {:?}",
                result.rows[0][0]
            );
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 4: SELECT * from CTE itself
    println!("\n4. Testing SELECT * from CTE directly:");
    let query = r#"
        WITH PublishedAllocations AS (
            SELECT a.SAP_PROJECT_ID, a.WBI_ID, a.PLANNED_EFFORT_HOURS
            FROM SF_PROJECT_ALLOCATIONS a
            WHERE a.VERSION_CODE = 'Published'
        )
        SELECT * FROM PublishedAllocations ORDER BY SAP_PROJECT_ID
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ SELECT * from CTE directly works!");
            assert_eq!(result.rows.len(), 2);
            assert_eq!(result.columns.len(), 3);
            // Column names come from the SQL query, which uses uppercase
            assert_eq!(result.columns[0], "SAP_PROJECT_ID");
            assert_eq!(result.columns[1], "WBI_ID");
            assert_eq!(result.columns[2], "PLANNED_EFFORT_HOURS");
            println!("   Retrieved all columns from CTE");
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    println!("\n✅ All wildcard projection CTE tests passed!");
}
