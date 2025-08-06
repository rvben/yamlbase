use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_complex_join_conditions() {
    println!("=== COMPLEX JOIN CONDITIONS TEST ===");

    // Create a test database with projects table
    let mut db = Database::new("test_db".to_string());

    // Create projects table with all required columns
    let mut projects_table = Table::new(
        "projects".to_string(),
        vec![
            Column {
                name: "project_id".to_string(),
                sql_type: SqlType::Varchar(20),
                primary_key: true,
                nullable: false,
                unique: true,
                default: None,
                references: None,
            },
            Column {
                name: "parent_id".to_string(),
                sql_type: SqlType::Varchar(20),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "version".to_string(),
                sql_type: SqlType::Varchar(20),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "status".to_string(),
                sql_type: SqlType::Varchar(20),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "is_active".to_string(),
                sql_type: SqlType::Varchar(1),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "is_locked".to_string(),
                sql_type: SqlType::Varchar(1),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "project_type".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Add test data
    // Parent projects
    projects_table
        .insert_row(vec![
            Value::Text("PROJ001".to_string()),
            Value::Null,
            Value::Text("Published".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Main Project".to_string()),
        ])
        .unwrap();

    projects_table
        .insert_row(vec![
            Value::Text("PROJ002".to_string()),
            Value::Null,
            Value::Text("Published".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Main Project".to_string()),
        ])
        .unwrap();

    // Child projects that should match
    projects_table
        .insert_row(vec![
            Value::Text("PROJ001-SUB1".to_string()),
            Value::Text("PROJ001".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Sub Project".to_string()),
        ])
        .unwrap();

    projects_table
        .insert_row(vec![
            Value::Text("PROJ002-SUB1".to_string()),
            Value::Text("PROJ002".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Sub Project".to_string()),
        ])
        .unwrap();

    // Child projects that should NOT match (various disqualifying conditions)
    projects_table
        .insert_row(vec![
            Value::Text("PROJ001-SUB2".to_string()),
            Value::Text("PROJ001".to_string()),
            Value::Text("Draft".to_string()), // Not Published
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Sub Project".to_string()),
        ])
        .unwrap();

    projects_table
        .insert_row(vec![
            Value::Text("PROJ001-SUB3".to_string()),
            Value::Text("PROJ001".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("Cancelled".to_string()), // Cancelled
            Value::Text("Y".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Sub Project".to_string()),
        ])
        .unwrap();

    projects_table
        .insert_row(vec![
            Value::Text("PROJ002-SUB2".to_string()),
            Value::Text("PROJ002".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("N".to_string()), // Not active
            Value::Text("N".to_string()),
            Value::Text("Sub Project".to_string()),
        ])
        .unwrap();

    db.add_table(projects_table).unwrap();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test 1: Single AND condition (currently works)
    println!("\n1. Testing single AND condition in JOIN:");
    let stmt = parse_sql(
        r#"
        SELECT COUNT(*)
        FROM projects parent
        INNER JOIN projects child
            ON parent.project_id = child.parent_id
            AND child.version = 'Published'
    "#,
    )
    .unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ Single AND condition works!");
            println!("   Result: {:?}", result.rows[0][0]);
            assert_eq!(result.rows[0][0], Value::Integer(4)); // Should match 4 children with Published version
        }
        Err(e) => {
            println!("   ❌ Failed with: {e}");
        }
    }

    // Test 2: Two AND conditions
    println!("\n2. Testing two AND conditions in JOIN:");
    let stmt = parse_sql(
        r#"
        SELECT COUNT(*)
        FROM projects parent
        INNER JOIN projects child
            ON parent.project_id = child.parent_id
            AND child.version = 'Published'
            AND child.is_active = 'Y'
    "#,
    )
    .unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ Two AND conditions work!");
            println!("   Result: {:?}", result.rows[0][0]);
            assert_eq!(result.rows[0][0], Value::Integer(3)); // Should match 3 active published children
        }
        Err(e) => {
            println!("   ❌ Failed with: {e}");
            println!("   This is expected to fail in 0.4.6");
        }
    }

    // Test 3: Mixed condition types (equality + NOT IN)
    println!("\n3. Testing mixed condition types (equality + NOT IN):");
    let stmt = parse_sql(
        r#"
        SELECT COUNT(*)
        FROM projects parent
        INNER JOIN projects child
            ON parent.project_id = child.parent_id
            AND child.version = 'Published'
            AND child.status NOT IN ('Cancelled', 'Closed')
    "#,
    )
    .unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ Mixed conditions work!");
            println!("   Result: {:?}", result.rows[0][0]);
            assert_eq!(result.rows[0][0], Value::Integer(3)); // Should exclude the cancelled one
        }
        Err(e) => {
            println!("   ❌ Failed with: {e}");
            println!("   This is expected to fail in 0.4.6");
        }
    }

    // Test 4: Full production query pattern
    println!("\n4. Testing full production query pattern:");
    let stmt = parse_sql(
        r#"
        WITH ProjectHierarchy AS (
            SELECT
                parent.project_id AS main_project_id,
                child.project_id AS sub_project_id
            FROM projects parent
            INNER JOIN projects child
                ON parent.project_id = child.parent_id
                AND child.version = 'Published'
                AND child.status NOT IN ('Cancelled', 'Closed')
                AND child.is_active = 'Y'
                AND child.is_locked = 'N'
                AND child.project_type = 'Sub Project'
            WHERE parent.version = 'Published'
                AND parent.status NOT IN ('Cancelled', 'Closed')
        )
        SELECT COUNT(*) FROM ProjectHierarchy
    "#,
    )
    .unwrap();
    match executor.execute(&stmt[0]).await {
        Ok(result) => {
            println!("   ✅ Full production query works!");
            println!("   Result: {:?}", result.rows[0][0]);
            assert_eq!(result.rows[0][0], Value::Integer(2)); // Should match only PROJ001-SUB1 and PROJ002-SUB1
        }
        Err(e) => {
            println!("   ❌ Failed with: {e}");
            println!("   This is the target query for 0.4.7");
        }
    }

    println!("\n=== COMPLEX JOIN CONDITIONS TEST COMPLETE ===");
}

#[tokio::test]
async fn test_complex_where_with_not_in_regression() {
    println!("=== COMPLEX WHERE WITH NOT IN REGRESSION TEST ===");

    // Create test database
    let mut db = Database::new("test_db".to_string());

    // Create SF_PROJECT_V2 table
    let mut projects_table = Table::new(
        "sf_project_v2".to_string(),
        vec![
            Column {
                name: "sap_project_id".to_string(),
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
                name: "version_code".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: true,
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
                name: "active_flag".to_string(),
                sql_type: SqlType::Varchar(1),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "project_structure".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Add test data
    projects_table
        .insert_row(vec![
            Value::Text("123001".to_string()),
            Value::Text("Test Project Alpha".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("Project".to_string()),
        ])
        .unwrap();
    projects_table
        .insert_row(vec![
            Value::Text("123002".to_string()),
            Value::Text("Test Project Beta".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("Cancelled".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Project".to_string()),
        ])
        .unwrap();

    db.add_table(projects_table).unwrap();

    // Create SF_PROJECT_ALLOCATIONS table
    let mut allocations_table = Table::new(
        "sf_project_allocations".to_string(),
        vec![
            Column {
                name: "sap_project_id".to_string(),
                sql_type: SqlType::Varchar(255),
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
                name: "version_code".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "assignment_type".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "project_status_code".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "planned_effort_hours".to_string(),
                sql_type: SqlType::Decimal(10, 2),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "actual_effort_hours".to_string(),
                sql_type: SqlType::Decimal(10, 2),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Add test data
    allocations_table
        .insert_row(vec![
            Value::Text("123001".to_string()),
            Value::Text("emp001".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("Hard Allocation".to_string()),
            Value::Text("Active".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(400, 1)), // 40.0
            Value::Decimal(rust_decimal::Decimal::new(385, 1)), // 38.5
        ])
        .unwrap();
    allocations_table
        .insert_row(vec![
            Value::Text("123001".to_string()),
            Value::Text("emp002".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("Hard Allocation".to_string()),
            Value::Text("Active".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(200, 1)), // 20.0
            Value::Decimal(rust_decimal::Decimal::new(0, 1)),   // 0.0
        ])
        .unwrap();

    db.add_table(allocations_table).unwrap();

    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test: Complex WHERE with multiple conditions and NOT IN (0.4.11 regression)
    println!("\n1. Testing complex WHERE clause with NOT IN in JOINed query:");
    let query = r#"
        SELECT 
            p.SAP_PROJECT_ID,
            p.PROJECT_NAME,
            COUNT(DISTINCT a.WBI_ID) AS MEMBERS
        FROM SF_PROJECT_V2 p
        INNER JOIN SF_PROJECT_ALLOCATIONS a ON p.SAP_PROJECT_ID = a.SAP_PROJECT_ID
        WHERE 
            p.VERSION_CODE = 'Published'
            AND p.STATUS_CODE NOT IN ('Cancelled', 'Closed')
            AND p.ACTIVE_FLAG = 'Y'
            AND p.PROJECT_STRUCTURE = 'Project'
            AND a.VERSION_CODE = 'Published'
            AND a.ASSIGNMENT_TYPE = 'Hard Allocation'
            AND a.PROJECT_STATUS_CODE NOT IN ('Cancelled', 'Closed')
            AND (a.PLANNED_EFFORT_HOURS > 0 OR a.ACTUAL_EFFORT_HOURS > 0)
        GROUP BY p.SAP_PROJECT_ID, p.PROJECT_NAME
        ORDER BY p.PROJECT_NAME
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ Complex WHERE clause with NOT IN works!");
            println!("   Number of rows returned: {}", result.rows.len());
            assert_eq!(result.rows.len(), 1); // Only project 123001 should match
            assert_eq!(result.rows[0][0], Value::Text("123001".to_string()));
            assert_eq!(result.rows[0][2], Value::Integer(2)); // Two distinct employees
        }
        Err(e) => {
            panic!(
                "   ❌ Failed with: {e}\n   This was a regression in 0.4.11 that should now be fixed"
            );
        }
    }

    println!("\n✅ Complex WHERE regression test passed!");
}
