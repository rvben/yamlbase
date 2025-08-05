use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_complex_where_regression() {
    println!("=== COMPLEX WHERE CLAUSE REGRESSION TEST (0.4.11) ===");

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
            Column {
                name: "hierarchy_parent_sap_id".to_string(),
                sql_type: SqlType::Varchar(255),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Add test data
    projects_table.insert_row(vec![
        Value::Text("123001".to_string()),
        Value::Text("Test Project Alpha".to_string()),
        Value::Text("Published".to_string()),
        Value::Text("Active".to_string()),
        Value::Text("Y".to_string()),
        Value::Text("Project".to_string()),
        Value::Null,
    ]).unwrap();
    projects_table.insert_row(vec![
        Value::Text("123002".to_string()),
        Value::Text("Test Project Beta".to_string()),
        Value::Text("Published".to_string()),
        Value::Text("Cancelled".to_string()),
        Value::Text("N".to_string()),
        Value::Text("Project".to_string()),
        Value::Null,
    ]).unwrap();

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
    allocations_table.insert_row(vec![
        Value::Text("123001".to_string()),
        Value::Text("nxf12345".to_string()),
        Value::Text("Published".to_string()),
        Value::Text("Hard Allocation".to_string()),
        Value::Text("Active".to_string()),
        Value::Decimal(rust_decimal::Decimal::new(400, 1)), // 40.0
        Value::Decimal(rust_decimal::Decimal::new(385, 1)), // 38.5
    ]).unwrap();
    allocations_table.insert_row(vec![
        Value::Text("123001".to_string()),
        Value::Text("nxf12346".to_string()),
        Value::Text("Published".to_string()),
        Value::Text("Hard Allocation".to_string()),
        Value::Text("Active".to_string()),
        Value::Decimal(rust_decimal::Decimal::new(200, 1)), // 20.0
        Value::Decimal(rust_decimal::Decimal::new(0, 1)), // 0.0
    ]).unwrap();

    db.add_table(allocations_table).unwrap();

    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test 1: Simple WHERE with single condition (should work)
    println!("\n1. Testing simple WHERE clause (baseline):");
    let query = r#"
        SELECT 
            p.SAP_PROJECT_ID,
            p.PROJECT_NAME,
            COUNT(DISTINCT a.WBI_ID) AS MEMBERS
        FROM SF_PROJECT_V2 p
        INNER JOIN SF_PROJECT_ALLOCATIONS a ON p.SAP_PROJECT_ID = a.SAP_PROJECT_ID
        WHERE p.VERSION_CODE = 'Published'
        GROUP BY p.SAP_PROJECT_ID, p.PROJECT_NAME
        ORDER BY p.PROJECT_NAME
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ Simple WHERE clause works!");
            println!("   Number of rows returned: {}", result.rows.len());
        }
        Err(e) => {
            panic!("   ❌ Simple WHERE failed: {e}");
        }
    }

    // Test 2: Complex WHERE with multiple conditions and NOT IN (regression in 0.4.11)
    println!("\n2. Testing complex WHERE clause with NOT IN:");
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
        }
        Err(e) => {
            println!("   ❌ Complex WHERE failed with: {e}");
            panic!("Regression: Complex WHERE clauses should work as in 0.4.10");
        }
    }

    // Test 3: Medium complexity WHERE to isolate the issue
    println!("\n3. Testing medium complexity WHERE:");
    let query = r#"
        SELECT p.SAP_PROJECT_ID, COUNT(*) 
        FROM SF_PROJECT_V2 p
        INNER JOIN SF_PROJECT_ALLOCATIONS a ON p.SAP_PROJECT_ID = a.SAP_PROJECT_ID
        WHERE p.VERSION_CODE = 'Published'
          AND p.STATUS_CODE NOT IN ('Cancelled', 'Closed')
          AND a.ASSIGNMENT_TYPE = 'Hard Allocation'
        GROUP BY p.SAP_PROJECT_ID
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ Medium complexity WHERE works!");
            println!("   Number of rows returned: {}", result.rows.len());
        }
        Err(e) => {
            println!("   ❌ Medium WHERE failed with: {e}");
        }
    }

    // Test 4: Test OR expression in WHERE clause
    println!("\n4. Testing OR expression in WHERE:");
    let query = r#"
        SELECT p.SAP_PROJECT_ID, a.WBI_ID
        FROM SF_PROJECT_V2 p
        INNER JOIN SF_PROJECT_ALLOCATIONS a ON p.SAP_PROJECT_ID = a.SAP_PROJECT_ID
        WHERE (a.PLANNED_EFFORT_HOURS > 0 OR a.ACTUAL_EFFORT_HOURS > 0)
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ OR expression in WHERE works!");
            println!("   Number of rows returned: {}", result.rows.len());
        }
        Err(e) => {
            println!("   ❌ OR expression failed with: {e}");
        }
    }

    println!("\n✅ Complex WHERE regression test completed!");
}

#[tokio::test]
async fn test_multi_condition_join_regression() {
    println!("=== MULTI-CONDITION JOIN REGRESSION TEST (0.4.11) ===");

    // Create test database
    let mut db = Database::new("test_db".to_string());

    // Create SF_PROJECT_V2 table with hierarchical data
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
            Column {
                name: "hierarchy_parent_sap_id".to_string(),
                sql_type: SqlType::Varchar(255),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Add parent project
    projects_table.insert_row(vec![
        Value::Text("123001".to_string()),
        Value::Text("Parent Project".to_string()),
        Value::Text("Published".to_string()),
        Value::Text("Active".to_string()),
        Value::Text("Y".to_string()),
        Value::Text("Project".to_string()),
        Value::Null,
    ]).unwrap();

    // Add child work packages
    projects_table.insert_row(vec![
        Value::Text("123001-WP1".to_string()),
        Value::Text("Work Package 1".to_string()),
        Value::Text("Published".to_string()),
        Value::Text("Active".to_string()),
        Value::Text("Y".to_string()),
        Value::Text("Work Package".to_string()),
        Value::Text("123001".to_string()),
    ]).unwrap();
    projects_table.insert_row(vec![
        Value::Text("123001-WP2".to_string()),
        Value::Text("Work Package 2".to_string()),
        Value::Text("Published".to_string()),
        Value::Text("Cancelled".to_string()),
        Value::Text("N".to_string()),
        Value::Text("Work Package".to_string()),
        Value::Text("123001".to_string()),
    ]).unwrap();

    db.add_table(projects_table).unwrap();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test 1: Simple JOIN with single condition (should work)
    println!("\n1. Testing simple JOIN ON with single condition:");
    let query = r#"
        SELECT parent.SAP_PROJECT_ID, child.SAP_PROJECT_ID 
        FROM SF_PROJECT_V2 parent
        INNER JOIN SF_PROJECT_V2 child ON parent.SAP_PROJECT_ID = child.HIERARCHY_PARENT_SAP_ID
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ Simple JOIN ON works!");
            println!("   Number of rows returned: {}", result.rows.len());
        }
        Err(e) => {
            panic!("   ❌ Simple JOIN failed: {e}");
        }
    }

    // Test 2: Multi-condition JOIN ON with AND (regression in 0.4.11)
    println!("\n2. Testing multi-condition JOIN ON:");
    let query = r#"
        SELECT parent.SAP_PROJECT_ID, child.SAP_PROJECT_ID 
        FROM SF_PROJECT_V2 parent
        INNER JOIN SF_PROJECT_V2 child
            ON parent.SAP_PROJECT_ID = child.HIERARCHY_PARENT_SAP_ID
            AND child.VERSION_CODE = 'Published'
            AND child.STATUS_CODE NOT IN ('Cancelled', 'Closed')
            AND child.ACTIVE_FLAG = 'Y'
            AND child.PROJECT_STRUCTURE = 'Work Package'
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ Multi-condition JOIN ON works!");
            println!("   Number of rows returned: {}", result.rows.len());
            assert_eq!(result.rows.len(), 1); // Only WP1 should match
        }
        Err(e) => {
            println!("   ❌ Multi-condition JOIN ON failed with: {e}");
            panic!("Regression: Multi-condition JOIN ON should work as in 0.4.10");
        }
    }

    // Test 3: Alternative workaround - move conditions to WHERE
    println!("\n3. Testing workaround - conditions in WHERE:");
    let query = r#"
        SELECT parent.SAP_PROJECT_ID, child.SAP_PROJECT_ID 
        FROM SF_PROJECT_V2 parent
        INNER JOIN SF_PROJECT_V2 child ON parent.SAP_PROJECT_ID = child.HIERARCHY_PARENT_SAP_ID
        WHERE child.VERSION_CODE = 'Published'
          AND child.STATUS_CODE NOT IN ('Cancelled', 'Closed')
          AND child.ACTIVE_FLAG = 'Y'
          AND child.PROJECT_STRUCTURE = 'Work Package'
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ Workaround with WHERE works!");
            println!("   Number of rows returned: {}", result.rows.len());
        }
        Err(e) => {
            println!("   ❌ Workaround also failed with: {e}");
        }
    }

    println!("\n✅ Multi-condition JOIN regression test completed!");
}

#[tokio::test]
async fn test_cte_features_still_work() {
    println!("=== VERIFY CTE FEATURES STILL WORK ===");

    // Create test database
    let mut db = Database::new("test_db".to_string());

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
                name: "project_status_code".to_string(),
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
    allocations_table.insert_row(vec![
        Value::Text("123001".to_string()),
        Value::Text("emp001".to_string()),
        Value::Text("Active".to_string()),
    ]).unwrap();
    allocations_table.insert_row(vec![
        Value::Text("123002".to_string()),
        Value::Text("emp002".to_string()),
        Value::Text("Cancelled".to_string()),
    ]).unwrap();

    db.add_table(allocations_table).unwrap();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test: CTE with NOT IN (should still work from 0.4.11)
    println!("\n1. Testing CTE with NOT IN:");
    let query = r#"
        WITH FilteredData AS (
            SELECT a.SAP_PROJECT_ID, a.WBI_ID
            FROM SF_PROJECT_ALLOCATIONS a
            WHERE a.PROJECT_STATUS_CODE NOT IN ('Cancelled', 'Closed')
        )
        SELECT COUNT(*) FROM FilteredData
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ CTE with NOT IN still works!");
            assert_eq!(result.rows[0][0], Value::Integer(1));
        }
        Err(e) => {
            panic!("   ❌ CTE feature broken: {e}");
        }
    }

    println!("\n✅ CTE features verification completed!");
}