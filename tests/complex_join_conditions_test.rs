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
