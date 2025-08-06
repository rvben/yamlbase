mod date_literals_in_cte_test {
    use std::sync::Arc;
    use chrono::NaiveDate;
    use yamlbase::database::{Column, Database, Storage, Table, Value};
    use yamlbase::sql::{QueryExecutor, parse_sql};
    use yamlbase::yaml::schema::SqlType;

    #[tokio::test]
    async fn test_date_literals_in_cte_where() {
    println!("=== DATE LITERALS IN CTE WHERE TEST ===");
    
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
                name: "start_date".to_string(),
                sql_type: SqlType::Date,
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
        Value::Text("Recent Project".to_string()),
        Value::Date(NaiveDate::parse_from_str("2025-01-15", "%Y-%m-%d").unwrap()),
    ]).unwrap();
    
    projects_table.insert_row(vec![
        Value::Text("123002".to_string()),
        Value::Text("Legacy Project".to_string()),
        Value::Date(NaiveDate::parse_from_str("2003-06-10", "%Y-%m-%d").unwrap()),
    ]).unwrap();
    
    projects_table.insert_row(vec![
        Value::Text("123003".to_string()),
        Value::Text("Future Project".to_string()),
        Value::Date(NaiveDate::parse_from_str("2025-06-01", "%Y-%m-%d").unwrap()),
    ]).unwrap();

    db.add_table(projects_table).unwrap();

    let storage = Storage::new(db);
    let storage_arc = Arc::new(storage);
    let executor = QueryExecutor::new(storage_arc).await.unwrap();

    // Test DATE literal in CTE WHERE clause
    let query = parse_sql(
        r#"
        WITH RecentProjects AS (
            SELECT p.SAP_PROJECT_ID, p.PROJECT_NAME, p.START_DATE
            FROM SF_PROJECT_V2 p
            WHERE p.START_DATE >= DATE '2004-01-01'
        )
        SELECT COUNT(*) AS project_count FROM RecentProjects
        "#,
    ).unwrap();

    let result = executor.execute(&query[0]).await;
    match result {
        Ok(res) => {
            println!("✅ DATE literal in CTE WHERE works!");
            assert_eq!(res.rows.len(), 1);
            assert_eq!(res.rows[0][0], Value::Integer(2)); // Should count 2 recent projects
        }
        Err(e) => {
            panic!("DATE literal in CTE WHERE should work: {}", e);
        }
    }
}

#[tokio::test]
async fn test_date_literals_in_cte_select() {
    println!("=== DATE LITERALS IN CTE SELECT TEST ===");
    
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
                name: "start_date".to_string(),
                sql_type: SqlType::Date,
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
        Value::Text("Recent Project".to_string()),
        Value::Date(NaiveDate::parse_from_str("2025-01-15", "%Y-%m-%d").unwrap()),
    ]).unwrap();
    
    projects_table.insert_row(vec![
        Value::Text("123002".to_string()),
        Value::Text("Older Project".to_string()),
        Value::Date(NaiveDate::parse_from_str("2023-06-10", "%Y-%m-%d").unwrap()),
    ]).unwrap();

    db.add_table(projects_table).unwrap();

    let storage = Storage::new(db);
    let storage_arc = Arc::new(storage);
    let executor = QueryExecutor::new(storage_arc).await.unwrap();

    // Test DATE literal in CTE SELECT with CASE
    let query = parse_sql(
        r#"
        WITH DateAnalysis AS (
            SELECT 
                p.SAP_PROJECT_ID,
                p.START_DATE,
                CASE 
                    WHEN p.START_DATE >= DATE '2025-01-01' THEN 'Recent'
                    ELSE 'Older'
                END AS project_age
            FROM SF_PROJECT_V2 p
        )
        SELECT project_age, COUNT(*) AS count FROM DateAnalysis GROUP BY project_age ORDER BY project_age
        "#,
    ).unwrap();

    let result = executor.execute(&query[0]).await;
    match result {
        Ok(res) => {
            println!("✅ DATE literal in CTE SELECT works!");
            assert_eq!(res.rows.len(), 2);
            // Results should be ordered by project_age
            assert_eq!(res.rows[0][0], Value::Text("Older".to_string()));
            assert_eq!(res.rows[0][1], Value::Integer(1));
            assert_eq!(res.rows[1][0], Value::Text("Recent".to_string()));
            assert_eq!(res.rows[1][1], Value::Integer(1));
        }
        Err(e) => {
            panic!("DATE literal in CTE SELECT should work: {}", e);
        }
    }
}

#[tokio::test]
async fn test_date_literals_with_between() {
    println!("=== DATE LITERALS WITH BETWEEN IN CTE TEST ===");
    
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
                name: "start_date".to_string(),
                sql_type: SqlType::Date,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Add test data spanning different years
    projects_table.insert_row(vec![
        Value::Text("123001".to_string()),
        Value::Text("Project 2023".to_string()),
        Value::Date(NaiveDate::parse_from_str("2023-06-15", "%Y-%m-%d").unwrap()),
    ]).unwrap();
    
    projects_table.insert_row(vec![
        Value::Text("123002".to_string()),
        Value::Text("Project 2024".to_string()),
        Value::Date(NaiveDate::parse_from_str("2024-03-10", "%Y-%m-%d").unwrap()),
    ]).unwrap();
    
    projects_table.insert_row(vec![
        Value::Text("123003".to_string()),
        Value::Text("Project 2025".to_string()),
        Value::Date(NaiveDate::parse_from_str("2025-01-20", "%Y-%m-%d").unwrap()),
    ]).unwrap();
    
    projects_table.insert_row(vec![
        Value::Text("123004".to_string()),
        Value::Text("Project 2026".to_string()),
        Value::Date(NaiveDate::parse_from_str("2026-02-01", "%Y-%m-%d").unwrap()),
    ]).unwrap();

    db.add_table(projects_table).unwrap();

    let storage = Storage::new(db);
    let storage_arc = Arc::new(storage);
    let executor = QueryExecutor::new(storage_arc).await.unwrap();

    // Test DATE literals with BETWEEN in CTE
    let query = parse_sql(
        r#"
        WITH FilteredByDateRange AS (
            SELECT p.*
            FROM SF_PROJECT_V2 p
            WHERE p.START_DATE BETWEEN DATE '2024-01-01' AND DATE '2025-12-31'
        )
        SELECT SAP_PROJECT_ID, PROJECT_NAME FROM FilteredByDateRange ORDER BY START_DATE
        "#,
    ).unwrap();

    let result = executor.execute(&query[0]).await;
    match result {
        Ok(res) => {
            println!("✅ DATE literals with BETWEEN in CTE works!");
            assert_eq!(res.rows.len(), 2);
            // Should return projects from 2024 and 2025
            assert_eq!(res.rows[0][0], Value::Text("123002".to_string()));
            assert_eq!(res.rows[1][0], Value::Text("123003".to_string()));
        }
        Err(e) => {
            panic!("DATE literals with BETWEEN in CTE should work: {}", e);
        }
    }
}

#[tokio::test]
async fn test_date_literals_in_nested_ctes() {
    println!("=== DATE LITERALS IN NESTED CTEs TEST ===");
    
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
                name: "start_date".to_string(),
                sql_type: SqlType::Date,
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
    projects_table.insert_row(vec![
        Value::Text("123001".to_string()),
        Value::Text("Main Project 1".to_string()),
        Value::Date(NaiveDate::parse_from_str("2025-01-15", "%Y-%m-%d").unwrap()),
        Value::Text("Project".to_string()),
    ]).unwrap();
    
    projects_table.insert_row(vec![
        Value::Text("123002".to_string()),
        Value::Text("Main Project 2".to_string()),
        Value::Date(NaiveDate::parse_from_str("2003-06-10", "%Y-%m-%d").unwrap()),
        Value::Text("Project".to_string()),
    ]).unwrap();
    
    projects_table.insert_row(vec![
        Value::Text("123003".to_string()),
        Value::Text("Sub Project 1".to_string()),
        Value::Date(NaiveDate::parse_from_str("2025-02-01", "%Y-%m-%d").unwrap()),
        Value::Text("Sub Project".to_string()),
    ]).unwrap();

    db.add_table(projects_table).unwrap();

    let storage = Storage::new(db);
    let storage_arc = Arc::new(storage);
    let executor = QueryExecutor::new(storage_arc).await.unwrap();

    // Test DATE literals in multiple CTEs (simplified without InSubquery)
    let query = parse_sql(
        r#"
        WITH AllProjects AS (
            SELECT SAP_PROJECT_ID AS MAIN_PROJECT_ID, PROJECT_NAME, START_DATE
            FROM SF_PROJECT_V2
            WHERE START_DATE >= DATE '2004-10-05'
              AND PROJECT_STRUCTURE = 'Project'
        ),
        RecentProjects AS (
            SELECT MAIN_PROJECT_ID
            FROM AllProjects
            WHERE START_DATE >= DATE '2025-01-01'
        )
        SELECT COUNT(*) AS count FROM RecentProjects
        "#,
    ).unwrap();

    let result = executor.execute(&query[0]).await;
    match result {
        Ok(res) => {
            println!("✅ DATE literals in nested CTEs work!");
            assert_eq!(res.rows.len(), 1);
            assert_eq!(res.rows[0][0], Value::Integer(1)); // Should count 1 recent main project
        }
        Err(e) => {
            panic!("DATE literals in nested CTEs should work: {}", e);
        }
    }
    }
}