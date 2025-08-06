mod complex_joins_in_cte_test {
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use std::sync::Arc;
    use yamlbase::database::{Column, Database, Storage, Table, Value};
    use yamlbase::sql::{QueryExecutor, parse_sql};
    use yamlbase::yaml::schema::SqlType;

    #[tokio::test]
    async fn test_complex_join_with_not_in_in_cte() {
        println!("=== COMPLEX JOIN WITH NOT IN IN CTE TEST ===");

        // Create test database
        let mut db = Database::new("test_db".to_string());

        // Create SF_PROJECT_V2 table for parent and child projects
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
        projects_table
            .insert_row(vec![
                Value::Text("123001".to_string()),
                Value::Text("Parent Project".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Active".to_string()),
                Value::Text("Y".to_string()),
                Value::Text("Project".to_string()),
                Value::Null,
            ])
            .unwrap();

        // Add valid child work package
        projects_table
            .insert_row(vec![
                Value::Text("123002".to_string()),
                Value::Text("Child Work Package 1".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Active".to_string()),
                Value::Text("Y".to_string()),
                Value::Text("Work Package".to_string()),
                Value::Text("123001".to_string()),
            ])
            .unwrap();

        // Add cancelled child (should be excluded)
        projects_table
            .insert_row(vec![
                Value::Text("123003".to_string()),
                Value::Text("Cancelled Work Package".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Cancelled".to_string()),
                Value::Text("Y".to_string()),
                Value::Text("Work Package".to_string()),
                Value::Text("123001".to_string()),
            ])
            .unwrap();

        // Add inactive child (should be excluded)
        projects_table
            .insert_row(vec![
                Value::Text("123004".to_string()),
                Value::Text("Inactive Work Package".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Active".to_string()),
                Value::Text("N".to_string()),
                Value::Text("Work Package".to_string()),
                Value::Text("123001".to_string()),
            ])
            .unwrap();

        // Add sub project child
        projects_table
            .insert_row(vec![
                Value::Text("123005".to_string()),
                Value::Text("Sub Project 1".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Active".to_string()),
                Value::Text("Y".to_string()),
                Value::Text("Sub Project".to_string()),
                Value::Text("123001".to_string()),
            ])
            .unwrap();

        db.add_table(projects_table).unwrap();

        let storage = Storage::new(db);
        let storage_arc = Arc::new(storage);
        let executor = QueryExecutor::new(storage_arc).await.unwrap();

        // Test complex JOIN with NOT IN and multiple AND conditions in CTE
        let query = parse_sql(
            r#"
        WITH HierarchicalData AS (
            SELECT 
                parent.SAP_PROJECT_ID AS parent_id,
                child.SAP_PROJECT_ID AS child_id,
                child.PROJECT_STRUCTURE
            FROM SF_PROJECT_V2 parent
            INNER JOIN SF_PROJECT_V2 child
                ON parent.SAP_PROJECT_ID = child.HIERARCHY_PARENT_SAP_ID
                AND child.VERSION_CODE = 'Published'
                AND child.STATUS_CODE NOT IN ('Cancelled', 'Closed', 'Suspended')
                AND child.ACTIVE_FLAG = 'Y'
                AND child.PROJECT_STRUCTURE IN ('Work Package', 'Sub Project')
        )
        SELECT parent_id, child_id, PROJECT_STRUCTURE 
        FROM HierarchicalData 
        ORDER BY child_id
        "#,
        )
        .unwrap();

        let result = executor.execute(&query[0]).await;
        match result {
            Ok(res) => {
                println!("✅ Complex JOIN with NOT IN in CTE works!");
                assert_eq!(res.rows.len(), 2);
                // Should return only valid children
                assert_eq!(res.rows[0][1], Value::Text("123002".to_string())); // Active work package
                assert_eq!(res.rows[1][1], Value::Text("123005".to_string())); // Sub project
            }
            Err(e) => {
                panic!("Complex JOIN with NOT IN in CTE should work: {e}");
            }
        }
    }

    #[tokio::test]
    async fn test_multi_table_join_with_complex_conditions_in_cte() {
        println!("=== MULTI-TABLE JOIN WITH COMPLEX CONDITIONS IN CTE TEST ===");

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
            ],
        );

        // Create SF_PROJECT_ALLOCATIONS table
        let mut allocations_table = Table::new(
            "sf_project_allocations".to_string(),
            vec![
                Column {
                    name: "allocation_id".to_string(),
                    sql_type: SqlType::Integer,
                    primary_key: true,
                    nullable: false,
                    unique: true,
                    default: None,
                    references: None,
                },
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

        // Add project data
        projects_table
            .insert_row(vec![
                Value::Text("123001".to_string()),
                Value::Text("Project Alpha".to_string()),
                Value::Text("Published".to_string()),
            ])
            .unwrap();

        projects_table
            .insert_row(vec![
                Value::Text("123002".to_string()),
                Value::Text("Project Beta".to_string()),
                Value::Text("Published".to_string()),
            ])
            .unwrap();

        // Add allocation data - mix of valid and invalid
        allocations_table
            .insert_row(vec![
                Value::Integer(1),
                Value::Text("123001".to_string()),
                Value::Text("USER001".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Hard Allocation".to_string()),
                Value::Text("Active".to_string()),
                Value::Decimal(Decimal::from_str("40.0").unwrap()),
                Value::Decimal(Decimal::from_str("35.0").unwrap()),
            ])
            .unwrap();

        allocations_table
            .insert_row(vec![
                Value::Integer(2),
                Value::Text("123001".to_string()),
                Value::Text("USER002".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Hard Allocation".to_string()),
                Value::Text("Cancelled".to_string()), // Should be excluded
                Value::Decimal(Decimal::from_str("20.0").unwrap()),
                Value::Decimal(Decimal::from_str("0.0").unwrap()),
            ])
            .unwrap();

        allocations_table
            .insert_row(vec![
                Value::Integer(3),
                Value::Text("123002".to_string()),
                Value::Text("USER001".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Soft Allocation".to_string()), // Different type
                Value::Text("Active".to_string()),
                Value::Decimal(Decimal::from_str("0.0").unwrap()),
                Value::Decimal(Decimal::from_str("10.0").unwrap()),
            ])
            .unwrap();

        allocations_table
            .insert_row(vec![
                Value::Integer(4),
                Value::Text("123002".to_string()),
                Value::Text("USER003".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Hard Allocation".to_string()),
                Value::Text("Active".to_string()),
                Value::Decimal(Decimal::from_str("0.0").unwrap()),
                Value::Decimal(Decimal::from_str("0.0").unwrap()), // No hours - should be excluded
            ])
            .unwrap();

        db.add_table(projects_table).unwrap();
        db.add_table(allocations_table).unwrap();

        let storage = Storage::new(db);
        let storage_arc = Arc::new(storage);
        let executor = QueryExecutor::new(storage_arc).await.unwrap();

        // Test multi-table JOIN with complex conditions in CTE
        let query = parse_sql(
            r#"
        WITH AllocationHierarchy AS (
            SELECT 
                p.SAP_PROJECT_ID,
                p.PROJECT_NAME,
                a.WBI_ID,
                a.PLANNED_EFFORT_HOURS,
                a.ACTUAL_EFFORT_HOURS
            FROM SF_PROJECT_V2 p
            INNER JOIN SF_PROJECT_ALLOCATIONS a
                ON p.SAP_PROJECT_ID = a.SAP_PROJECT_ID
                AND a.VERSION_CODE = 'Published'
                AND a.ASSIGNMENT_TYPE = 'Hard Allocation'
                AND a.PROJECT_STATUS_CODE NOT IN ('Cancelled', 'Closed')
                AND (a.PLANNED_EFFORT_HOURS > 0 OR a.ACTUAL_EFFORT_HOURS > 0)
            WHERE p.VERSION_CODE = 'Published'
        )
        SELECT SAP_PROJECT_ID, COUNT(DISTINCT WBI_ID) AS unique_members
        FROM AllocationHierarchy
        GROUP BY SAP_PROJECT_ID
        ORDER BY SAP_PROJECT_ID
        "#,
        )
        .unwrap();

        let result = executor.execute(&query[0]).await;
        match result {
            Ok(res) => {
                println!("✅ Multi-table JOIN with complex conditions in CTE works!");
                assert_eq!(res.rows.len(), 1);
                // Should only return Project Alpha with USER001 (only valid allocation)
                assert_eq!(res.rows[0][0], Value::Text("123001".to_string()));
                assert_eq!(res.rows[0][1], Value::Integer(1));
            }
            Err(e) => {
                panic!("Multi-table JOIN with complex conditions in CTE should work: {e}");
            }
        }
    }

    #[tokio::test]
    async fn test_aac_production_query_pattern() {
        println!("=== AAC PRODUCTION QUERY PATTERN TEST ===");

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
                    name: "closed_for_time_entry".to_string(),
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
        projects_table
            .insert_row(vec![
                Value::Text("MAIN001".to_string()),
                Value::Text("Main Project 1".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Active".to_string()),
                Value::Text("Y".to_string()),
                Value::Text("N".to_string()),
                Value::Text("Project".to_string()),
                Value::Null,
            ])
            .unwrap();

        // Add valid work package
        projects_table
            .insert_row(vec![
                Value::Text("WP001".to_string()),
                Value::Text("Work Package 1".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Active".to_string()),
                Value::Text("Y".to_string()),
                Value::Text("N".to_string()),
                Value::Text("Work Package".to_string()),
                Value::Text("MAIN001".to_string()),
            ])
            .unwrap();

        // Add work package that is closed for time entry (should be excluded)
        projects_table
            .insert_row(vec![
                Value::Text("WP002".to_string()),
                Value::Text("Closed Work Package".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Active".to_string()),
                Value::Text("Y".to_string()),
                Value::Text("Y".to_string()), // Closed for time entry
                Value::Text("Work Package".to_string()),
                Value::Text("MAIN001".to_string()),
            ])
            .unwrap();

        db.add_table(projects_table).unwrap();

        let storage = Storage::new(db);
        let storage_arc = Arc::new(storage);
        let executor = QueryExecutor::new(storage_arc).await.unwrap();

        // Test AAC production query pattern with all complex conditions
        let query = parse_sql(
            r#"
        WITH ProjectHierarchy AS (
            SELECT
                parent.SAP_PROJECT_ID AS MAIN_PROJECT_ID,
                child.SAP_PROJECT_ID AS SUB_PROJECT_ID,
                child.PROJECT_NAME AS SUB_PROJECT_NAME
            FROM SF_PROJECT_V2 parent
            INNER JOIN SF_PROJECT_V2 child
                ON parent.SAP_PROJECT_ID = child.HIERARCHY_PARENT_SAP_ID
                AND child.VERSION_CODE = 'Published'
                AND child.STATUS_CODE NOT IN ('Cancelled', 'Closed')
                AND child.ACTIVE_FLAG = 'Y'
                AND child.CLOSED_FOR_TIME_ENTRY = 'N'
                AND child.PROJECT_STRUCTURE = 'Work Package'
            WHERE parent.PROJECT_STRUCTURE IN ('Project', 'Sub Project')
        )
        SELECT MAIN_PROJECT_ID, SUB_PROJECT_ID, SUB_PROJECT_NAME
        FROM ProjectHierarchy
        ORDER BY SUB_PROJECT_ID
        "#,
        )
        .unwrap();

        let result = executor.execute(&query[0]).await;
        match result {
            Ok(res) => {
                println!("✅ AAC production query pattern works!");
                assert_eq!(res.rows.len(), 1);
                // Should only return WP001 (WP002 is closed for time entry)
                assert_eq!(res.rows[0][0], Value::Text("MAIN001".to_string()));
                assert_eq!(res.rows[0][1], Value::Text("WP001".to_string()));
                assert_eq!(res.rows[0][2], Value::Text("Work Package 1".to_string()));
            }
            Err(e) => {
                panic!("Enterprise production query pattern should work: {e}");
            }
        }
    }
}
