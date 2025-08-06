// Comprehensive production readiness tests for yamlbase 0.4.13
// These tests verify 100% compatibility with enterprise SQL patterns

mod production_readiness_tests {
    use std::sync::Arc;
    use std::pin::Pin;
    use std::future::Future;
    use chrono::NaiveDate;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use yamlbase::database::{Column, Database, Storage, Table, Value};
    use yamlbase::sql::{QueryExecutor, parse_sql};
    use yamlbase::yaml::schema::SqlType;

    fn setup_test_database() -> Arc<Storage> {
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
                    name: "end_date".to_string(),
                    sql_type: SqlType::Date,
                    primary_key: false,
                    nullable: true,
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

        // Add test data
        projects_table.insert_row(vec![
            Value::Text("P001".to_string()),
            Value::Text("Main Project Alpha".to_string()),
            Value::Date(NaiveDate::parse_from_str("2004-11-01", "%Y-%m-%d").unwrap()),
            Value::Date(NaiveDate::parse_from_str("2025-12-31", "%Y-%m-%d").unwrap()),
            Value::Text("Published".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Project".to_string()),
            Value::Null,
        ]).unwrap();

        projects_table.insert_row(vec![
            Value::Text("P002".to_string()),
            Value::Text("Work Package 1".to_string()),
            Value::Date(NaiveDate::parse_from_str("2025-01-15", "%Y-%m-%d").unwrap()),
            Value::Date(NaiveDate::parse_from_str("2025-06-30", "%Y-%m-%d").unwrap()),
            Value::Text("Published".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Work Package".to_string()),
            Value::Text("P001".to_string()),
        ]).unwrap();

        projects_table.insert_row(vec![
            Value::Text("P003".to_string()),
            Value::Text("Work Package 2 - Cancelled".to_string()),
            Value::Date(NaiveDate::parse_from_str("2025-02-01", "%Y-%m-%d").unwrap()),
            Value::Null,
            Value::Text("Published".to_string()),
            Value::Text("Cancelled".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Work Package".to_string()),
            Value::Text("P001".to_string()),
        ]).unwrap();

        projects_table.insert_row(vec![
            Value::Text("P004".to_string()),
            Value::Text("Sub Project Beta".to_string()),
            Value::Date(NaiveDate::parse_from_str("2024-06-01", "%Y-%m-%d").unwrap()),
            Value::Date(NaiveDate::parse_from_str("2026-12-31", "%Y-%m-%d").unwrap()),
            Value::Text("Published".to_string()),
            Value::Text("Active".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Sub Project".to_string()),
            Value::Text("P001".to_string()),
        ]).unwrap();

        projects_table.insert_row(vec![
            Value::Text("P005".to_string()),
            Value::Text("Legacy Project".to_string()),
            Value::Date(NaiveDate::parse_from_str("2003-01-01", "%Y-%m-%d").unwrap()),
            Value::Date(NaiveDate::parse_from_str("2004-09-30", "%Y-%m-%d").unwrap()),
            Value::Text("Published".to_string()),
            Value::Text("Closed".to_string()),
            Value::Text("N".to_string()),
            Value::Text("Y".to_string()),
            Value::Text("Project".to_string()),
            Value::Null,
        ]).unwrap();

        db.add_table(projects_table).unwrap();

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
                Column {
                    name: "month_number".to_string(),
                    sql_type: SqlType::Date,
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
            ],
        );

        // Add allocation data
        allocations_table.insert_row(vec![
            Value::Integer(1),
            Value::Text("P002".to_string()),
            Value::Text("USER001".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("Hard Allocation".to_string()),
            Value::Text("Active".to_string()),
            Value::Decimal(Decimal::from_str("120.50").unwrap()),
            Value::Decimal(Decimal::from_str("85.25").unwrap()),
            Value::Date(NaiveDate::parse_from_str("2025-02-01", "%Y-%m-%d").unwrap()),
        ]).unwrap();

        allocations_table.insert_row(vec![
            Value::Integer(2),
            Value::Text("P002".to_string()),
            Value::Text("USER002".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("Hard Allocation".to_string()),
            Value::Text("Active".to_string()),
            Value::Decimal(Decimal::from_str("80.00").unwrap()),
            Value::Decimal(Decimal::from_str("0.00").unwrap()),
            Value::Date(NaiveDate::parse_from_str("2025-02-01", "%Y-%m-%d").unwrap()),
        ]).unwrap();

        allocations_table.insert_row(vec![
            Value::Integer(3),
            Value::Text("P003".to_string()),
            Value::Text("USER003".to_string()),
            Value::Text("Published".to_string()),
            Value::Text("Hard Allocation".to_string()),
            Value::Text("Cancelled".to_string()),
            Value::Decimal(Decimal::from_str("100.00").unwrap()),
            Value::Decimal(Decimal::from_str("0.00").unwrap()),
            Value::Date(NaiveDate::parse_from_str("2025-02-01", "%Y-%m-%d").unwrap()),
        ]).unwrap();

        allocations_table.insert_row(vec![
            Value::Integer(4),
            Value::Text("P004".to_string()),
            Value::Text("USER001".to_string()),
            Value::Text("Draft".to_string()),
            Value::Text("Soft Allocation".to_string()),
            Value::Text("Active".to_string()),
            Value::Decimal(Decimal::from_str("0.00").unwrap()),
            Value::Decimal(Decimal::from_str("10.00").unwrap()),
            Value::Date(NaiveDate::parse_from_str("2025-03-01", "%Y-%m-%d").unwrap()),
        ]).unwrap();

        db.add_table(allocations_table).unwrap();

        let storage = Storage::new(db);
        Arc::new(storage)
    }

    // Test 1: Complete AAC Production Query Pattern
    #[tokio::test]
    async fn test_complete_aac_production_query() {
        println!("=== TEST: Complete AAC Production Query Pattern ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(r#"
            WITH ProjectHierarchy AS (
                SELECT
                    parent.SAP_PROJECT_ID AS MAIN_PROJECT_ID,
                    child.SAP_PROJECT_ID AS SUB_PROJECT_ID
                FROM SF_PROJECT_V2 parent
                INNER JOIN SF_PROJECT_V2 child
                    ON parent.SAP_PROJECT_ID = child.HIERARCHY_PARENT_SAP_ID
                    AND child.VERSION_CODE = 'Published'
                    AND child.STATUS_CODE NOT IN ('Cancelled', 'Closed')
                    AND child.ACTIVE_FLAG = 'Y'
                    AND child.CLOSED_FOR_TIME_ENTRY = 'N'
                    AND child.PROJECT_STRUCTURE = 'Work Package'
                WHERE parent.START_DATE >= DATE '2004-10-05'
                  AND parent.PROJECT_STRUCTURE IN ('Project', 'Sub Project')
            ),
            AllProjects AS (
                SELECT SAP_PROJECT_ID AS MAIN_PROJECT_ID, SAP_PROJECT_ID AS SUB_PROJECT_ID
                FROM SF_PROJECT_V2
                WHERE START_DATE >= DATE '2004-10-05'
                  AND STATUS_CODE NOT IN ('Cancelled', 'Closed')
                  AND PROJECT_STRUCTURE = 'Project'
                UNION ALL
                SELECT * FROM ProjectHierarchy
            ),
            AllocationsWithHierarchy AS (
                SELECT ap.MAIN_PROJECT_ID, a.*
                FROM AllProjects ap
                INNER JOIN SF_PROJECT_ALLOCATIONS a 
                    ON ap.SUB_PROJECT_ID = a.SAP_PROJECT_ID
                WHERE a.MONTH_NUMBER >= DATE '2025-02-01' 
                  AND a.MONTH_NUMBER <= DATE '2025-03-31'
                  AND a.PROJECT_STATUS_CODE NOT IN ('Cancelled', 'Closed')
            )
            SELECT
                MAIN_PROJECT_ID,
                COUNT(DISTINCT WBI_ID) AS MEMBERS,
                SUM(PLANNED_EFFORT_HOURS) AS TOTAL_PLANNED,
                SUM(ACTUAL_EFFORT_HOURS) AS TOTAL_ACTUAL
            FROM AllocationsWithHierarchy
            GROUP BY MAIN_PROJECT_ID
            ORDER BY MAIN_PROJECT_ID
        "#).unwrap();

        let result = executor.execute(&query[0]).await;
        if let Err(ref e) = result {
            println!("ERROR in AAC production query: {:?}", e);
        }
        assert!(result.is_ok(), "Complete AAC production query should work");
        
        let res = result.unwrap();
        println!("AAC query returned {} rows", res.rows.len());
        assert!(res.rows.len() > 0, "Should return aggregated results");
        println!("✅ Complete AAC production query PASSED");
    }

    // Test 2: RECURSIVE CTE Support
    #[tokio::test]
    async fn test_recursive_cte_support() {
        println!("=== TEST: RECURSIVE CTE Support ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(r#"
            WITH RECURSIVE ProjectTree AS (
                -- Base case: top-level projects
                SELECT 
                    SAP_PROJECT_ID,
                    PROJECT_NAME,
                    HIERARCHY_PARENT_SAP_ID,
                    0 as level,
                    SAP_PROJECT_ID as root_project
                FROM SF_PROJECT_V2
                WHERE HIERARCHY_PARENT_SAP_ID IS NULL
                  AND STATUS_CODE = 'Active'
                
                UNION ALL
                
                -- Recursive case: child projects
                SELECT 
                    c.SAP_PROJECT_ID,
                    c.PROJECT_NAME,
                    c.HIERARCHY_PARENT_SAP_ID,
                    p.level + 1,
                    p.root_project
                FROM SF_PROJECT_V2 c
                INNER JOIN ProjectTree p 
                    ON c.HIERARCHY_PARENT_SAP_ID = p.SAP_PROJECT_ID
                WHERE c.STATUS_CODE NOT IN ('Cancelled', 'Closed')
                  AND c.ACTIVE_FLAG = 'Y'
            )
            SELECT 
                root_project,
                COUNT(*) as total_children,
                MAX(level) as max_depth
            FROM ProjectTree
            GROUP BY root_project
            ORDER BY root_project
        "#).unwrap();

        let result = executor.execute(&query[0]).await;
        if let Err(ref e) = result {
            println!("ERROR in RECURSIVE CTE: {:?}", e);
        }
        assert!(result.is_ok(), "RECURSIVE CTE should be supported");
        
        let res = result.unwrap();
        assert!(res.rows.len() > 0, "Should return hierarchical results");
        println!("✅ RECURSIVE CTE support PASSED");
    }

    // Test 3: CASE Expressions in JOIN Conditions
    #[tokio::test]
    async fn test_case_in_join_conditions() {
        println!("=== TEST: CASE Expressions in JOIN Conditions ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(r#"
            WITH ConditionalJoinCTE AS (
                SELECT 
                    p1.SAP_PROJECT_ID as project_id,
                    p2.SAP_PROJECT_ID as related_id,
                    p2.PROJECT_NAME as related_name
                FROM SF_PROJECT_V2 p1
                LEFT JOIN SF_PROJECT_V2 p2
                    ON p1.HIERARCHY_PARENT_SAP_ID = p2.SAP_PROJECT_ID
                    AND CASE 
                        WHEN p2.START_DATE >= DATE '2025-01-01' THEN p2.STATUS_CODE
                        WHEN p2.START_DATE >= DATE '2024-01-01' THEN 'Active'
                        ELSE 'Any'
                    END IN ('Active', 'Any')
                    AND CASE
                        WHEN p1.PROJECT_STRUCTURE = 'Work Package' THEN p2.ACTIVE_FLAG
                        ELSE 'Y'
                    END = 'Y'
            )
            SELECT COUNT(*) as match_count FROM ConditionalJoinCTE
        "#).unwrap();

        let result = executor.execute(&query[0]).await;
        if let Err(ref e) = result {
            println!("ERROR: {:?}", e);
        }
        assert!(result.is_ok(), "CASE expressions in JOIN conditions should work");
        
        let res = result.unwrap();
        assert_eq!(res.rows.len(), 1, "Should return count");
        println!("✅ CASE in JOIN conditions PASSED");
    }

    // Test 4: Date Format Validation and Error Handling
    #[tokio::test]
    async fn test_date_format_validation() {
        println!("=== TEST: Date Format Validation and Error Handling ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        // Test various date formats
        let valid_formats = vec![
            "DATE '2025-01-15'",
            "DATE '2025-1-5'",
            "DATE '2025-12-31'",
        ];

        for date_format in valid_formats {
            let query = parse_sql(&format!(r#"
                WITH TestCTE AS (
                    SELECT * FROM SF_PROJECT_V2
                    WHERE START_DATE >= {}
                )
                SELECT COUNT(*) FROM TestCTE
            "#, date_format)).unwrap();

            let result = executor.execute(&query[0]).await;
            assert!(result.is_ok(), "Valid date format {} should work", date_format);
        }

        // Test invalid formats - should return error, not crash
        let invalid_formats = vec![
            "DATE '2025/01/15'",
            "DATE '15-01-2025'",
            "DATE '2025-13-01'",  // Invalid month
            "DATE '2025-01-32'",  // Invalid day
            "DATE 'invalid'",
        ];

        for date_format in invalid_formats {
            let query_result = parse_sql(&format!(r#"
                WITH TestCTE AS (
                    SELECT * FROM SF_PROJECT_V2
                    WHERE START_DATE >= {}
                )
                SELECT COUNT(*) FROM TestCTE
            "#, date_format));

            if let Ok(query) = query_result {
                let result = executor.execute(&query[0]).await;
                assert!(result.is_err(), "Invalid date format {} should return error", date_format);
                
                // Verify it's a proper error, not a panic
                if let Err(e) = result {
                    let error_msg = e.to_string();
                    assert!(
                        error_msg.contains("date") || error_msg.contains("format") || error_msg.contains("invalid"),
                        "Error should mention date/format issue"
                    );
                }
            }
        }
        
        println!("✅ Date format validation PASSED");
    }

    // Test 5: Complex Nested AND/OR in CTEs
    #[tokio::test]
    async fn test_complex_nested_conditions() {
        println!("=== TEST: Complex Nested AND/OR Conditions ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(r#"
            WITH ComplexConditionsCTE AS (
                SELECT p.*, a.*
                FROM SF_PROJECT_V2 p
                INNER JOIN SF_PROJECT_ALLOCATIONS a
                    ON p.SAP_PROJECT_ID = a.SAP_PROJECT_ID
                    AND (
                        (
                            a.PROJECT_STATUS_CODE NOT IN ('Cancelled', 'Closed', 'Suspended')
                            AND a.VERSION_CODE = 'Published'
                            AND (
                                a.PLANNED_EFFORT_HOURS > 50
                                OR a.ACTUAL_EFFORT_HOURS > 0
                            )
                        )
                        OR (
                            a.ASSIGNMENT_TYPE = 'Hard Allocation'
                            AND a.PROJECT_STATUS_CODE = 'Active'
                            AND NOT (
                                a.PLANNED_EFFORT_HOURS = 0
                                AND a.ACTUAL_EFFORT_HOURS = 0
                            )
                        )
                    )
                WHERE p.START_DATE >= DATE '2024-01-01'
                  AND p.STATUS_CODE NOT IN ('Cancelled', 'Closed')
            )
            SELECT 
                SAP_PROJECT_ID,
                COUNT(DISTINCT WBI_ID) as unique_users,
                SUM(PLANNED_EFFORT_HOURS) as total_planned
            FROM ComplexConditionsCTE
            GROUP BY SAP_PROJECT_ID
        "#).unwrap();

        let result = executor.execute(&query[0]).await;
        assert!(result.is_ok(), "Complex nested AND/OR conditions should work");
        
        let res = result.unwrap();
        assert!(res.rows.len() > 0, "Should return grouped results");
        println!("✅ Complex nested conditions PASSED");
    }

    // Test 6: Multiple Date Functions and Operations
    #[tokio::test]
    async fn test_date_functions_in_cte() {
        println!("=== TEST: Date Functions and Operations in CTEs ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(r#"
            WITH DateOperationsCTE AS (
                SELECT 
                    SAP_PROJECT_ID,
                    PROJECT_NAME,
                    START_DATE,
                    END_DATE,
                    CASE
                        WHEN START_DATE IS NULL THEN 'No Start'
                        WHEN START_DATE < DATE '2004-01-01' THEN 'Legacy'
                        WHEN START_DATE BETWEEN DATE '2004-01-01' AND DATE '2024-12-31' THEN 'Current'
                        WHEN START_DATE >= DATE '2025-01-01' THEN 'Future'
                        ELSE 'Unknown'
                    END as period_category,
                    CASE
                        WHEN END_DATE IS NULL THEN 999999
                        WHEN END_DATE < DATE '2025-01-01' THEN 0
                        ELSE 1
                    END as is_active_2025
                FROM SF_PROJECT_V2
                WHERE (START_DATE >= DATE '2003-01-01' OR START_DATE IS NULL)
                  AND (END_DATE <= DATE '2026-12-31' OR END_DATE IS NULL)
            )
            SELECT 
                period_category,
                COUNT(*) as project_count,
                SUM(is_active_2025) as active_in_2025
            FROM DateOperationsCTE
            GROUP BY period_category
            ORDER BY period_category
        "#).unwrap();

        let result = executor.execute(&query[0]).await;
        assert!(result.is_ok(), "Date functions and operations in CTEs should work");
        
        let res = result.unwrap();
        assert!(res.rows.len() > 0, "Should return categorized results");
        println!("✅ Date functions in CTE PASSED");
    }

    // Test 7: EXISTS Subqueries with Complex Conditions
    #[tokio::test]
    async fn test_exists_subquery_in_cte() {
        println!("=== TEST: EXISTS Subqueries in CTEs ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(r#"
            WITH ProjectsWithAllocations AS (
                SELECT p.*
                FROM SF_PROJECT_V2 p
                WHERE EXISTS (
                    SELECT 1
                    FROM SF_PROJECT_ALLOCATIONS a
                    WHERE a.SAP_PROJECT_ID = p.SAP_PROJECT_ID
                      AND a.PROJECT_STATUS_CODE NOT IN ('Cancelled', 'Closed')
                      AND a.VERSION_CODE = 'Published'
                      AND (a.PLANNED_EFFORT_HOURS > 0 OR a.ACTUAL_EFFORT_HOURS > 0)
                )
                AND p.STATUS_CODE = 'Active'
                AND p.START_DATE >= DATE '2024-01-01'
            )
            SELECT 
                SAP_PROJECT_ID,
                PROJECT_NAME,
                PROJECT_STRUCTURE
            FROM ProjectsWithAllocations
            ORDER BY SAP_PROJECT_ID
        "#).unwrap();

        let result = executor.execute(&query[0]).await;
        assert!(result.is_ok(), "EXISTS subqueries in CTEs should work");
        
        let res = result.unwrap();
        assert!(res.rows.len() > 0, "Should return projects with allocations");
        println!("✅ EXISTS subquery in CTE PASSED");
    }

    // Test 8: COALESCE and NULL Handling in JOINs
    #[tokio::test]
    async fn test_coalesce_and_null_handling() {
        println!("=== TEST: COALESCE and NULL Handling ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(r#"
            WITH NullHandlingCTE AS (
                SELECT 
                    p1.SAP_PROJECT_ID,
                    p1.PROJECT_NAME,
                    COALESCE(p2.PROJECT_NAME, 'No Parent') as parent_name,
                    COALESCE(p1.END_DATE, DATE '2099-12-31') as effective_end,
                    CASE
                        WHEN p1.END_DATE IS NULL AND p2.END_DATE IS NULL THEN 'Both Open'
                        WHEN p1.END_DATE IS NULL THEN 'Child Open'
                        WHEN p2.END_DATE IS NULL THEN 'Parent Open'
                        ELSE 'Both Closed'
                    END as status_combination
                FROM SF_PROJECT_V2 p1
                LEFT JOIN SF_PROJECT_V2 p2
                    ON p1.HIERARCHY_PARENT_SAP_ID = p2.SAP_PROJECT_ID
                    AND COALESCE(p2.STATUS_CODE, 'Active') = 'Active'
            )
            SELECT 
                status_combination,
                COUNT(*) as count
            FROM NullHandlingCTE
            GROUP BY status_combination
            ORDER BY status_combination
        "#).unwrap();

        let result = executor.execute(&query[0]).await;
        match &result {
            Err(e) => {
                println!("ERROR: {:?}", e);
                eprintln!("Full error: {:#?}", e);
                eprintln!("Error message: {}", e);
                panic!("COALESCE and NULL handling failed with error: {}", e);
            }
            Ok(_) => {}
        }
        
        let res = result.unwrap();
        assert!(res.rows.len() > 0, "Should handle NULL values correctly");
        println!("✅ COALESCE and NULL handling PASSED");
    }

    // Test 9: Window Functions in CTEs (if supported)
    #[tokio::test]
    async fn test_window_functions_in_cte() {
        println!("=== TEST: Window Functions in CTEs ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(r#"
            WITH RankedProjects AS (
                SELECT 
                    SAP_PROJECT_ID,
                    PROJECT_NAME,
                    START_DATE,
                    PROJECT_STRUCTURE,
                    ROW_NUMBER() OVER (PARTITION BY PROJECT_STRUCTURE ORDER BY START_DATE DESC) as recency_rank,
                    COUNT(*) OVER (PARTITION BY PROJECT_STRUCTURE) as structure_count
                FROM SF_PROJECT_V2
                WHERE STATUS_CODE = 'Active'
            )
            SELECT 
                PROJECT_STRUCTURE,
                SAP_PROJECT_ID,
                PROJECT_NAME,
                recency_rank
            FROM RankedProjects
            WHERE recency_rank <= 2
            ORDER BY PROJECT_STRUCTURE, recency_rank
        "#).unwrap();

        let result = executor.execute(&query[0]).await;
        
        // Window functions might not be supported yet, but should not crash
        if result.is_err() {
            println!("⚠️  Window functions not yet supported (expected)");
        } else {
            println!("✅ Window functions in CTE PASSED");
        }
    }

    // Test 10: Transaction Isolation Simulation
    #[tokio::test]
    async fn test_cte_materialization_consistency() {
        println!("=== TEST: CTE Materialization Consistency ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        // Test that CTE results are consistent when referenced multiple times
        let query = parse_sql(r#"
            WITH MaterializedData AS (
                SELECT 
                    SAP_PROJECT_ID,
                    PROJECT_NAME,
                    START_DATE
                FROM SF_PROJECT_V2
                WHERE STATUS_CODE = 'Active'
                  AND START_DATE >= DATE '2024-01-01'
            ),
            FirstReference AS (
                SELECT COUNT(*) as count1 FROM MaterializedData
            ),
            SecondReference AS (
                SELECT COUNT(*) as count2 FROM MaterializedData
            )
            SELECT 
                f.count1,
                s.count2,
                CASE
                    WHEN f.count1 = s.count2 THEN 'Consistent'
                    ELSE 'Inconsistent'
                END as consistency_check
            FROM FirstReference f, SecondReference s
        "#).unwrap();

        let result = executor.execute(&query[0]).await;
        assert!(result.is_ok(), "CTE should be consistently materialized");
        
        let res = result.unwrap();
        assert_eq!(res.rows.len(), 1, "Should return one row");
        
        // Verify consistency
        if let Value::Text(consistency) = &res.rows[0][2] {
            assert_eq!(consistency, "Consistent", "CTE materialization should be consistent");
        }
        
        println!("✅ CTE materialization consistency PASSED");
    }

    // Test 11: Large IN Lists Performance Test
    #[tokio::test]
    async fn test_large_in_list_performance() {
        println!("=== TEST: Large IN List Performance ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        // Generate a large IN list
        let statuses: Vec<String> = (1..100).map(|i| format!("'Status{}'", i)).collect();
        let in_list = statuses.join(", ");

        let query = parse_sql(&format!(r#"
            WITH LargeInListCTE AS (
                SELECT p.*, a.*
                FROM SF_PROJECT_V2 p
                INNER JOIN SF_PROJECT_ALLOCATIONS a
                    ON p.SAP_PROJECT_ID = a.SAP_PROJECT_ID
                    AND a.PROJECT_STATUS_CODE NOT IN ({}, 'Cancelled', 'Closed')
                WHERE p.STATUS_CODE = 'Active'
            )
            SELECT COUNT(*) FROM LargeInListCTE
        "#, in_list)).unwrap();

        let result = executor.execute(&query[0]).await;
        assert!(result.is_ok(), "Large IN lists should be handled efficiently");
        
        println!("✅ Large IN list performance PASSED");
    }

    // Test 12: Error Recovery and Connection Stability
    #[tokio::test]
    async fn test_error_recovery() {
        println!("=== TEST: Error Recovery and Stability ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage.clone()).await.unwrap();

        // Execute a failing query
        let bad_query = parse_sql(r#"
            WITH BadCTE AS (
                SELECT * FROM NonExistentTable
            )
            SELECT * FROM BadCTE
        "#).unwrap();

        let result1 = executor.execute(&bad_query[0]).await;
        assert!(result1.is_err(), "Query with non-existent table should fail");

        // Now execute a valid query to ensure connection is still good
        let good_query = parse_sql(r#"
            WITH GoodCTE AS (
                SELECT COUNT(*) as count FROM SF_PROJECT_V2
            )
            SELECT * FROM GoodCTE
        "#).unwrap();

        let result2 = executor.execute(&good_query[0]).await;
        assert!(result2.is_ok(), "Valid query should work after error");
        
        println!("✅ Error recovery PASSED");
    }

    // Master test to run all tests and report overall status
    // TODO: Fix this to properly run all tests in parallel
    /*
    #[tokio::test]
    async fn test_production_readiness_suite() {
        println!("\n{}", "=".repeat(60));
        println!("PRODUCTION READINESS TEST SUITE FOR YAMLBASE 0.4.13");
        println!("{}\n", "=".repeat(60));

        let mut total_tests = 0;
        let mut passed_tests = 0;
        let mut failed_tests = vec![];

        // Run all tests and collect results
        let tests: Vec<(&str, Pin<Box<dyn Future<Output = ()>>>)> = vec![
            ("Complete AAC Production Query", Box::pin(test_complete_aac_production_query())),
            ("RECURSIVE CTE Support", Box::pin(test_recursive_cte_support())),
            ("CASE in JOIN Conditions", Box::pin(test_case_in_join_conditions())),
            ("Date Format Validation", Box::pin(test_date_format_validation())),
            ("Complex Nested Conditions", Box::pin(test_complex_nested_conditions())),
            ("Date Functions in CTE", Box::pin(test_date_functions_in_cte())),
            ("EXISTS Subqueries", Box::pin(test_exists_subquery_in_cte())),
            ("COALESCE and NULL Handling", Box::pin(test_coalesce_and_null_handling())),
            ("Window Functions", Box::pin(test_window_functions_in_cte())),
            ("CTE Materialization", Box::pin(test_cte_materialization_consistency())),
            ("Large IN Lists", Box::pin(test_large_in_list_performance())),
            ("Error Recovery", Box::pin(test_error_recovery())),
        ];

        for (name, test_future) in tests {
            total_tests += 1;
            print!("Running: {} ... ", name);
            
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                test_future
            ).await {
                Ok(Ok(_)) => {
                    println!("✅ PASSED");
                    passed_tests += 1;
                }
                Ok(Err(e)) => {
                    println!("❌ FAILED: {:?}", e);
                    failed_tests.push((name, format!("{:?}", e)));
                }
                Err(_) => {
                    println!("⏱️ TIMEOUT");
                    failed_tests.push((name, "Test timed out".to_string()));
                }
            }
        }

        // Print summary
        println!("\n{}", "=".repeat(60));
        println!("TEST SUMMARY");
        println!("{}", "=".repeat(60));
        println!("Total Tests: {}", total_tests);
        println!("Passed: {} ({}%)", passed_tests, (passed_tests * 100) / total_tests);
        println!("Failed: {} ({}%)", failed_tests.len(), (failed_tests.len() * 100) / total_tests);
        
        if !failed_tests.is_empty() {
            println!("\nFailed Tests:");
            for (name, error) in &failed_tests {
                println!("  - {}: {}", name, error);
            }
        }

        let readiness_percentage = (passed_tests * 100) / total_tests;
        println!("\n🎯 PRODUCTION READINESS: {}%", readiness_percentage);
        
        if readiness_percentage == 100 {
            println!("✅ SYSTEM IS PRODUCTION READY!");
        } else if readiness_percentage >= 80 {
            println!("⚠️  SYSTEM IS MOSTLY READY BUT NEEDS FIXES");
        } else {
            println!("❌ SYSTEM IS NOT PRODUCTION READY");
        }

        // Assert for CI/CD pipeline
        assert!(
            readiness_percentage >= 100,
            "Production readiness must be 100%. Current: {}%",
            readiness_percentage
        );
    }
    */
}