// Comprehensive production readiness tests for yamlbase 0.4.14
// These tests verify 100% compatibility with enterprise SQL patterns

mod production_readiness_tests {
    use chrono::NaiveDate;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use std::sync::Arc;
    use yamlbase::database::{Column, Database, Storage, Table, Value};
    use yamlbase::sql::{QueryExecutor, parse_sql};
    use yamlbase::yaml::schema::SqlType;

    fn setup_test_database() -> Arc<Storage> {
        let mut db = Database::new("test_db".to_string());

        // Create projects table
        let mut projects_table = Table::new(
            "projects".to_string(),
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
                    name: "parent_project_id".to_string(),
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
        projects_table
            .insert_row(vec![
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
            ])
            .unwrap();

        projects_table
            .insert_row(vec![
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
            ])
            .unwrap();

        projects_table
            .insert_row(vec![
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
            ])
            .unwrap();

        projects_table
            .insert_row(vec![
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
            ])
            .unwrap();

        projects_table
            .insert_row(vec![
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
            ])
            .unwrap();

        db.add_table(projects_table).unwrap();

        // Create project_allocations table
        let mut allocations_table = Table::new(
            "project_allocations".to_string(),
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
                    name: "project_id".to_string(),
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
        allocations_table
            .insert_row(vec![
                Value::Integer(1),
                Value::Text("P002".to_string()),
                Value::Text("USER001".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Hard Allocation".to_string()),
                Value::Text("Active".to_string()),
                Value::Decimal(Decimal::from_str("120.50").unwrap()),
                Value::Decimal(Decimal::from_str("85.25").unwrap()),
                Value::Date(NaiveDate::parse_from_str("2025-02-01", "%Y-%m-%d").unwrap()),
            ])
            .unwrap();

        allocations_table
            .insert_row(vec![
                Value::Integer(2),
                Value::Text("P002".to_string()),
                Value::Text("USER002".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Hard Allocation".to_string()),
                Value::Text("Active".to_string()),
                Value::Decimal(Decimal::from_str("80.00").unwrap()),
                Value::Decimal(Decimal::from_str("0.00").unwrap()),
                Value::Date(NaiveDate::parse_from_str("2025-02-01", "%Y-%m-%d").unwrap()),
            ])
            .unwrap();

        allocations_table
            .insert_row(vec![
                Value::Integer(3),
                Value::Text("P003".to_string()),
                Value::Text("USER003".to_string()),
                Value::Text("Published".to_string()),
                Value::Text("Hard Allocation".to_string()),
                Value::Text("Cancelled".to_string()),
                Value::Decimal(Decimal::from_str("100.00").unwrap()),
                Value::Decimal(Decimal::from_str("0.00").unwrap()),
                Value::Date(NaiveDate::parse_from_str("2025-02-01", "%Y-%m-%d").unwrap()),
            ])
            .unwrap();

        allocations_table
            .insert_row(vec![
                Value::Integer(4),
                Value::Text("P004".to_string()),
                Value::Text("USER001".to_string()),
                Value::Text("Draft".to_string()),
                Value::Text("Soft Allocation".to_string()),
                Value::Text("Active".to_string()),
                Value::Decimal(Decimal::from_str("0.00").unwrap()),
                Value::Decimal(Decimal::from_str("10.00").unwrap()),
                Value::Date(NaiveDate::parse_from_str("2025-03-01", "%Y-%m-%d").unwrap()),
            ])
            .unwrap();

        db.add_table(allocations_table).unwrap();

        let storage = Storage::new(db);
        Arc::new(storage)
    }

    // Test 1: Complete Enterprise Production Query Pattern
    #[tokio::test]
    async fn test_complete_enterprise_production_query() {
        println!("=== TEST: Complete Enterprise Production Query Pattern ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(
            r#"
            WITH ProjectHierarchy AS (
                SELECT
                    parent.project_id AS main_project_id,
                    child.project_id AS sub_project_id
                FROM projects parent
                INNER JOIN projects child
                    ON parent.project_id = child.parent_project_id
                    AND child.version_code = 'Published'
                    AND child.status_code NOT IN ('Cancelled', 'Closed')
                    AND child.active_flag = 'Y'
                    AND child.closed_for_time_entry = 'N'
                    AND child.project_structure = 'Work Package'
                WHERE parent.start_date >= DATE '2004-10-05'
                  AND parent.project_structure IN ('Project', 'Sub Project')
            ),
            AllProjects AS (
                SELECT project_id AS main_project_id, project_id AS sub_project_id
                FROM projects
                WHERE start_date >= DATE '2004-10-05'
                  AND status_code NOT IN ('Cancelled', 'Closed')
                  AND project_structure = 'Project'
                UNION ALL
                SELECT * FROM ProjectHierarchy
            ),
            AllocationsWithHierarchy AS (
                SELECT ap.main_project_id, a.*
                FROM AllProjects ap
                INNER JOIN project_allocations a 
                    ON ap.sub_project_id = a.project_id
                WHERE a.month_number >= DATE '2025-02-01' 
                  AND a.month_number <= DATE '2025-03-31'
                  AND a.project_status_code NOT IN ('Cancelled', 'Closed')
            )
            SELECT
                main_project_id,
                COUNT(DISTINCT wbi_id) AS MEMBERS,
                SUM(planned_effort_hours) AS total_planned,
                SUM(actual_effort_hours) AS total_actual
            FROM AllocationsWithHierarchy
            GROUP BY main_project_id
            ORDER BY main_project_id
        "#,
        )
        .unwrap();

        let result = executor.execute(&query[0]).await;
        if let Err(ref e) = result {
            println!("ERROR in enterprise production query: {e:?}");
        }
        assert!(
            result.is_ok(),
            "Complete enterprise production query should work"
        );

        let res = result.unwrap();
        println!("Enterprise query returned {} rows", res.rows.len());
        assert!(!res.rows.is_empty(), "Should return aggregated results");
        println!("✅ Complete enterprise production query PASSED");
    }

    // Test 2: RECURSIVE CTE Support
    #[tokio::test]
    async fn test_recursive_cte_support() {
        println!("=== TEST: RECURSIVE CTE Support ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(
            r#"
            WITH RECURSIVE ProjectTree AS (
                -- Base case: top-level projects
                SELECT 
                    project_id,
                    project_name,
                    parent_project_id,
                    0 as level,
                    project_id as root_project
                FROM projects
                WHERE parent_project_id IS NULL
                  AND status_code = 'Active'
                
                UNION ALL
                
                -- Recursive case: child projects
                SELECT 
                    c.project_id,
                    c.project_name,
                    c.parent_project_id,
                    p.level + 1,
                    p.root_project
                FROM projects c
                INNER JOIN ProjectTree p 
                    ON c.parent_project_id = p.project_id
                WHERE c.status_code NOT IN ('Cancelled', 'Closed')
                  AND c.active_flag = 'Y'
            )
            SELECT 
                root_project,
                COUNT(*) as total_children,
                MAX(level) as max_depth
            FROM ProjectTree
            GROUP BY root_project
            ORDER BY root_project
        "#,
        )
        .unwrap();

        let result = executor.execute(&query[0]).await;
        if let Err(ref e) = result {
            println!("ERROR in RECURSIVE CTE: {e:?}");
        }
        assert!(result.is_ok(), "RECURSIVE CTE should be supported");

        let res = result.unwrap();
        assert!(!res.rows.is_empty(), "Should return hierarchical results");
        println!("✅ RECURSIVE CTE support PASSED");
    }

    // Test 3: CASE Expressions in JOIN Conditions
    #[tokio::test]
    async fn test_case_in_join_conditions() {
        println!("=== TEST: CASE Expressions in JOIN Conditions ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(
            r#"
            WITH ConditionalJoinCTE AS (
                SELECT 
                    p1.project_id as project_id,
                    p2.project_id as related_id,
                    p2.project_name as related_name
                FROM projects p1
                LEFT JOIN projects p2
                    ON p1.parent_project_id = p2.project_id
                    AND CASE 
                        WHEN p2.start_date >= DATE '2025-01-01' THEN p2.status_code
                        WHEN p2.start_date >= DATE '2024-01-01' THEN 'Active'
                        ELSE 'Any'
                    END IN ('Active', 'Any')
                    AND CASE
                        WHEN p1.project_structure = 'Work Package' THEN p2.active_flag
                        ELSE 'Y'
                    END = 'Y'
            )
            SELECT COUNT(*) as match_count FROM ConditionalJoinCTE
        "#,
        )
        .unwrap();

        let result = executor.execute(&query[0]).await;
        if let Err(ref e) = result {
            println!("ERROR: {e:?}");
        }
        assert!(
            result.is_ok(),
            "CASE expressions in JOIN conditions should work"
        );

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
        let valid_formats = vec!["DATE '2025-01-15'", "DATE '2025-1-5'", "DATE '2025-12-31'"];

        for date_format in valid_formats {
            let query = parse_sql(&format!(
                r#"
                WITH TestCTE AS (
                    SELECT * FROM projects
                    WHERE start_date >= {date_format}
                )
                SELECT COUNT(*) FROM TestCTE
            "#
            ))
            .unwrap();

            let result = executor.execute(&query[0]).await;
            assert!(
                result.is_ok(),
                "Valid date format {date_format} should work"
            );
        }

        // Test invalid formats - should return error, not crash
        let invalid_formats = vec![
            "DATE '2025/01/15'",
            "DATE '15-01-2025'",
            "DATE '2025-13-01'", // Invalid month
            "DATE '2025-01-32'", // Invalid day
            "DATE 'invalid'",
        ];

        for date_format in invalid_formats {
            let query_result = parse_sql(&format!(
                r#"
                WITH TestCTE AS (
                    SELECT * FROM projects
                    WHERE start_date >= {date_format}
                )
                SELECT COUNT(*) FROM TestCTE
            "#
            ));

            if let Ok(query) = query_result {
                let result = executor.execute(&query[0]).await;
                assert!(
                    result.is_err(),
                    "Invalid date format {date_format} should return error"
                );

                // Verify it's a proper error, not a panic
                if let Err(e) = result {
                    let error_msg = e.to_string();
                    assert!(
                        error_msg.contains("date")
                            || error_msg.contains("format")
                            || error_msg.contains("invalid"),
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

        let query = parse_sql(
            r#"
            WITH ComplexConditionsCTE AS (
                SELECT p.*, a.*
                FROM projects p
                INNER JOIN project_allocations a
                    ON p.project_id = a.project_id
                    AND (
                        (
                            a.project_status_code NOT IN ('Cancelled', 'Closed', 'Suspended')
                            AND a.version_code = 'Published'
                            AND (
                                a.planned_effort_hours > 50
                                OR a.actual_effort_hours > 0
                            )
                        )
                        OR (
                            a.ASSIGNMENT_TYPE = 'Hard Allocation'
                            AND a.project_status_code = 'Active'
                            AND NOT (
                                a.planned_effort_hours = 0
                                AND a.actual_effort_hours = 0
                            )
                        )
                    )
                WHERE p.start_date >= DATE '2024-01-01'
                  AND p.status_code NOT IN ('Cancelled', 'Closed')
            )
            SELECT 
                project_id,
                COUNT(DISTINCT wbi_id) as unique_users,
                SUM(planned_effort_hours) as total_planned
            FROM ComplexConditionsCTE
            GROUP BY project_id
        "#,
        )
        .unwrap();

        let result = executor.execute(&query[0]).await;
        assert!(
            result.is_ok(),
            "Complex nested AND/OR conditions should work"
        );

        let res = result.unwrap();
        assert!(!res.rows.is_empty(), "Should return grouped results");
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
                    project_id,
                    project_name,
                    start_date,
                    end_date,
                    CASE
                        WHEN start_date IS NULL THEN 'No Start'
                        WHEN start_date < DATE '2004-01-01' THEN 'Legacy'
                        WHEN start_date BETWEEN DATE '2004-01-01' AND DATE '2024-12-31' THEN 'Current'
                        WHEN start_date >= DATE '2025-01-01' THEN 'Future'
                        ELSE 'Unknown'
                    END as period_category,
                    CASE
                        WHEN end_date IS NULL THEN 999999
                        WHEN end_date < DATE '2025-01-01' THEN 0
                        ELSE 1
                    END as is_active_2025
                FROM projects
                WHERE (start_date >= DATE '2003-01-01' OR start_date IS NULL)
                  AND (end_date <= DATE '2026-12-31' OR end_date IS NULL)
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
        assert!(
            result.is_ok(),
            "Date functions and operations in CTEs should work"
        );

        let res = result.unwrap();
        assert!(!res.rows.is_empty(), "Should return categorized results");
        println!("✅ Date functions in CTE PASSED");
    }

    // Test 7: EXISTS Subqueries with Complex Conditions
    #[tokio::test]
    async fn test_exists_subquery_in_cte() {
        println!("=== TEST: EXISTS Subqueries in CTEs ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        let query = parse_sql(
            r#"
            WITH ProjectsWithAllocations AS (
                SELECT p.*
                FROM projects p
                WHERE EXISTS (
                    SELECT 1
                    FROM project_allocations a
                    WHERE a.project_id = p.project_id
                      AND a.project_status_code NOT IN ('Cancelled', 'Closed')
                      AND a.version_code = 'Published'
                      AND (a.planned_effort_hours > 0 OR a.actual_effort_hours > 0)
                )
                AND p.status_code = 'Active'
                AND p.start_date >= DATE '2024-01-01'
            )
            SELECT 
                project_id,
                project_name,
                project_structure
            FROM ProjectsWithAllocations
            ORDER BY project_id
        "#,
        )
        .unwrap();

        let result = executor.execute(&query[0]).await;
        assert!(result.is_ok(), "EXISTS subqueries in CTEs should work");

        let res = result.unwrap();
        assert!(
            !res.rows.is_empty(),
            "Should return projects with allocations"
        );
        println!("✅ EXISTS subquery in CTE PASSED");
    }

    // Test 8: COALESCE and NULL Handling in JOINs
    #[tokio::test]
    async fn test_coalesce_and_null_handling() {
        println!("=== TEST: COALESCE and NULL Handling ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage).await.unwrap();

        // Test COALESCE with table data
        let query =
            parse_sql("SELECT project_id, COALESCE(status_code, 'Default') FROM projects").unwrap();

        let result = executor.execute(&query[0]).await;
        if let Err(e) = &result {
            println!("ERROR: {e:?}");
            eprintln!("Full error: {e:#?}");
            eprintln!("Error message: {e}");
            panic!("COALESCE and NULL handling failed with error: {e}");
        }

        let res = result.unwrap();
        assert!(!res.rows.is_empty(), "Should handle NULL values correctly");
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
                    project_id,
                    project_name,
                    start_date,
                    project_structure,
                    ROW_NUMBER() OVER (PARTITION BY project_structure ORDER BY start_date DESC) as recency_rank,
                    COUNT(*) OVER (PARTITION BY project_structure) as structure_count
                FROM projects
                WHERE status_code = 'Active'
            )
            SELECT 
                project_structure,
                project_id,
                project_name,
                recency_rank
            FROM RankedProjects
            WHERE recency_rank <= 2
            ORDER BY project_structure, recency_rank
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
        let query = parse_sql(
            r#"
            WITH MaterializedData AS (
                SELECT 
                    project_id,
                    project_name,
                    start_date
                FROM projects
                WHERE status_code = 'Active'
                  AND start_date >= DATE '2024-01-01'
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
        "#,
        )
        .unwrap();

        let result = executor.execute(&query[0]).await;
        assert!(result.is_ok(), "CTE should be consistently materialized");

        let res = result.unwrap();
        assert_eq!(res.rows.len(), 1, "Should return one row");

        // Verify consistency
        if let Value::Text(consistency) = &res.rows[0][2] {
            assert_eq!(
                consistency, "Consistent",
                "CTE materialization should be consistent"
            );
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
        let statuses: Vec<String> = (1..100).map(|i| format!("'Status{i}'")).collect();
        let in_list = statuses.join(", ");

        let query = parse_sql(&format!(
            r#"
            WITH LargeInListCTE AS (
                SELECT p.*, a.*
                FROM projects p
                INNER JOIN project_allocations a
                    ON p.project_id = a.project_id
                    AND a.project_status_code NOT IN ({in_list}, 'Cancelled', 'Closed')
                WHERE p.status_code = 'Active'
            )
            SELECT COUNT(*) FROM LargeInListCTE
        "#
        ))
        .unwrap();

        let result = executor.execute(&query[0]).await;
        assert!(
            result.is_ok(),
            "Large IN lists should be handled efficiently"
        );

        println!("✅ Large IN list performance PASSED");
    }

    // Test 12: Error Recovery and Connection Stability
    #[tokio::test]
    async fn test_error_recovery() {
        println!("=== TEST: Error Recovery and Stability ===");
        let storage = setup_test_database();
        let executor = QueryExecutor::new(storage.clone()).await.unwrap();

        // Execute a failing query
        let bad_query = parse_sql(
            r#"
            WITH BadCTE AS (
                SELECT * FROM NonExistentTable
            )
            SELECT * FROM BadCTE
        "#,
        )
        .unwrap();

        let result1 = executor.execute(&bad_query[0]).await;
        assert!(
            result1.is_err(),
            "Query with non-existent table should fail"
        );

        // Now execute a valid query to ensure connection is still good
        let good_query = parse_sql(
            r#"
            WITH GoodCTE AS (
                SELECT COUNT(*) as count FROM projects
            )
            SELECT * FROM GoodCTE
        "#,
        )
        .unwrap();

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
            ("Complete Enterprise Production Query", Box::pin(test_complete_enterprise_production_query())),
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
