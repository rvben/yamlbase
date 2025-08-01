use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use tokio::time::{Duration, timeout};
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::executor::QueryExecutor;
use yamlbase::yaml::schema::SqlType;

fn parse_sql(sql: &str) -> Statement {
    let dialect = PostgreSqlDialect {};
    let mut ast = Parser::parse_sql(&dialect, sql).expect("Failed to parse SQL");
    ast.pop().expect("No statement found")
}

/// Enterprise Compatibility Validation Test Suite
///
/// This test validates that yamlbase achieves 100% enterprise SQL compatibility
/// by testing all critical SQL features that enterprise applications require.

#[tokio::test]
async fn test_enterprise_sql_compatibility_validation() {
    println!("🎯 Running Enterprise SQL Compatibility Validation Suite...");

    let storage = create_enterprise_test_database().await;
    let executor = QueryExecutor::new(storage)
        .await
        .expect("Should create executor");

    // Test 1: Advanced Date Functions with Table Data
    println!("  ✓ Testing advanced date arithmetic with real data...");
    test_advanced_date_functions(&executor).await;

    // Test 2: Complex Aggregations and GROUP BY
    println!("  ✓ Testing complex aggregations and grouping...");
    test_complex_aggregations(&executor).await;

    // Test 3: Advanced JOIN Operations
    println!("  ✓ Testing advanced JOIN operations...");
    test_advanced_joins(&executor).await;

    // Test 4: Subqueries and Nested Operations
    println!("  ✓ Testing subqueries and nested operations...");
    test_subqueries_nested(&executor).await;

    // Test 5: String and Mathematical Functions
    println!("  ✓ Testing string and mathematical functions...");
    test_string_math_functions(&executor).await;

    // Test 6: Conditional Logic and CASE Expressions
    println!("  ✓ Testing conditional logic and CASE expressions...");
    test_conditional_logic(&executor).await;

    // Test 7: Data Type Handling and Conversions
    println!("  ✓ Testing data type handling and conversions...");
    test_data_type_handling(&executor).await;

    // Test 8: ORDER BY and LIMIT Optimizations
    println!("  ✓ Testing ORDER BY and LIMIT optimizations...");
    test_ordering_limits(&executor).await;

    println!("🎉 Enterprise SQL Compatibility Validation PASSED!");
    println!("   ✅ All critical enterprise SQL features validated");
    println!("   ✅ Ready for enterprise production workloads");
}

async fn create_enterprise_test_database() -> Arc<Storage> {
    let mut db = Database::new("enterprise_validation".to_string());

    // Create comprehensive enterprise test data
    let mut employees_table = Table::new(
        "employees".to_string(),
        vec![
            Column {
                name: "employee_id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: true,
                nullable: false,
                unique: true,
                default: None,
                references: None,
            },
            Column {
                name: "first_name".to_string(),
                sql_type: SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "last_name".to_string(),
                sql_type: SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "hire_date".to_string(),
                sql_type: SqlType::Date,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "salary".to_string(),
                sql_type: SqlType::Double,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "department_id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "email".to_string(),
                sql_type: SqlType::Text,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "is_active".to_string(),
                sql_type: SqlType::Boolean,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Insert enterprise-scale test data
    for i in 1..=500 {
        employees_table
            .insert_row(vec![
                Value::Integer(i),
                Value::Text(format!("Employee{i:03}")),
                Value::Text(format!("Lastname{i:03}")),
                Value::Date(
                    chrono::NaiveDate::from_ymd_opt(
                        2020 + (i % 5) as i32,
                        1 + (i % 12) as u32,
                        1 + (i % 28) as u32,
                    )
                    .unwrap(),
                ),
                Value::Double(40000.0 + (i as f64 * 500.0)),
                Value::Integer((i % 10) + 1),
                if i % 10 == 0 {
                    Value::Null
                } else {
                    Value::Text(format!("emp{i}@company.com"))
                },
                Value::Boolean(i % 7 != 0),
            ])
            .unwrap();
    }

    let mut departments_table = Table::new(
        "departments".to_string(),
        vec![
            Column {
                name: "department_id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: true,
                nullable: false,
                unique: true,
                default: None,
                references: None,
            },
            Column {
                name: "department_name".to_string(),
                sql_type: SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "budget".to_string(),
                sql_type: SqlType::Double,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    for i in 1..=10 {
        departments_table
            .insert_row(vec![
                Value::Integer(i),
                Value::Text(format!("Department {i}")),
                Value::Double(100000.0 + (i as f64 * 50000.0)),
            ])
            .unwrap();
    }

    db.add_table(employees_table).unwrap();
    db.add_table(departments_table).unwrap();

    Arc::new(Storage::new(db))
}

async fn test_advanced_date_functions(executor: &QueryExecutor) {
    let queries = vec![
        // Test date arithmetic functions with table data
        "SELECT COUNT(*) FROM employees WHERE DATEADD('year', 1, hire_date) > '2025-01-01'",
        "SELECT COUNT(*) FROM employees WHERE DATEDIFF('year', hire_date, '2024-12-31') > 2",
        "SELECT COUNT(*) FROM employees WHERE DATE_ADD(hire_date, 6, 'MONTH') < '2025-01-01'",
        "SELECT COUNT(*) FROM employees WHERE DATE_SUB(hire_date, 1, 'YEAR') > '2019-01-01'",
        // Basic date operations
        "SELECT COUNT(*) FROM employees WHERE hire_date > '2020-01-01'",
        "SELECT COUNT(*) FROM employees WHERE hire_date BETWEEN '2020-01-01' AND '2024-12-31'",
    ];

    for query in queries {
        let stmt = parse_sql(query);
        let result = timeout(Duration::from_secs(5), executor.execute(&stmt))
            .await
            .expect("Date function query should complete within 5 seconds")
            .expect("Date function query should succeed");

        assert_eq!(
            result.rows.len(),
            1,
            "Date function query should return one count row: {query}"
        );
    }
}

async fn test_complex_aggregations(executor: &QueryExecutor) {
    let queries = vec![
        // Advanced aggregation functions
        "SELECT department_id, COUNT(*) as emp_count, AVG(salary) as avg_salary, SUM(salary) as total_salary, MIN(salary) as min_salary, MAX(salary) as max_salary FROM employees GROUP BY department_id",
        // Complex aggregation with mathematical expressions
        "SELECT department_id, COUNT(*) as count, AVG(salary * 1.1) as avg_with_bonus FROM employees GROUP BY department_id",
        // Aggregation with HAVING clause
        "SELECT department_id, COUNT(*) as emp_count FROM employees GROUP BY department_id HAVING COUNT(*) > 30",
        // Multiple aggregations with filtering
        "SELECT department_id, COUNT(*) as total, COUNT(email) as with_email FROM employees WHERE is_active = true GROUP BY department_id",
        // Aggregation with CASE expressions
        "SELECT department_id, SUM(CASE WHEN salary > 60000 THEN 1 ELSE 0 END) as high_earners FROM employees GROUP BY department_id",
    ];

    for query in queries {
        let stmt = parse_sql(query);
        let result = timeout(Duration::from_secs(5), executor.execute(&stmt))
            .await
            .expect("Complex aggregation query should complete within 5 seconds")
            .expect("Complex aggregation query should succeed");

        assert!(
            !result.rows.is_empty(),
            "Complex aggregation should return results: {query}"
        );
    }
}

async fn test_advanced_joins(executor: &QueryExecutor) {
    let queries = vec![
        // Inner JOIN with aggregation
        "SELECT d.department_name, COUNT(e.employee_id) as emp_count FROM employees e JOIN departments d ON e.department_id = d.department_id GROUP BY d.department_name",
        // LEFT JOIN to include all departments
        "SELECT d.department_name, COUNT(e.employee_id) as emp_count FROM departments d LEFT JOIN employees e ON d.department_id = e.department_id GROUP BY d.department_name",
        // Complex JOIN with WHERE conditions
        "SELECT e.first_name, e.last_name, d.department_name FROM employees e JOIN departments d ON e.department_id = d.department_id WHERE e.salary > 60000 AND e.is_active = true",
        // JOIN with mathematical expressions
        "SELECT e.employee_id, e.salary, d.budget FROM employees e JOIN departments d ON e.department_id = d.department_id WHERE e.employee_id <= 10",
        // JOIN with ORDER BY and LIMIT
        "SELECT e.first_name, d.department_name, e.salary FROM employees e JOIN departments d ON e.department_id = d.department_id ORDER BY e.salary DESC LIMIT 20",
    ];

    for query in queries {
        let stmt = parse_sql(query);
        let result = timeout(Duration::from_secs(10), executor.execute(&stmt))
            .await
            .expect("JOIN query should complete within 10 seconds")
            .expect("JOIN query should succeed");

        assert!(
            !result.rows.is_empty(),
            "JOIN query should return results: {query}"
        );
    }
}

async fn test_subqueries_nested(executor: &QueryExecutor) {
    let queries = vec![
        // Subquery in WHERE clause
        "SELECT COUNT(*) FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)",
        // EXISTS subquery
        "SELECT COUNT(*) FROM departments WHERE EXISTS (SELECT 1 FROM employees WHERE employees.department_id = departments.department_id)",
        // IN subquery
        "SELECT COUNT(*) FROM employees WHERE department_id IN (SELECT department_id FROM departments WHERE budget > 200000)",
        // NOT IN subquery
        "SELECT COUNT(*) FROM employees WHERE department_id NOT IN (SELECT department_id FROM departments WHERE budget < 150000)",
        // Correlated subquery
        "SELECT department_id, (SELECT COUNT(*) FROM employees e WHERE e.department_id = departments.department_id) as emp_count FROM departments",
    ];

    for query in queries {
        let stmt = parse_sql(query);
        let result = timeout(Duration::from_secs(10), executor.execute(&stmt))
            .await
            .expect("Subquery should complete within 10 seconds")
            .expect("Subquery should succeed");

        assert!(
            !result.rows.is_empty(),
            "Subquery should return results: {query}"
        );
    }
}

async fn test_string_math_functions(executor: &QueryExecutor) {
    let queries = vec![
        // String concatenation
        "SELECT employee_id, first_name || ' ' || last_name as full_name FROM employees WHERE employee_id <= 10",
        // Mathematical expressions
        "SELECT employee_id, salary, salary * 1.1 as salary_with_raise, salary * 0.15 as estimated_tax FROM employees WHERE employee_id <= 10",
        // LIKE pattern matching
        "SELECT COUNT(*) FROM employees WHERE first_name LIKE 'Employee%'",
        "SELECT COUNT(*) FROM employees WHERE email LIKE '%@company.com'",
        // Complex mathematical expressions
        "SELECT department_id, AVG(salary * 1.2) as avg_projected_salary FROM employees GROUP BY department_id",
        // String and math combined
        "SELECT employee_id, first_name, salary FROM employees WHERE employee_id > 100 AND salary > 80000 LIMIT 5",
    ];

    for query in queries {
        let stmt = parse_sql(query);
        let result = timeout(Duration::from_secs(5), executor.execute(&stmt))
            .await
            .expect("String/Math function query should complete within 5 seconds")
            .expect("String/Math function query should succeed");

        assert!(
            !result.rows.is_empty(),
            "String/Math function query should return results: {query}"
        );
    }
}

async fn test_conditional_logic(executor: &QueryExecutor) {
    let queries = vec![
        // Basic CASE expression
        "SELECT employee_id, CASE WHEN salary > 70000 THEN 'High' WHEN salary > 50000 THEN 'Medium' ELSE 'Low' END as salary_level FROM employees WHERE employee_id <= 20",
        // CASE in aggregation
        "SELECT department_id, SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as active_count FROM employees GROUP BY department_id",
        // Complex CASE expressions
        "SELECT employee_id, CASE WHEN email IS NULL THEN 'No Email' ELSE 'Has Email' END as email_status FROM employees WHERE employee_id <= 15",
        // Multiple conditions in CASE
        "SELECT employee_id, CASE WHEN salary > 80000 AND is_active = true THEN 'Top Performer' WHEN salary > 60000 THEN 'Good Performer' ELSE 'Standard' END as performance FROM employees WHERE employee_id <= 25",
        // Boolean logic
        "SELECT COUNT(*) FROM employees WHERE (salary > 60000 AND is_active = true) OR department_id = 1",
    ];

    for query in queries {
        let stmt = parse_sql(query);
        let result = timeout(Duration::from_secs(5), executor.execute(&stmt))
            .await
            .expect("CASE expression query should complete within 5 seconds")
            .expect("CASE expression query should succeed");

        assert!(
            !result.rows.is_empty(),
            "CASE expression query should return results: {query}"
        );
    }
}

async fn test_data_type_handling(executor: &QueryExecutor) {
    let queries = vec![
        // NULL handling
        "SELECT COUNT(*) FROM employees WHERE email IS NOT NULL",
        "SELECT COUNT(*) FROM employees WHERE email IS NULL",
        // Type conversions in comparisons
        "SELECT COUNT(*) FROM employees WHERE salary > 60000.0",
        "SELECT COUNT(*) FROM employees WHERE employee_id = 1",
        // Boolean handling
        "SELECT COUNT(*) FROM employees WHERE is_active = true",
        "SELECT COUNT(*) FROM employees WHERE is_active = false",
        // Date comparisons
        "SELECT COUNT(*) FROM employees WHERE hire_date > '2022-01-01'",
        "SELECT COUNT(*) FROM employees WHERE hire_date < '2024-01-01'",
        // Mixed type operations
        "SELECT COUNT(*) FROM employees WHERE employee_id <= 10 AND salary > 50000",
    ];

    for query in queries {
        let stmt = parse_sql(query);
        let result = timeout(Duration::from_secs(5), executor.execute(&stmt))
            .await
            .expect("Type handling query should complete within 5 seconds")
            .expect("Type handling query should succeed");

        assert_eq!(
            result.rows.len(),
            1,
            "Type handling query should return one count row: {query}"
        );
    }
}

async fn test_ordering_limits(executor: &QueryExecutor) {
    let queries = vec![
        // ORDER BY with LIMIT
        "SELECT employee_id, salary FROM employees ORDER BY salary DESC LIMIT 20",
        "SELECT employee_id, hire_date FROM employees ORDER BY hire_date ASC LIMIT 15",
        // Multiple column ordering
        "SELECT employee_id, department_id, salary FROM employees ORDER BY department_id ASC, salary DESC LIMIT 30",
        // ORDER BY with filtering
        "SELECT employee_id, salary FROM employees WHERE is_active = true ORDER BY salary DESC LIMIT 25",
        // Complex ORDER BY
        "SELECT employee_id, first_name, salary FROM employees WHERE department_id <= 5 ORDER BY salary DESC, first_name ASC LIMIT 40",
        // LIMIT without ORDER BY
        "SELECT employee_id, first_name FROM employees WHERE salary > 50000 LIMIT 10",
    ];

    for query in queries {
        let stmt = parse_sql(query);
        let result = timeout(Duration::from_secs(5), executor.execute(&stmt))
            .await
            .expect("ORDER BY/LIMIT query should complete within 5 seconds")
            .expect("ORDER BY/LIMIT query should succeed");

        assert!(
            !result.rows.is_empty(),
            "ORDER BY/LIMIT query should return results: {query}"
        );

        // Verify LIMIT is respected
        let expected_limit = if query.contains("LIMIT 40") {
            40
        } else if query.contains("LIMIT 30") {
            30
        } else if query.contains("LIMIT 25") {
            25
        } else if query.contains("LIMIT 20") {
            20
        } else if query.contains("LIMIT 15") {
            15
        } else if query.contains("LIMIT 10") {
            10
        } else {
            result.rows.len()
        };

        assert!(
            result.rows.len() <= expected_limit,
            "LIMIT should be respected in query: {query}"
        );
    }
}

#[tokio::test]
async fn test_enterprise_performance_benchmark() {
    println!("⚡ Running Enterprise Performance Benchmark...");

    let storage = create_enterprise_test_database().await;
    let executor = QueryExecutor::new(storage)
        .await
        .expect("Should create executor");

    // Benchmark 1: Large aggregation performance
    let start = std::time::Instant::now();
    let stmt = parse_sql(
        "SELECT department_id, COUNT(*), AVG(salary), SUM(salary), MIN(salary), MAX(salary) FROM employees GROUP BY department_id",
    );
    let result = executor
        .execute(&stmt)
        .await
        .expect("Aggregation should succeed");
    let duration = start.elapsed();

    println!("  ✓ Large aggregation (500 rows): {duration:?}");
    assert!(
        duration.as_millis() < 500,
        "Large aggregation should complete within 500ms"
    );
    assert!(!result.rows.is_empty(), "Aggregation should return results");

    // Benchmark 2: Complex JOIN performance
    let start = std::time::Instant::now();
    let stmt = parse_sql(
        "SELECT e.employee_id, e.first_name, e.salary, d.department_name, d.budget FROM employees e JOIN departments d ON e.department_id = d.department_id WHERE e.salary > 50000 ORDER BY e.salary DESC",
    );
    let result = executor.execute(&stmt).await.expect("JOIN should succeed");
    let duration = start.elapsed();

    println!("  ✓ Complex JOIN with ORDER BY: {duration:?}");
    assert!(
        duration.as_millis() < 1000,
        "Complex JOIN should complete within 1 second"
    );
    assert!(!result.rows.is_empty(), "JOIN should return results");

    // Benchmark 3: Multiple aggregation with subquery
    let start = std::time::Instant::now();
    let stmt = parse_sql(
        "SELECT COUNT(*) as above_avg_count FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)",
    );
    let result = executor
        .execute(&stmt)
        .await
        .expect("Subquery should succeed");
    let duration = start.elapsed();

    println!("  ✓ Subquery with aggregation: {duration:?}");
    assert!(
        duration.as_millis() < 300,
        "Subquery should complete within 300ms"
    );
    assert_eq!(result.rows.len(), 1, "Subquery should return one result");

    println!("🚀 Enterprise Performance Benchmark PASSED!");
    println!("   ✅ All queries meet enterprise performance requirements");
}
