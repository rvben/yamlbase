use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use yamlbase::database::Value;
use yamlbase::database::{Column, Database, Storage, Table};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

/// Enterprise load testing to validate performance with large result sets
/// Tests various scenarios that enterprise clients would encounter

#[tokio::test]
async fn test_large_result_set_performance() {
    println!("Testing large result set performance (10,000 rows)...");

    let mut db = Database::new("enterprise_test".to_string());

    // Create a large table with 10,000 rows and multiple data types
    let mut large_table = Table::new(
        "large_dataset".to_string(),
        vec![
            Column {
                name: "id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: true,
                nullable: false,
                unique: true,
                default: None,
                references: None,
            },
            Column {
                name: "name".to_string(),
                sql_type: SqlType::Text,
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
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "age".to_string(),
                sql_type: SqlType::Integer,
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
                name: "department".to_string(),
                sql_type: SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "created_at".to_string(),
                sql_type: SqlType::Timestamp,
                primary_key: false,
                nullable: false,
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

    // Generate 10,000 test records
    for i in 1..=10000 {
        let dept = match i % 5 {
            0 => "Engineering",
            1 => "Marketing",
            2 => "Sales",
            3 => "HR",
            _ => "Finance",
        };

        large_table
            .insert_row(vec![
                Value::Integer(i),
                Value::Text(format!("User_{i:04}")),
                Value::Text(format!("user{i}@company.com")),
                Value::Integer(25 + (i % 40)),
                Value::Double(50000.0 + (i as f64 * 100.0)),
                Value::Text(dept.to_string()),
                Value::Timestamp(
                    chrono::DateTime::from_timestamp(1640995200 + (i * 86400), 0)
                        .unwrap()
                        .naive_utc(),
                ),
                Value::Boolean(i % 3 != 0),
            ])
            .unwrap();
    }

    db.add_table(large_table).unwrap();
    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone()).await.unwrap();

    // Wait for indexes to be built
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test 1: Full table scan (should complete within 5 seconds)
    println!("  Test 1: Full table scan of 10,000 rows...");
    let start = Instant::now();
    let queries = parse_sql("SELECT * FROM large_dataset").unwrap();
    let result = timeout(Duration::from_secs(5), executor.execute(&queries[0]))
        .await
        .expect("Query should complete within 5 seconds")
        .expect("Query should succeed");
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 10000);
    assert_eq!(result.columns.len(), 8);
    println!(
        "  ✓ Full scan completed in {:?} ({} rows/sec)",
        duration,
        (10000.0 / duration.as_secs_f64()) as i32
    );

    // Test 2: Filtered query with WHERE clause (should be fast due to indexing)
    println!("  Test 2: Filtered query with WHERE clause...");
    let start = Instant::now();
    let queries =
        parse_sql("SELECT * FROM large_dataset WHERE department = 'Engineering'").unwrap();
    let result = timeout(Duration::from_secs(2), executor.execute(&queries[0]))
        .await
        .expect("Filtered query should complete within 2 seconds")
        .expect("Query should succeed");
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 2000); // Every 5th record
    println!(
        "  ✓ Filtered query completed in {:?} ({} matching rows)",
        duration,
        result.rows.len()
    );

    // Test 3: Aggregation over large dataset
    println!("  Test 3: Aggregation query over large dataset...");
    let start = Instant::now();
    let queries = parse_sql(
        "SELECT department, COUNT(*), AVG(salary) FROM large_dataset GROUP BY department",
    )
    .unwrap();
    let result = timeout(Duration::from_secs(3), executor.execute(&queries[0]))
        .await
        .expect("Aggregation should complete within 3 seconds")
        .expect("Query should succeed");
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 5); // 5 departments
    println!(
        "  ✓ Aggregation completed in {:?} ({} groups)",
        duration,
        result.rows.len()
    );

    // Test 4: ORDER BY with LIMIT (should be optimized)
    println!("  Test 4: ORDER BY with LIMIT optimization...");
    let start = Instant::now();
    let queries = parse_sql("SELECT * FROM large_dataset ORDER BY salary DESC LIMIT 100").unwrap();
    let result = timeout(Duration::from_secs(2), executor.execute(&queries[0]))
        .await
        .expect("ORDER BY LIMIT should complete within 2 seconds")
        .expect("Query should succeed");
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 100);
    println!(
        "  ✓ ORDER BY LIMIT completed in {:?} (top {} rows)",
        duration,
        result.rows.len()
    );

    println!("✓ Large result set performance test passed!");
}

#[tokio::test]
async fn test_memory_efficient_processing() {
    println!("Testing memory-efficient processing with streaming...");

    let mut db = Database::new("memory_test".to_string());

    // Create table with wide rows (many columns)
    let mut wide_table = Table::new(
        "wide_data".to_string(),
        (0..50)
            .map(|i| Column {
                name: format!("col_{i}"),
                sql_type: SqlType::Text,
                primary_key: i == 0,
                nullable: false,
                unique: i == 0,
                default: None,
                references: None,
            })
            .collect(),
    );

    // Generate 5,000 wide rows
    for i in 1..=5000 {
        let mut row = vec![];
        for j in 0..50 {
            row.push(Value::Text(format!("data_{i}_{j}")));
        }
        wide_table.insert_row(row).unwrap();
    }

    db.add_table(wide_table).unwrap();
    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone()).await.unwrap();

    // Test memory usage remains reasonable with wide result sets
    println!("  Testing wide result set processing...");
    let start = Instant::now();
    let queries = parse_sql("SELECT * FROM wide_data").unwrap();
    let result = timeout(Duration::from_secs(10), executor.execute(&queries[0]))
        .await
        .expect("Wide query should complete within 10 seconds")
        .expect("Query should succeed");
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 5000);
    assert_eq!(result.columns.len(), 50);
    println!(
        "  ✓ Wide result set processed in {:?} ({}x{} = {} cells)",
        duration,
        result.rows.len(),
        result.columns.len(),
        result.rows.len() * result.columns.len()
    );

    println!("✓ Memory-efficient processing test passed!");
}

#[tokio::test]
async fn test_concurrent_large_queries() {
    println!("Testing concurrent large query handling...");

    let mut db = Database::new("concurrent_test".to_string());

    // Create test table
    let mut test_table = Table::new(
        "concurrent_data".to_string(),
        vec![
            Column {
                name: "id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: true,
                nullable: false,
                unique: true,
                default: None,
                references: None,
            },
            Column {
                name: "data".to_string(),
                sql_type: SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Generate 5,000 rows
    for i in 1..=5000 {
        test_table
            .insert_row(vec![
                Value::Integer(i),
                Value::Text(format!("concurrent_data_{i}")),
            ])
            .unwrap();
    }

    db.add_table(test_table).unwrap();
    let storage = Arc::new(Storage::new(db));
    let executor = Arc::new(QueryExecutor::new(storage.clone()).await.unwrap());

    // Launch 5 concurrent queries
    println!("  Launching 5 concurrent large queries...");
    let start = Instant::now();

    let mut handles = vec![];
    for i in 1..=5 {
        let executor_clone = executor.clone();
        let handle = tokio::spawn(async move {
            let query = format!(
                "SELECT * FROM concurrent_data WHERE id > {} ORDER BY id",
                i * 1000
            );
            let queries = parse_sql(&query).unwrap();
            timeout(Duration::from_secs(10), executor_clone.execute(&queries[0]))
                .await
                .expect("Concurrent query should complete within 10 seconds")
                .expect("Concurrent query should succeed")
        });
        handles.push(handle);
    }

    // Wait for all queries to complete
    let mut total_rows = 0;
    for handle in handles {
        let result = handle.await.expect("Concurrent query should complete");
        total_rows += result.rows.len();
        println!("  ✓ Concurrent query returned {} rows", result.rows.len());
    }

    let duration = start.elapsed();
    println!("  ✓ All 5 concurrent queries completed in {duration:?} (total {total_rows} rows)");

    // Queries should complete in reasonable time even when concurrent
    assert!(
        duration < Duration::from_secs(15),
        "Concurrent queries took too long: {duration:?}"
    );

    println!("✓ Concurrent large query test passed!");
}

#[tokio::test]
async fn test_complex_join_performance() {
    println!("Testing complex JOIN performance with large datasets...");

    let mut db = Database::new("join_test".to_string());

    // Create users table (2,000 records)
    let mut users_table = Table::new(
        "users".to_string(),
        vec![
            Column {
                name: "user_id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: true,
                nullable: false,
                unique: true,
                default: None,
                references: None,
            },
            Column {
                name: "username".to_string(),
                sql_type: SqlType::Text,
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
        ],
    );

    for i in 1..=2000 {
        users_table
            .insert_row(vec![
                Value::Integer(i),
                Value::Text(format!("user_{i}")),
                Value::Integer((i % 10) + 1), // 10 departments
            ])
            .unwrap();
    }

    // Create departments table (10 records)
    let mut departments_table = Table::new(
        "departments".to_string(),
        vec![
            Column {
                name: "dept_id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: true,
                nullable: false,
                unique: true,
                default: None,
                references: None,
            },
            Column {
                name: "dept_name".to_string(),
                sql_type: SqlType::Text,
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
                Value::Text(format!("Department_{i}")),
            ])
            .unwrap();
    }

    // Create orders table (5,000 records)
    let mut orders_table = Table::new(
        "orders".to_string(),
        vec![
            Column {
                name: "order_id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: true,
                nullable: false,
                unique: true,
                default: None,
                references: None,
            },
            Column {
                name: "user_id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "amount".to_string(),
                sql_type: SqlType::Double,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    for i in 1..=5000 {
        orders_table
            .insert_row(vec![
                Value::Integer(i),
                Value::Integer((i % 2000) + 1), // Reference users
                Value::Double(100.0 + (i as f64 * 10.0)),
            ])
            .unwrap();
    }

    db.add_table(users_table).unwrap();
    db.add_table(departments_table).unwrap();
    db.add_table(orders_table).unwrap();

    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone()).await.unwrap();

    // Wait for indexes
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test complex 3-table JOIN with aggregation
    println!("  Testing 3-table JOIN with aggregation...");
    let start = Instant::now();
    let query = "
        SELECT d.dept_name, COUNT(o.order_id) as order_count, SUM(o.amount) as total_amount
        FROM users u
        JOIN departments d ON u.department_id = d.dept_id
        JOIN orders o ON u.user_id = o.user_id
        GROUP BY d.dept_name
        ORDER BY total_amount DESC
    ";

    let queries = parse_sql(query).unwrap();
    let result = timeout(Duration::from_secs(5), executor.execute(&queries[0]))
        .await
        .expect("Complex JOIN should complete within 5 seconds")
        .expect("Complex JOIN should succeed");
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 10); // 10 departments
    assert_eq!(result.columns.len(), 3);
    println!(
        "  ✓ Complex JOIN completed in {:?} ({} result rows)",
        duration,
        result.rows.len()
    );

    // Verify the results make sense
    for row in &result.rows {
        if let (Value::Text(_), Value::Integer(count), Value::Double(amount)) =
            (&row[0], &row[1], &row[2])
        {
            assert!(*count > 0, "Order count should be positive");
            assert!(*amount > 0.0, "Total amount should be positive");
        } else {
            panic!("Unexpected row format");
        }
    }

    println!("✓ Complex JOIN performance test passed!");
}

#[tokio::test]
async fn test_pagination_performance() {
    println!("Testing pagination performance for enterprise applications...");

    let mut db = Database::new("pagination_test".to_string());

    // Create large dataset for pagination testing
    let mut large_table = Table::new(
        "paginated_data".to_string(),
        vec![
            Column {
                name: "id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: true,
                nullable: false,
                unique: true,
                default: None,
                references: None,
            },
            Column {
                name: "title".to_string(),
                sql_type: SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "content".to_string(),
                sql_type: SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "score".to_string(),
                sql_type: SqlType::Double,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Generate 8,000 records
    for i in 1..=8000 {
        large_table
            .insert_row(vec![
                Value::Integer(i),
                Value::Text(format!("Title {i}")),
                Value::Text(format!(
                    "Content for item {i} with some additional text to make it realistic"
                )),
                Value::Double((i as f64) * 1.5 + (i % 100) as f64),
            ])
            .unwrap();
    }

    db.add_table(large_table).unwrap();
    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone()).await.unwrap();

    // Test various pagination scenarios
    let page_sizes = vec![20, 50, 100, 500];

    for page_size in page_sizes {
        println!("  Testing pagination with page size {page_size}...");

        // Test first page
        let start = Instant::now();
        let query = format!("SELECT * FROM paginated_data ORDER BY score DESC LIMIT {page_size}");
        let queries = parse_sql(&query).unwrap();
        let result = timeout(Duration::from_secs(2), executor.execute(&queries[0]))
            .await
            .expect("Pagination query should complete within 2 seconds")
            .expect("Pagination query should succeed");
        let duration = start.elapsed();

        assert_eq!(result.rows.len(), page_size);
        println!("    ✓ First page ({page_size} rows) completed in {duration:?}");

        // Test middle page (using OFFSET simulation with WHERE)
        let start = Instant::now();
        let offset_id = page_size * 5; // Skip to middle pages
        let query = format!(
            "SELECT * FROM paginated_data WHERE id > {offset_id} ORDER BY id LIMIT {page_size}"
        );
        let queries = parse_sql(&query).unwrap();
        let result = timeout(Duration::from_secs(2), executor.execute(&queries[0]))
            .await
            .expect("Offset pagination should complete within 2 seconds")
            .expect("Offset pagination should succeed");
        let duration = start.elapsed();

        assert_eq!(result.rows.len(), page_size);
        println!("    ✓ Middle page ({page_size} rows) completed in {duration:?}");
    }

    println!("✓ Pagination performance test passed!");
}
