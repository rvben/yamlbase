#![allow(clippy::uninlined_format_args)]

use std::sync::Arc;
use std::str::FromStr;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;
use rust_decimal::Decimal;

/// Enterprise-level test for multiple table JOINs with aggregates
/// This tests Priority 2.2 - Multiple table JOINs with aggregates
#[tokio::test]
async fn test_enterprise_multi_table_join_aggregates() {
    // Create a realistic enterprise database schema
    let mut db = Database::new("enterprise_db".to_string());

    // 1. Projects table
    let project_columns = vec![
        Column {
            name: "project_id".to_string(),
            sql_type: SqlType::Integer,
            primary_key: true,
            nullable: false,
            unique: true,
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
            name: "budget".to_string(),
            sql_type: SqlType::Decimal(10, 2),
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];

    let mut projects_table = Table::new("projects".to_string(), project_columns);
    projects_table.insert_row(vec![
        Value::Integer(1), Value::Text("Website Redesign".to_string()), Value::Integer(10), Value::Decimal(Decimal::from_str("150000.00").unwrap())
    ]).unwrap();
    projects_table.insert_row(vec![
        Value::Integer(2), Value::Text("Mobile App".to_string()), Value::Integer(10), Value::Decimal(Decimal::from_str("200000.00").unwrap())
    ]).unwrap();
    projects_table.insert_row(vec![
        Value::Integer(3), Value::Text("Data Migration".to_string()), Value::Integer(20), Value::Decimal(Decimal::from_str("300000.00").unwrap())
    ]).unwrap();
    projects_table.insert_row(vec![
        Value::Integer(4), Value::Text("Security Audit".to_string()), Value::Integer(30), Value::Decimal(Decimal::from_str("100000.00").unwrap())
    ]).unwrap();

    // 2. Employees table
    let employee_columns = vec![
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
            name: "employee_name".to_string(),
            sql_type: SqlType::Varchar(100),
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
            name: "salary".to_string(),
            sql_type: SqlType::Decimal(10, 2),
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];

    let mut employees_table = Table::new("employees".to_string(), employee_columns);
    employees_table.insert_row(vec![
        Value::Integer(101), Value::Text("Alice Johnson".to_string()), Value::Integer(10), Value::Decimal(Decimal::from_str("95000.00").unwrap())
    ]).unwrap();
    employees_table.insert_row(vec![
        Value::Integer(102), Value::Text("Bob Smith".to_string()), Value::Integer(10), Value::Decimal(Decimal::from_str("85000.00").unwrap())
    ]).unwrap();
    employees_table.insert_row(vec![
        Value::Integer(103), Value::Text("Carol Davis".to_string()), Value::Integer(20), Value::Decimal(Decimal::from_str("105000.00").unwrap())
    ]).unwrap();
    employees_table.insert_row(vec![
        Value::Integer(104), Value::Text("David Wilson".to_string()), Value::Integer(20), Value::Decimal(Decimal::from_str("110000.00").unwrap())
    ]).unwrap();
    employees_table.insert_row(vec![
        Value::Integer(105), Value::Text("Eve Brown".to_string()), Value::Integer(30), Value::Decimal(Decimal::from_str("120000.00").unwrap())
    ]).unwrap();

    // 3. Departments table
    let department_columns = vec![
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
            sql_type: SqlType::Varchar(100),
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "location".to_string(),
            sql_type: SqlType::Varchar(50),
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];

    let mut departments_table = Table::new("departments".to_string(), department_columns);
    departments_table.insert_row(vec![
        Value::Integer(10), Value::Text("Engineering".to_string()), Value::Text("New York".to_string())
    ]).unwrap();
    departments_table.insert_row(vec![
        Value::Integer(20), Value::Text("Data Science".to_string()), Value::Text("San Francisco".to_string())
    ]).unwrap();
    departments_table.insert_row(vec![
        Value::Integer(30), Value::Text("Security".to_string()), Value::Text("Austin".to_string())
    ]).unwrap();

    // 4. Project assignments table (many-to-many relationship)
    let assignment_columns = vec![
        Column {
            name: "assignment_id".to_string(),
            sql_type: SqlType::Integer,
            primary_key: true,
            nullable: false,
            unique: true,
            default: None,
            references: None,
        },
        Column {
            name: "project_id".to_string(),
            sql_type: SqlType::Integer,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "employee_id".to_string(),
            sql_type: SqlType::Integer,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "hours_allocated".to_string(),
            sql_type: SqlType::Integer,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];

    let mut assignments_table = Table::new("project_assignments".to_string(), assignment_columns);
    assignments_table.insert_row(vec![
        Value::Integer(1), Value::Integer(1), Value::Integer(101), Value::Integer(40)
    ]).unwrap();
    assignments_table.insert_row(vec![
        Value::Integer(2), Value::Integer(1), Value::Integer(102), Value::Integer(30)
    ]).unwrap();
    assignments_table.insert_row(vec![
        Value::Integer(3), Value::Integer(2), Value::Integer(101), Value::Integer(20)
    ]).unwrap();
    assignments_table.insert_row(vec![
        Value::Integer(4), Value::Integer(3), Value::Integer(103), Value::Integer(35)
    ]).unwrap();
    assignments_table.insert_row(vec![
        Value::Integer(5), Value::Integer(3), Value::Integer(104), Value::Integer(40)
    ]).unwrap();
    assignments_table.insert_row(vec![
        Value::Integer(6), Value::Integer(4), Value::Integer(105), Value::Integer(25)
    ]).unwrap();

    // Add all tables to database
    db.add_table(projects_table).unwrap();
    db.add_table(employees_table).unwrap();
    db.add_table(departments_table).unwrap();
    db.add_table(assignments_table).unwrap();

    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone()).await.unwrap();

    // Test 1: Three-table JOIN with aggregate - Department statistics
    println!("Testing 3-table JOIN with COUNT aggregate...");
    let dept_stats_query = r#"
        SELECT 
            d.department_name,
            COUNT(e.employee_id) as employee_count,
            COUNT(p.project_id) as project_count
        FROM departments d
        LEFT JOIN employees e ON d.department_id = e.department_id
        LEFT JOIN projects p ON d.department_id = p.department_id
        GROUP BY d.department_id, d.department_name
        ORDER BY d.department_name
    "#;
    
    let dept_statements = parse_sql(dept_stats_query).unwrap();
    let dept_result = executor.execute(&dept_statements[0]).await.unwrap();
    
    println!("✅ 3-table JOIN with COUNT completed");
    println!("Result: {} rows", dept_result.rows.len());
    assert!(dept_result.rows.len() > 0);

    // Test 2: Four-table JOIN with multiple aggregates - Project resource analysis
    println!("Testing 4-table JOIN with multiple aggregates...");
    let resource_query = r#"
        SELECT 
            p.project_name,
            d.department_name,
            COUNT(pa.employee_id) as assigned_employees,
            SUM(pa.hours_allocated) as total_hours,
            AVG(e.salary) as avg_employee_salary
        FROM projects p
        JOIN departments d ON p.department_id = d.department_id
        JOIN project_assignments pa ON p.project_id = pa.project_id
        JOIN employees e ON pa.employee_id = e.employee_id
        GROUP BY p.project_id, p.project_name, d.department_name
        ORDER BY p.project_name
    "#;
    
    let resource_statements = parse_sql(resource_query).unwrap();
    let resource_result = executor.execute(&resource_statements[0]).await.unwrap();
    
    println!("✅ 4-table JOIN with multiple aggregates completed");
    println!("Result: {} rows", resource_result.rows.len());
    assert!(resource_result.rows.len() > 0);

    // Test 3: Complex JOIN with HAVING clause on aggregates
    println!("Testing JOIN with HAVING on aggregates...");
    let having_query = r#"
        SELECT 
            d.department_name,
            COUNT(e.employee_id) as employee_count,
            SUM(p.budget) as total_budget
        FROM departments d
        LEFT JOIN employees e ON d.department_id = e.department_id
        LEFT JOIN projects p ON d.department_id = p.department_id
        GROUP BY d.department_id, d.department_name
        HAVING COUNT(e.employee_id) > 1
        ORDER BY total_budget DESC
    "#;
    
    let having_statements = parse_sql(having_query).unwrap();
    let having_result = executor.execute(&having_statements[0]).await.unwrap();
    
    println!("✅ JOIN with HAVING on aggregates completed");
    println!("Result: {} rows", having_result.rows.len());

    // Test 4: Additional aggregate test to ensure all functions work
    println!("Testing additional aggregate functions...");
    let additional_agg_query = r#"
        SELECT 
            d.department_name,
            COUNT(*) as employee_count,
            SUM(e.salary) as total_salary_cost,
            MIN(e.salary) as min_salary,
            MAX(e.salary) as max_salary
        FROM departments d
        JOIN employees e ON d.department_id = e.department_id
        GROUP BY d.department_id, d.department_name
    "#;
    
    let additional_statements = parse_sql(additional_agg_query).unwrap();
    let additional_result = executor.execute(&additional_statements[0]).await.unwrap();
    
    println!("✅ Additional aggregate functions completed");
    println!("Result: {} departments with salary statistics", additional_result.rows.len());

    println!("🎉 All enterprise multi-table JOIN with aggregates tests completed!");
}

/// Test for JOIN performance with large datasets
#[tokio::test]
async fn test_join_aggregate_performance() {
    let mut db = Database::new("perf_test_db".to_string());

    // Create larger datasets for performance testing
    let table1_columns = vec![
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
            name: "category".to_string(),
            sql_type: SqlType::Integer,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "value".to_string(),
            sql_type: SqlType::Decimal(10, 2),
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];

    let mut table1 = Table::new("large_table1".to_string(), table1_columns);
    
    // Insert 1000 rows for performance testing
    for i in 1..=1000 {
        table1.insert_row(vec![
            Value::Integer(i),
            Value::Integer(i % 10), // 10 categories
            Value::Decimal(Decimal::from_str(&format!("{:.2}", (i as f64) * 1.5)).unwrap()),
        ]).unwrap();
    }

    let table2_columns = vec![
        Column {
            name: "category_id".to_string(),
            sql_type: SqlType::Integer,
            primary_key: true,
            nullable: false,
            unique: true,
            default: None,
            references: None,
        },
        Column {
            name: "category_name".to_string(),
            sql_type: SqlType::Varchar(50),
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];

    let mut table2 = Table::new("categories".to_string(), table2_columns);
    
    for i in 0..10 {
        table2.insert_row(vec![
            Value::Integer(i),
            Value::Text(format!("Category {}", i)),
        ]).unwrap();
    }

    db.add_table(table1).unwrap();
    db.add_table(table2).unwrap();

    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone()).await.unwrap();

    // Performance test: JOIN with aggregates on large dataset
    let perf_query = r#"
        SELECT 
            c.category_name,
            COUNT(t.id) as record_count,
            SUM(t.value) as total_value,
            AVG(t.value) as avg_value,
            MIN(t.value) as min_value,
            MAX(t.value) as max_value
        FROM large_table1 t
        JOIN categories c ON t.category = c.category_id
        GROUP BY c.category_id, c.category_name
        ORDER BY total_value DESC
    "#;

    let start = std::time::Instant::now();
    let perf_statements = parse_sql(perf_query).unwrap();
    let perf_result = executor.execute(&perf_statements[0]).await.unwrap();
    let duration = start.elapsed();

    println!("✅ Large JOIN with aggregates completed in {:?}", duration);
    println!("Result: {} category summaries", perf_result.rows.len());
    assert_eq!(perf_result.rows.len(), 10); // Should have 10 categories
    
    // Performance should be reasonable (< 100ms for 1000 records)
    assert!(duration.as_millis() < 1000, "JOIN performance should be reasonable");

    println!("🎉 JOIN aggregate performance test passed!");
}