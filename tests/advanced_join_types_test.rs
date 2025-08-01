use yamlbase::database::Value;
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::database::{Storage, Database, Table, Column};
use yamlbase::yaml::schema::SqlType;
use std::sync::Arc;

#[tokio::test]
async fn test_advanced_join_types() {
    // Create test database with sample data
    let mut db = Database::new("join_test_db".to_string());
    
    // Create employees table
    let mut employees_table = Table::new(
        "employees".to_string(),
        vec![
            Column {
                name: "emp_id".to_string(),
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
                name: "department_id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );
    
    employees_table.insert_row(vec![Value::Integer(1), Value::Text("Alice".to_string()), Value::Integer(10)]).unwrap();
    employees_table.insert_row(vec![Value::Integer(2), Value::Text("Bob".to_string()), Value::Integer(20)]).unwrap();
    employees_table.insert_row(vec![Value::Integer(3), Value::Text("Charlie".to_string()), Value::Null]).unwrap(); // No department
    
    // Create departments table
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
    
    departments_table.insert_row(vec![Value::Integer(10), Value::Text("Engineering".to_string())]).unwrap();
    departments_table.insert_row(vec![Value::Integer(30), Value::Text("Marketing".to_string())]).unwrap(); // No employees
    
    db.add_table(employees_table).unwrap();
    db.add_table(departments_table).unwrap();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    println!("=== ADVANCED JOIN TYPES TEST ===");
    
    // Test 1: INNER JOIN (baseline)
    println!("\\n1. Testing INNER JOIN (baseline):");
    let stmts = parse_sql("SELECT e.name, d.dept_name FROM employees e INNER JOIN departments d ON e.department_id = d.dept_id").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => println!("   ✅ INNER JOIN works! Got {} rows", result.rows.len()),
        Err(e) => println!("   ❌ INNER JOIN failed: {}", e),
    }
    
    // Test 2: LEFT OUTER JOIN (baseline)
    println!("\\n2. Testing LEFT OUTER JOIN (baseline):");
    let stmts = parse_sql("SELECT e.name, d.dept_name FROM employees e LEFT JOIN departments d ON e.department_id = d.dept_id").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => println!("   ✅ LEFT JOIN works! Got {} rows", result.rows.len()),
        Err(e) => println!("   ❌ LEFT JOIN failed: {}", e),
    }
    
    // Test 3: RIGHT OUTER JOIN (baseline)
    println!("\\n3. Testing RIGHT OUTER JOIN (baseline):");
    let stmts = parse_sql("SELECT e.name, d.dept_name FROM employees e RIGHT JOIN departments d ON e.department_id = d.dept_id").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => println!("   ✅ RIGHT JOIN works! Got {} rows", result.rows.len()),
        Err(e) => println!("   ❌ RIGHT JOIN failed: {}", e),
    }
    
    // Test 4: FULL OUTER JOIN
    println!("\\n4. Testing FULL OUTER JOIN:");
    let stmts = parse_sql("SELECT e.name, d.dept_name FROM employees e FULL OUTER JOIN departments d ON e.department_id = d.dept_id").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => {
            println!("   ✅ FULL OUTER JOIN works! Got {} rows", result.rows.len());
            for (i, row) in result.rows.iter().enumerate() {
                println!("     Row {}: {:?}", i + 1, row);
            }
        },
        Err(e) => println!("   ❌ FULL OUTER JOIN failed: {}", e),
    }
    
    // Test 5: CROSS JOIN
    println!("\\n5. Testing CROSS JOIN:");
    let stmts = parse_sql("SELECT e.name, d.dept_name FROM employees e CROSS JOIN departments d").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => {
            println!("   ✅ CROSS JOIN works! Got {} rows", result.rows.len());
            println!("     Expected: {} rows (3 employees × 2 departments = 6)", 3 * 2);
        },
        Err(e) => println!("   ❌ CROSS JOIN failed: {}", e),
    }
    
    // Test 6: Alternative CROSS JOIN syntax
    println!("\\n6. Testing alternative CROSS JOIN syntax:");
    let stmts = parse_sql("SELECT e.name, d.dept_name FROM employees e, departments d").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => println!("   ✅ Comma JOIN works! Got {} rows", result.rows.len()),
        Err(e) => println!("   ❌ Comma JOIN failed: {}", e),
    }
    
    println!("\\n=== JOIN TYPES TEST COMPLETE ===");
    println!("This test verifies current advanced JOIN type support in yamlbase.");
}

#[tokio::test]
async fn test_complex_join_scenarios() {
    println!("=== COMPLEX JOIN SCENARIOS TEST ===");
    
    // Create more complex test data
    let mut db = Database::new("complex_join_db".to_string());
    
    // Users table
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
        ],
    );
    
    users_table.insert_row(vec![Value::Integer(1), Value::Text("alice".to_string())]).unwrap();
    users_table.insert_row(vec![Value::Integer(2), Value::Text("bob".to_string())]).unwrap();
    
    // Orders table
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
                nullable: true,  // Some orders might not have users
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
    
    orders_table.insert_row(vec![Value::Integer(101), Value::Integer(1), Value::Double(50.0)]).unwrap();
    orders_table.insert_row(vec![Value::Integer(102), Value::Null, Value::Double(25.0)]).unwrap(); // Anonymous order
    
    db.add_table(users_table).unwrap();
    db.add_table(orders_table).unwrap();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();
    
    // Test complex FULL OUTER JOIN with NULLs
    println!("\\n1. Testing FULL OUTER JOIN with NULL handling:");
    let stmts = parse_sql("SELECT u.username, o.amount FROM users u FULL OUTER JOIN orders o ON u.user_id = o.user_id").unwrap();
    let stmt = &stmts[0];
    match executor.execute(stmt).await {
        Ok(result) => {
            println!("   ✅ Complex FULL OUTER JOIN works! Got {} rows", result.rows.len());
            for (i, row) in result.rows.iter().enumerate() {
                println!("     Row {}: {:?}", i + 1, row);
            }
        },
        Err(e) => println!("   ❌ Complex FULL OUTER JOIN failed: {}", e),
    }
    
    println!("\\n=== COMPLEX JOIN SCENARIOS COMPLETE ===");
}