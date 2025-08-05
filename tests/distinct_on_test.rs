use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_distinct_on() {
    println!("=== DISTINCT ON TEST ===");

    // Create test database
    let mut db = Database::new("test_db".to_string());

    // Create employees table with duplicate departments
    let mut employees_table = Table::new(
        "employees".to_string(),
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
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "department".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "salary".to_string(),
                sql_type: SqlType::Integer,
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
        ],
    );

    // Add test data - multiple employees per department
    employees_table.insert_row(vec![
        Value::Integer(1),
        Value::Text("Alice".to_string()),
        Value::Text("Engineering".to_string()),
        Value::Integer(90000),
        Value::Date(chrono::NaiveDate::from_ymd_opt(2020, 1, 15).unwrap()),
    ]).unwrap();
    employees_table.insert_row(vec![
        Value::Integer(2),
        Value::Text("Bob".to_string()),
        Value::Text("Engineering".to_string()),
        Value::Integer(85000),
        Value::Date(chrono::NaiveDate::from_ymd_opt(2021, 3, 20).unwrap()),
    ]).unwrap();
    employees_table.insert_row(vec![
        Value::Integer(3),
        Value::Text("Charlie".to_string()),
        Value::Text("Sales".to_string()),
        Value::Integer(75000),
        Value::Date(chrono::NaiveDate::from_ymd_opt(2019, 6, 10).unwrap()),
    ]).unwrap();
    employees_table.insert_row(vec![
        Value::Integer(4),
        Value::Text("Diana".to_string()),
        Value::Text("Sales".to_string()),
        Value::Integer(80000),
        Value::Date(chrono::NaiveDate::from_ymd_opt(2022, 2, 1).unwrap()),
    ]).unwrap();
    employees_table.insert_row(vec![
        Value::Integer(5),
        Value::Text("Eve".to_string()),
        Value::Text("Engineering".to_string()),
        Value::Integer(95000),
        Value::Date(chrono::NaiveDate::from_ymd_opt(2018, 11, 30).unwrap()),
    ]).unwrap();

    db.add_table(employees_table).unwrap();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test 1: DISTINCT ON single column - get one employee per department
    println!("\n1. Testing DISTINCT ON (department):");
    let query = r#"
        SELECT DISTINCT ON (department) 
            department, name, salary
        FROM employees
        ORDER BY department, salary DESC
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ DISTINCT ON works!");
            println!("   Number of rows returned: {}", result.rows.len());
            assert_eq!(result.rows.len(), 2); // One per department
            
            // Should get highest paid employee per department
            // Engineering: Eve (95000)
            // Sales: Diana (80000)
            
            println!("   Results:");
            for row in &result.rows {
                println!("     Department: {}, Employee: {}, Salary: {:?}", 
                    row[0], row[1], row[2]);
            }
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 2: DISTINCT ON multiple columns
    println!("\n2. Testing DISTINCT ON with multiple columns:");
    let query = r#"
        SELECT DISTINCT ON (department, EXTRACT(YEAR FROM hire_date)) 
            department, name, hire_date
        FROM employees
        ORDER BY department, EXTRACT(YEAR FROM hire_date), hire_date
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ DISTINCT ON with multiple columns works!");
            println!("   Number of rows returned: {}", result.rows.len());
            
            println!("   Results:");
            for row in &result.rows {
                println!("     Department: {}, Employee: {}, Hire Date: {:?}", 
                    row[0], row[1], row[2]);
            }
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 3: DISTINCT ON with WHERE clause
    println!("\n3. Testing DISTINCT ON with WHERE clause:");
    let query = r#"
        SELECT DISTINCT ON (department) 
            department, name, salary
        FROM employees
        WHERE salary > 80000
        ORDER BY department, salary DESC
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ DISTINCT ON with WHERE clause works!");
            println!("   Number of rows returned: {}", result.rows.len());
            
            println!("   Results:");
            for row in &result.rows {
                println!("     Department: {}, Employee: {}, Salary: {:?}", 
                    row[0], row[1], row[2]);
            }
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 4: DISTINCT ON with expressions
    println!("\n4. Testing DISTINCT ON with expressions:");
    let query = r#"
        SELECT DISTINCT ON (department, salary > 85000) 
            department, name, salary, salary > 85000 as high_earner
        FROM employees
        ORDER BY department, salary > 85000, salary DESC
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ DISTINCT ON with expressions works!");
            println!("   Number of rows returned: {}", result.rows.len());
            
            println!("   Results:");
            for row in &result.rows {
                println!("     Department: {}, Employee: {}, Salary: {:?}, High Earner: {:?}", 
                    row[0], row[1], row[2], row[3]);
            }
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    println!("\n✅ All DISTINCT ON tests completed!");
}