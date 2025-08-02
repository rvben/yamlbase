use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_debug_max_function() {
    let mut db = Database::new("test_db".to_string());
    
    // Create employees table
    let mut employees_table = Table::new(
        "employees".to_string(),
        vec![
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
                sql_type: SqlType::Double,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );
    
    // Insert test data
    employees_table.insert_row(vec![Value::Integer(1), Value::Double(50000.0)]).unwrap();
    employees_table.insert_row(vec![Value::Integer(1), Value::Double(60000.0)]).unwrap();
    employees_table.insert_row(vec![Value::Integer(2), Value::Double(70000.0)]).unwrap();
    employees_table.insert_row(vec![Value::Integer(2), Value::Double(80000.0)]).unwrap();
    
    db.add_table(employees_table).unwrap();
    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage).await.unwrap();
    
    // Test 1: Simple MAX function should work
    println!("Testing: SELECT department_id, MAX(salary) FROM employees GROUP BY department_id");
    let stmts = parse_sql("SELECT department_id, MAX(salary) FROM employees GROUP BY department_id").unwrap();
    let result = executor.execute(&stmts[0]).await;
    
    match result {
        Ok(result) => {
            println!("Simple MAX success! Got {} rows", result.rows.len());
            for (i, row) in result.rows.iter().enumerate() {
                println!("Row {}: {:?}", i, row);
            }
        }
        Err(e) => {
            println!("Simple MAX error: {}", e);
            panic!("Simple MAX should work");
        }
    }
    
    // Test 2: Binary operation with MAX functions
    println!("Testing: SELECT department_id, MAX(salary) - MIN(salary) as salary_range FROM employees GROUP BY department_id");
    let stmts = parse_sql("SELECT department_id, MAX(salary) - MIN(salary) as salary_range FROM employees GROUP BY department_id").unwrap();
    let result = executor.execute(&stmts[0]).await;
    
    match result {
        Ok(result) => {
            println!("Binary MAX success! Got {} rows", result.rows.len());
            for (i, row) in result.rows.iter().enumerate() {
                println!("Row {}: {:?}", i, row);
            }
        }
        Err(e) => {
            println!("Binary MAX error: {}", e);
            panic!("Binary MAX should work with our fix");
        }
    }
}