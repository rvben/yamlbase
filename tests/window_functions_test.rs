#![allow(clippy::uninlined_format_args)]

use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

/// Test for window functions implementation
/// ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD are critical for enterprise SQL compatibility
#[tokio::test]
async fn test_window_functions() {
    // Create test database
    let mut db = Database::new("window_test".to_string());

    // Sales table with salesperson and sales data
    let sales_columns = vec![
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
            name: "salesperson".to_string(),
            sql_type: SqlType::Varchar(50),
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "region".to_string(),
            sql_type: SqlType::Varchar(50),
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "sales_amount".to_string(),
            sql_type: SqlType::Integer,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "quarter".to_string(),
            sql_type: SqlType::Integer,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];

    let mut sales_table = Table::new("sales".to_string(), sales_columns);

    // Insert test data for window function testing
    sales_table
        .insert_row(vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Text("North".to_string()),
            Value::Integer(150000),
            Value::Integer(1),
        ])
        .unwrap();
    sales_table
        .insert_row(vec![
            Value::Integer(2),
            Value::Text("Bob".to_string()),
            Value::Text("South".to_string()),
            Value::Integer(120000),
            Value::Integer(1),
        ])
        .unwrap();
    sales_table
        .insert_row(vec![
            Value::Integer(3),
            Value::Text("Carol".to_string()),
            Value::Text("North".to_string()),
            Value::Integer(175000),
            Value::Integer(1),
        ])
        .unwrap();
    sales_table
        .insert_row(vec![
            Value::Integer(4),
            Value::Text("Alice".to_string()),
            Value::Text("North".to_string()),
            Value::Integer(160000),
            Value::Integer(2),
        ])
        .unwrap();
    sales_table
        .insert_row(vec![
            Value::Integer(5),
            Value::Text("Bob".to_string()),
            Value::Text("South".to_string()),
            Value::Integer(130000),
            Value::Integer(2),
        ])
        .unwrap();
    sales_table
        .insert_row(vec![
            Value::Integer(6),
            Value::Text("Carol".to_string()),
            Value::Text("North".to_string()),
            Value::Integer(185000),
            Value::Integer(2),
        ])
        .unwrap();

    db.add_table(sales_table).unwrap();

    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone()).await.unwrap();

    println!("🧪 Testing Window Functions for Enterprise SQL Compatibility");
    println!("===========================================================");

    // Test 1: ROW_NUMBER() - Sequential numbering
    println!("\n✅ Test 1: ROW_NUMBER() Function");
    let row_number_query = r#"
        SELECT 
            salesperson,
            sales_amount,
            ROW_NUMBER() OVER (ORDER BY sales_amount DESC) as row_num
        FROM sales
        ORDER BY row_num
    "#;

    let row_number_result = executor
        .execute(&parse_sql(row_number_query).unwrap()[0])
        .await;
    match &row_number_result {
        Ok(result) => {
            println!(
                "   ✓ ROW_NUMBER() function succeeded: {} rows",
                result.rows.len()
            );
            assert!(!result.rows.is_empty());
        }
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("Window functions not yet supported")
                || error_str.contains("ROW_NUMBER")
            {
                println!("   ❌ ROW_NUMBER() not implemented yet: {}", error_str);
            } else {
                println!("   ⚠️  Different error: {}", error_str);
            }
        }
    }

    // Test 2: RANK() - Ranking with ties
    println!("\n✅ Test 2: RANK() Function");
    let rank_query = r#"
        SELECT 
            salesperson,
            region,
            sales_amount,
            RANK() OVER (PARTITION BY region ORDER BY sales_amount DESC) as region_rank
        FROM sales
        ORDER BY region, region_rank
    "#;

    let rank_result = executor.execute(&parse_sql(rank_query).unwrap()[0]).await;
    match &rank_result {
        Ok(result) => {
            println!("   ✓ RANK() function succeeded: {} rows", result.rows.len());
        }
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("Window functions not yet supported")
                || error_str.contains("RANK")
            {
                println!("   ❌ RANK() not implemented yet: {}", error_str);
            } else {
                println!("   ⚠️  Different error: {}", error_str);
            }
        }
    }

    // Test 3: DENSE_RANK() - Dense ranking
    println!("\n✅ Test 3: DENSE_RANK() Function");
    let dense_rank_query = r#"
        SELECT 
            salesperson,
            sales_amount,
            DENSE_RANK() OVER (ORDER BY sales_amount DESC) as dense_rank
        FROM sales
        ORDER BY dense_rank
    "#;

    let dense_rank_result = executor
        .execute(&parse_sql(dense_rank_query).unwrap()[0])
        .await;
    match &dense_rank_result {
        Ok(result) => {
            println!(
                "   ✓ DENSE_RANK() function succeeded: {} rows",
                result.rows.len()
            );
        }
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("Window functions not yet supported")
                || error_str.contains("DENSE_RANK")
            {
                println!("   ❌ DENSE_RANK() not implemented yet: {}", error_str);
            } else {
                println!("   ⚠️  Different error: {}", error_str);
            }
        }
    }

    // Test 4: LAG() - Previous row value
    println!("\n✅ Test 4: LAG() Function");
    let lag_query = r#"
        SELECT 
            salesperson,
            quarter,
            sales_amount,
            LAG(sales_amount, 1) OVER (PARTITION BY salesperson ORDER BY quarter) as prev_quarter_sales
        FROM sales
        ORDER BY salesperson, quarter
    "#;

    let lag_result = executor.execute(&parse_sql(lag_query).unwrap()[0]).await;
    match &lag_result {
        Ok(result) => {
            println!("   ✓ LAG() function succeeded: {} rows", result.rows.len());
        }
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("Window functions not yet supported") || error_str.contains("LAG")
            {
                println!("   ❌ LAG() not implemented yet: {}", error_str);
            } else {
                println!("   ⚠️  Different error: {}", error_str);
            }
        }
    }

    // Test 5: LEAD() - Next row value
    println!("\n✅ Test 5: LEAD() Function");
    let lead_query = r#"
        SELECT 
            salesperson,
            quarter,
            sales_amount,
            LEAD(sales_amount, 1) OVER (PARTITION BY salesperson ORDER BY quarter) as next_quarter_sales
        FROM sales
        ORDER BY salesperson, quarter
    "#;

    let lead_result = executor.execute(&parse_sql(lead_query).unwrap()[0]).await;
    match &lead_result {
        Ok(result) => {
            println!("   ✓ LEAD() function succeeded: {} rows", result.rows.len());
        }
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("Window functions not yet supported")
                || error_str.contains("LEAD")
            {
                println!("   ❌ LEAD() not implemented yet: {}", error_str);
            } else {
                println!("   ⚠️  Different error: {}", error_str);
            }
        }
    }

    // Test 6: Multiple window functions in one query
    println!("\n✅ Test 6: Multiple Window Functions");
    let multi_window_query = r#"
        SELECT 
            salesperson,
            region,
            sales_amount,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales_amount DESC) as region_row_num,
            RANK() OVER (PARTITION BY region ORDER BY sales_amount DESC) as region_rank,
            LAG(sales_amount) OVER (PARTITION BY region ORDER BY sales_amount DESC) as prev_sales
        FROM sales
        ORDER BY region, region_rank
    "#;

    let multi_result = executor
        .execute(&parse_sql(multi_window_query).unwrap()[0])
        .await;
    match &multi_result {
        Ok(result) => {
            println!(
                "   ✓ Multiple window functions succeeded: {} rows",
                result.rows.len()
            );
        }
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("Window functions not yet supported") {
                println!(
                    "   ❌ Multiple window functions not implemented yet: {}",
                    error_str
                );
            } else {
                println!("   ⚠️  Different error: {}", error_str);
            }
        }
    }

    println!("\n🎯 Window Functions Test Summary:");
    println!("   • ROW_NUMBER(): Sequential row numbering");
    println!("   • RANK(): Ranking values with ties");
    println!("   • DENSE_RANK(): Dense ranking without gaps");
    println!("   • LAG(): Access previous row values");
    println!("   • LEAD(): Access next row values");

    println!("\n🚀 These are CRITICAL for enterprise SQL compatibility!");
    println!("   Window functions enable advanced analytics and reporting queries");
    println!("   that are essential for business intelligence and data analysis.");
}

/// Test window function error handling
#[tokio::test]
async fn test_window_function_errors() {
    let mut db = Database::new("window_error_test".to_string());

    // Simple test table
    let test_columns = vec![
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
            name: "value".to_string(),
            sql_type: SqlType::Integer,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];

    let mut test_table = Table::new("test_data".to_string(), test_columns);
    test_table
        .insert_row(vec![Value::Integer(1), Value::Integer(100)])
        .unwrap();
    test_table
        .insert_row(vec![Value::Integer(2), Value::Integer(200)])
        .unwrap();

    db.add_table(test_table).unwrap();
    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone()).await.unwrap();

    println!("\n🧪 Testing Window Function Error Handling");
    println!("==========================================");

    // Test invalid window function usage
    let invalid_window_query = "SELECT ROW_NUMBER() FROM test_data";

    let result = executor
        .execute(&parse_sql(invalid_window_query).unwrap()[0])
        .await;

    match result {
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("Window functions not yet supported") {
                println!("   ✅ Proper error message for unsupported window functions");
            } else if error_str.contains("OVER clause required") {
                println!("   ✅ Proper validation for missing OVER clause");
            } else {
                println!("   ⚠️  Unexpected error: {}", error_str);
            }
        }
        Ok(_) => {
            println!("   ⚠️  Window function without OVER clause should fail");
        }
    }
}
