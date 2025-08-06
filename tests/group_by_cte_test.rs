use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_group_by_with_cte_columns() {
    println!("=== GROUP BY WITH CTE COLUMNS TEST ===");

    // Create test database
    let mut db = Database::new("test_db".to_string());

    // Create orders table
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
                name: "customer_id".to_string(),
                sql_type: SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "product".to_string(),
                sql_type: SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "amount".to_string(),
                sql_type: SqlType::Decimal(10, 2),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "order_date".to_string(),
                sql_type: SqlType::Date,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ],
    );

    // Add test data
    orders_table
        .insert_row(vec![
            Value::Integer(1),
            Value::Integer(101),
            Value::Text("Widget".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(1000, 1)), // 100.0
            Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 1, 15).unwrap()),
        ])
        .unwrap();
    orders_table
        .insert_row(vec![
            Value::Integer(2),
            Value::Integer(102),
            Value::Text("Widget".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(1500, 1)), // 150.0
            Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 1, 16).unwrap()),
        ])
        .unwrap();
    orders_table
        .insert_row(vec![
            Value::Integer(3),
            Value::Integer(101),
            Value::Text("Gadget".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(2000, 1)), // 200.0
            Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 1, 17).unwrap()),
        ])
        .unwrap();
    orders_table
        .insert_row(vec![
            Value::Integer(4),
            Value::Integer(103),
            Value::Text("Widget".to_string()),
            Value::Decimal(rust_decimal::Decimal::new(1200, 1)), // 120.0
            Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 1, 18).unwrap()),
        ])
        .unwrap();

    db.add_table(orders_table).unwrap();
    let storage = Storage::new(db);
    let executor = QueryExecutor::new(Arc::new(storage)).await.unwrap();

    // Test 1: Simple GROUP BY with CTE
    println!("\n1. Testing simple GROUP BY with CTE:");
    let query = r#"
        WITH ProductSales AS (
            SELECT product, amount 
            FROM orders
        )
        SELECT product, SUM(amount) as total_sales
        FROM ProductSales
        GROUP BY product
        ORDER BY product
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ GROUP BY with CTE works!");
            println!("   Number of rows returned: {}", result.rows.len());
            println!("   Columns: {:?}", result.columns);
            for (i, row) in result.rows.iter().enumerate() {
                println!("   Row {i}: {row:?}");
            }
            assert_eq!(result.rows.len(), 2); // Widget and Gadget

            // Check Gadget total (200) - comes first alphabetically
            assert_eq!(result.rows[0][0], Value::Text("Gadget".to_string()));
            match &result.rows[0][1] {
                Value::Decimal(d) => assert_eq!(*d, rust_decimal::Decimal::new(2000, 1)),
                Value::Integer(i) => assert_eq!(*i, 200),
                other => panic!("Expected Decimal or Integer, got {other:?}"),
            }

            // Check Widget total (100 + 150 + 120 = 370)
            assert_eq!(result.rows[1][0], Value::Text("Widget".to_string()));
            match &result.rows[1][1] {
                Value::Decimal(d) => assert_eq!(*d, rust_decimal::Decimal::new(3700, 1)),
                Value::Integer(i) => assert_eq!(*i, 370),
                other => panic!("Expected Decimal or Integer, got {other:?}"),
            }

            println!("   Results:");
            for row in &result.rows {
                println!("     Product: {}, Total: {:?}", row[0], row[1]);
            }
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 2: GROUP BY with multiple columns from CTE
    println!("\n2. Testing GROUP BY with multiple columns from CTE:");
    let query = r#"
        WITH CustomerOrders AS (
            SELECT customer_id, product, amount
            FROM orders
        )
        SELECT customer_id, product, COUNT(*) as order_count, SUM(amount) as total_amount
        FROM CustomerOrders
        GROUP BY customer_id, product
        ORDER BY customer_id, product
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ Multi-column GROUP BY with CTE works!");
            println!("   Number of rows returned: {}", result.rows.len());
            println!("   Columns: {:?}", result.columns);
            for (i, row) in result.rows.iter().enumerate() {
                println!("   Row {i}: {row:?}");
            }
            assert_eq!(result.rows.len(), 4); // Actually 4 unique customer-product combinations

            println!("   Results:");
            for row in &result.rows {
                println!(
                    "     Customer: {}, Product: {}, Count: {:?}, Total: {:?}",
                    row[0], row[1], row[2], row[3]
                );
            }
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 3: GROUP BY with expressions on CTE columns
    println!("\n3. Testing GROUP BY with expressions on CTE columns:");
    let query = r#"
        WITH MonthlyOrders AS (
            SELECT 
                EXTRACT(MONTH FROM order_date) as order_month,
                product,
                amount
            FROM orders
        )
        SELECT order_month, SUM(amount) as monthly_total
        FROM MonthlyOrders
        GROUP BY order_month
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ GROUP BY with expressions on CTE columns works!");
            assert_eq!(result.rows.len(), 1); // All orders are in January

            println!("   Results:");
            for row in &result.rows {
                println!("     Month: {:?}, Total: {:?}", row[0], row[1]);
            }
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    // Test 4: GROUP BY with HAVING clause on CTE
    println!("\n4. Testing GROUP BY with HAVING clause on CTE:");
    let query = r#"
        WITH ProductSales AS (
            SELECT product, amount 
            FROM orders
        )
        SELECT product, SUM(amount) as total_sales
        FROM ProductSales
        GROUP BY product
        HAVING SUM(amount) > 250
        ORDER BY product
    "#;

    let parsed = parse_sql(query).unwrap();
    match executor.execute(&parsed[0]).await {
        Ok(result) => {
            println!("   ✅ GROUP BY with HAVING on CTE works!");
            assert_eq!(result.rows.len(), 1); // Only Widget has total > 250
            assert_eq!(result.rows[0][0], Value::Text("Widget".to_string()));

            println!("   Results:");
            for row in &result.rows {
                println!("     Product: {}, Total: {:?}", row[0], row[1]);
            }
        }
        Err(e) => {
            panic!("   ❌ Failed with: {e}");
        }
    }

    println!("\n✅ All GROUP BY with CTE tests passed!");
}
