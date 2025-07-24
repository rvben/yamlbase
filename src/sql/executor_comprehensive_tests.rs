#[cfg(test)]
#[allow(clippy::approx_constant)]
mod comprehensive_tests {
    use crate::database::storage::Storage;
    use crate::database::{Column, Database, Table, Value};
    use crate::sql::{QueryExecutor, parse_sql};
    use crate::yaml::schema::SqlType;
    use chrono::Datelike;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_string_functions_comprehensive() {
        // Create test database
        let mut db = Database::new("test_db".to_string());

        // Create a test table with string data
        let columns = vec![
            Column {
                name: "id".to_string(),
                sql_type: SqlType::Integer,
                nullable: false,
                default: None,
                unique: false,
                primary_key: true,
                references: None,
            },
            Column {
                name: "text_data".to_string(),
                sql_type: SqlType::Text,
                nullable: true,
                default: None,
                unique: false,
                primary_key: false,
                references: None,
            },
        ];

        let mut table = Table::new("test_strings".to_string(), columns);
        table.rows = vec![
            vec![Value::Integer(1), Value::Text("Hello World".to_string())],
            vec![Value::Integer(2), Value::Text("Testing".to_string())],
            vec![Value::Integer(3), Value::Null],
            vec![Value::Integer(4), Value::Text("".to_string())],
            vec![
                Value::Integer(5),
                Value::Text("🎉 Unicode 测试".to_string()),
            ],
        ];

        db.add_table(table).unwrap();
        let storage = Storage::new(db);
        let storage_arc = Arc::new(storage);
        let executor = QueryExecutor::new(storage_arc).await.unwrap();

        // Test LEFT function with constants first
        let query = parse_sql("SELECT LEFT('Hello World', 5)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hello".to_string()));

        // Test RIGHT function with constants
        let query = parse_sql("SELECT RIGHT('Hello World', 5)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("World".to_string()));

        // Test POSITION function with constants
        let query = parse_sql("SELECT POSITION('World', 'Hello World')").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(7));

        // Note: Table-based string function tests are commented out because
        // the current implementation doesn't support column references inside
        // these functions. This would need to be implemented in evaluate_function_with_row

        // Test edge cases with negative lengths
        let query = parse_sql("SELECT LEFT('test', -10)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("".to_string()));

        let query = parse_sql("SELECT RIGHT('test', -5)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("".to_string()));

        // Test with lengths larger than string
        let query = parse_sql("SELECT LEFT('test', 100)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("test".to_string()));

        let query = parse_sql("SELECT RIGHT('test', 100)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("test".to_string()));

        // Test LTRIM and RTRIM (already implemented)
        let query = parse_sql("SELECT LTRIM('  hello  '), RTRIM('  hello  ')").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("hello  ".to_string()));
        assert_eq!(result.rows[0][1], Value::Text("  hello".to_string()));

        // Test string functions with NULL values
        let query =
            parse_sql("SELECT LEFT(NULL, 5), RIGHT(NULL, 5), POSITION('test', NULL)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);
        assert_eq!(result.rows[0][1], Value::Null);
        assert_eq!(result.rows[0][2], Value::Null);

        // Test with empty strings
        let query = parse_sql("SELECT LEFT('', 5), RIGHT('', 5), POSITION('', 'Hello')").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("".to_string()));
        assert_eq!(result.rows[0][1], Value::Text("".to_string()));
        assert_eq!(result.rows[0][2], Value::Integer(1)); // Empty needle is found at position 1 (SQL standard)

        // Test POSITION with empty haystack
        let query = parse_sql("SELECT POSITION('test', '')").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(0)); // Not found in empty string

        // Test POSITION with both empty
        let query = parse_sql("SELECT POSITION('', '')").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(1)); // Empty string is found at position 1 in empty string

        // Test LEFT with zero length
        let query = parse_sql("SELECT LEFT('test', 0)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("".to_string()));

        // Test POSITION with multiple occurrences (should return first)
        let query = parse_sql("SELECT POSITION('test', 'test this test string')").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(1)); // First occurrence at position 1

        // Test POSITION where needle equals haystack
        let query = parse_sql("SELECT POSITION('Hello', 'Hello')").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(1)); // Full match at position 1

        // Test with Unicode emojis
        let query =
            parse_sql("SELECT LEFT('🎉🎊🎈', 2), RIGHT('🎉🎊🎈', 1), POSITION('🎊', '🎉🎊🎈')")
                .unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("🎉🎊".to_string()));
        assert_eq!(result.rows[0][1], Value::Text("🎈".to_string()));
        assert_eq!(result.rows[0][2], Value::Integer(2)); // 🎊 is at character position 2

        // Test with whitespace strings
        let query = parse_sql("SELECT LEFT('   ', 2), LENGTH(LEFT('   ', 2))").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("  ".to_string()));
        assert_eq!(result.rows[0][1], Value::Integer(2));

        // Test case sensitivity for POSITION (should be case-sensitive)
        let query =
            parse_sql("SELECT POSITION('world', 'Hello World'), POSITION('World', 'Hello World')")
                .unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(0)); // 'world' not found (case-sensitive)
        assert_eq!(result.rows[0][1], Value::Integer(7)); // 'World' found at position 7
    }

    #[tokio::test]
    async fn test_math_functions_comprehensive() {
        let db = Database::new("test_db".to_string());
        let storage = Storage::new(db);
        let storage_arc = Arc::new(storage);
        let executor = QueryExecutor::new(storage_arc).await.unwrap();

        // Test ROUND with different decimal places
        let query =
            parse_sql("SELECT ROUND(3.14159), ROUND(3.14159, 2), ROUND(3.14159, 4)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(3.0));
        assert_eq!(result.rows[0][1], Value::Double(3.14));
        assert_eq!(result.rows[0][2], Value::Double(3.1416));

        // Test ROUND with negative numbers
        let query = parse_sql("SELECT ROUND(-2.5), ROUND(-2.6), ROUND(2.5)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(-3.0)); // Rust uses round half away from zero 
        assert_eq!(result.rows[0][1], Value::Double(-3.0));
        assert_eq!(result.rows[0][2], Value::Double(3.0));

        // Test CEIL
        let query = parse_sql("SELECT CEIL(3.1), CEIL(3.9), CEIL(-3.1), CEIL(-3.9)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(4.0));
        assert_eq!(result.rows[0][1], Value::Double(4.0));
        assert_eq!(result.rows[0][2], Value::Double(-3.0));
        assert_eq!(result.rows[0][3], Value::Double(-3.0));

        // Test FLOOR
        let query = parse_sql("SELECT FLOOR(3.1), FLOOR(3.9), FLOOR(-3.1), FLOOR(-3.9)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(3.0));
        assert_eq!(result.rows[0][1], Value::Double(3.0));
        assert_eq!(result.rows[0][2], Value::Double(-4.0));
        assert_eq!(result.rows[0][3], Value::Double(-4.0));

        // Test ABS
        let query = parse_sql("SELECT ABS(-5), ABS(5), ABS(-3.14), ABS(0)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(5));
        assert_eq!(result.rows[0][1], Value::Integer(5));
        assert_eq!(result.rows[0][2], Value::Double(3.14));
        assert_eq!(result.rows[0][3], Value::Integer(0));

        // Test MOD
        let query = parse_sql("SELECT MOD(10, 3), MOD(10, -3), MOD(-10, 3), MOD(-10, -3)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[0][1], Value::Integer(1));
        assert_eq!(result.rows[0][2], Value::Integer(-1));
        assert_eq!(result.rows[0][3], Value::Integer(-1));

        // Test MOD with zero divisor
        let query = parse_sql("SELECT MOD(10, 0)").unwrap();
        let result = executor.execute(&query[0]).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Division by zero") || err_msg.contains("division by zero"),
            "Expected division by zero error, got: {}",
            err_msg
        );

        // Test with NULL values
        let query =
            parse_sql("SELECT ROUND(NULL), CEIL(NULL), FLOOR(NULL), ABS(NULL), MOD(NULL, 2)")
                .unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);
        assert_eq!(result.rows[0][1], Value::Null);
        assert_eq!(result.rows[0][2], Value::Null);
        assert_eq!(result.rows[0][3], Value::Null);
        assert_eq!(result.rows[0][4], Value::Null);
    }

    #[tokio::test]
    async fn test_cast_function_comprehensive() {
        let db = Database::new("test_db".to_string());
        let storage = Storage::new(db);
        let storage_arc = Arc::new(storage);
        let executor = QueryExecutor::new(storage_arc).await.unwrap();

        // Test casting to INTEGER
        let query = parse_sql(
            "SELECT CAST('123' AS INTEGER), CAST(45.67 AS INTEGER), CAST(true AS INTEGER)",
        )
        .unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(123));
        assert_eq!(result.rows[0][1], Value::Integer(45));
        assert_eq!(result.rows[0][2], Value::Integer(1));

        // Test casting to FLOAT/DOUBLE
        let query = parse_sql("SELECT CAST('3.14' AS FLOAT), CAST(42 AS DOUBLE)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Float(3.14));
        assert_eq!(result.rows[0][1], Value::Double(42.0));

        // Test casting to TEXT/VARCHAR
        let query =
            parse_sql("SELECT CAST(123 AS TEXT), CAST(45.67 AS VARCHAR), CAST(true AS TEXT)")
                .unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("123".to_string()));
        assert_eq!(result.rows[0][1], Value::Text("45.67".to_string()));
        assert_eq!(result.rows[0][2], Value::Text("true".to_string()));

        // Test casting to DATE
        let query = parse_sql("SELECT CAST('2025-01-15' AS DATE)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        match &result.rows[0][0] {
            Value::Date(d) => {
                assert_eq!(d.year(), 2025);
                assert_eq!(d.month(), 1);
                assert_eq!(d.day(), 15);
            }
            _ => panic!("Expected Date value"),
        }

        // Test casting to BOOLEAN
        let query = parse_sql("SELECT CAST(1 AS BOOLEAN), CAST(0 AS BOOLEAN), CAST('true' AS BOOLEAN), CAST('false' AS BOOLEAN)").unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Boolean(true));
        assert_eq!(result.rows[0][1], Value::Boolean(false));
        assert_eq!(result.rows[0][2], Value::Boolean(true));
        assert_eq!(result.rows[0][3], Value::Boolean(false));

        // Test NULL casting
        let query =
            parse_sql("SELECT CAST(NULL AS INTEGER), CAST(NULL AS TEXT), CAST(NULL AS DATE)")
                .unwrap();
        let result = executor.execute(&query[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);
        assert_eq!(result.rows[0][1], Value::Null);
        assert_eq!(result.rows[0][2], Value::Null);

        // Test invalid casts
        let query = parse_sql("SELECT CAST('abc' AS INTEGER)").unwrap();
        let result = executor.execute(&query[0]).await;
        assert!(result.is_err());

        let query = parse_sql("SELECT CAST('invalid-date' AS DATE)").unwrap();
        let result = executor.execute(&query[0]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_subquery_placeholders() {
        let mut db = Database::new("test_db".to_string());

        // Create users table
        let users_columns = vec![
            Column {
                name: "id".to_string(),
                sql_type: SqlType::Integer,
                nullable: false,
                default: None,
                unique: false,
                primary_key: true,
                references: None,
            },
            Column {
                name: "name".to_string(),
                sql_type: SqlType::Text,
                nullable: false,
                default: None,
                unique: false,
                primary_key: false,
                references: None,
            },
        ];

        let mut users_table = Table::new("users".to_string(), users_columns);
        users_table.rows = vec![
            vec![Value::Integer(1), Value::Text("Alice".to_string())],
            vec![Value::Integer(2), Value::Text("Bob".to_string())],
        ];

        // Create orders table
        let orders_columns = vec![
            Column {
                name: "id".to_string(),
                sql_type: SqlType::Integer,
                nullable: false,
                default: None,
                unique: false,
                primary_key: true,
                references: None,
            },
            Column {
                name: "user_id".to_string(),
                sql_type: SqlType::Integer,
                nullable: false,
                default: None,
                unique: false,
                primary_key: false,
                references: None,
            },
        ];

        let mut orders_table = Table::new("orders".to_string(), orders_columns);
        orders_table.rows = vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(1)],
        ];

        db.add_table(users_table).unwrap();
        db.add_table(orders_table).unwrap();

        let storage = Storage::new(db);
        let storage_arc = Arc::new(storage);
        let executor = QueryExecutor::new(storage_arc).await.unwrap();

        // Test IN subquery placeholder
        let query =
            parse_sql("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)").unwrap();
        let result = executor.execute(&query[0]).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Expression type not supported: InSubquery"));

        // Test EXISTS subquery placeholder
        let query = parse_sql("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)").unwrap();
        let result = executor.execute(&query[0]).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Expression type not supported: Exists"));

        // Test NOT EXISTS subquery placeholder
        let query = parse_sql("SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)").unwrap();
        let result = executor.execute(&query[0]).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        // NOT EXISTS gets parsed as UnaryOp { op: Not, expr: Exists(...) }
        assert!(
            err.contains("Unary operator Not not supported")
                || err.contains("Expression type not supported")
        );
    }
}
