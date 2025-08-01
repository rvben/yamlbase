#[cfg(test)]
mod tests {
    use crate::database::storage::Storage;
    use crate::database::{Database, Value};
    use crate::sql::{QueryExecutor, parse_sql};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_left_right_position_functions() {
        // Create a simple test database with string functions test
        let db = Database::new("test_db".to_string());
        let storage = Arc::new(Storage::new(db));
        let executor = QueryExecutor::new(storage).await.unwrap();

        // Test constant expressions (SELECT without FROM) - these are easier to test
        let queries = parse_sql("SELECT LEFT('Hello World', 5)").unwrap();
        let result = executor.execute(&queries[0]).await;
        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.rows[0][0], Value::Text("Hello".to_string()));

        let queries = parse_sql("SELECT RIGHT('Hello World', 5)").unwrap();
        let result = executor.execute(&queries[0]).await;
        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.rows[0][0], Value::Text("World".to_string()));

        let queries = parse_sql("SELECT POSITION('World', 'Hello World')").unwrap();
        let result = executor.execute(&queries[0]).await;
        if let Err(ref e) = result {
            println!("Error executing POSITION query: {:?}", e);
        }
        assert!(
            result.is_ok(),
            "Failed to execute POSITION query: {:?}",
            result
        );
        let query_result = result.unwrap();
        assert_eq!(query_result.rows[0][0], Value::Integer(7)); // "World" starts at position 7

        // Test edge cases
        let queries = parse_sql("SELECT LEFT('test', -1)").unwrap();
        let result = executor.execute(&queries[0]).await;
        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.rows[0][0], Value::Text("".to_string())); // Negative length returns empty string

        let queries = parse_sql("SELECT RIGHT('test', 0)").unwrap();
        let result = executor.execute(&queries[0]).await;
        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.rows[0][0], Value::Text("".to_string())); // Zero length returns empty string

        let queries = parse_sql("SELECT POSITION('xyz', 'Hello World')").unwrap();
        let result = executor.execute(&queries[0]).await;
        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.rows[0][0], Value::Integer(0)); // Not found returns 0

        // Test with longer strings
        let queries = parse_sql("SELECT LEFT('Testing String Functions', 7)").unwrap();
        let result = executor.execute(&queries[0]).await;
        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.rows[0][0], Value::Text("Testing".to_string()));

        let queries = parse_sql("SELECT RIGHT('Testing String Functions', 9)").unwrap();
        let result = executor.execute(&queries[0]).await;
        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(
            query_result.rows[0][0],
            Value::Text("Functions".to_string())
        );

        let queries = parse_sql("SELECT POSITION('String', 'Testing String Functions')").unwrap();
        let result = executor.execute(&queries[0]).await;
        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.rows[0][0], Value::Integer(9)); // "String" starts at position 9
    }
}
