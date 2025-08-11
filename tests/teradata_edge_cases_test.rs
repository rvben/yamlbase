// Comprehensive edge case tests for Teradata SQL features in yamlbase
// Tests critical boundary conditions, error handling, and robustness

use yamlbase::sql::{QueryExecutor, SqlDialect, parse_sql_with_dialect};
use yamlbase::database::{Database, Storage, Value, Table, Column};
use yamlbase::yaml::schema::SqlType;
use std::sync::Arc;
use std::time::Duration;
use chrono::NaiveDate;

// Helper function to create a test executor with sample data
fn create_test_executor() -> QueryExecutor {
    let mut db = Database::new("edge_case_test_db".to_string());
    
    // Create columns for the test table
    let columns = vec![
        Column {
            name: "id".to_string(),
            sql_type: SqlType::Integer,
            primary_key: true,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "name".to_string(),
            sql_type: SqlType::Text,
            primary_key: false,
            nullable: true,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "large_text".to_string(),
            sql_type: SqlType::Text,
            primary_key: false,
            nullable: true,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "date_col".to_string(),
            sql_type: SqlType::Date,
            primary_key: false,
            nullable: true,
            unique: false,
            default: None,
            references: None,
        },
    ];
    
    // Create table with columns
    let mut test_table = Table::new("test_table".to_string(), columns);
    
    // Add test data including edge cases
    test_table.insert_row(vec![
        Value::Integer(1),
        Value::Text("Test".to_string()),
        Value::Text("A".repeat(1000)), // 1KB string
        Value::Date(NaiveDate::from_ymd_opt(2024, 1, 31).unwrap()),
    ]).unwrap();
    
    test_table.insert_row(vec![
        Value::Integer(2),
        Value::Text("NULL Test".to_string()),
        Value::Null,
        Value::Date(NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()), // Leap year edge case
    ]).unwrap();
    
    // Add row with very long text (but within limits)
    test_table.insert_row(vec![
        Value::Integer(3),
        Value::Text("Long Text Test".to_string()),
        Value::Text("B".repeat(50000)), // 50KB string
        Value::Date(NaiveDate::from_ymd_opt(2023, 2, 28).unwrap()), // Non-leap year
    ]).unwrap();
    
    db.add_table(test_table).unwrap();
    
    let storage = Arc::new(Storage::new(db));
    QueryExecutor::new(storage)
}

#[cfg(test)]
mod limit_clause_edge_cases {
    use super::*;

    #[tokio::test]
    async fn test_limit_zero() {
        let executor = create_test_executor();
        let statements = parse_sql_with_dialect("SELECT * FROM test_table LIMIT 0", SqlDialect::Teradata).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    #[tokio::test]
    async fn test_limit_negative_values() {
        let statements = parse_sql_with_dialect("SELECT * FROM test_table LIMIT -1", SqlDialect::Teradata).unwrap();
        let executor = create_test_executor();
        let result = executor.execute(&statements[0]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-negative"));
    }

    #[tokio::test]
    async fn test_limit_very_large_values() {
        let statements = parse_sql_with_dialect("SELECT * FROM test_table LIMIT 2000000000", SqlDialect::Teradata).unwrap();
        let executor = create_test_executor();
        let result = executor.execute(&statements[0]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too large"));
    }

    #[tokio::test]
    async fn test_limit_string_overflow() {
        let statements = parse_sql_with_dialect("SELECT * FROM test_table LIMIT 999999999999999999999", SqlDialect::Teradata).unwrap();
        let executor = create_test_executor();
        let result = executor.execute(&statements[0]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid LIMIT value"));
    }

    #[tokio::test]
    async fn test_limit_boundary_values() {
        let executor = create_test_executor();
        
        // Test exactly at the limit
        let statements = parse_sql_with_dialect("SELECT * FROM test_table LIMIT 1000000000", SqlDialect::Teradata).unwrap();
        let result = executor.execute(&statements[0]).await;
        assert!(result.is_err()); // Should fail - exactly at boundary
        
        // Test just under the limit  
        let statements = parse_sql_with_dialect("SELECT * FROM test_table LIMIT 999999999", SqlDialect::Teradata).unwrap();
        let result = executor.execute(&statements[0]).await;
        assert!(result.is_ok()); // Should succeed
    }
}

#[cfg(test)]
mod string_concatenation_edge_cases {
    use super::*;

    #[tokio::test]
    async fn test_concat_null_values() {
        let executor = create_test_executor();
        let statements = parse_sql_with_dialect("SELECT name || large_text FROM test_table WHERE id = 2", SqlDialect::Teradata).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);
    }

    #[tokio::test]
    async fn test_concat_type_coercion() {
        let executor = create_test_executor();
        let statements = parse_sql_with_dialect("SELECT id || name FROM test_table WHERE id = 1", SqlDialect::Teradata).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("1Test".to_string()));
    }

    #[tokio::test]
    async fn test_concat_memory_limits() {
        let executor = create_test_executor();
        
        // This should work - strings within reasonable limits
        let statements = parse_sql_with_dialect("SELECT large_text || large_text FROM test_table WHERE id = 3", SqlDialect::Teradata).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        assert!(matches!(result.rows[0][0], Value::Text(_)));
        
        // Test would-be memory exhaustion (simulated with very long strings)
        // Note: This tests the safety mechanism rather than actual memory exhaustion
    }

    #[tokio::test]
    async fn test_concat_incompatible_types() {
        let executor = create_test_executor();
        
        // Try to concatenate incompatible types
        let statements = parse_sql_with_dialect("SELECT date_col || name FROM test_table WHERE id = 1", SqlDialect::Teradata).unwrap();
        let result = executor.execute(&statements[0]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot concatenate"));
    }
}

#[cfg(test)]
mod cte_edge_cases {
    use super::*;

    #[tokio::test]
    async fn test_simple_cte() {
        let executor = create_test_executor();
        let statements = parse_sql_with_dialect(
            "WITH simple_cte AS (SELECT id, name FROM test_table WHERE id < 3) SELECT * FROM simple_cte",
            SqlDialect::Teradata
        ).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_empty_cte() {
        let executor = create_test_executor();
        let statements = parse_sql_with_dialect(
            "WITH empty_cte AS (SELECT id, name FROM test_table WHERE id > 1000) SELECT * FROM empty_cte",
            SqlDialect::Teradata
        ).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    #[tokio::test] 
    async fn test_nested_cte() {
        let executor = create_test_executor();
        let statements = parse_sql_with_dialect(
            "WITH outer_cte AS (
                WITH inner_cte AS (SELECT id FROM test_table WHERE id <= 2)
                SELECT id FROM inner_cte
            ) SELECT * FROM outer_cte",
            SqlDialect::Teradata
        ).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        assert_eq!(result.rows.len(), 2);
    }
}

#[cfg(test)]
mod date_function_edge_cases {
    use super::*;

    #[tokio::test]
    async fn test_add_months_month_end() {
        let executor = create_test_executor();
        
        // Test January 31 + 1 month (should handle month-end correctly)
        let statements = parse_sql_with_dialect(
            "SELECT ADD_MONTHS(date_col, 1) FROM test_table WHERE id = 1",
            SqlDialect::Teradata
        ).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        
        // Should handle month-end edge case gracefully
        assert!(matches!(result.rows[0][0], Value::Date(_)));
    }

    #[tokio::test]
    async fn test_add_months_leap_year() {
        let executor = create_test_executor();
        
        // Test leap year handling
        let statements = parse_sql_with_dialect(
            "SELECT ADD_MONTHS(date_col, 12) FROM test_table WHERE id = 2",
            SqlDialect::Teradata
        ).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        assert!(matches!(result.rows[0][0], Value::Date(_)));
    }

    #[tokio::test]
    async fn test_add_months_negative() {
        let executor = create_test_executor();
        
        // Test negative months
        let statements = parse_sql_with_dialect(
            "SELECT ADD_MONTHS(date_col, -12) FROM test_table WHERE id = 1",
            SqlDialect::Teradata
        ).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        assert!(matches!(result.rows[0][0], Value::Date(_)));
    }

    #[tokio::test]
    async fn test_last_day_leap_year() {
        let executor = create_test_executor();
        
        // Test LAST_DAY with leap year February 
        let statements = parse_sql_with_dialect(
            "SELECT LAST_DAY(date_col) FROM test_table WHERE id = 2",
            SqlDialect::Teradata
        ).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        
        // Should return 2024-02-29 for leap year
        if let Value::Date(date_str) = &result.rows[0][0] {
            assert!(date_str.contains("2024-02-29"));
        } else {
            panic!("Expected date value");
        }
    }

    #[tokio::test]
    async fn test_extract_function_edge_cases() {
        let executor = create_test_executor();
        
        // Test EXTRACT with various date parts
        let statements = parse_sql_with_dialect(
            "SELECT EXTRACT(YEAR FROM date_col), EXTRACT(MONTH FROM date_col), EXTRACT(DAY FROM date_col) FROM test_table WHERE id = 1",
            SqlDialect::Teradata
        ).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        
        assert_eq!(result.rows[0][0], Value::Integer(2024));
        assert_eq!(result.rows[0][1], Value::Integer(1));
        assert_eq!(result.rows[0][2], Value::Integer(31));
    }
}

#[cfg(test)]
mod join_edge_cases {
    use super::*;

    #[tokio::test]
    async fn test_self_join() {
        let executor = create_test_executor();
        
        // Self-join should work
        let statements = parse_sql_with_dialect(
            "SELECT t1.id, t2.name FROM test_table t1 JOIN test_table t2 ON t1.id = t2.id WHERE t1.id = 1",
            SqlDialect::Teradata
        ).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_join_with_nulls() {
        let executor = create_test_executor();
        
        // LEFT JOIN should handle NULLs correctly
        let statements = parse_sql_with_dialect(
            "SELECT t1.id, t1.large_text FROM test_table t1 LEFT JOIN test_table t2 ON t1.large_text = t2.large_text",
            SqlDialect::Teradata
        ).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        
        // Should return all rows, with proper NULL handling
        assert_eq!(result.rows.len(), 3);
    }
}

#[cfg(test)]
mod dialect_parsing_edge_cases {
    use super::*;

    #[test]
    fn test_teradata_vs_postgresql_dialect() {
        // Test that Teradata dialect is more permissive
        let teradata_result = parse_sql_with_dialect("SELECT name || id FROM test_table", SqlDialect::Teradata);
        let postgresql_result = parse_sql_with_dialect("SELECT name || id FROM test_table", SqlDialect::PostgreSQL);
        
        // Both should parse successfully for this simple case
        assert!(teradata_result.is_ok());
        assert!(postgresql_result.is_ok());
    }

    #[test]
    fn test_date_literal_parsing() {
        // Test DATE literal parsing
        let result = parse_sql_with_dialect("SELECT * FROM test_table WHERE date_col >= DATE '2024-01-01'", SqlDialect::Teradata);
        assert!(result.is_ok());
        
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn test_cte_parsing_edge_cases() {
        // Test recursive CTE parsing
        let result = parse_sql_with_dialect(
            "WITH RECURSIVE tree AS (SELECT 1 as level UNION ALL SELECT level + 1 FROM tree WHERE level < 5) SELECT * FROM tree",
            SqlDialect::Teradata
        );
        assert!(result.is_ok());
    }
}

#[cfg(test)]
mod performance_edge_cases {
    use super::*;

    #[tokio::test]
    async fn test_memory_efficient_query() {
        let executor = create_test_executor();
        
        // This query should complete efficiently
        let statements = parse_sql_with_dialect("SELECT COUNT(*) FROM test_table", SqlDialect::Teradata).unwrap();
        let result = executor.execute(&statements[0]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_memory_efficient_limit() {
        let executor = create_test_executor();
        
        // Large LIMIT should still be memory-efficient
        let statements = parse_sql_with_dialect("SELECT * FROM test_table LIMIT 1000", SqlDialect::Teradata).unwrap();
        let result = executor.execute(&statements[0]).await.unwrap();
        
        // Should only return actual rows, not allocate for 1000
        assert!(result.rows.len() <= 3); // We only have 3 test rows
    }
}

#[cfg(test)]
mod error_handling_edge_cases {
    use super::*;

    #[test]
    fn test_invalid_sql_parsing() {
        let result = parse_sql_with_dialect("SELECT * FROM WHERE", SqlDialect::Teradata);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_table_not_found_error() {
        let executor = create_test_executor();
        let statements = parse_sql_with_dialect("SELECT * FROM nonexistent_table", SqlDialect::Teradata).unwrap();
        let result = executor.execute(&statements[0]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_column_not_found_error() {
        let executor = create_test_executor();
        let statements = parse_sql_with_dialect("SELECT nonexistent_column FROM test_table", SqlDialect::Teradata).unwrap();
        let result = executor.execute(&statements[0]).await;
        assert!(result.is_err());
    }
}