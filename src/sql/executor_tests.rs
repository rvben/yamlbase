#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{Column, ColumnType, Database, Row, Table, Value};
    use crate::sql::parse_sql;
    use sqlparser::ast::Statement;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    async fn create_test_database() -> Arc<RwLock<Database>> {
        let mut db = Database::new("test_db".to_string());
        
        // Create a test table with various column types
        let mut table = Table::new();
        table.columns.push(Column {
            name: "id".to_string(),
            column_type: ColumnType::Integer,
        });
        table.columns.push(Column {
            name: "name".to_string(),
            column_type: ColumnType::Text,
        });
        table.columns.push(Column {
            name: "status".to_string(),
            column_type: ColumnType::Text,
        });
        table.columns.push(Column {
            name: "active".to_string(),
            column_type: ColumnType::Text,
        });
        table.columns.push(Column {
            name: "category".to_string(),
            column_type: ColumnType::Text,
        });
        table.columns.push(Column {
            name: "created_date".to_string(),
            column_type: ColumnType::Date,
        });
        table.columns.push(Column {
            name: "score".to_string(),
            column_type: ColumnType::Integer,
        });
        table.columns.push(Column {
            name: "type".to_string(),
            column_type: ColumnType::Text,
        });
        table.columns.push(Column {
            name: "group_name".to_string(),
            column_type: ColumnType::Text,
        });

        // Add test data
        table.rows.push(Row {
            values: vec![
                Value::Integer(1),
                Value::Text("Alpha Project".to_string()),
                Value::Text("Active".to_string()),
                Value::Text("Y".to_string()),
                Value::Text("NS-High".to_string()),
                Value::Date("2025-02-01".to_string()),
                Value::Integer(100),
                Value::Text("Development".to_string()),
                Value::Text("Engineering".to_string()),
            ],
        });
        table.rows.push(Row {
            values: vec![
                Value::Integer(2),
                Value::Text("Beta Test".to_string()),
                Value::Text("In Progress".to_string()),
                Value::Text("Y".to_string()),
                Value::Text("NS-Medium".to_string()),
                Value::Date("2025-03-15".to_string()),
                Value::Integer(85),
                Value::Text("Research".to_string()),
                Value::Text("Science".to_string()),
            ],
        });
        table.rows.push(Row {
            values: vec![
                Value::Integer(3),
                Value::Text("Legacy System".to_string()),
                Value::Text("Closed".to_string()),
                Value::Text("N".to_string()),
                Value::Text("Public".to_string()),
                Value::Date("2024-01-01".to_string()),
                Value::Integer(50),
                Value::Text("Development".to_string()),
                Value::Text("Support IT".to_string()),
            ],
        });
        table.rows.push(Row {
            values: vec![
                Value::Integer(4),
                Value::Text("Gamma Ray".to_string()),
                Value::Text("Cancelled".to_string()),
                Value::Text("Y".to_string()),
                Value::Text("NS-Low".to_string()),
                Value::Date("2024-06-01".to_string()),
                Value::Integer(0),
                Value::Text("Testing".to_string()),
                Value::Text("QA".to_string()),
            ],
        });

        db.tables.insert("test_table".to_string(), table);
        Arc::new(RwLock::new(db))
    }

    fn parse_statement(sql: &str) -> Statement {
        let statements = parse_sql(sql).unwrap();
        statements.into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn test_not_in_operator() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // Test NOT IN with multiple values
        let stmt = parse_statement("SELECT id, name, status FROM test_table WHERE status NOT IN ('Cancelled', 'Closed')");
        let result = executor.execute(&stmt).await.unwrap();
        
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[0][2], Value::Text("Active".to_string()));
        assert_eq!(result.rows[1][0], Value::Integer(2));
        assert_eq!(result.rows[1][2], Value::Text("In Progress".to_string()));
    }

    #[tokio::test]
    async fn test_in_operator() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // Test IN operator
        let stmt = parse_statement("SELECT id, type FROM test_table WHERE type IN ('Development', 'Research')");
        let result = executor.execute(&stmt).await.unwrap();
        
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0][1], Value::Text("Development".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("Research".to_string()));
        assert_eq!(result.rows[2][1], Value::Text("Development".to_string()));
    }

    #[tokio::test]
    async fn test_like_operator() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // Test LIKE with % wildcard at end
        let stmt = parse_statement("SELECT id, name FROM test_table WHERE name LIKE 'Alpha%'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Value::Text("Alpha Project".to_string()));

        // Test LIKE with % wildcard at beginning
        let stmt = parse_statement("SELECT id, name FROM test_table WHERE name LIKE '%Test'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Value::Text("Beta Test".to_string()));

        // Test LIKE with % wildcard in middle
        let stmt = parse_statement("SELECT id, category FROM test_table WHERE category LIKE 'NS%'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 3);

        // Test LIKE with _ wildcard
        let stmt = parse_statement("SELECT id, name FROM test_table WHERE name LIKE 'Bet_ Test'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Value::Text("Beta Test".to_string()));
    }

    #[tokio::test]
    async fn test_not_like_operator() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // Test NOT LIKE
        let stmt = parse_statement("SELECT id, category FROM test_table WHERE category NOT LIKE 'NS%'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(3));
    }

    #[tokio::test]
    async fn test_not_equals_operator() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // Test <> operator
        let stmt = parse_statement("SELECT id, active FROM test_table WHERE active <> 'Y'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(3));
        assert_eq!(result.rows[0][1], Value::Text("N".to_string()));

        // Test != operator (alternative syntax)
        let stmt = parse_statement("SELECT id, active FROM test_table WHERE active != 'Y'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(3));
    }

    #[tokio::test]
    async fn test_date_literals() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // Test DATE literal comparison
        let stmt = parse_statement("SELECT id, name, created_date FROM test_table WHERE created_date > DATE '2025-01-01'");
        let result = executor.execute(&stmt).await.unwrap();
        
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[0][2], Value::Date("2025-02-01".to_string()));
        assert_eq!(result.rows[1][0], Value::Integer(2));
        assert_eq!(result.rows[1][2], Value::Date("2025-03-15".to_string()));
    }

    #[tokio::test]
    async fn test_complex_where_clauses() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // Test complex nested AND/OR conditions
        let stmt = parse_statement(
            "SELECT id, name, status, category FROM test_table 
             WHERE (status = 'Active' OR status = 'In Progress') 
             AND category LIKE 'NS%' 
             AND created_date > DATE '2025-01-01'"
        );
        let result = executor.execute(&stmt).await.unwrap();
        
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[1][0], Value::Integer(2));
    }

    #[tokio::test]
    async fn test_multiple_conditions_with_all_operators() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // Test query similar to the Enterprise query with all operators
        let stmt = parse_statement(
            "SELECT id, name FROM test_table 
             WHERE status NOT IN ('Cancelled', 'Closed')
             AND active = 'Y'
             AND category LIKE 'NS%'
             AND created_date > DATE '2025-01-01'
             AND group_name NOT IN ('Support IT', 'QA')
             AND type IN ('Development', 'Research')
             AND score <> 0"
        );
        let result = executor.execute(&stmt).await.unwrap();
        
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][1], Value::Text("Alpha Project".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("Beta Test".to_string()));
    }

    #[tokio::test]
    async fn test_like_with_special_regex_chars() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // Add a row with special regex characters
        {
            let mut db_write = db.write().await;
            if let Some(table) = db_write.tables.get_mut("test_table") {
                table.rows.push(Row {
                    values: vec![
                        Value::Integer(5),
                        Value::Text("Test.Project".to_string()),
                        Value::Text("Active".to_string()),
                        Value::Text("Y".to_string()),
                        Value::Text("NS[Test]".to_string()),
                        Value::Date("2025-04-01".to_string()),
                        Value::Integer(90),
                        Value::Text("Development".to_string()),
                        Value::Text("Engineering".to_string()),
                    ],
                });
            }
        }

        // Test LIKE with dots (should match literal dots, not any character)
        let stmt = parse_statement("SELECT id, name FROM test_table WHERE name LIKE 'Test.Project'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Value::Text("Test.Project".to_string()));

        // Test LIKE with brackets
        let stmt = parse_statement("SELECT id, category FROM test_table WHERE category LIKE 'NS[Test]'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Value::Text("NS[Test]".to_string()));
    }

    #[tokio::test]
    async fn test_empty_in_list() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // This should parse but match nothing
        let stmt = parse_statement("SELECT id FROM test_table WHERE status IN ()");
        match executor.execute(&stmt).await {
            Ok(result) => assert_eq!(result.rows.len(), 0),
            Err(_) => {
                // Some SQL parsers might reject empty IN lists, which is also acceptable
            }
        }
    }

    #[tokio::test]
    async fn test_case_sensitivity() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // Test that string comparisons are case-sensitive
        let stmt = parse_statement("SELECT id FROM test_table WHERE status = 'active'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 0); // Should not match 'Active'

        let stmt = parse_statement("SELECT id FROM test_table WHERE status = 'Active'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_null_handling_in_comparisons() {
        let db = create_test_database().await;
        
        // Add a row with NULL values
        {
            let mut db_write = db.write().await;
            if let Some(table) = db_write.tables.get_mut("test_table") {
                table.rows.push(Row {
                    values: vec![
                        Value::Integer(6),
                        Value::Text("Null Test".to_string()),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Date("2025-05-01".to_string()),
                        Value::Null,
                        Value::Text("Development".to_string()),
                        Value::Null,
                    ],
                });
            }
        }

        let executor = QueryExecutor::new(db);

        // NULL should not match any comparison
        let stmt = parse_statement("SELECT id FROM test_table WHERE status = 'Active'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1); // Only the non-NULL Active row

        // NULL should not match NOT IN
        let stmt = parse_statement("SELECT id FROM test_table WHERE status NOT IN ('Cancelled', 'Closed')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2); // NULL status is not included
    }
}