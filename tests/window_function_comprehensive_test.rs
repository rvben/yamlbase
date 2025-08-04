use yamlbase::sql::parser::parse_sql;

#[test]
fn test_window_function_parsing() {
    // Test basic window functions
    let test_cases = vec![
        // Basic ROW_NUMBER
        "SELECT username, ROW_NUMBER() OVER (ORDER BY id) as row_num FROM users",
        // ROW_NUMBER with PARTITION BY
        "SELECT username, ROW_NUMBER() OVER (PARTITION BY department ORDER BY id) as row_num FROM users",
        // RANK function
        "SELECT username, RANK() OVER (ORDER BY score DESC) as rank FROM users",
        // RANK with PARTITION BY
        "SELECT username, RANK() OVER (PARTITION BY department ORDER BY score DESC) as rank FROM users",
        // Multiple window functions
        "SELECT username, 
         ROW_NUMBER() OVER (ORDER BY id) as row_num,
         RANK() OVER (ORDER BY score DESC) as rank 
         FROM users",
        // Window function with multiple PARTITION BY columns
        "SELECT username, ROW_NUMBER() OVER (PARTITION BY department, team ORDER BY id) as row_num FROM users",
        // Empty OVER clause
        "SELECT username, ROW_NUMBER() OVER () as row_num FROM users",
    ];

    for sql in test_cases {
        match parse_sql(sql) {
            Ok(statements) => {
                assert_eq!(statements.len(), 1, "Should parse to one statement");

                // Verify it's a SELECT statement with window functions
                if let sqlparser::ast::Statement::Query(query) = &statements[0] {
                    if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
                        // Check that we have window functions in projection
                        let has_window_function = select.projection.iter().any(|item| {
                            if let sqlparser::ast::SelectItem::ExprWithAlias {
                                expr: sqlparser::ast::Expr::Function(func),
                                ..
                            } = item
                            {
                                return func.over.is_some();
                            }
                            false
                        });

                        assert!(
                            has_window_function,
                            "Query should contain window function: {sql}"
                        );
                    }
                }
            }
            Err(e) => {
                panic!("Failed to parse window function SQL '{sql}': {e}");
            }
        }
    }
}

#[test]
fn test_window_function_with_partition_by_parsing() {
    let sql = "SELECT username, ROW_NUMBER() OVER (PARTITION BY department ORDER BY id) as row_num FROM users";

    let result = parse_sql(sql);
    assert!(result.is_ok(), "Should parse successfully");

    let statements = result.unwrap();
    if let sqlparser::ast::Statement::Query(query) = &statements[0] {
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
            // Find the window function
            for item in &select.projection {
                if let sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } = item {
                    if alias.value == "row_num" {
                        if let sqlparser::ast::Expr::Function(func) = expr {
                            assert!(func.over.is_some(), "Should have OVER clause");

                            if let Some(sqlparser::ast::WindowType::WindowSpec(spec)) = &func.over {
                                assert!(!spec.partition_by.is_empty(), "Should have PARTITION BY");
                                assert_eq!(
                                    spec.partition_by.len(),
                                    1,
                                    "Should have one partition column"
                                );

                                assert!(!spec.order_by.is_empty(), "Should have ORDER BY");
                                assert_eq!(spec.order_by.len(), 1, "Should have one order column");
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn test_multiple_window_functions_in_query() {
    let sql = "SELECT 
        username,
        ROW_NUMBER() OVER (ORDER BY id) as row_num,
        RANK() OVER (PARTITION BY department ORDER BY score DESC) as dept_rank
    FROM users";

    let result = parse_sql(sql);
    assert!(result.is_ok(), "Should parse successfully");

    let statements = result.unwrap();
    if let sqlparser::ast::Statement::Query(query) = &statements[0] {
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
            let window_function_count = select
                .projection
                .iter()
                .filter(|item| {
                    if let sqlparser::ast::SelectItem::ExprWithAlias {
                        expr: sqlparser::ast::Expr::Function(func),
                        ..
                    } = item
                    {
                        return func.over.is_some();
                    }
                    false
                })
                .count();

            assert_eq!(window_function_count, 2, "Should have two window functions");
        }
    }
}
