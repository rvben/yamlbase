use sqlparser::ast::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

use crate::database::{Database, Table, Value};
use crate::YamlBaseError;

pub struct QueryExecutor {
    database: Arc<RwLock<Database>>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl QueryExecutor {
    pub fn new(database: Arc<RwLock<Database>>) -> Self {
        Self { database }
    }

    pub async fn execute(&self, statement: &Statement) -> crate::Result<QueryResult> {
        match statement {
            Statement::Query(query) => self.execute_query(query).await,
            _ => Err(YamlBaseError::NotImplemented(
                "Only SELECT queries are supported".to_string(),
            )),
        }
    }

    async fn execute_query(&self, query: &Query) -> crate::Result<QueryResult> {
        let db = self.database.read().await;

        match &query.body.as_ref() {
            SetExpr::Select(select) => self.execute_select(&db, select, query).await,
            _ => Err(YamlBaseError::NotImplemented(
                "Only simple SELECT queries are supported".to_string(),
            )),
        }
    }

    async fn execute_select(
        &self,
        db: &Database,
        select: &Select,
        query: &Query,
    ) -> crate::Result<QueryResult> {
        debug!("Executing SELECT query");

        // Handle SELECT without FROM (e.g., SELECT 1, SELECT @@version)
        if select.from.is_empty() {
            return self.execute_select_without_from(select).await;
        }

        // Get the table
        let table_name = self.extract_table_name(&select.from)?;
        let table = db
            .get_table(&table_name)
            .ok_or_else(|| YamlBaseError::Database {
                message: format!("Table '{}' not found", table_name),
            })?;

        // Get column names for projection
        let columns = self.extract_columns(select, table)?;

        // Filter rows based on WHERE clause
        let filtered_rows = self.filter_rows(table, &select.selection)?;

        // Project columns
        let projected_rows = self.project_columns(&filtered_rows, &columns, table)?;

        // Apply ORDER BY
        let sorted_rows = if let Some(order_by) = &query.order_by {
            self.sort_rows(projected_rows, &order_by.exprs, &columns)?
        } else {
            projected_rows
        };

        // Apply LIMIT and OFFSET
        let final_rows = if let Some(limit_expr) = &query.limit {
            self.apply_limit(sorted_rows, limit_expr)?
        } else {
            sorted_rows
        };

        Ok(QueryResult {
            columns: columns.iter().map(|c| c.0.clone()).collect(),
            rows: final_rows,
        })
    }

    async fn execute_select_without_from(&self, select: &Select) -> crate::Result<QueryResult> {
        let mut columns = Vec::new();
        let mut row_values = Vec::new();

        for (idx, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let value = self.evaluate_constant_expr(expr)?;
                    let col_name = format!("column_{}", idx + 1);
                    columns.push(col_name);
                    row_values.push(value);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let value = self.evaluate_constant_expr(expr)?;
                    columns.push(alias.value.clone());
                    row_values.push(value);
                }
                _ => {
                    return Err(YamlBaseError::NotImplemented(
                        "Complex projections in SELECT without FROM are not supported".to_string(),
                    ))
                }
            }
        }

        Ok(QueryResult {
            columns,
            rows: vec![row_values],
        })
    }

    fn evaluate_constant_expr(&self, expr: &Expr) -> crate::Result<Value> {
        match expr {
            Expr::Value(val) => self.sql_value_to_db_value(val),
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Minus => {
                    let val = self.evaluate_constant_expr(expr)?;
                    match val {
                        Value::Integer(i) => Ok(Value::Integer(-i)),
                        Value::Double(d) => Ok(Value::Double(-d)),
                        _ => Err(YamlBaseError::Database {
                            message: "Cannot negate non-numeric value".to_string(),
                        }),
                    }
                }
                _ => Err(YamlBaseError::NotImplemented(
                    "Unsupported unary operator".to_string(),
                )),
            },
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_constant_expr(left)?;
                let right_val = self.evaluate_constant_expr(right)?;
                self.evaluate_binary_op_constant(&left_val, op, &right_val)
            }
            _ => Err(YamlBaseError::NotImplemented(
                "Only constant expressions are supported in SELECT without FROM".to_string(),
            )),
        }
    }

    fn evaluate_binary_op_constant(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> crate::Result<Value> {
        match op {
            BinaryOperator::Plus => match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a + b)),
                (Value::Double(a), Value::Double(b)) => Ok(Value::Double(a + b)),
                (Value::Integer(a), Value::Double(b)) => Ok(Value::Double(*a as f64 + b)),
                (Value::Double(a), Value::Integer(b)) => Ok(Value::Double(a + *b as f64)),
                _ => Err(YamlBaseError::Database {
                    message: "Cannot add non-numeric values".to_string(),
                }),
            },
            BinaryOperator::Minus => match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a - b)),
                (Value::Double(a), Value::Double(b)) => Ok(Value::Double(a - b)),
                (Value::Integer(a), Value::Double(b)) => Ok(Value::Double(*a as f64 - b)),
                (Value::Double(a), Value::Integer(b)) => Ok(Value::Double(a - *b as f64)),
                _ => Err(YamlBaseError::Database {
                    message: "Cannot subtract non-numeric values".to_string(),
                }),
            },
            BinaryOperator::Multiply => match (left, right) {
                (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a * b)),
                (Value::Double(a), Value::Double(b)) => Ok(Value::Double(a * b)),
                (Value::Integer(a), Value::Double(b)) => Ok(Value::Double(*a as f64 * b)),
                (Value::Double(a), Value::Integer(b)) => Ok(Value::Double(a * *b as f64)),
                _ => Err(YamlBaseError::Database {
                    message: "Cannot multiply non-numeric values".to_string(),
                }),
            },
            BinaryOperator::Divide => match (left, right) {
                (_, Value::Integer(0)) => Err(YamlBaseError::Database {
                    message: "Division by zero".to_string(),
                }),
                (_, Value::Double(d)) if *d == 0.0 => Err(YamlBaseError::Database {
                    message: "Division by zero".to_string(),
                }),
                (Value::Integer(a), Value::Integer(b)) => Ok(Value::Double(*a as f64 / *b as f64)),
                (Value::Double(a), Value::Double(b)) => Ok(Value::Double(a / b)),
                (Value::Integer(a), Value::Double(b)) => Ok(Value::Double(*a as f64 / b)),
                (Value::Double(a), Value::Integer(b)) => Ok(Value::Double(a / *b as f64)),
                _ => Err(YamlBaseError::Database {
                    message: "Cannot divide non-numeric values".to_string(),
                }),
            },
            _ => Err(YamlBaseError::NotImplemented(
                "Binary operator not supported in constant expressions".to_string(),
            )),
        }
    }

    fn extract_table_name(&self, from: &[TableWithJoins]) -> crate::Result<String> {
        if from.is_empty() {
            return Err(YamlBaseError::Database {
                message: "No FROM clause specified".to_string(),
            });
        }

        if from.len() > 1 || !from[0].joins.is_empty() {
            return Err(YamlBaseError::NotImplemented(
                "Joins are not yet supported".to_string(),
            ));
        }

        match &from[0].relation {
            TableFactor::Table { name, .. } => Ok(name
                .0
                .first()
                .ok_or_else(|| YamlBaseError::Database {
                    message: "Invalid table name".to_string(),
                })?
                .value
                .clone()),
            _ => Err(YamlBaseError::NotImplemented(
                "Only simple table references are supported".to_string(),
            )),
        }
    }

    fn extract_columns(
        &self,
        select: &Select,
        table: &Table,
    ) -> crate::Result<Vec<(String, usize)>> {
        let mut columns = Vec::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = &ident.value;
                    let col_idx = table.get_column_index(col_name).ok_or_else(|| {
                        YamlBaseError::Database {
                            message: format!("Column '{}' not found", col_name),
                        }
                    })?;
                    columns.push((col_name.clone(), col_idx));
                }
                SelectItem::Wildcard(_) => {
                    for (idx, col) in table.columns.iter().enumerate() {
                        columns.push((col.name.clone(), idx));
                    }
                }
                _ => {
                    return Err(YamlBaseError::NotImplemented(
                        "Complex projections are not yet supported".to_string(),
                    ))
                }
            }
        }

        Ok(columns)
    }

    fn filter_rows<'a>(
        &self,
        table: &'a Table,
        selection: &Option<Expr>,
    ) -> crate::Result<Vec<&'a Vec<Value>>> {
        let mut result = Vec::new();

        for row in &table.rows {
            if let Some(where_expr) = selection {
                if self.evaluate_expr(where_expr, row, table)? {
                    result.push(row);
                }
            } else {
                result.push(row);
            }
        }

        Ok(result)
    }

    fn evaluate_expr(&self, expr: &Expr, row: &[Value], table: &Table) -> crate::Result<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                self.evaluate_binary_op(left, op, right, row, table)
            }
            Expr::Value(sqlparser::ast::Value::Boolean(b)) => Ok(*b),
            _ => Err(YamlBaseError::NotImplemented(format!(
                "Expression type not supported: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        row: &[Value],
        table: &Table,
    ) -> crate::Result<bool> {
        let left_val = self.get_expr_value(left, row, table)?;
        let right_val = self.get_expr_value(right, row, table)?;

        match op {
            BinaryOperator::Eq => Ok(left_val == right_val),
            BinaryOperator::NotEq => Ok(left_val != right_val),
            BinaryOperator::Lt => {
                if let Some(ord) = left_val.compare(&right_val) {
                    Ok(ord.is_lt())
                } else {
                    Ok(false)
                }
            }
            BinaryOperator::LtEq => {
                if let Some(ord) = left_val.compare(&right_val) {
                    Ok(ord.is_le())
                } else {
                    Ok(false)
                }
            }
            BinaryOperator::Gt => {
                if let Some(ord) = left_val.compare(&right_val) {
                    Ok(ord.is_gt())
                } else {
                    Ok(false)
                }
            }
            BinaryOperator::GtEq => {
                if let Some(ord) = left_val.compare(&right_val) {
                    Ok(ord.is_ge())
                } else {
                    Ok(false)
                }
            }
            BinaryOperator::And => {
                let left_bool = self.evaluate_expr(left, row, table)?;
                let right_bool = self.evaluate_expr(right, row, table)?;
                Ok(left_bool && right_bool)
            }
            BinaryOperator::Or => {
                let left_bool = self.evaluate_expr(left, row, table)?;
                let right_bool = self.evaluate_expr(right, row, table)?;
                Ok(left_bool || right_bool)
            }
            _ => Err(YamlBaseError::NotImplemented(format!(
                "Binary operator not supported: {:?}",
                op
            ))),
        }
    }

    fn get_expr_value(&self, expr: &Expr, row: &[Value], table: &Table) -> crate::Result<Value> {
        match expr {
            Expr::Identifier(ident) => {
                let col_idx = table.get_column_index(&ident.value).ok_or_else(|| {
                    YamlBaseError::Database {
                        message: format!("Column '{}' not found", ident.value),
                    }
                })?;
                Ok(row[col_idx].clone())
            }
            Expr::Value(val) => self.sql_value_to_db_value(val),
            _ => Err(YamlBaseError::NotImplemented(format!(
                "Expression type not supported: {:?}",
                expr
            ))),
        }
    }

    fn sql_value_to_db_value(&self, val: &sqlparser::ast::Value) -> crate::Result<Value> {
        match val {
            sqlparser::ast::Value::Number(n, _) => {
                if n.contains('.') {
                    Ok(Value::Double(n.parse().map_err(|_| {
                        YamlBaseError::TypeConversion(format!("Invalid number: {}", n))
                    })?))
                } else {
                    Ok(Value::Integer(n.parse().map_err(|_| {
                        YamlBaseError::TypeConversion(format!("Invalid integer: {}", n))
                    })?))
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => Ok(Value::Text(s.clone())),
            sqlparser::ast::Value::Boolean(b) => Ok(Value::Boolean(*b)),
            sqlparser::ast::Value::Null => Ok(Value::Null),
            _ => Err(YamlBaseError::NotImplemented(format!(
                "Value type not supported: {:?}",
                val
            ))),
        }
    }

    fn project_columns(
        &self,
        rows: &[&Vec<Value>],
        columns: &[(String, usize)],
        _table: &Table,
    ) -> crate::Result<Vec<Vec<Value>>> {
        let mut result = Vec::new();

        for row in rows {
            let mut projected_row = Vec::new();
            for (_, idx) in columns {
                projected_row.push(row[*idx].clone());
            }
            result.push(projected_row);
        }

        Ok(result)
    }

    fn sort_rows(
        &self,
        mut rows: Vec<Vec<Value>>,
        order_by: &[OrderByExpr],
        columns: &[(String, usize)],
    ) -> crate::Result<Vec<Vec<Value>>> {
        // Create a mapping from column names to indices in the projected rows
        let col_map: std::collections::HashMap<&str, usize> = columns
            .iter()
            .enumerate()
            .map(|(idx, (name, _))| (name.as_str(), idx))
            .collect();

        rows.sort_by(|a, b| {
            for order_expr in order_by {
                if let Expr::Identifier(ident) = &order_expr.expr {
                    if let Some(&idx) = col_map.get(ident.value.as_str()) {
                        if let Some(ord) = a[idx].compare(&b[idx]) {
                            let ord = if order_expr.asc.unwrap_or(true) {
                                ord
                            } else {
                                ord.reverse()
                            };
                            if !ord.is_eq() {
                                return ord;
                            }
                        }
                    }
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    fn apply_limit(&self, rows: Vec<Vec<Value>>, limit: &Expr) -> crate::Result<Vec<Vec<Value>>> {
        if let Expr::Value(sqlparser::ast::Value::Number(n, _)) = limit {
            let limit_val: usize = n.parse().map_err(|_| YamlBaseError::Database {
                message: format!("Invalid LIMIT value: {}", n),
            })?;
            Ok(rows.into_iter().take(limit_val).collect())
        } else {
            Err(YamlBaseError::NotImplemented(
                "Only numeric LIMIT values are supported".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{Column, Database, Table, Value};
    use sqlparser::ast::Statement;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    async fn create_test_database() -> Arc<RwLock<Database>> {
        let mut db = Database::new("test_db".to_string());

        // Add a test table
        let columns = vec![
            Column {
                name: "id".to_string(),
                sql_type: crate::yaml::schema::SqlType::Integer,
                primary_key: true,
                nullable: false,
                unique: true,
                default: None,
                references: None,
            },
            Column {
                name: "name".to_string(),
                sql_type: crate::yaml::schema::SqlType::Varchar(100),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ];

        let mut table = Table::new("users".to_string(), columns);

        table
            .insert_row(vec![Value::Integer(1), Value::Text("Alice".to_string())])
            .unwrap();
        table
            .insert_row(vec![Value::Integer(2), Value::Text("Bob".to_string())])
            .unwrap();

        db.add_table(table).unwrap();
        Arc::new(RwLock::new(db))
    }

    fn parse_statement(sql: &str) -> Statement {
        crate::sql::parse_sql(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    #[tokio::test]
    async fn test_select_without_from_simple() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        let stmt = parse_statement("SELECT 1");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0], "column_1");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(1));
    }

    #[tokio::test]
    async fn test_select_without_from_multiple_values() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        let stmt = parse_statement("SELECT 1, 2, 3");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0], "column_1");
        assert_eq!(result.columns[1], "column_2");
        assert_eq!(result.columns[2], "column_3");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[0][1], Value::Integer(2));
        assert_eq!(result.rows[0][2], Value::Integer(3));
    }

    #[tokio::test]
    async fn test_select_without_from_with_alias() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        let stmt = parse_statement("SELECT 1 AS num, 'hello' AS greeting");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0], "num");
        assert_eq!(result.columns[1], "greeting");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[0][1], Value::Text("hello".to_string()));
    }

    #[tokio::test]
    async fn test_select_without_from_arithmetic() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        // Test addition
        let stmt = parse_statement("SELECT 1 + 1");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(2));

        // Test subtraction
        let stmt = parse_statement("SELECT 5 - 3");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(2));

        // Test multiplication
        let stmt = parse_statement("SELECT 3 * 4");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(12));

        // Test division
        let stmt = parse_statement("SELECT 10 / 2");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(5.0));
    }

    #[tokio::test]
    async fn test_select_without_from_mixed_types() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        let stmt = parse_statement("SELECT 42, 'test', true, null");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 4);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(42));
        assert_eq!(result.rows[0][1], Value::Text("test".to_string()));
        assert_eq!(result.rows[0][2], Value::Boolean(true));
        assert_eq!(result.rows[0][3], Value::Null);
    }

    #[tokio::test]
    async fn test_select_without_from_negative_numbers() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        let stmt = parse_statement("SELECT -5");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(-5));

        let stmt = parse_statement("SELECT -3.14");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(-3.14));
    }

    #[tokio::test]
    async fn test_select_without_from_division_by_zero() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        let stmt = parse_statement("SELECT 1 / 0");
        let result = executor.execute(&stmt).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Division by zero"));
    }

    #[tokio::test]
    async fn test_select_with_from_still_works() {
        let db = create_test_database().await;
        let executor = QueryExecutor::new(db);

        let stmt = parse_statement("SELECT * FROM users");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0], "id");
        assert_eq!(result.columns[1], "name");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[0][1], Value::Text("Alice".to_string()));
        assert_eq!(result.rows[1][0], Value::Integer(2));
        assert_eq!(result.rows[1][1], Value::Text("Bob".to_string()));
    }
}
