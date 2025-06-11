use std::sync::Arc;
use tokio::sync::RwLock;
use sqlparser::ast::*;
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
                "Only SELECT queries are supported".to_string()
            )),
        }
    }

    async fn execute_query(&self, query: &Box<Query>) -> crate::Result<QueryResult> {
        let db = self.database.read().await;
        
        match &query.body.as_ref() {
            SetExpr::Select(select) => self.execute_select(&db, select, query).await,
            _ => Err(YamlBaseError::NotImplemented(
                "Only simple SELECT queries are supported".to_string()
            )),
        }
    }

    async fn execute_select(&self, db: &Database, select: &Select, query: &Query) -> crate::Result<QueryResult> {
        debug!("Executing SELECT query");
        
        // Get the table
        let table_name = self.extract_table_name(&select.from)?;
        let table = db.get_table(&table_name)
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

    fn extract_table_name(&self, from: &Vec<TableWithJoins>) -> crate::Result<String> {
        if from.is_empty() {
            return Err(YamlBaseError::Database {
                message: "No FROM clause specified".to_string(),
            });
        }
        
        if from.len() > 1 || !from[0].joins.is_empty() {
            return Err(YamlBaseError::NotImplemented(
                "Joins are not yet supported".to_string()
            ));
        }
        
        match &from[0].relation {
            TableFactor::Table { name, .. } => {
                Ok(name.0.first()
                    .ok_or_else(|| YamlBaseError::Database {
                        message: "Invalid table name".to_string(),
                    })?
                    .value.clone())
            }
            _ => Err(YamlBaseError::NotImplemented(
                "Only simple table references are supported".to_string()
            )),
        }
    }

    fn extract_columns(&self, select: &Select, table: &Table) -> crate::Result<Vec<(String, usize)>> {
        let mut columns = Vec::new();
        
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = &ident.value;
                    let col_idx = table.get_column_index(col_name)
                        .ok_or_else(|| YamlBaseError::Database {
                            message: format!("Column '{}' not found", col_name),
                        })?;
                    columns.push((col_name.clone(), col_idx));
                }
                SelectItem::Wildcard(_) => {
                    for (idx, col) in table.columns.iter().enumerate() {
                        columns.push((col.name.clone(), idx));
                    }
                }
                _ => return Err(YamlBaseError::NotImplemented(
                    "Complex projections are not yet supported".to_string()
                )),
            }
        }
        
        Ok(columns)
    }

    fn filter_rows<'a>(&self, table: &'a Table, selection: &Option<Expr>) -> crate::Result<Vec<&'a Vec<Value>>> {
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
            _ => Err(YamlBaseError::NotImplemented(
                format!("Expression type not supported: {:?}", expr)
            )),
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
            _ => Err(YamlBaseError::NotImplemented(
                format!("Binary operator not supported: {:?}", op)
            )),
        }
    }

    fn get_expr_value(&self, expr: &Expr, row: &[Value], table: &Table) -> crate::Result<Value> {
        match expr {
            Expr::Identifier(ident) => {
                let col_idx = table.get_column_index(&ident.value)
                    .ok_or_else(|| YamlBaseError::Database {
                        message: format!("Column '{}' not found", ident.value),
                    })?;
                Ok(row[col_idx].clone())
            }
            Expr::Value(val) => self.sql_value_to_db_value(val),
            _ => Err(YamlBaseError::NotImplemented(
                format!("Expression type not supported: {:?}", expr)
            )),
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
            _ => Err(YamlBaseError::NotImplemented(
                format!("Value type not supported: {:?}", val)
            )),
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
            let limit_val: usize = n.parse().map_err(|_| {
                YamlBaseError::Database {
                    message: format!("Invalid LIMIT value: {}", n),
                }
            })?;
            Ok(rows.into_iter().take(limit_val).collect())
        } else {
            Err(YamlBaseError::NotImplemented(
                "Only numeric LIMIT values are supported".to_string()
            ))
        }
    }
}