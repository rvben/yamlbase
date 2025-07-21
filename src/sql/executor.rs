use chrono::{self, Datelike, NaiveDate};
use regex::Regex;
use rust_decimal::prelude::*;
use sqlparser::ast::{
    BinaryOperator, DataType, DateTimeField, DuplicateTreatment, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, OrderByExpr,
    Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, UnaryOperator,
    With,
};
use std::sync::Arc;
use tracing::debug;

use crate::YamlBaseError;
use crate::database::{Database, Storage, Table, Value};

pub struct QueryExecutor {
    storage: Arc<Storage>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub column_types: Vec<crate::yaml::schema::SqlType>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone)]
enum ProjectionItem {
    // A column from the table (name, index)
    TableColumn(String, usize),
    // A constant expression with its computed value and column alias
    Constant(String, Value),
    // An expression that needs to be evaluated per row
    Expression(String, Box<Expr>),
}

#[derive(Debug, Clone)]
enum JoinedColumn {
    // A column from a specific table (display_name, table_idx, column_idx)
    TableColumn(String, usize, usize),
    // A constant expression with its computed value and column alias
    Constant(String, Value),
}

impl JoinedColumn {
    fn get_name(&self) -> String {
        match self {
            JoinedColumn::TableColumn(name, _, _) => name.clone(),
            JoinedColumn::Constant(name, _) => name.clone(),
        }
    }

    fn get_type(&self, tables: &[(String, &Table)]) -> crate::yaml::schema::SqlType {
        match self {
            JoinedColumn::TableColumn(_, table_idx, col_idx) => {
                tables[*table_idx].1.columns[*col_idx].sql_type.clone()
            }
            JoinedColumn::Constant(_, value) => {
                match value {
                    Value::Integer(i) => {
                        if *i > i32::MAX as i64 || *i < i32::MIN as i64 {
                            crate::yaml::schema::SqlType::BigInt
                        } else {
                            crate::yaml::schema::SqlType::Integer
                        }
                    }
                    Value::Float(_) => crate::yaml::schema::SqlType::Float,
                    Value::Double(_) => crate::yaml::schema::SqlType::Double,
                    Value::Decimal(_) => crate::yaml::schema::SqlType::Decimal(10, 2), // Default precision
                    Value::Text(_) => crate::yaml::schema::SqlType::Text,
                    Value::Date(_) => crate::yaml::schema::SqlType::Date,
                    Value::Time(_) => crate::yaml::schema::SqlType::Time,
                    Value::Timestamp(_) => crate::yaml::schema::SqlType::Timestamp,
                    Value::Boolean(_) => crate::yaml::schema::SqlType::Boolean,
                    Value::Uuid(_) => crate::yaml::schema::SqlType::Uuid,
                    Value::Json(_) => crate::yaml::schema::SqlType::Text, // JSON as text
                    Value::Null => crate::yaml::schema::SqlType::Text,
                }
            }
        }
    }
}

impl QueryExecutor {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    pub fn storage(&self) -> &Arc<Storage> {
        &self.storage
    }

    pub async fn execute(&self, statement: &Statement) -> crate::Result<QueryResult> {
        match statement {
            Statement::Query(query) => self.execute_query(query).await,
            Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. } => {
                // Return empty result for transaction commands (no-op in read-only mode)
                Ok(QueryResult {
                    columns: vec![],
                    column_types: vec![],
                    rows: vec![],
                })
            }
            _ => Err(YamlBaseError::NotImplemented(
                "Only SELECT queries are supported".to_string(),
            )),
        }
    }

    async fn execute_query(&self, query: &Query) -> crate::Result<QueryResult> {
        let db_arc = self.storage.database();
        let db = db_arc.read().await;

        // Handle CTEs if present
        if let Some(with) = &query.with {
            return self.execute_query_with_ctes(&db, query, with).await;
        }

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

        // Check if query has joins
        if self.has_joins(&select.from) {
            return self.execute_select_with_joins(db, select, query).await;
        }

        // Get the table
        let table_name = self.extract_table_name(&select.from)?;
        let table = db
            .get_table(&table_name)
            .ok_or_else(|| YamlBaseError::Database {
                message: format!("Table '{}' not found", table_name),
            })?;

        // Check if this is an aggregate query
        if self.is_aggregate_query(select) {
            return self
                .execute_aggregate_select(db, select, query, table, &table_name)
                .await;
        }

        // Get column names for projection
        let columns = self.extract_columns(select, table)?;

        // Filter rows based on WHERE clause
        let filtered_rows = self
            .filter_rows(table, &table_name, &select.selection)
            .await?;

        // Project columns
        let projected_rows = self.project_columns(&filtered_rows, &columns, table)?;

        // Apply DISTINCT if specified
        let distinct_rows = if select.distinct.is_some() {
            self.apply_distinct(projected_rows)?
        } else {
            projected_rows
        };

        // Apply ORDER BY
        let sorted_rows = if let Some(order_by) = &query.order_by {
            // Convert ProjectionItem to (String, usize) for compatibility with sort_rows
            let col_info: Vec<(String, usize)> = columns
                .iter()
                .enumerate()
                .map(|(idx, item)| match item {
                    ProjectionItem::TableColumn(name, _) => (name.clone(), idx),
                    ProjectionItem::Constant(name, _) => (name.clone(), idx),
                    ProjectionItem::Expression(name, _) => (name.clone(), idx),
                })
                .collect();
            self.sort_rows(distinct_rows, &order_by.exprs, &col_info)?
        } else {
            distinct_rows
        };

        // Apply LIMIT and OFFSET
        let final_rows = if let Some(limit_expr) = &query.limit {
            self.apply_limit(sorted_rows, limit_expr)?
        } else {
            sorted_rows
        };

        // Get column types
        let column_types = columns
            .iter()
            .map(|item| {
                match item {
                    ProjectionItem::TableColumn(_, idx) => table.columns[*idx].sql_type.clone(),
                    ProjectionItem::Constant(_, value) => {
                        // Infer type from value
                        match value {
                            Value::Integer(i) => {
                                if *i > i32::MAX as i64 || *i < i32::MIN as i64 {
                                    crate::yaml::schema::SqlType::BigInt
                                } else {
                                    crate::yaml::schema::SqlType::Integer
                                }
                            }
                            Value::Double(_) | Value::Float(_) => {
                                crate::yaml::schema::SqlType::Double
                            }
                            Value::Boolean(_) => crate::yaml::schema::SqlType::Boolean,
                            Value::Date(_) => crate::yaml::schema::SqlType::Date,
                            Value::Time(_) => crate::yaml::schema::SqlType::Time,
                            Value::Timestamp(_) => crate::yaml::schema::SqlType::Timestamp,
                            Value::Uuid(_) => crate::yaml::schema::SqlType::Uuid,
                            Value::Json(_) => crate::yaml::schema::SqlType::Text,
                            Value::Decimal(_) => crate::yaml::schema::SqlType::Decimal(10, 2),
                            Value::Text(_) => crate::yaml::schema::SqlType::Text,
                            Value::Null => crate::yaml::schema::SqlType::Text,
                        }
                    }
                    ProjectionItem::Expression(_, _) => {
                        // For expressions, default to Text type since we can't easily infer
                        // This could be improved by analyzing the expression
                        crate::yaml::schema::SqlType::Text
                    }
                }
            })
            .collect();

        // Get column names
        let column_names = columns
            .iter()
            .map(|item| match item {
                ProjectionItem::TableColumn(name, _) => name.clone(),
                ProjectionItem::Constant(name, _) => name.clone(),
                ProjectionItem::Expression(name, _) => name.clone(),
            })
            .collect();

        Ok(QueryResult {
            columns: column_names,
            column_types,
            rows: final_rows,
        })
    }

    async fn execute_select_without_from(&self, select: &Select) -> crate::Result<QueryResult> {
        debug!("Executing SELECT without FROM");
        let mut columns = Vec::new();
        let mut row_values = Vec::new();

        for (idx, item) in select.projection.iter().enumerate() {
            debug!("Processing projection item {}: {:?}", idx, item);
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let value = self.evaluate_constant_expr(expr)?;
                    let col_name = format!("column_{}", idx + 1);
                    debug!("Adding column: {} with value: {:?}", col_name, value);
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
                    ));
                }
            }
        }

        // Infer types from the values
        let column_types = row_values
            .iter()
            .map(|value| {
                match value {
                    Value::Integer(i) => {
                        // Use BigInt for values that might be larger than i32
                        if *i > i32::MAX as i64 || *i < i32::MIN as i64 {
                            crate::yaml::schema::SqlType::BigInt
                        } else {
                            crate::yaml::schema::SqlType::Integer
                        }
                    }
                    Value::Double(_) | Value::Float(_) => crate::yaml::schema::SqlType::Double,
                    Value::Boolean(_) => crate::yaml::schema::SqlType::Boolean,
                    Value::Date(_) => crate::yaml::schema::SqlType::Date,
                    Value::Time(_) => crate::yaml::schema::SqlType::Time,
                    Value::Timestamp(_) => crate::yaml::schema::SqlType::Timestamp,
                    Value::Uuid(_) => crate::yaml::schema::SqlType::Uuid,
                    Value::Json(_) => crate::yaml::schema::SqlType::Text,
                    Value::Decimal(_) => crate::yaml::schema::SqlType::Decimal(10, 2),
                    Value::Text(_) => crate::yaml::schema::SqlType::Text,
                    Value::Null => crate::yaml::schema::SqlType::Text,
                }
            })
            .collect();

        let result = QueryResult {
            columns: columns.clone(),
            column_types,
            rows: vec![row_values],
        };
        debug!(
            "SELECT without FROM complete. Columns: {:?}, Rows: {:?}",
            result.columns, result.rows
        );
        Ok(result)
    }

    fn evaluate_constant_expr(&self, expr: &Expr) -> crate::Result<Value> {
        debug!("Evaluating constant expression: {:?}", expr);
        match expr {
            Expr::Value(val) => {
                debug!("Converting SQL value to DB value: {:?}", val);
                self.sql_value_to_db_value(val)
            }
            Expr::Identifier(ident) => {
                debug!("Evaluating identifier: {}", ident.value);
                // Handle NULL as a special identifier
                if ident.value.to_uppercase() == "NULL" {
                    Ok(Value::Null)
                } else if ident.value.starts_with("@@") {
                    // Handle system variables (@@variable_name)
                    self.get_system_variable(&ident.value)
                } else {
                    Err(YamlBaseError::NotImplemented(format!(
                        "Identifier '{}' not supported in SELECT without FROM",
                        ident.value
                    )))
                }
            }
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
            Expr::Function(func) => {
                // Handle functions in SELECT without FROM
                self.evaluate_constant_function(func)
            }
            Expr::Extract { field, expr, .. } => {
                // Handle EXTRACT expression
                let date_val = self.evaluate_constant_expr(expr)?;

                let date_str = match &date_val {
                    Value::Text(s) => s,
                    Value::Date(d) => {
                        // Convert date to string for processing
                        let formatted = d.format("%Y-%m-%d").to_string();
                        return self.evaluate_extract_from_date(field, &formatted);
                    }
                    _ => {
                        return Err(YamlBaseError::Database {
                            message: "EXTRACT requires date argument".to_string(),
                        });
                    }
                };

                self.evaluate_extract_from_date(field, date_str)
            }
            Expr::TypedString { data_type, value } => {
                // Handle DATE '2025-01-01' and similar typed strings
                match data_type {
                    DataType::Date => {
                        // Return as text for now, as we handle dates as strings
                        Ok(Value::Text(value.clone()))
                    }
                    _ => Ok(Value::Text(value.clone())),
                }
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                // CASE WHEN in SELECT without FROM
                self.evaluate_case_when_constant(
                    operand.as_deref(),
                    conditions,
                    results,
                    else_result.as_deref(),
                )
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                // PostgreSQL-style SUBSTRING expression
                let str_val = self.evaluate_constant_expr(expr)?;

                let start = if let Some(from_expr) = substring_from {
                    let start_val = self.evaluate_constant_expr(from_expr)?;
                    match start_val {
                        Value::Integer(n) => n,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(YamlBaseError::Database {
                                message: "SUBSTRING start position must be an integer".to_string(),
                            });
                        }
                    }
                } else {
                    1 // Default to 1 if no FROM specified
                };

                match str_val {
                    Value::Text(s) => {
                        // SQL uses 1-based indexing
                        let start_idx = if start > 0 {
                            (start as usize).saturating_sub(1)
                        } else {
                            0
                        };

                        if let Some(for_expr) = substring_for {
                            let len_val = self.evaluate_constant_expr(for_expr)?;
                            match len_val {
                                Value::Integer(len) => {
                                    let length = if len > 0 { len as usize } else { 0 };
                                    let chars: Vec<char> = s.chars().collect();
                                    let result: String =
                                        chars.iter().skip(start_idx).take(length).collect();
                                    Ok(Value::Text(result))
                                }
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "SUBSTRING length must be an integer".to_string(),
                                }),
                            }
                        } else {
                            // No length specified, take rest of string
                            let chars: Vec<char> = s.chars().collect();
                            let result: String = chars.iter().skip(start_idx).collect();
                            Ok(Value::Text(result))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(YamlBaseError::Database {
                        message: "SUBSTRING requires a string argument".to_string(),
                    }),
                }
            }
            Expr::Floor { expr, .. } => {
                let val = self.evaluate_constant_expr(expr)?;
                match val {
                    Value::Integer(i) => Ok(Value::Integer(i)),
                    Value::Double(d) => Ok(Value::Double(d.floor())),
                    Value::Float(f) => Ok(Value::Float(f.floor())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(YamlBaseError::Database {
                        message: "FLOOR requires numeric argument".to_string(),
                    }),
                }
            }
            Expr::Ceil { expr, .. } => {
                let val = self.evaluate_constant_expr(expr)?;
                match val {
                    Value::Integer(i) => Ok(Value::Integer(i)),
                    Value::Double(d) => Ok(Value::Double(d.ceil())),
                    Value::Float(f) => Ok(Value::Float(f.ceil())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(YamlBaseError::Database {
                        message: "CEIL requires numeric argument".to_string(),
                    }),
                }
            }
            Expr::Cast { expr, data_type, .. } => {
                // Handle CAST expression
                let value = self.evaluate_constant_expr(expr)?;
                self.cast_value(value, data_type)
            }
            _ => {
                debug!(
                    "Unsupported expression type in evaluate_constant_expr: {:?}",
                    expr
                );
                Err(YamlBaseError::NotImplemented(
                    "Only constant expressions are supported in SELECT without FROM".to_string(),
                ))
            }
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
            // Comparison operators
            BinaryOperator::Eq => Ok(Value::Boolean(left == right)),
            BinaryOperator::NotEq => Ok(Value::Boolean(left != right)),
            BinaryOperator::Lt => {
                if let Some(ord) = left.compare(right) {
                    Ok(Value::Boolean(ord.is_lt()))
                } else {
                    Ok(Value::Boolean(false))
                }
            }
            BinaryOperator::LtEq => {
                if let Some(ord) = left.compare(right) {
                    Ok(Value::Boolean(ord.is_le()))
                } else {
                    Ok(Value::Boolean(false))
                }
            }
            BinaryOperator::Gt => {
                if let Some(ord) = left.compare(right) {
                    Ok(Value::Boolean(ord.is_gt()))
                } else {
                    Ok(Value::Boolean(false))
                }
            }
            BinaryOperator::GtEq => {
                if let Some(ord) = left.compare(right) {
                    Ok(Value::Boolean(ord.is_ge()))
                } else {
                    Ok(Value::Boolean(false))
                }
            }
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

    fn has_joins(&self, from: &[TableWithJoins]) -> bool {
        from.len() > 1 || (!from.is_empty() && !from[0].joins.is_empty())
    }

    async fn execute_select_with_joins(
        &self,
        db: &Database,
        select: &Select,
        query: &Query,
    ) -> crate::Result<QueryResult> {
        debug!("Executing SELECT with JOINs");

        // Extract all tables involved in the query
        let mut all_tables = Vec::new();
        let mut table_aliases = std::collections::HashMap::new();

        for table_with_joins in &select.from {
            // Add the main table
            let (table_name, alias) = self.extract_table_info(&table_with_joins.relation)?;
            let table = db
                .get_table(&table_name)
                .ok_or_else(|| YamlBaseError::Database {
                    message: format!("Table '{}' not found", table_name),
                })?;

            if let Some(alias_name) = alias {
                table_aliases.insert(alias_name.clone(), table_name.clone());
            }
            all_tables.push((table_name.clone(), table));

            // Add joined tables
            for join in &table_with_joins.joins {
                let (join_table_name, join_alias) = self.extract_table_info(&join.relation)?;
                let join_table =
                    db.get_table(&join_table_name)
                        .ok_or_else(|| YamlBaseError::Database {
                            message: format!("Table '{}' not found", join_table_name),
                        })?;

                if let Some(alias_name) = join_alias {
                    table_aliases.insert(alias_name.clone(), join_table_name.clone());
                }
                all_tables.push((join_table_name.clone(), join_table));
            }
        }

        // Perform the join operation
        let joined_rows = self
            .perform_join(&select.from, &all_tables, &table_aliases)
            .await?;

        // Check if this is an aggregate query
        if self.is_aggregate_query(select) {
            return self
                .execute_aggregate_with_joined_rows(db, select, query, &joined_rows, &all_tables)
                .await;
        }

        // Extract columns with table qualifiers
        let columns = self.extract_columns_for_join(select, &all_tables, &table_aliases)?;

        // Filter rows based on WHERE clause
        let filtered_rows =
            self.filter_joined_rows(&joined_rows, &select.selection, &all_tables, &table_aliases)?;

        // Project columns
        let projected_rows = self.project_joined_columns(&filtered_rows, &columns, &all_tables)?;

        // Apply DISTINCT if specified
        let distinct_rows = if select.distinct.is_some() {
            self.apply_distinct(projected_rows)?
        } else {
            projected_rows
        };

        // Apply ORDER BY
        let sorted_rows = if let Some(order_by) = &query.order_by {
            self.sort_joined_rows(distinct_rows, &order_by.exprs, &columns)?
        } else {
            distinct_rows
        };

        // Apply LIMIT and OFFSET
        let final_rows = if let Some(limit_expr) = &query.limit {
            self.apply_limit(sorted_rows, limit_expr)?
        } else {
            sorted_rows
        };

        // Get column types
        let column_types = columns
            .iter()
            .map(|col| col.get_type(&all_tables))
            .collect();

        let column_names = columns.iter().map(|col| col.get_name()).collect();

        Ok(QueryResult {
            columns: column_names,
            column_types,
            rows: final_rows,
        })
    }

    fn extract_table_info(
        &self,
        table_factor: &TableFactor,
    ) -> crate::Result<(String, Option<String>)> {
        match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name
                    .0
                    .first()
                    .ok_or_else(|| YamlBaseError::Database {
                        message: "Invalid table name".to_string(),
                    })?
                    .value
                    .clone();

                let alias_name = alias.as_ref().map(|a| a.name.value.clone());

                Ok((table_name, alias_name))
            }
            _ => Err(YamlBaseError::NotImplemented(
                "Only simple table references are supported in joins".to_string(),
            )),
        }
    }

    fn extract_columns(
        &self,
        select: &Select,
        table: &Table,
    ) -> crate::Result<Vec<ProjectionItem>> {
        let mut columns = Vec::new();
        let mut column_counter = 1;

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    match expr {
                        Expr::Identifier(ident) => {
                            // This is a table column reference
                            let col_name = &ident.value;
                            let col_idx = table.get_column_index(col_name).ok_or_else(|| {
                                YamlBaseError::Database {
                                    message: format!("Column '{}' not found", col_name),
                                }
                            })?;
                            columns.push(ProjectionItem::TableColumn(col_name.clone(), col_idx));
                        }
                        _ => {
                            // Check if this is a function that needs row context
                            if let Expr::Function(_) = expr {
                                let col_name = format!("column_{}", column_counter);
                                column_counter += 1;
                                columns.push(ProjectionItem::Expression(
                                    col_name,
                                    Box::new(expr.clone()),
                                ));
                            } else {
                                // This is a constant expression (like SELECT 1, SELECT 'hello', etc.)
                                match self.evaluate_constant_expr(expr) {
                                    Ok(value) => {
                                        let col_name = format!("column_{}", column_counter);
                                        column_counter += 1;
                                        columns.push(ProjectionItem::Constant(col_name, value));
                                    }
                                    Err(_) => {
                                        // If constant evaluation fails, treat it as an expression
                                        let col_name = format!("column_{}", column_counter);
                                        column_counter += 1;
                                        columns.push(ProjectionItem::Expression(
                                            col_name,
                                            Box::new(expr.clone()),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    match expr {
                        Expr::Identifier(ident) => {
                            // Table column with alias
                            let col_idx =
                                table.get_column_index(&ident.value).ok_or_else(|| {
                                    YamlBaseError::Database {
                                        message: format!("Column '{}' not found", ident.value),
                                    }
                                })?;
                            columns.push(ProjectionItem::TableColumn(alias.value.clone(), col_idx));
                        }
                        _ => {
                            // Check if this is a function that needs row context
                            if let Expr::Function(_) = expr {
                                columns.push(ProjectionItem::Expression(
                                    alias.value.clone(),
                                    Box::new(expr.clone()),
                                ));
                            } else {
                                // Constant expression with alias
                                match self.evaluate_constant_expr(expr) {
                                    Ok(value) => {
                                        columns.push(ProjectionItem::Constant(
                                            alias.value.clone(),
                                            value,
                                        ));
                                    }
                                    Err(_) => {
                                        // If constant evaluation fails, treat it as an expression
                                        columns.push(ProjectionItem::Expression(
                                            alias.value.clone(),
                                            Box::new(expr.clone()),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    for (idx, col) in table.columns.iter().enumerate() {
                        columns.push(ProjectionItem::TableColumn(col.name.clone(), idx));
                    }
                }
                _ => {
                    return Err(YamlBaseError::NotImplemented(
                        "Complex projections are not yet supported".to_string(),
                    ));
                }
            }
        }

        Ok(columns)
    }

    async fn filter_rows<'a>(
        &self,
        table: &'a Table,
        table_name: &str,
        selection: &Option<Expr>,
    ) -> crate::Result<Vec<&'a Vec<Value>>> {
        // Check if this is a simple primary key lookup
        if let Some(pk_value) = self.extract_primary_key_lookup(selection, table) {
            debug!("Using primary key index for lookup: {:?}", pk_value);

            // Use the index for O(1) lookup
            if let Some(row) = self
                .storage
                .find_by_primary_key(table_name, &pk_value)
                .await
            {
                // We need to find the reference in the table's rows vector
                // This is a bit inefficient but maintains the existing API
                for table_row in &table.rows {
                    if table_row == &row {
                        return Ok(vec![table_row]);
                    }
                }
            }
            return Ok(vec![]);
        }

        // Fall back to full table scan
        let mut result = Vec::new();

        for row in table.rows.iter() {
            if let Some(where_expr) = selection {
                let matches = self.evaluate_expr(where_expr, row, table)?;
                if matches {
                    result.push(row);
                }
            } else {
                result.push(row);
            }
        }

        Ok(result)
    }

    /// Extract primary key value if WHERE clause is a simple equality check on primary key
    fn extract_primary_key_lookup(&self, selection: &Option<Expr>, table: &Table) -> Option<Value> {
        let where_expr = selection.as_ref()?;

        // Check if we have a primary key
        let pk_idx = table.primary_key_index?;
        let pk_column = &table.columns[pk_idx].name;

        // Look for simple equality: WHERE primary_key = value
        if let Expr::BinaryOp { left, op, right } = where_expr {
            if matches!(op, BinaryOperator::Eq) {
                // Check if left side is the primary key column
                if let Expr::Identifier(ident) = left.as_ref() {
                    if ident.value.to_lowercase() == pk_column.to_lowercase() {
                        // Extract the value from the right side
                        if let Expr::Value(sql_value) = right.as_ref() {
                            return self.sql_value_to_db_value(sql_value).ok();
                        }
                    }
                }
                // Also check if right side is the column (value = primary_key)
                if let Expr::Identifier(ident) = right.as_ref() {
                    if ident.value.to_lowercase() == pk_column.to_lowercase() {
                        if let Expr::Value(sql_value) = left.as_ref() {
                            return self.sql_value_to_db_value(sql_value).ok();
                        }
                    }
                }
            }
        }

        None
    }

    fn evaluate_expr(&self, expr: &Expr, row: &[Value], table: &Table) -> crate::Result<bool> {
        debug!("Evaluating expression: {:?}", expr);
        match expr {
            Expr::BinaryOp { left, op, right } => {
                self.evaluate_binary_op(left, op, right, row, table)
            }
            Expr::Value(sqlparser::ast::Value::Boolean(b)) => Ok(*b),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                debug!(
                    "Found InList expression: expr={:?}, negated={}",
                    expr, negated
                );
                self.evaluate_in_list(expr, list, *negated, row, table)
            }
            Expr::Like {
                expr,
                pattern,
                negated,
                ..
            } => {
                debug!(
                    "Found Like expression: expr={:?}, pattern={:?}, negated={}",
                    expr, pattern, negated
                );
                self.evaluate_like(expr, pattern, *negated, row, table)
            }
            Expr::IsNull(expr) => {
                debug!("Found IsNull expression: expr={:?}", expr);
                let value = self.get_expr_value(expr, row, table)?;
                Ok(matches!(value, Value::Null))
            }
            Expr::IsNotNull(expr) => {
                debug!("Found IsNotNull expression: expr={:?}", expr);
                let value = self.get_expr_value(expr, row, table)?;
                Ok(!matches!(value, Value::Null))
            }
            Expr::Nested(inner) => {
                // Handle parenthesized expressions by evaluating the inner expression
                self.evaluate_expr(inner, row, table)
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                debug!(
                    "Found Between expression: expr={:?}, negated={}, low={:?}, high={:?}",
                    expr, negated, low, high
                );
                self.evaluate_between(expr, *negated, low, high, row, table)
            }
            _ => Err(YamlBaseError::NotImplemented(format!(
                "Expression type not supported: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_between(
        &self,
        expr: &Expr,
        negated: bool,
        low: &Expr,
        high: &Expr,
        row: &[Value],
        table: &Table,
    ) -> crate::Result<bool> {
        let value = self.get_expr_value(expr, row, table)?;
        let mut low_value = self.get_expr_value(low, row, table)?;
        let mut high_value = self.get_expr_value(high, row, table)?;

        // Handle NULL cases - if any value is NULL, the result is NULL (which we treat as false)
        if matches!(value, Value::Null)
            || matches!(low_value, Value::Null)
            || matches!(high_value, Value::Null)
        {
            return Ok(false);
        }

        // Type conversion: if comparing dates with text, try to parse text as date
        if matches!(value, Value::Date(_)) {
            if let Value::Text(s) = &low_value {
                if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    low_value = Value::Date(date);
                }
            }
            if let Value::Text(s) = &high_value {
                if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    high_value = Value::Date(date);
                }
            }
        }

        // Check if value is between low and high (inclusive)
        let is_between = match (&value, &low_value, &high_value) {
            (Value::Integer(v), Value::Integer(l), Value::Integer(h)) => *l <= *v && *v <= *h,
            (Value::Double(v), Value::Double(l), Value::Double(h)) => *l <= *v && *v <= *h,
            (Value::Float(v), Value::Float(l), Value::Float(h)) => *l <= *v && *v <= *h,
            (Value::Text(v), Value::Text(l), Value::Text(h)) => l <= v && v <= h,
            (Value::Date(v), Value::Date(l), Value::Date(h)) => *l <= *v && *v <= *h,
            (Value::Time(v), Value::Time(l), Value::Time(h)) => *l <= *v && *v <= *h,
            (Value::Timestamp(v), Value::Timestamp(l), Value::Timestamp(h)) => *l <= *v && *v <= *h,

            // Handle mixed numeric types
            (Value::Integer(v), Value::Double(l), Value::Double(h)) => {
                *l <= *v as f64 && (*v as f64) <= *h
            }
            (Value::Double(v), Value::Integer(l), Value::Integer(h)) => {
                (*l as f64) <= *v && *v <= (*h as f64)
            }
            (Value::Integer(v), Value::Float(l), Value::Float(h)) => {
                *l <= *v as f32 && (*v as f32) <= *h
            }
            (Value::Float(v), Value::Integer(l), Value::Integer(h)) => {
                (*l as f32) <= *v && *v <= (*h as f32)
            }
            (Value::Double(v), Value::Float(l), Value::Float(h)) => {
                (*l as f64) <= *v && *v <= (*h as f64)
            }
            (Value::Float(v), Value::Double(l), Value::Double(h)) => {
                (*l as f32) <= *v && *v <= (*h as f32)
            }

            _ => {
                return Err(YamlBaseError::Database {
                    message: format!(
                        "BETWEEN requires compatible types, got {:?} BETWEEN {:?} AND {:?}",
                        value, low_value, high_value
                    ),
                });
            }
        };

        Ok(if negated { !is_between } else { is_between })
    }

    fn evaluate_in_list(
        &self,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
        row: &[Value],
        table: &Table,
    ) -> crate::Result<bool> {
        let value = self.get_expr_value(expr, row, table)?;

        for list_expr in list {
            let list_value = self.get_expr_value(list_expr, row, table)?;
            if value == list_value {
                return Ok(!negated);
            }
        }

        Ok(negated)
    }

    fn evaluate_like(
        &self,
        expr: &Expr,
        pattern: &Expr,
        negated: bool,
        row: &[Value],
        table: &Table,
    ) -> crate::Result<bool> {
        let value = self.get_expr_value(expr, row, table)?;
        let pattern_value = self.get_expr_value(pattern, row, table)?;

        // Convert values to strings for LIKE comparison
        let value_str = match &value {
            Value::Text(s) => s.clone(),
            _ => return Ok(negated), // Non-text values don't match LIKE patterns
        };

        let pattern_str = match &pattern_value {
            Value::Text(s) => s.clone(),
            _ => {
                return Err(YamlBaseError::Database {
                    message: "LIKE pattern must be a string".to_string(),
                });
            }
        };

        // Convert SQL LIKE pattern to regex
        // Handle SQL escape sequences and wildcards
        let mut regex_pattern = String::new();
        let chars: Vec<char> = pattern_str.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            if chars[i] == '\\' && i + 1 < chars.len() {
                // Handle SQL escape sequences
                match chars[i + 1] {
                    '%' => {
                        regex_pattern.push('%');
                        i += 2;
                    }
                    '_' => {
                        regex_pattern.push('_');
                        i += 2;
                    }
                    '\\' => {
                        regex_pattern.push_str("\\\\");
                        i += 2;
                    }
                    _ => {
                        // Invalid escape sequence, treat as literal backslash
                        regex_pattern.push_str("\\\\");
                        i += 1;
                    }
                }
            } else {
                match chars[i] {
                    '%' => regex_pattern.push_str(".*"),
                    '_' => regex_pattern.push('.'),
                    c => {
                        // Escape regex special characters
                        if "^$.*+?{}[]|()".contains(c) {
                            regex_pattern.push('\\');
                        }
                        regex_pattern.push(c);
                    }
                }
                i += 1;
            }
        }

        let matches = match Regex::new(&format!("^{}$", regex_pattern)) {
            Ok(re) => re.is_match(&value_str),
            Err(_) => {
                return Err(YamlBaseError::Database {
                    message: format!("Invalid LIKE pattern: {}", pattern_str),
                });
            }
        };

        Ok(if negated { !matches } else { matches })
    }

    fn evaluate_binary_op(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        row: &[Value],
        table: &Table,
    ) -> crate::Result<bool> {
        // Handle AND/OR operations specially to support nested expressions
        match op {
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
            _ => {
                // For other operators, evaluate the values first
                let left_val = self.get_expr_value(left, row, table)?;
                let right_val = self.get_expr_value(right, row, table)?;
                debug!(
                    "Comparing values: left={:?}, right={:?}, op={:?}",
                    left_val, right_val, op
                );

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
                    _ => Err(YamlBaseError::NotImplemented(format!(
                        "Binary operator not supported: {:?}",
                        op
                    ))),
                }
            }
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
            Expr::TypedString { data_type, value } => {
                // Handle DATE '2025-01-01' and similar typed strings
                match data_type {
                    DataType::Date => {
                        // Parse the date string into NaiveDate
                        match chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                            Ok(date) => Ok(Value::Date(date)),
                            Err(_) => Err(YamlBaseError::TypeConversion(format!(
                                "Invalid date format: {}",
                                value
                            ))),
                        }
                    }
                    _ => Ok(Value::Text(value.clone())),
                }
            }
            Expr::Function(func) => {
                // Evaluate functions with row context
                self.evaluate_function_with_row(func, row, table)
            }
            Expr::Extract { field, expr, .. } => {
                // Handle EXTRACT expression
                let date_val = self.get_expr_value(expr, row, table)?;

                let date_str = match &date_val {
                    Value::Text(s) => s,
                    Value::Date(d) => &d.format("%Y-%m-%d").to_string(),
                    _ => {
                        return Err(YamlBaseError::Database {
                            message: "EXTRACT requires date argument".to_string(),
                        });
                    }
                };

                if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    use sqlparser::ast::DateTimeField;
                    let result = match field {
                        DateTimeField::Day => date.day() as i64,
                        DateTimeField::Month => date.month() as i64,
                        DateTimeField::Year => date.year() as i64,
                        _ => {
                            return Err(YamlBaseError::Database {
                                message: format!("EXTRACT field '{:?}' not supported", field),
                            });
                        }
                    };
                    Ok(Value::Integer(result))
                } else {
                    Err(YamlBaseError::Database {
                        message: "Invalid date format for EXTRACT".to_string(),
                    })
                }
            }
            Expr::Trim { expr, .. } => {
                // Handle TRIM expression
                let inner_val = self.get_expr_value(expr, row, table)?;
                match &inner_val {
                    Value::Text(s) => Ok(Value::Text(s.trim().to_string())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(YamlBaseError::Database {
                        message: "TRIM requires string argument".to_string(),
                    }),
                }
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                // CASE WHEN expression
                self.evaluate_case_when(
                    operand.as_deref(),
                    conditions,
                    results,
                    else_result.as_deref(),
                    row,
                    table,
                )
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                // PostgreSQL-style SUBSTRING expression with row context
                let str_val = self.get_expr_value(expr, row, table)?;

                let start = if let Some(from_expr) = substring_from {
                    let start_val = self.get_expr_value(from_expr, row, table)?;
                    match start_val {
                        Value::Integer(n) => n,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(YamlBaseError::Database {
                                message: "SUBSTRING start position must be an integer".to_string(),
                            });
                        }
                    }
                } else {
                    1 // Default to 1 if no FROM specified
                };

                match str_val {
                    Value::Text(s) => {
                        // SQL uses 1-based indexing
                        let start_idx = if start > 0 {
                            (start as usize).saturating_sub(1)
                        } else {
                            0
                        };

                        if let Some(for_expr) = substring_for {
                            let len_val = self.get_expr_value(for_expr, row, table)?;
                            match len_val {
                                Value::Integer(len) => {
                                    let length = if len > 0 { len as usize } else { 0 };
                                    let chars: Vec<char> = s.chars().collect();
                                    let result: String =
                                        chars.iter().skip(start_idx).take(length).collect();
                                    Ok(Value::Text(result))
                                }
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "SUBSTRING length must be an integer".to_string(),
                                }),
                            }
                        } else {
                            // No length specified, take rest of string
                            let chars: Vec<char> = s.chars().collect();
                            let result: String = chars.iter().skip(start_idx).collect();
                            Ok(Value::Text(result))
                        }
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(YamlBaseError::Database {
                        message: "SUBSTRING requires a string argument".to_string(),
                    }),
                }
            }
            Expr::Cast { expr, data_type, .. } => {
                // Handle CAST expression
                let value = self.get_expr_value(expr, row, table)?;
                self.cast_value(value, data_type)
            }
            _ => Err(YamlBaseError::NotImplemented(format!(
                "Expression type not supported in get_expr_value: {:?}",
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
        columns: &[ProjectionItem],
        table: &Table,
    ) -> crate::Result<Vec<Vec<Value>>> {
        let mut result = Vec::new();

        for row in rows {
            let mut projected_row = Vec::new();
            for item in columns {
                match item {
                    ProjectionItem::TableColumn(_, idx) => {
                        projected_row.push(row[*idx].clone());
                    }
                    ProjectionItem::Constant(_, value) => {
                        projected_row.push(value.clone());
                    }
                    ProjectionItem::Expression(_, expr) => {
                        let value = self.get_expr_value(expr.as_ref(), row, table)?;
                        projected_row.push(value);
                    }
                }
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

    fn get_system_variable(&self, var_name: &str) -> crate::Result<Value> {
        // Remove @@ prefix and handle session/global prefixes
        let name = if let Some(stripped) = var_name.strip_prefix("@@") {
            stripped
        } else {
            var_name
        };

        // Handle session. and global. prefixes
        let name = if let Some(stripped) = name.strip_prefix("session.") {
            stripped
        } else if let Some(stripped) = name.strip_prefix("SESSION.") {
            stripped
        } else if let Some(stripped) = name.strip_prefix("global.") {
            stripped
        } else if let Some(stripped) = name.strip_prefix("GLOBAL.") {
            stripped
        } else {
            name
        };

        // Convert to lowercase for comparison
        let name_lower = name.to_lowercase();

        // Return appropriate values for known system variables
        match name_lower.as_str() {
            "version" => Ok(Value::Text("8.0.35-yamlbase".to_string())),
            "version_comment" => Ok(Value::Text("1".to_string())),
            _ => {
                // Default all other system variables to "1"
                Ok(Value::Text("1".to_string()))
            }
        }
    }

    fn evaluate_extract_from_date(
        &self,
        field: &DateTimeField,
        date_str: &str,
    ) -> crate::Result<Value> {
        if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
            let result = match field {
                DateTimeField::Day => date.day() as i64,
                DateTimeField::Month => date.month() as i64,
                DateTimeField::Year => date.year() as i64,
                _ => {
                    return Err(YamlBaseError::Database {
                        message: format!("EXTRACT field '{:?}' not supported", field),
                    });
                }
            };
            Ok(Value::Integer(result))
        } else {
            Err(YamlBaseError::Database {
                message: "Invalid date format for EXTRACT".to_string(),
            })
        }
    }

    fn evaluate_function_with_row(
        &self,
        func: &Function,
        row: &[Value],
        table: &Table,
    ) -> crate::Result<Value> {
        let func_name = func
            .name
            .0
            .first()
            .map(|ident| ident.value.to_uppercase())
            .unwrap_or_default();

        match func_name.as_str() {
            "UPPER" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val = self.get_expr_value(str_expr, row, table)?;

                            match &str_val {
                                Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "UPPER requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for UPPER".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "UPPER requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "UPPER requires arguments".to_string(),
                    })
                }
            }
            "LOWER" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val = self.get_expr_value(str_expr, row, table)?;

                            match &str_val {
                                Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "LOWER requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for LOWER".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "LOWER requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "LOWER requires arguments".to_string(),
                    })
                }
            }
            "TRIM" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val = self.get_expr_value(str_expr, row, table)?;

                            match &str_val {
                                Value::Text(s) => Ok(Value::Text(s.trim().to_string())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "TRIM requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for TRIM".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "TRIM requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "TRIM requires arguments".to_string(),
                    })
                }
            }
            "COALESCE" => {
                if let FunctionArguments::List(args) = &func.args {
                    // COALESCE returns the first non-NULL value
                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                            let val = self.get_expr_value(expr, row, table)?;
                            if !matches!(val, Value::Null) {
                                return Ok(val);
                            }
                        } else {
                            return Err(YamlBaseError::Database {
                                message: "Invalid argument for COALESCE".to_string(),
                            });
                        }
                    }
                    // If all values are NULL, return NULL
                    Ok(Value::Null)
                } else {
                    Err(YamlBaseError::Database {
                        message: "COALESCE requires arguments".to_string(),
                    })
                }
            }
            "NULLIF" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 2 {
                        if let (
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr1)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr2)),
                        ) = (&args.args[0], &args.args[1])
                        {
                            let val1 = self.get_expr_value(expr1, row, table)?;
                            let val2 = self.get_expr_value(expr2, row, table)?;

                            // NULLIF returns NULL if val1 == val2, otherwise returns val1
                            if val1 == val2 {
                                Ok(Value::Null)
                            } else {
                                Ok(val1)
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid arguments for NULLIF".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "NULLIF requires exactly 2 arguments".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "NULLIF requires arguments".to_string(),
                    })
                }
            }
            "LENGTH" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val = self.get_expr_value(str_expr, row, table)?;

                            match &str_val {
                                Value::Text(s) => Ok(Value::Integer(s.len() as i64)),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "LENGTH requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for LENGTH".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "LENGTH requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "LENGTH requires arguments".to_string(),
                    })
                }
            }
            "SUBSTRING" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 2 || args.args.len() == 3 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val = self.get_expr_value(str_expr, row, table)?;

                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(start_expr)) =
                                &args.args[1]
                            {
                                let start_val = self.get_expr_value(start_expr, row, table)?;

                                match (&str_val, &start_val) {
                                    (Value::Text(s), Value::Integer(start)) => {
                                        // SQL uses 1-based indexing
                                        let start_idx = if *start > 0 {
                                            (*start as usize).saturating_sub(1)
                                        } else {
                                            0
                                        };

                                        if args.args.len() == 3 {
                                            // SUBSTRING with length
                                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                                len_expr,
                                            )) = &args.args[2]
                                            {
                                                let len_val =
                                                    self.get_expr_value(len_expr, row, table)?;

                                                if let Value::Integer(len) = len_val {
                                                    let length = len.max(0) as usize;
                                                    let result: String = s
                                                        .chars()
                                                        .skip(start_idx)
                                                        .take(length)
                                                        .collect();
                                                    Ok(Value::Text(result))
                                                } else {
                                                    Err(YamlBaseError::Database {
                                                        message:
                                                            "SUBSTRING length must be an integer"
                                                                .to_string(),
                                                    })
                                                }
                                            } else {
                                                Err(YamlBaseError::Database {
                                                    message:
                                                        "Invalid length argument for SUBSTRING"
                                                            .to_string(),
                                                })
                                            }
                                        } else {
                                            // SUBSTRING without length
                                            let result: String =
                                                s.chars().skip(start_idx).collect();
                                            Ok(Value::Text(result))
                                        }
                                    }
                                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                                    _ => Err(YamlBaseError::Database {
                                        message: "SUBSTRING requires string and integer arguments"
                                            .to_string(),
                                    }),
                                }
                            } else {
                                Err(YamlBaseError::Database {
                                    message: "Invalid start argument for SUBSTRING".to_string(),
                                })
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid string argument for SUBSTRING".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "SUBSTRING requires 2 or 3 arguments".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "SUBSTRING requires arguments".to_string(),
                    })
                }
            }
            "CONCAT" => {
                if let FunctionArguments::List(args) = &func.args {
                    if !args.args.is_empty() {
                        let mut result = String::new();

                        for arg in &args.args {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                                let val = self.get_expr_value(expr, row, table)?;

                                match val {
                                    Value::Text(s) => result.push_str(&s),
                                    Value::Integer(i) => result.push_str(&i.to_string()),
                                    Value::Float(f) => result.push_str(&f.to_string()),
                                    Value::Double(d) => result.push_str(&d.to_string()),
                                    Value::Boolean(b) => result.push_str(&b.to_string()),
                                    Value::Null => return Ok(Value::Null), // CONCAT returns NULL if any argument is NULL
                                    _ => result.push_str(&val.to_string()),
                                }
                            } else {
                                return Err(YamlBaseError::Database {
                                    message: "Invalid argument for CONCAT".to_string(),
                                });
                            }
                        }

                        Ok(Value::Text(result))
                    } else {
                        Err(YamlBaseError::Database {
                            message: "CONCAT requires at least 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "CONCAT requires arguments".to_string(),
                    })
                }
            }
            "REPLACE" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 3 {
                        if let (
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(from_expr)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(to_expr)),
                        ) = (&args.args[0], &args.args[1], &args.args[2])
                        {
                            let str_val = self.get_expr_value(str_expr, row, table)?;
                            let from_val = self.get_expr_value(from_expr, row, table)?;
                            let to_val = self.get_expr_value(to_expr, row, table)?;

                            match (&str_val, &from_val, &to_val) {
                                (Value::Text(s), Value::Text(from), Value::Text(to)) => {
                                    // Handle empty search string - return original string
                                    if from.is_empty() {
                                        Ok(Value::Text(s.clone()))
                                    } else {
                                        Ok(Value::Text(s.replace(from, to)))
                                    }
                                }
                                (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => {
                                    Ok(Value::Null)
                                }
                                _ => Err(YamlBaseError::Database {
                                    message: "REPLACE requires string arguments".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid arguments for REPLACE".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "REPLACE requires exactly 3 arguments".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "REPLACE requires arguments".to_string(),
                    })
                }
            }
            "ROUND" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 || args.args.len() == 2 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(num_expr)) = &args.args[0]
                        {
                            let num_val = self.get_expr_value(num_expr, row, table)?;

                            let precision = if args.args.len() == 2 {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(prec_expr)) =
                                    &args.args[1]
                                {
                                    let prec_val = self.get_expr_value(prec_expr, row, table)?;
                                    match prec_val {
                                        Value::Integer(p) => p as i32,
                                        Value::Null => return Ok(Value::Null),
                                        _ => {
                                            return Err(YamlBaseError::Database {
                                                message: "ROUND precision must be an integer"
                                                    .to_string(),
                                            });
                                        }
                                    }
                                } else {
                                    return Err(YamlBaseError::Database {
                                        message: "Invalid ROUND precision argument".to_string(),
                                    });
                                }
                            } else {
                                0
                            };

                            match num_val {
                                Value::Integer(n) => {
                                    if precision == 0 {
                                        Ok(Value::Integer(n))
                                    } else {
                                        let f = n as f64;
                                        let factor = 10f64.powi(precision);
                                        Ok(Value::Double((f * factor).round() / factor))
                                    }
                                }
                                Value::Float(f) => {
                                    let factor = 10f64.powi(precision);
                                    Ok(Value::Double(((f as f64) * factor).round() / factor))
                                }
                                Value::Double(d) => {
                                    let factor = 10f64.powi(precision);
                                    Ok(Value::Double((d * factor).round() / factor))
                                }
                                Value::Decimal(d) => {
                                    let factor = 10f64.powi(precision);
                                    let f = d.to_f64().ok_or_else(|| YamlBaseError::Database {
                                        message: "Cannot convert decimal to float".to_string(),
                                    })?;
                                    Ok(Value::Double((f * factor).round() / factor))
                                }
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "ROUND requires numeric argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for ROUND".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "ROUND requires 1 or 2 arguments".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "ROUND requires arguments".to_string(),
                    })
                }
            }
            "FLOOR" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(num_expr)) = &args.args[0]
                        {
                            let num_val = self.get_expr_value(num_expr, row, table)?;

                            match num_val {
                                Value::Integer(n) => Ok(Value::Integer(n)),
                                Value::Float(f) => Ok(Value::Double((f as f64).floor())),
                                Value::Double(d) => Ok(Value::Double(d.floor())),
                                Value::Decimal(d) => {
                                    let f = d.to_f64().ok_or_else(|| YamlBaseError::Database {
                                        message: "Cannot convert decimal to float".to_string(),
                                    })?;
                                    Ok(Value::Double(f.floor()))
                                }
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "FLOOR requires numeric argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for FLOOR".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "FLOOR requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "FLOOR requires arguments".to_string(),
                    })
                }
            }
            "CEIL" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(num_expr)) = &args.args[0]
                        {
                            let num_val = self.get_expr_value(num_expr, row, table)?;

                            match num_val {
                                Value::Integer(n) => Ok(Value::Integer(n)),
                                Value::Float(f) => Ok(Value::Double((f as f64).ceil())),
                                Value::Double(d) => Ok(Value::Double(d.ceil())),
                                Value::Decimal(d) => {
                                    let f = d.to_f64().ok_or_else(|| YamlBaseError::Database {
                                        message: "Cannot convert decimal to float".to_string(),
                                    })?;
                                    Ok(Value::Double(f.ceil()))
                                }
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "CEIL requires numeric argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for CEIL".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "CEIL requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "CEIL requires arguments".to_string(),
                    })
                }
            }
            "ABS" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(num_expr)) = &args.args[0]
                        {
                            let num_val = self.get_expr_value(num_expr, row, table)?;

                            match num_val {
                                Value::Integer(n) => Ok(Value::Integer(n.abs())),
                                Value::Float(f) => Ok(Value::Float(f.abs())),
                                Value::Double(d) => Ok(Value::Double(d.abs())),
                                Value::Decimal(d) => Ok(Value::Decimal(d.abs())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "ABS requires numeric argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for ABS".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "ABS requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "ABS requires arguments".to_string(),
                    })
                }
            }
            "MOD" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 2 {
                        if let (
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(num_expr)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(div_expr)),
                        ) = (&args.args[0], &args.args[1])
                        {
                            let num_val = self.get_expr_value(num_expr, row, table)?;
                            let div_val = self.get_expr_value(div_expr, row, table)?;

                            match (&num_val, &div_val) {
                                (Value::Integer(n), Value::Integer(d)) => {
                                    if *d == 0 {
                                        Err(YamlBaseError::Database {
                                            message: "Division by zero in MOD".to_string(),
                                        })
                                    } else {
                                        Ok(Value::Integer(n % d))
                                    }
                                }
                                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "MOD requires integer arguments".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid arguments for MOD".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "MOD requires exactly 2 arguments".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "MOD requires arguments".to_string(),
                    })
                }
            }
            // For functions that don't need row context, delegate to constant version
            _ => self.evaluate_constant_function(func),
        }
    }

    fn evaluate_constant_function(&self, func: &Function) -> crate::Result<Value> {
        let func_name = func
            .name
            .0
            .first()
            .map(|ident| ident.value.to_uppercase())
            .unwrap_or_default();

        match func_name.as_str() {
            "VERSION" => {
                // MySQL-compatible version string
                Ok(Value::Text("8.0.35-yamlbase".to_string()))
            }
            "CURRENT_DATE" => {
                // Return current date as Date value
                let today = chrono::Local::now().date_naive();
                Ok(Value::Date(today))
            }
            "CURRENT_TIMESTAMP" => {
                // Return current datetime as YYYY-MM-DD HH:MM:SS string
                let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                Ok(Value::Text(now))
            }
            "NOW" => {
                // Return current datetime as YYYY-MM-DD HH:MM:SS string
                let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                Ok(Value::Text(now))
            }
            "ADD_MONTHS" => {
                // ADD_MONTHS(date, n) - adds n months to date
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 2 {
                        if let (
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(date_expr)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(months_expr)),
                        ) = (&args.args[0], &args.args[1])
                        {
                            let date_val = self.evaluate_constant_expr(date_expr)?;
                            let months_val = self.evaluate_constant_expr(months_expr)?;

                            // Parse date
                            let date = match &date_val {
                                Value::Date(d) => *d,
                                Value::Text(s) => {
                                    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                                        .map_err(|_| YamlBaseError::Database {
                                            message: format!("Invalid date format: {}", s),
                                        })?
                                }
                                _ => {
                                    return Err(YamlBaseError::Database {
                                        message: "ADD_MONTHS requires date as first argument"
                                            .to_string(),
                                    });
                                }
                            };

                            // Get months to add
                            let months = match &months_val {
                                Value::Integer(n) => *n,
                                _ => {
                                    return Err(YamlBaseError::Database {
                                        message: "ADD_MONTHS requires integer as second argument"
                                            .to_string(),
                                    });
                                }
                            };

                            // Add or subtract months
                            let result = if months >= 0 {
                                date + chrono::Months::new(months as u32)
                            } else {
                                date - chrono::Months::new((-months) as u32)
                            };
                            Ok(Value::Text(result.format("%Y-%m-%d").to_string()))
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid arguments for ADD_MONTHS".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "ADD_MONTHS requires exactly 2 arguments".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "ADD_MONTHS requires arguments".to_string(),
                    })
                }
            }
            "LAST_DAY" => {
                // LAST_DAY(date) - returns last day of month
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(date_expr)) =
                            &args.args[0]
                        {
                            let date_val = self.evaluate_constant_expr(date_expr)?;

                            let date = match &date_val {
                                Value::Date(d) => *d,
                                Value::Text(s) => {
                                    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                                        .map_err(|_| YamlBaseError::Database {
                                            message: format!("Invalid date format: {}", s),
                                        })?
                                }
                                _ => {
                                    return Err(YamlBaseError::Database {
                                        message: "LAST_DAY requires date argument".to_string(),
                                    });
                                }
                            };
                            
                            // Get first day of next month
                            let next_month = if date.month() == 12 {
                                chrono::NaiveDate::from_ymd_opt(date.year() + 1, 1, 1).unwrap()
                            } else {
                                chrono::NaiveDate::from_ymd_opt(
                                    date.year(),
                                    date.month() + 1,
                                    1,
                                )
                                .unwrap()
                            };
                            // Subtract one day to get last day of current month
                            let last_day = next_month - chrono::Duration::days(1);
                            Ok(Value::Text(last_day.format("%Y-%m-%d").to_string()))
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for LAST_DAY".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "LAST_DAY requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "LAST_DAY requires arguments".to_string(),
                    })
                }
            }
            "UPPER" => {
                // For SELECT without FROM, handle string literals
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val = self.evaluate_constant_expr(str_expr)?;
                            match &str_val {
                                Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "UPPER requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "UPPER function requires single argument".to_string(),
                            ))
                        }
                    } else {
                        Err(YamlBaseError::NotImplemented(
                            "UPPER function requires exactly one argument".to_string(),
                        ))
                    }
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "UPPER function requires arguments".to_string(),
                    ))
                }
            }
            "LOWER" => {
                // For SELECT without FROM, handle string literals
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val = self.evaluate_constant_expr(str_expr)?;
                            match &str_val {
                                Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "LOWER requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "LOWER function requires single argument".to_string(),
                            ))
                        }
                    } else {
                        Err(YamlBaseError::NotImplemented(
                            "LOWER function requires exactly one argument".to_string(),
                        ))
                    }
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "LOWER function requires arguments".to_string(),
                    ))
                }
            }
            "TRIM" => {
                // For SELECT without FROM, handle string literals
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val = self.evaluate_constant_expr(str_expr)?;
                            match &str_val {
                                Value::Text(s) => Ok(Value::Text(s.trim().to_string())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "TRIM requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "TRIM function requires single argument".to_string(),
                            ))
                        }
                    } else {
                        Err(YamlBaseError::NotImplemented(
                            "TRIM function requires exactly one argument".to_string(),
                        ))
                    }
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "TRIM function requires arguments".to_string(),
                    ))
                }
            }
            "COALESCE" => {
                if let FunctionArguments::List(args) = &func.args {
                    // COALESCE returns the first non-NULL value
                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                            let val = self.evaluate_constant_expr(expr)?;
                            if !matches!(val, Value::Null) {
                                return Ok(val);
                            }
                        } else {
                            return Err(YamlBaseError::Database {
                                message: "Invalid argument for COALESCE".to_string(),
                            });
                        }
                    }
                    // If all values are NULL, return NULL
                    Ok(Value::Null)
                } else {
                    Err(YamlBaseError::Database {
                        message: "COALESCE requires arguments".to_string(),
                    })
                }
            }
            "NULLIF" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 2 {
                        if let (
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr1)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr2)),
                        ) = (&args.args[0], &args.args[1])
                        {
                            let val1 = self.evaluate_constant_expr(expr1)?;
                            let val2 = self.evaluate_constant_expr(expr2)?;

                            // NULLIF returns NULL if val1 == val2, otherwise returns val1
                            if val1 == val2 {
                                Ok(Value::Null)
                            } else {
                                Ok(val1)
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid arguments for NULLIF".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "NULLIF requires exactly 2 arguments".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "NULLIF requires arguments".to_string(),
                    })
                }
            }
            "LENGTH" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val = self.evaluate_constant_expr(str_expr)?;

                            match &str_val {
                                Value::Text(s) => Ok(Value::Integer(s.chars().count() as i64)),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "LENGTH requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for LENGTH".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "LENGTH requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "LENGTH requires arguments".to_string(),
                    })
                }
            }
            "SUBSTRING" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 2 || args.args.len() == 3 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val = self.evaluate_constant_expr(str_expr)?;

                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(start_expr)) =
                                &args.args[1]
                            {
                                let start_val = self.evaluate_constant_expr(start_expr)?;

                                match (&str_val, &start_val) {
                                    (Value::Text(s), Value::Integer(start)) => {
                                        // SQL uses 1-based indexing
                                        let start_idx = if *start > 0 {
                                            (*start as usize).saturating_sub(1)
                                        } else {
                                            0
                                        };

                                        if args.args.len() == 3 {
                                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                                len_expr,
                                            )) = &args.args[2]
                                            {
                                                let len_val =
                                                    self.evaluate_constant_expr(len_expr)?;
                                                match &len_val {
                                                    Value::Integer(len) => {
                                                        let length = if *len > 0 {
                                                            *len as usize
                                                        } else {
                                                            0
                                                        };
                                                        let chars: Vec<char> = s.chars().collect();
                                                        let result: String = chars
                                                            .iter()
                                                            .skip(start_idx)
                                                            .take(length)
                                                            .collect();
                                                        Ok(Value::Text(result))
                                                    }
                                                    Value::Null => Ok(Value::Null),
                                                    _ => Err(YamlBaseError::Database {
                                                        message: "SUBSTRING length must be integer"
                                                            .to_string(),
                                                    }),
                                                }
                                            } else {
                                                Err(YamlBaseError::NotImplemented(
                                                    "Invalid SUBSTRING length argument".to_string(),
                                                ))
                                            }
                                        } else {
                                            // No length specified, take rest of string
                                            let chars: Vec<char> = s.chars().collect();
                                            let result: String =
                                                chars.iter().skip(start_idx).collect();
                                            Ok(Value::Text(result))
                                        }
                                    }
                                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                                    _ => Err(YamlBaseError::Database {
                                        message: "SUBSTRING requires string and integer arguments"
                                            .to_string(),
                                    }),
                                }
                            } else {
                                Err(YamlBaseError::NotImplemented(
                                    "Invalid SUBSTRING start argument".to_string(),
                                ))
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "Invalid SUBSTRING string argument".to_string(),
                            ))
                        }
                    } else {
                        Err(YamlBaseError::NotImplemented(
                            "SUBSTRING requires 2 or 3 arguments".to_string(),
                        ))
                    }
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "SUBSTRING function requires arguments".to_string(),
                    ))
                }
            }
            "CONCAT" => {
                if let FunctionArguments::List(args) = &func.args {
                    let mut result = String::new();
                    let mut has_null = false;

                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                            let val = self.evaluate_constant_expr(expr)?;
                            match &val {
                                Value::Text(s) => result.push_str(s),
                                Value::Integer(i) => result.push_str(&i.to_string()),
                                Value::Double(d) => result.push_str(&d.to_string()),
                                Value::Boolean(b) => result.push_str(&b.to_string()),
                                Value::Null => {
                                    has_null = true;
                                    break;
                                }
                                _ => result.push_str(&format!("{:?}", val)),
                            }
                        } else {
                            return Err(YamlBaseError::NotImplemented(
                                "Invalid CONCAT argument".to_string(),
                            ));
                        }
                    }

                    if has_null {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Text(result))
                    }
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "CONCAT function requires arguments".to_string(),
                    ))
                }
            }
            "REPLACE" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 3 {
                        if let (
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(from_expr)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(to_expr)),
                        ) = (&args.args[0], &args.args[1], &args.args[2])
                        {
                            let str_val = self.evaluate_constant_expr(str_expr)?;
                            let from_val = self.evaluate_constant_expr(from_expr)?;
                            let to_val = self.evaluate_constant_expr(to_expr)?;

                            match (&str_val, &from_val, &to_val) {
                                (Value::Text(s), Value::Text(from), Value::Text(to)) => {
                                    // Handle empty search string - return original string
                                    if from.is_empty() {
                                        Ok(Value::Text(s.clone()))
                                    } else {
                                        Ok(Value::Text(s.replace(from, to)))
                                    }
                                }
                                (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => {
                                    Ok(Value::Null)
                                }
                                _ => Err(YamlBaseError::Database {
                                    message: "REPLACE requires string arguments".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "Invalid REPLACE arguments".to_string(),
                            ))
                        }
                    } else {
                        Err(YamlBaseError::NotImplemented(
                            "REPLACE requires exactly 3 arguments".to_string(),
                        ))
                    }
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "REPLACE function requires arguments".to_string(),
                    ))
                }
            }
            "ROUND" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 || args.args.len() == 2 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(num_expr)) = &args.args[0]
                        {
                            let num_val = self.evaluate_constant_expr(num_expr)?;

                            let precision = if args.args.len() == 2 {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(prec_expr)) =
                                    &args.args[1]
                                {
                                    let prec_val = self.evaluate_constant_expr(prec_expr)?;
                                    match prec_val {
                                        Value::Integer(p) => p as i32,
                                        Value::Null => return Ok(Value::Null),
                                        _ => {
                                            return Err(YamlBaseError::Database {
                                                message: "ROUND precision must be an integer"
                                                    .to_string(),
                                            });
                                        }
                                    }
                                } else {
                                    return Err(YamlBaseError::NotImplemented(
                                        "Invalid ROUND precision argument".to_string(),
                                    ));
                                }
                            } else {
                                0
                            };

                            match num_val {
                                Value::Integer(i) => Ok(Value::Integer(i)),
                                Value::Double(d) => {
                                    let multiplier = 10f64.powi(precision);
                                    let rounded = (d * multiplier).round() / multiplier;
                                    Ok(Value::Double(rounded))
                                }
                                Value::Float(f) => {
                                    let multiplier = 10f32.powi(precision);
                                    let rounded = (f * multiplier).round() / multiplier;
                                    Ok(Value::Float(rounded))
                                }
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "ROUND requires numeric argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "Invalid ROUND argument".to_string(),
                            ))
                        }
                    } else {
                        Err(YamlBaseError::NotImplemented(
                            "ROUND requires 1 or 2 arguments".to_string(),
                        ))
                    }
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "ROUND function requires arguments".to_string(),
                    ))
                }
            }
            "FLOOR" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(num_expr)) = &args.args[0]
                        {
                            let num_val = self.evaluate_constant_expr(num_expr)?;

                            match num_val {
                                Value::Integer(i) => Ok(Value::Integer(i)),
                                Value::Double(d) => Ok(Value::Double(d.floor())),
                                Value::Float(f) => Ok(Value::Float(f.floor())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "FLOOR requires numeric argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "Invalid FLOOR argument".to_string(),
                            ))
                        }
                    } else {
                        Err(YamlBaseError::NotImplemented(
                            "FLOOR requires exactly 1 argument".to_string(),
                        ))
                    }
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "FLOOR function requires arguments".to_string(),
                    ))
                }
            }
            "CEIL" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(num_expr)) = &args.args[0]
                        {
                            let num_val = self.evaluate_constant_expr(num_expr)?;

                            match num_val {
                                Value::Integer(i) => Ok(Value::Integer(i)),
                                Value::Double(d) => Ok(Value::Double(d.ceil())),
                                Value::Float(f) => Ok(Value::Float(f.ceil())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "CEIL requires numeric argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "Invalid CEIL argument".to_string(),
                            ))
                        }
                    } else {
                        Err(YamlBaseError::NotImplemented(
                            "CEIL requires exactly 1 argument".to_string(),
                        ))
                    }
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "CEIL function requires arguments".to_string(),
                    ))
                }
            }
            "ABS" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(num_expr)) = &args.args[0]
                        {
                            let num_val = self.evaluate_constant_expr(num_expr)?;

                            match num_val {
                                Value::Integer(i) => Ok(Value::Integer(i.wrapping_abs())),
                                Value::Double(d) => Ok(Value::Double(d.abs())),
                                Value::Float(f) => Ok(Value::Float(f.abs())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "ABS requires numeric argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "Invalid ABS argument".to_string(),
                            ))
                        }
                    } else {
                        Err(YamlBaseError::NotImplemented(
                            "ABS requires exactly 1 argument".to_string(),
                        ))
                    }
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "ABS function requires arguments".to_string(),
                    ))
                }
            }
            "MOD" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 2 {
                        if let (
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(num_expr)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(div_expr)),
                        ) = (&args.args[0], &args.args[1])
                        {
                            let num_val = self.evaluate_constant_expr(num_expr)?;
                            let div_val = self.evaluate_constant_expr(div_expr)?;

                            match (&num_val, &div_val) {
                                (Value::Integer(n), Value::Integer(d)) => {
                                    if *d == 0 {
                                        return Err(YamlBaseError::Database {
                                            message: "Division by zero in MOD".to_string(),
                                        });
                                    }
                                    Ok(Value::Integer(n % d))
                                }
                                (Value::Double(n), Value::Double(d)) => {
                                    if *d == 0.0 {
                                        return Err(YamlBaseError::Database {
                                            message: "Division by zero in MOD".to_string(),
                                        });
                                    }
                                    Ok(Value::Double(n % d))
                                }
                                (Value::Float(n), Value::Float(d)) => {
                                    if *d == 0.0 {
                                        return Err(YamlBaseError::Database {
                                            message: "Division by zero in MOD".to_string(),
                                        });
                                    }
                                    Ok(Value::Float(n % d))
                                }
                                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "MOD requires numeric arguments".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "Invalid MOD arguments".to_string(),
                            ))
                        }
                    } else {
                        Err(YamlBaseError::NotImplemented(
                            "MOD requires exactly 2 arguments".to_string(),
                        ))
                    }
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "MOD function requires arguments".to_string(),
                    ))
                }
            }
            "DATE_FORMAT" => {
                // DATE_FORMAT(date, format) - formats date using MySQL format specifiers
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 2 {
                        if let (
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(date_expr)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(format_expr)),
                        ) = (&args.args[0], &args.args[1])
                        {
                            let date_val = self.evaluate_constant_expr(date_expr)?;
                            let format_val = self.evaluate_constant_expr(format_expr)?;
                            
                            // Get the date
                            let date = match &date_val {
                                Value::Date(d) => *d,
                                Value::Text(s) => {
                                    // Try to parse as date or datetime
                                    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                        date
                                    } else if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                                        datetime.date()
                                    } else {
                                        return Err(YamlBaseError::Database {
                                            message: format!("Invalid date format: {}", s),
                                        });
                                    }
                                }
                                _ => {
                                    return Err(YamlBaseError::Database {
                                        message: "DATE_FORMAT requires date as first argument".to_string(),
                                    });
                                }
                            };
                            
                            // Get the format string
                            let format_str = match &format_val {
                                Value::Text(s) => s,
                                _ => {
                                    return Err(YamlBaseError::Database {
                                        message: "DATE_FORMAT requires string format as second argument".to_string(),
                                    });
                                }
                            };
                            
                            // Convert MySQL format to chrono format
                            let chrono_format = self.mysql_to_chrono_format(format_str);
                            let formatted = date.format(&chrono_format).to_string();
                            Ok(Value::Text(formatted))
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid arguments for DATE_FORMAT".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "DATE_FORMAT requires exactly 2 arguments".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "DATE_FORMAT requires arguments".to_string(),
                    })
                }
            }
            _ => Err(YamlBaseError::NotImplemented(format!(
                "Function '{}' is not implemented",
                func_name
            ))),
        }
    }

    fn mysql_to_chrono_format(&self, mysql_format: &str) -> String {
        // Convert MySQL date format specifiers to chrono format
        // This is a simplified version - MySQL has many more format specifiers
        mysql_format
            .replace("%Y", "%Y")      // 4-digit year (same)
            .replace("%y", "%y")      // 2-digit year (same)
            .replace("%m", "%m")      // Month as number (01-12) (same)
            .replace("%c", "%-m")     // Month as number (1-12)
            .replace("%M", "%B")      // Month name (January, February, etc.)
            .replace("%b", "%b")      // Abbreviated month name (Jan, Feb, etc.)
            .replace("%d", "%d")      // Day of month (01-31) (same)
            .replace("%e", "%-d")     // Day of month (1-31)
            .replace("%D", "%dth")    // Day with suffix (1st, 2nd, 3rd, etc.) - approximation
            .replace("%W", "%A")      // Weekday name (Monday, Tuesday, etc.)
            .replace("%w", "%w")      // Day of week (0=Sunday, 6=Saturday) (same)
            .replace("%H", "%H")      // Hour (00-23) (same)
            .replace("%h", "%I")      // Hour (01-12)
            .replace("%I", "%I")      // Hour (01-12) (same)
            .replace("%k", "%-H")     // Hour (0-23)
            .replace("%l", "%-I")     // Hour (1-12)
            .replace("%i", "%M")      // Minutes (00-59)
            .replace("%s", "%S")      // Seconds (00-59)
            .replace("%p", "%p")      // AM/PM (same)
            .replace("%r", "%I:%M:%S %p") // Time 12-hour format
            .replace("%T", "%H:%M:%S")    // Time 24-hour format
            .replace("%%", "%")           // Literal %
    }

    fn cast_value(&self, value: Value, data_type: &DataType) -> crate::Result<Value> {
        use sqlparser::ast::DataType;
        
        match data_type {
            DataType::Int(_) | DataType::Integer(_) | DataType::BigInt(_) => {
                match value {
                    Value::Integer(i) => Ok(Value::Integer(i)),
                    Value::Double(d) => Ok(Value::Integer(d as i64)),
                    Value::Float(f) => Ok(Value::Integer(f as i64)),
                    Value::Text(s) => {
                        s.trim().parse::<i64>()
                            .map(Value::Integer)
                            .map_err(|_| YamlBaseError::Database {
                                message: format!("Cannot cast '{}' to INTEGER", s),
                            })
                    }
                    Value::Boolean(b) => Ok(Value::Integer(if b { 1 } else { 0 })),
                    Value::Null => Ok(Value::Null),
                    _ => Err(YamlBaseError::Database {
                        message: format!("Cannot cast {:?} to INTEGER", value),
                    }),
                }
            }
            DataType::Float(_) | DataType::Real => {
                match value {
                    Value::Integer(i) => Ok(Value::Float(i as f32)),
                    Value::Double(d) => Ok(Value::Float(d as f32)),
                    Value::Float(f) => Ok(Value::Float(f)),
                    Value::Text(s) => {
                        s.trim().parse::<f32>()
                            .map(Value::Float)
                            .map_err(|_| YamlBaseError::Database {
                                message: format!("Cannot cast '{}' to FLOAT", s),
                            })
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(YamlBaseError::Database {
                        message: format!("Cannot cast {:?} to FLOAT", value),
                    }),
                }
            }
            DataType::Double | DataType::DoublePrecision => {
                match value {
                    Value::Integer(i) => Ok(Value::Double(i as f64)),
                    Value::Double(d) => Ok(Value::Double(d)),
                    Value::Float(f) => Ok(Value::Double(f as f64)),
                    Value::Text(s) => {
                        s.trim().parse::<f64>()
                            .map(Value::Double)
                            .map_err(|_| YamlBaseError::Database {
                                message: format!("Cannot cast '{}' to DOUBLE", s),
                            })
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(YamlBaseError::Database {
                        message: format!("Cannot cast {:?} to DOUBLE", value),
                    }),
                }
            }
            DataType::Varchar(_) | DataType::Char(_) | DataType::Text => {
                match value {
                    Value::Integer(i) => Ok(Value::Text(i.to_string())),
                    Value::Double(d) => Ok(Value::Text(d.to_string())),
                    Value::Float(f) => Ok(Value::Text(f.to_string())),
                    Value::Boolean(b) => Ok(Value::Text(b.to_string())),
                    Value::Text(s) => Ok(Value::Text(s)),
                    Value::Date(d) => Ok(Value::Text(d.format("%Y-%m-%d").to_string())),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Text(format!("{:?}", value))),
                }
            }
            DataType::Date => {
                match value {
                    Value::Text(s) => {
                        // Try to parse various date formats
                        if let Ok(date) = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                            Ok(Value::Date(date))
                        } else if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
                            Ok(Value::Date(datetime.date()))
                        } else {
                            Err(YamlBaseError::Database {
                                message: format!("Cannot cast '{}' to DATE", s),
                            })
                        }
                    }
                    Value::Date(d) => Ok(Value::Date(d)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(YamlBaseError::Database {
                        message: format!("Cannot cast {:?} to DATE", value),
                    }),
                }
            }
            DataType::Boolean => {
                match value {
                    Value::Boolean(b) => Ok(Value::Boolean(b)),
                    Value::Integer(i) => Ok(Value::Boolean(i != 0)),
                    Value::Double(d) => Ok(Value::Boolean(d != 0.0)),
                    Value::Float(f) => Ok(Value::Boolean(f != 0.0)),
                    Value::Text(s) => {
                        let s_lower = s.to_lowercase();
                        Ok(Value::Boolean(
                            s_lower == "true" || s_lower == "1" || s_lower == "yes" || s_lower == "on"
                        ))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(YamlBaseError::Database {
                        message: format!("Cannot cast {:?} to BOOLEAN", value),
                    }),
                }
            }
            _ => Err(YamlBaseError::NotImplemented(format!(
                "CAST to {:?} is not supported",
                data_type
            ))),
        }
    }

    fn apply_distinct(&self, rows: Vec<Vec<Value>>) -> crate::Result<Vec<Vec<Value>>> {
        if rows.is_empty() {
            return Ok(rows);
        }

        let mut seen = std::collections::HashSet::new();
        let mut distinct_rows = Vec::new();

        for row in rows {
            // Create a hashable key from the row
            // Note: This assumes Value implements Hash and Eq properly
            if seen.insert(row.clone()) {
                distinct_rows.push(row);
            }
        }

        Ok(distinct_rows)
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

    fn is_aggregate_query(&self, select: &Select) -> bool {
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if self.contains_aggregate_function(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    fn contains_aggregate_function(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                let func_name = func
                    .name
                    .0
                    .first()
                    .map(|ident| ident.value.to_uppercase())
                    .unwrap_or_default();
                matches!(func_name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            _ => false,
        }
    }

    async fn execute_aggregate_select(
        &self,
        _db: &Database,
        select: &Select,
        _query: &Query,
        table: &Table,
        table_name: &str,
    ) -> crate::Result<QueryResult> {
        debug!("Executing aggregate SELECT query");

        // Filter rows based on WHERE clause
        let filtered_rows = self
            .filter_rows(table, table_name, &select.selection)
            .await?;

        // Check if we have GROUP BY
        match &select.group_by {
            GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
                return self
                    .execute_group_by_aggregate(select, &select.group_by, &filtered_rows, table)
                    .await;
            }
            GroupByExpr::All(_) => {
                return Err(YamlBaseError::NotImplemented(
                    "GROUP BY ALL is not supported yet".to_string(),
                ));
            }
            _ => {}
        }

        // Simple aggregate without GROUP BY (existing logic)
        let mut columns = Vec::new();
        let mut row_values = Vec::new();

        for (idx, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (col_name, value) =
                        self.evaluate_aggregate_expr(expr, &filtered_rows, table, idx)?;
                    columns.push(col_name);
                    row_values.push(value);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (_, value) =
                        self.evaluate_aggregate_expr(expr, &filtered_rows, table, idx)?;
                    columns.push(alias.value.clone());
                    row_values.push(value);
                }
                _ => {
                    return Err(YamlBaseError::NotImplemented(
                        "Complex projections in aggregate queries are not supported".to_string(),
                    ));
                }
            }
        }

        // Determine column types for aggregate results
        let column_types = select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    self.get_aggregate_result_type(expr)
                }
                _ => crate::yaml::schema::SqlType::Text,
            })
            .collect();

        Ok(QueryResult {
            columns,
            column_types,
            rows: vec![row_values],
        })
    }

    async fn execute_group_by_aggregate(
        &self,
        select: &Select,
        group_by: &GroupByExpr,
        filtered_rows: &[&Vec<Value>],
        table: &Table,
    ) -> crate::Result<QueryResult> {
        debug!("Executing GROUP BY aggregate");

        // Extract GROUP BY expressions
        let group_by_exprs = match group_by {
            GroupByExpr::Expressions(exprs, _) => exprs,
            GroupByExpr::All(_) => {
                return Err(YamlBaseError::NotImplemented(
                    "GROUP BY ALL is not supported yet".to_string(),
                ));
            }
        };

        // Step 1: Evaluate GROUP BY expressions for each row to create groups
        let mut groups: std::collections::HashMap<Vec<Value>, Vec<&Vec<Value>>> =
            std::collections::HashMap::new();

        for row in filtered_rows {
            let mut group_key = Vec::new();
            for expr in group_by_exprs {
                let value = self.get_expr_value(expr, row, table)?;
                group_key.push(value);
            }
            groups.entry(group_key).or_default().push(row);
        }

        // Step 2: Process each group
        let mut result_rows = Vec::new();
        let mut columns = Vec::new();
        let mut column_types = Vec::new();
        let mut first_row = true;

        for (group_values, group_rows) in groups {
            let mut row_values = Vec::new();

            // Process each projection item
            for item in select.projection.iter() {
                match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        let (col_name, col_type, value) = self.evaluate_group_by_expr(
                            expr,
                            &group_rows,
                            &group_values,
                            group_by_exprs,
                            table,
                        )?;

                        // Collect column metadata on first row
                        if first_row {
                            match item {
                                SelectItem::ExprWithAlias { alias, .. } => {
                                    columns.push(alias.value.clone());
                                }
                                _ => {
                                    columns.push(col_name);
                                }
                            }
                            column_types.push(col_type);
                        }

                        row_values.push(value);
                    }
                    _ => {
                        return Err(YamlBaseError::NotImplemented(
                            "Complex projections in GROUP BY queries are not supported".to_string(),
                        ));
                    }
                }
            }

            // Apply HAVING clause if present
            if let Some(having_expr) = &select.having {
                // Create a synthetic row with aggregate values for HAVING evaluation
                let having_result = self.evaluate_having_expr(
                    having_expr,
                    &group_rows,
                    &group_values,
                    group_by_exprs,
                    table,
                )?;

                match having_result {
                    Value::Boolean(true) => {
                        result_rows.push(row_values);
                    }
                    Value::Boolean(false) => {
                        // Skip this group
                    }
                    _ => {
                        return Err(YamlBaseError::Database {
                            message: "HAVING clause must evaluate to boolean".to_string(),
                        });
                    }
                }
            } else {
                result_rows.push(row_values);
            }

            first_row = false;
        }

        Ok(QueryResult {
            columns,
            column_types,
            rows: result_rows,
        })
    }

    fn evaluate_group_by_expr(
        &self,
        expr: &Expr,
        group_rows: &[&Vec<Value>],
        group_values: &[Value],
        group_by_exprs: &[Expr],
        table: &Table,
    ) -> crate::Result<(String, crate::yaml::schema::SqlType, Value)> {
        match expr {
            // If this is one of the GROUP BY expressions, return the group value
            _ if self.is_group_by_expr(expr, group_by_exprs) => {
                let idx = self.get_group_by_expr_index(expr, group_by_exprs).unwrap();
                let value = group_values[idx].clone();
                let col_name = self.expr_to_string(expr);
                let col_type = self.infer_value_type(&value);
                Ok((col_name, col_type, value))
            }
            // If this is an aggregate function, evaluate it over the group
            Expr::Function(func) if self.is_aggregate_function(&func.name.0[0].value) => {
                let (col_name, value) = self.evaluate_aggregate_expr(expr, group_rows, table, 0)?;
                let col_type = self.get_aggregate_result_type(expr);
                Ok((col_name, col_type, value))
            }
            // Regular column references in GROUP BY context
            Expr::Identifier(ident) => {
                // This should be one of the GROUP BY columns
                if self.is_group_by_expr(expr, group_by_exprs) {
                    let idx = self.get_group_by_expr_index(expr, group_by_exprs).unwrap();
                    let value = group_values[idx].clone();
                    let col_type = self.infer_value_type(&value);
                    Ok((ident.value.clone(), col_type, value))
                } else {
                    Err(YamlBaseError::Database {
                        message: format!(
                            "Column '{}' must appear in GROUP BY clause or be used in an aggregate function",
                            ident.value
                        ),
                    })
                }
            }
            _ => Err(YamlBaseError::NotImplemented(
                "This expression type is not supported in GROUP BY queries".to_string(),
            )),
        }
    }

    fn evaluate_having_expr(
        &self,
        expr: &Expr,
        group_rows: &[&Vec<Value>],
        _group_values: &[Value],
        _group_by_exprs: &[Expr],
        table: &Table,
    ) -> crate::Result<Value> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_having_expr(
                    left,
                    group_rows,
                    _group_values,
                    _group_by_exprs,
                    table,
                )?;
                let right_val = self.evaluate_having_expr(
                    right,
                    group_rows,
                    _group_values,
                    _group_by_exprs,
                    table,
                )?;
                self.evaluate_comparison(&left_val, op, &right_val)
            }
            Expr::Function(func) if self.is_aggregate_function(&func.name.0[0].value) => {
                let (_, value) = self.evaluate_aggregate_expr(expr, group_rows, table, 0)?;
                Ok(value)
            }
            Expr::Value(val) => self.sql_value_to_db_value(val),
            _ => Err(YamlBaseError::NotImplemented(
                "This expression type is not supported in HAVING clause".to_string(),
            )),
        }
    }

    fn is_group_by_expr(&self, expr: &Expr, group_by_exprs: &[Expr]) -> bool {
        group_by_exprs.iter().any(|gbe| self.exprs_equal(expr, gbe))
    }

    fn get_group_by_expr_index(&self, expr: &Expr, group_by_exprs: &[Expr]) -> Option<usize> {
        group_by_exprs
            .iter()
            .position(|gbe| self.exprs_equal(expr, gbe))
    }

    fn exprs_equal(&self, expr1: &Expr, expr2: &Expr) -> bool {
        // Simple expression equality check - can be enhanced
        match (expr1, expr2) {
            (Expr::Identifier(id1), Expr::Identifier(id2)) => id1.value == id2.value,
            _ => false,
        }
    }

    fn expr_to_string(&self, expr: &Expr) -> String {
        match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::Function(func) => func.name.0[0].value.clone(),
            _ => "expr".to_string(),
        }
    }

    fn infer_value_type(&self, value: &Value) -> crate::yaml::schema::SqlType {
        match value {
            Value::Integer(_) => crate::yaml::schema::SqlType::BigInt,
            Value::Float(_) => crate::yaml::schema::SqlType::Float,
            Value::Double(_) => crate::yaml::schema::SqlType::Double,
            Value::Decimal(_) => crate::yaml::schema::SqlType::Decimal(10, 2),
            Value::Text(_) => crate::yaml::schema::SqlType::Text,
            Value::Boolean(_) => crate::yaml::schema::SqlType::Boolean,
            Value::Date(_) => crate::yaml::schema::SqlType::Date,
            Value::Timestamp(_) => crate::yaml::schema::SqlType::Timestamp,
            Value::Time(_) => crate::yaml::schema::SqlType::Time,
            Value::Uuid(_) => crate::yaml::schema::SqlType::Text, // UUIDs as text
            Value::Json(_) => crate::yaml::schema::SqlType::Json,
            Value::Null => crate::yaml::schema::SqlType::Text,
        }
    }

    fn is_aggregate_function(&self, func_name: &str) -> bool {
        matches!(
            func_name.to_uppercase().as_str(),
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
        )
    }

    fn evaluate_case_when(
        &self,
        operand: Option<&Expr>,
        conditions: &[Expr],
        results: &[Expr],
        else_result: Option<&Expr>,
        row: &[Value],
        table: &Table,
    ) -> crate::Result<Value> {
        // Handle simple CASE (with operand) vs searched CASE (without operand)
        if let Some(operand_expr) = operand {
            // Simple CASE: CASE expr WHEN val1 THEN result1 WHEN val2 THEN result2 ... END
            let operand_value = self.get_expr_value(operand_expr, row, table)?;

            for (condition, result) in conditions.iter().zip(results.iter()) {
                let condition_value = self.get_expr_value(condition, row, table)?;
                if operand_value == condition_value {
                    return self.get_expr_value(result, row, table);
                }
            }
        } else {
            // Searched CASE: CASE WHEN condition1 THEN result1 WHEN condition2 THEN result2 ... END
            for (condition, result) in conditions.iter().zip(results.iter()) {
                let condition_result = self.evaluate_expr(condition, row, table)?;
                if condition_result {
                    return self.get_expr_value(result, row, table);
                }
            }
        }

        // If no conditions matched, return ELSE result or NULL
        if let Some(else_expr) = else_result {
            self.get_expr_value(else_expr, row, table)
        } else {
            Ok(Value::Null)
        }
    }

    fn evaluate_case_when_constant(
        &self,
        operand: Option<&Expr>,
        conditions: &[Expr],
        results: &[Expr],
        else_result: Option<&Expr>,
    ) -> crate::Result<Value> {
        // Handle simple CASE (with operand) vs searched CASE (without operand) for constant expressions
        if let Some(operand_expr) = operand {
            // Simple CASE: CASE expr WHEN val1 THEN result1 WHEN val2 THEN result2 ... END
            let operand_value = self.evaluate_constant_expr(operand_expr)?;

            for (condition, result) in conditions.iter().zip(results.iter()) {
                let condition_value = self.evaluate_constant_expr(condition)?;
                if operand_value == condition_value {
                    return self.evaluate_constant_expr(result);
                }
            }
        } else {
            // Searched CASE: CASE WHEN condition1 THEN result1 WHEN condition2 THEN result2 ... END
            for (condition, result) in conditions.iter().zip(results.iter()) {
                let condition_result = self.evaluate_constant_expr_as_bool(condition)?;
                if condition_result {
                    return self.evaluate_constant_expr(result);
                }
            }
        }

        // If no conditions matched, return ELSE result or NULL
        if let Some(else_expr) = else_result {
            self.evaluate_constant_expr(else_expr)
        } else {
            Ok(Value::Null)
        }
    }

    fn evaluate_constant_expr_as_bool(&self, expr: &Expr) -> crate::Result<bool> {
        let value = self.evaluate_constant_expr(expr)?;
        match value {
            Value::Boolean(b) => Ok(b),
            Value::Null => Ok(false),
            _ => Err(YamlBaseError::Database {
                message: "CASE WHEN condition must evaluate to boolean".to_string(),
            }),
        }
    }

    fn evaluate_comparison(
        &self,
        left_val: &Value,
        op: &BinaryOperator,
        right_val: &Value,
    ) -> crate::Result<Value> {
        let result = match op {
            BinaryOperator::Eq => left_val == right_val,
            BinaryOperator::NotEq => left_val != right_val,
            BinaryOperator::Lt => {
                if let Some(ord) = left_val.compare(right_val) {
                    ord.is_lt()
                } else {
                    false
                }
            }
            BinaryOperator::LtEq => {
                if let Some(ord) = left_val.compare(right_val) {
                    ord.is_le()
                } else {
                    false
                }
            }
            BinaryOperator::Gt => {
                if let Some(ord) = left_val.compare(right_val) {
                    ord.is_gt()
                } else {
                    false
                }
            }
            BinaryOperator::GtEq => {
                if let Some(ord) = left_val.compare(right_val) {
                    ord.is_ge()
                } else {
                    false
                }
            }
            _ => {
                return Err(YamlBaseError::NotImplemented(format!(
                    "Binary operator {:?} not supported in HAVING",
                    op
                )));
            }
        };
        Ok(Value::Boolean(result))
    }

    fn get_aggregate_result_type(&self, expr: &Expr) -> crate::yaml::schema::SqlType {
        match expr {
            Expr::Function(func) => {
                let func_name = func
                    .name
                    .0
                    .first()
                    .map(|ident| ident.value.to_uppercase())
                    .unwrap_or_default();

                match func_name.as_str() {
                    "COUNT" => crate::yaml::schema::SqlType::BigInt, // COUNT returns i64
                    "SUM" => crate::yaml::schema::SqlType::Double,   // SUM returns double
                    "AVG" => crate::yaml::schema::SqlType::Double,
                    "MIN" | "MAX" => crate::yaml::schema::SqlType::Text, // Depends on input type, default to text
                    _ => crate::yaml::schema::SqlType::Text,
                }
            }
            _ => crate::yaml::schema::SqlType::Text,
        }
    }

    fn evaluate_aggregate_expr(
        &self,
        expr: &Expr,
        rows: &[&Vec<Value>],
        table: &Table,
        _idx: usize,
    ) -> crate::Result<(String, Value)> {
        match expr {
            Expr::Function(func) => {
                let func_name = func
                    .name
                    .0
                    .first()
                    .map(|ident| ident.value.to_uppercase())
                    .unwrap_or_default();

                match func_name.as_str() {
                    "COUNT" => {
                        let count = match &func.args {
                            FunctionArguments::None => {
                                // COUNT() - should be an error but treat as COUNT(*)
                                rows.len() as i64
                            }
                            FunctionArguments::List(args) => {
                                // Check for DISTINCT
                                let is_distinct = args
                                    .duplicate_treatment
                                    .as_ref()
                                    .map(|dt| matches!(dt, DuplicateTreatment::Distinct))
                                    .unwrap_or(false);

                                if args.args.is_empty() {
                                    // COUNT() - should be an error but treat as COUNT(*)
                                    rows.len() as i64
                                } else if args.args.len() == 1 {
                                    match &args.args[0] {
                                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                                            // COUNT(*) - DISTINCT is not allowed with *
                                            rows.len() as i64
                                        }
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                            // COUNT(column) or COUNT(DISTINCT column)
                                            if is_distinct {
                                                // COUNT(DISTINCT column)
                                                let mut unique_values =
                                                    std::collections::HashSet::new();
                                                for row in rows {
                                                    let value =
                                                        self.get_expr_value(expr, row, table)?;
                                                    if !matches!(value, Value::Null) {
                                                        unique_values.insert(value);
                                                    }
                                                }
                                                unique_values.len() as i64
                                            } else {
                                                // COUNT(column)
                                                let mut count = 0i64;
                                                for row in rows {
                                                    let value =
                                                        self.get_expr_value(expr, row, table)?;
                                                    if !matches!(value, Value::Null) {
                                                        count += 1;
                                                    }
                                                }
                                                count
                                            }
                                        }
                                        _ => {
                                            return Err(YamlBaseError::NotImplemented(
                                                "Unsupported COUNT argument".to_string(),
                                            ));
                                        }
                                    }
                                } else {
                                    return Err(YamlBaseError::Database {
                                        message: "COUNT expects at most one argument".to_string(),
                                    });
                                }
                            }
                            _ => {
                                return Err(YamlBaseError::NotImplemented(
                                    "Unsupported function arguments".to_string(),
                                ));
                            }
                        };
                        // Generate proper column name
                        let col_name = match &func.args {
                            FunctionArguments::List(args) if !args.args.is_empty() => {
                                match &args.args[0] {
                                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                                        "COUNT(*)".to_string()
                                    }
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                        if args
                                            .duplicate_treatment
                                            .as_ref()
                                            .map(|dt| matches!(dt, DuplicateTreatment::Distinct))
                                            .unwrap_or(false)
                                        {
                                            format!("COUNT(DISTINCT {})", self.expr_to_string(expr))
                                        } else {
                                            format!("COUNT({})", self.expr_to_string(expr))
                                        }
                                    }
                                    _ => func_name.clone(),
                                }
                            }
                            _ => func_name.clone(),
                        };
                        Ok((col_name, Value::Integer(count)))
                    }
                    "SUM" => {
                        match &func.args {
                            FunctionArguments::List(args) if args.args.len() == 1 => {
                                match &args.args[0] {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                        let mut sum = 0.0;
                                        for row in rows {
                                            let value = self.get_expr_value(expr, row, table)?;
                                            match value {
                                                Value::Integer(i) => sum += i as f64,
                                                Value::Double(d) => sum += d,
                                                Value::Float(f) => sum += f as f64,
                                                Value::Decimal(d) => {
                                                    sum +=
                                                        d.to_string().parse::<f64>().unwrap_or(0.0)
                                                }
                                                Value::Null => {} // Skip NULL values
                                                _ => {
                                                    return Err(YamlBaseError::Database {
                                                        message: "Cannot sum non-numeric values"
                                                            .to_string(),
                                                    });
                                                }
                                            }
                                        }
                                        // Return as Double, not Text
                                        let col_name =
                                            format!("SUM({})", self.expr_to_string(expr));
                                        Ok((col_name, Value::Double(sum)))
                                    }
                                    _ => Err(YamlBaseError::NotImplemented(
                                        "Unsupported SUM argument".to_string(),
                                    )),
                                }
                            }
                            _ => Err(YamlBaseError::Database {
                                message: "SUM requires exactly one argument".to_string(),
                            }),
                        }
                    }
                    "AVG" => {
                        if let FunctionArguments::List(args) = &func.args {
                            if args.args.len() == 1 {
                                match &args.args[0] {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                        let mut sum = 0.0;
                                        let mut count = 0;

                                        for row in rows {
                                            let value = self.get_expr_value(expr, row, table)?;
                                            match value {
                                                Value::Integer(i) => {
                                                    sum += i as f64;
                                                    count += 1;
                                                }
                                                Value::Double(d) => {
                                                    sum += d;
                                                    count += 1;
                                                }
                                                Value::Float(f) => {
                                                    sum += f as f64;
                                                    count += 1;
                                                }
                                                Value::Decimal(d) => {
                                                    sum += d.to_f64().unwrap_or(0.0);
                                                    count += 1;
                                                }
                                                Value::Null => {} // Skip NULL values
                                                _ => {
                                                    return Err(YamlBaseError::Database {
                                                        message: "AVG requires numeric values"
                                                            .to_string(),
                                                    });
                                                }
                                            }
                                        }

                                        let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                                        let col_name =
                                            format!("AVG({})", self.expr_to_string(expr));
                                        Ok((col_name, Value::Double(avg)))
                                    }
                                    _ => Err(YamlBaseError::NotImplemented(
                                        "Unsupported AVG argument".to_string(),
                                    )),
                                }
                            } else {
                                Err(YamlBaseError::Database {
                                    message: "AVG requires exactly one argument".to_string(),
                                })
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "Unsupported function arguments".to_string(),
                            ))
                        }
                    }
                    "MIN" => {
                        if let FunctionArguments::List(args) = &func.args {
                            if args.args.len() == 1 {
                                match &args.args[0] {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                        let mut min_value: Option<Value> = None;

                                        for row in rows {
                                            let value = self.get_expr_value(expr, row, table)?;
                                            if !matches!(value, Value::Null) {
                                                match &min_value {
                                                    None => min_value = Some(value),
                                                    Some(current_min) => {
                                                        if let Some(ord) =
                                                            value.compare(current_min)
                                                        {
                                                            if ord.is_lt() {
                                                                min_value = Some(value);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        let col_name =
                                            format!("MIN({})", self.expr_to_string(expr));
                                        Ok((col_name, min_value.unwrap_or(Value::Null)))
                                    }
                                    _ => Err(YamlBaseError::NotImplemented(
                                        "Unsupported MIN argument".to_string(),
                                    )),
                                }
                            } else {
                                Err(YamlBaseError::Database {
                                    message: "MIN requires exactly one argument".to_string(),
                                })
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "Unsupported function arguments".to_string(),
                            ))
                        }
                    }
                    "MAX" => {
                        if let FunctionArguments::List(args) = &func.args {
                            if args.args.len() == 1 {
                                match &args.args[0] {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                        let mut max_value: Option<Value> = None;

                                        for row in rows {
                                            let value = self.get_expr_value(expr, row, table)?;
                                            if !matches!(value, Value::Null) {
                                                match &max_value {
                                                    None => max_value = Some(value),
                                                    Some(current_max) => {
                                                        if let Some(ord) =
                                                            value.compare(current_max)
                                                        {
                                                            if ord.is_gt() {
                                                                max_value = Some(value);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        let col_name =
                                            format!("MAX({})", self.expr_to_string(expr));
                                        Ok((col_name, max_value.unwrap_or(Value::Null)))
                                    }
                                    _ => Err(YamlBaseError::NotImplemented(
                                        "Unsupported MAX argument".to_string(),
                                    )),
                                }
                            } else {
                                Err(YamlBaseError::Database {
                                    message: "MAX requires exactly one argument".to_string(),
                                })
                            }
                        } else {
                            Err(YamlBaseError::NotImplemented(
                                "Unsupported function arguments".to_string(),
                            ))
                        }
                    }
                    _ => Err(YamlBaseError::NotImplemented(format!(
                        "Aggregate function {} not supported",
                        func_name
                    ))),
                }
            }
            _ => Err(YamlBaseError::NotImplemented(
                "Only aggregate functions are supported in aggregate queries".to_string(),
            )),
        }
    }

    // Join-related methods
    async fn perform_join(
        &self,
        from: &[TableWithJoins],
        tables: &[(String, &Table)],
        table_aliases: &std::collections::HashMap<String, String>,
    ) -> crate::Result<Vec<Vec<Value>>> {
        // Start with the first table
        let mut result_rows = Vec::new();

        if tables.is_empty() {
            return Ok(result_rows);
        }

        // Initialize with rows from the first table
        for row in &tables[0].1.rows {
            result_rows.push(row.clone());
        }

        // Process joins
        let mut table_idx = 1;
        for table_with_joins in from {
            for join in &table_with_joins.joins {
                if table_idx >= tables.len() {
                    return Err(YamlBaseError::Database {
                        message: "Invalid join structure".to_string(),
                    });
                }

                let join_table = tables[table_idx].1;
                result_rows = self.apply_join(
                    result_rows,
                    join_table,
                    &join.join_operator,
                    tables,
                    table_aliases,
                    table_idx,
                )?;

                table_idx += 1;
            }
        }

        Ok(result_rows)
    }

    fn apply_join(
        &self,
        left_rows: Vec<Vec<Value>>,
        right_table: &Table,
        join_type: &JoinOperator,
        all_tables: &[(String, &Table)],
        table_aliases: &std::collections::HashMap<String, String>,
        _right_table_idx: usize,
    ) -> crate::Result<Vec<Vec<Value>>> {
        let mut result = Vec::new();

        match join_type {
            JoinOperator::Inner(constraint) | JoinOperator::LeftOuter(constraint) => {
                // For INNER and LEFT JOIN
                let is_left_join = matches!(join_type, JoinOperator::LeftOuter(_));

                for left_row in &left_rows {
                    let mut matched = false;

                    for right_row in &right_table.rows {
                        // Combine rows for evaluation
                        let mut combined_row = left_row.clone();
                        combined_row.extend(right_row.clone());

                        // Evaluate ON condition
                        let matches = match constraint {
                            JoinConstraint::On(expr) => self.evaluate_join_condition(
                                expr,
                                &combined_row,
                                all_tables,
                                table_aliases,
                            )?,
                            JoinConstraint::Using(_) => {
                                return Err(YamlBaseError::NotImplemented(
                                    "JOIN USING is not yet supported".to_string(),
                                ));
                            }
                            JoinConstraint::Natural => {
                                return Err(YamlBaseError::NotImplemented(
                                    "NATURAL JOIN is not yet supported".to_string(),
                                ));
                            }
                            JoinConstraint::None => true,
                        };

                        if matches {
                            result.push(combined_row);
                            matched = true;
                        }
                    }

                    // For LEFT JOIN, include unmatched left rows with NULLs
                    if is_left_join && !matched {
                        let mut combined_row = left_row.clone();
                        // Add NULL values for all columns from the right table
                        for _ in &right_table.columns {
                            combined_row.push(Value::Null);
                        }
                        result.push(combined_row);
                    }
                }
            }
            JoinOperator::CrossJoin => {
                // Cartesian product
                for left_row in &left_rows {
                    for right_row in &right_table.rows {
                        let mut combined_row = left_row.clone();
                        combined_row.extend(right_row.clone());
                        result.push(combined_row);
                    }
                }
            }
            _ => {
                return Err(YamlBaseError::NotImplemented(
                    "This JOIN type is not yet supported".to_string(),
                ));
            }
        }

        Ok(result)
    }

    fn evaluate_join_condition(
        &self,
        expr: &Expr,
        row: &[Value],
        tables: &[(String, &Table)],
        table_aliases: &std::collections::HashMap<String, String>,
    ) -> crate::Result<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.get_join_expr_value(left, row, tables, table_aliases)?;
                let right_val = self.get_join_expr_value(right, row, tables, table_aliases)?;

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
                        let left_bool =
                            self.evaluate_join_condition(left, row, tables, table_aliases)?;
                        let right_bool =
                            self.evaluate_join_condition(right, row, tables, table_aliases)?;
                        Ok(left_bool && right_bool)
                    }
                    BinaryOperator::Or => {
                        let left_bool =
                            self.evaluate_join_condition(left, row, tables, table_aliases)?;
                        let right_bool =
                            self.evaluate_join_condition(right, row, tables, table_aliases)?;
                        Ok(left_bool || right_bool)
                    }
                    _ => Err(YamlBaseError::NotImplemented(
                        "This operator is not supported in JOIN conditions".to_string(),
                    )),
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let value = self.get_join_expr_value(expr, row, tables, table_aliases)?;
                let mut low_value = self.get_join_expr_value(low, row, tables, table_aliases)?;
                let mut high_value = self.get_join_expr_value(high, row, tables, table_aliases)?;

                // NULL handling - any NULL value results in false
                if matches!(value, Value::Null)
                    || matches!(low_value, Value::Null)
                    || matches!(high_value, Value::Null)
                {
                    return Ok(false);
                }

                // Type conversion: if comparing dates with text, try to parse text as date
                if matches!(value, Value::Date(_)) {
                    if let Value::Text(s) = &low_value {
                        if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                            low_value = Value::Date(date);
                        }
                    }
                    if let Value::Text(s) = &high_value {
                        if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                            high_value = Value::Date(date);
                        }
                    }
                }

                let in_range = match (&value, &low_value, &high_value) {
                    // Numeric comparisons with mixed types
                    (Value::Integer(v), Value::Integer(l), Value::Integer(h)) => {
                        *l <= *v && *v <= *h
                    }
                    (Value::Integer(v), Value::Double(l), Value::Double(h)) => {
                        *l <= *v as f64 && (*v as f64) <= *h
                    }
                    (Value::Integer(v), Value::Float(l), Value::Float(h)) => {
                        *l <= *v as f32 && (*v as f32) <= *h
                    }
                    (Value::Double(v), Value::Integer(l), Value::Integer(h)) => {
                        (*l as f64) <= *v && *v <= (*h as f64)
                    }
                    (Value::Double(v), Value::Double(l), Value::Double(h)) => *l <= *v && *v <= *h,
                    (Value::Double(v), Value::Float(l), Value::Float(h)) => {
                        (*l as f64) <= *v && *v <= (*h as f64)
                    }
                    (Value::Float(v), Value::Integer(l), Value::Integer(h)) => {
                        (*l as f32) <= *v && *v <= (*h as f32)
                    }
                    (Value::Float(v), Value::Double(l), Value::Double(h)) => {
                        *l <= (*v as f64) && (*v as f64) <= *h
                    }
                    (Value::Float(v), Value::Float(l), Value::Float(h)) => *l <= *v && *v <= *h,

                    // Mixed numeric types
                    (Value::Integer(v), Value::Integer(l), Value::Double(h)) => {
                        (*l as f64) <= (*v as f64) && (*v as f64) <= *h
                    }
                    (Value::Integer(v), Value::Double(l), Value::Integer(h)) => {
                        *l <= (*v as f64) && *v <= *h
                    }
                    (Value::Integer(v), Value::Integer(l), Value::Float(h)) => {
                        (*l as f32) <= (*v as f32) && (*v as f32) <= *h
                    }
                    (Value::Integer(v), Value::Float(l), Value::Integer(h)) => {
                        *l <= (*v as f32) && *v <= *h
                    }
                    (Value::Double(v), Value::Double(l), Value::Integer(h)) => {
                        *l <= *v && *v <= (*h as f64)
                    }
                    (Value::Double(v), Value::Integer(l), Value::Double(h)) => {
                        (*l as f64) <= *v && *v <= *h
                    }
                    (Value::Double(v), Value::Float(l), Value::Integer(h)) => {
                        (*l as f64) <= *v && *v <= (*h as f64)
                    }
                    (Value::Double(v), Value::Integer(l), Value::Float(h)) => {
                        (*l as f64) <= *v && *v <= (*h as f64)
                    }
                    (Value::Float(v), Value::Float(l), Value::Integer(h)) => {
                        *l <= *v && *v <= (*h as f32)
                    }
                    (Value::Float(v), Value::Integer(l), Value::Float(h)) => {
                        (*l as f32) <= *v && *v <= *h
                    }
                    (Value::Float(v), Value::Double(l), Value::Float(h)) => {
                        *l <= (*v as f64) && (*v as f64) <= (*h as f64)
                    }
                    (Value::Float(v), Value::Float(l), Value::Double(h)) => {
                        (*l as f64) <= (*v as f64) && (*v as f64) <= *h
                    }

                    // Text comparison
                    (Value::Text(v), Value::Text(l), Value::Text(h)) => l <= v && v <= h,

                    // Date/Time comparisons
                    (Value::Date(v), Value::Date(l), Value::Date(h)) => l <= v && v <= h,
                    (Value::Time(v), Value::Time(l), Value::Time(h)) => l <= v && v <= h,
                    (Value::Timestamp(v), Value::Timestamp(l), Value::Timestamp(h)) => {
                        l <= v && v <= h
                    }

                    _ => {
                        return Err(YamlBaseError::Database {
                            message: format!(
                                "Cannot compare {:?} BETWEEN {:?} AND {:?}",
                                value, low_value, high_value
                            ),
                        });
                    }
                };

                Ok(if *negated { !in_range } else { in_range })
            }
            Expr::IsNull(expr) => {
                let value = self.get_join_expr_value(expr, row, tables, table_aliases)?;
                Ok(matches!(value, Value::Null))
            }
            Expr::IsNotNull(expr) => {
                let value = self.get_join_expr_value(expr, row, tables, table_aliases)?;
                Ok(!matches!(value, Value::Null))
            }
            _ => Err(YamlBaseError::NotImplemented(
                "Complex JOIN conditions are not yet supported".to_string(),
            )),
        }
    }

    fn evaluate_function_with_join_row(
        &self,
        func: &Function,
        row: &[Value],
        tables: &[(String, &Table)],
        table_aliases: &std::collections::HashMap<String, String>,
    ) -> crate::Result<Value> {
        let func_name = func
            .name
            .0
            .first()
            .map(|ident| ident.value.to_uppercase())
            .unwrap_or_default();

        match func_name.as_str() {
            "UPPER" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val =
                                self.get_join_expr_value(str_expr, row, tables, table_aliases)?;

                            match &str_val {
                                Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "UPPER requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for UPPER".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "UPPER requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "UPPER requires arguments".to_string(),
                    })
                }
            }
            "LOWER" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val =
                                self.get_join_expr_value(str_expr, row, tables, table_aliases)?;

                            match &str_val {
                                Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "LOWER requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for LOWER".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "LOWER requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "LOWER requires arguments".to_string(),
                    })
                }
            }
            "TRIM" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val =
                                self.get_join_expr_value(str_expr, row, tables, table_aliases)?;

                            match &str_val {
                                Value::Text(s) => Ok(Value::Text(s.trim().to_string())),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "TRIM requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for TRIM".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "TRIM requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "TRIM requires arguments".to_string(),
                    })
                }
            }
            "COALESCE" => {
                if let FunctionArguments::List(args) = &func.args {
                    // COALESCE returns the first non-NULL value
                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                            let val = self.get_join_expr_value(expr, row, tables, table_aliases)?;
                            if !matches!(val, Value::Null) {
                                return Ok(val);
                            }
                        } else {
                            return Err(YamlBaseError::Database {
                                message: "Invalid argument for COALESCE".to_string(),
                            });
                        }
                    }
                    // If all values are NULL, return NULL
                    Ok(Value::Null)
                } else {
                    Err(YamlBaseError::Database {
                        message: "COALESCE requires arguments".to_string(),
                    })
                }
            }
            "NULLIF" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 2 {
                        if let (
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr1)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr2)),
                        ) = (&args.args[0], &args.args[1])
                        {
                            let val1 =
                                self.get_join_expr_value(expr1, row, tables, table_aliases)?;
                            let val2 =
                                self.get_join_expr_value(expr2, row, tables, table_aliases)?;

                            // NULLIF returns NULL if val1 == val2, otherwise returns val1
                            if val1 == val2 {
                                Ok(Value::Null)
                            } else {
                                Ok(val1)
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid arguments for NULLIF".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "NULLIF requires exactly 2 arguments".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "NULLIF requires arguments".to_string(),
                    })
                }
            }
            "LENGTH" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val =
                                self.get_join_expr_value(str_expr, row, tables, table_aliases)?;

                            match &str_val {
                                Value::Text(s) => Ok(Value::Integer(s.len() as i64)),
                                Value::Null => Ok(Value::Null),
                                _ => Err(YamlBaseError::Database {
                                    message: "LENGTH requires string argument".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid argument for LENGTH".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "LENGTH requires exactly 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "LENGTH requires arguments".to_string(),
                    })
                }
            }
            "SUBSTRING" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 2 || args.args.len() == 3 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)) = &args.args[0]
                        {
                            let str_val =
                                self.get_join_expr_value(str_expr, row, tables, table_aliases)?;

                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(start_expr)) =
                                &args.args[1]
                            {
                                let start_val = self.get_join_expr_value(
                                    start_expr,
                                    row,
                                    tables,
                                    table_aliases,
                                )?;

                                match (&str_val, &start_val) {
                                    (Value::Text(s), Value::Integer(start)) => {
                                        // SQL uses 1-based indexing
                                        let start_idx = if *start > 0 {
                                            (*start as usize).saturating_sub(1)
                                        } else {
                                            0
                                        };

                                        if args.args.len() == 3 {
                                            // SUBSTRING with length
                                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                                len_expr,
                                            )) = &args.args[2]
                                            {
                                                let len_val = self.get_join_expr_value(
                                                    len_expr,
                                                    row,
                                                    tables,
                                                    table_aliases,
                                                )?;

                                                if let Value::Integer(len) = len_val {
                                                    let length = len.max(0) as usize;
                                                    let result: String = s
                                                        .chars()
                                                        .skip(start_idx)
                                                        .take(length)
                                                        .collect();
                                                    Ok(Value::Text(result))
                                                } else {
                                                    Err(YamlBaseError::Database {
                                                        message:
                                                            "SUBSTRING length must be an integer"
                                                                .to_string(),
                                                    })
                                                }
                                            } else {
                                                Err(YamlBaseError::Database {
                                                    message:
                                                        "Invalid length argument for SUBSTRING"
                                                            .to_string(),
                                                })
                                            }
                                        } else {
                                            // SUBSTRING without length
                                            let result: String =
                                                s.chars().skip(start_idx).collect();
                                            Ok(Value::Text(result))
                                        }
                                    }
                                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                                    _ => Err(YamlBaseError::Database {
                                        message: "SUBSTRING requires string and integer arguments"
                                            .to_string(),
                                    }),
                                }
                            } else {
                                Err(YamlBaseError::Database {
                                    message: "Invalid start argument for SUBSTRING".to_string(),
                                })
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid string argument for SUBSTRING".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "SUBSTRING requires 2 or 3 arguments".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "SUBSTRING requires arguments".to_string(),
                    })
                }
            }
            "CONCAT" => {
                if let FunctionArguments::List(args) = &func.args {
                    if !args.args.is_empty() {
                        let mut result = String::new();

                        for arg in &args.args {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                                let val =
                                    self.get_join_expr_value(expr, row, tables, table_aliases)?;

                                match val {
                                    Value::Text(s) => result.push_str(&s),
                                    Value::Integer(i) => result.push_str(&i.to_string()),
                                    Value::Float(f) => result.push_str(&f.to_string()),
                                    Value::Double(d) => result.push_str(&d.to_string()),
                                    Value::Boolean(b) => result.push_str(&b.to_string()),
                                    Value::Null => return Ok(Value::Null), // CONCAT returns NULL if any argument is NULL
                                    _ => result.push_str(&val.to_string()),
                                }
                            } else {
                                return Err(YamlBaseError::Database {
                                    message: "Invalid argument for CONCAT".to_string(),
                                });
                            }
                        }

                        Ok(Value::Text(result))
                    } else {
                        Err(YamlBaseError::Database {
                            message: "CONCAT requires at least 1 argument".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "CONCAT requires arguments".to_string(),
                    })
                }
            }
            "REPLACE" => {
                if let FunctionArguments::List(args) = &func.args {
                    if args.args.len() == 3 {
                        if let (
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(str_expr)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(from_expr)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(to_expr)),
                        ) = (&args.args[0], &args.args[1], &args.args[2])
                        {
                            let str_val =
                                self.get_join_expr_value(str_expr, row, tables, table_aliases)?;
                            let from_val =
                                self.get_join_expr_value(from_expr, row, tables, table_aliases)?;
                            let to_val =
                                self.get_join_expr_value(to_expr, row, tables, table_aliases)?;

                            match (&str_val, &from_val, &to_val) {
                                (Value::Text(s), Value::Text(from), Value::Text(to)) => {
                                    // Handle empty search string - return original string
                                    if from.is_empty() {
                                        Ok(Value::Text(s.clone()))
                                    } else {
                                        Ok(Value::Text(s.replace(from, to)))
                                    }
                                }
                                (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => {
                                    Ok(Value::Null)
                                }
                                _ => Err(YamlBaseError::Database {
                                    message: "REPLACE requires string arguments".to_string(),
                                }),
                            }
                        } else {
                            Err(YamlBaseError::Database {
                                message: "Invalid arguments for REPLACE".to_string(),
                            })
                        }
                    } else {
                        Err(YamlBaseError::Database {
                            message: "REPLACE requires exactly 3 arguments".to_string(),
                        })
                    }
                } else {
                    Err(YamlBaseError::Database {
                        message: "REPLACE requires arguments".to_string(),
                    })
                }
            }
            // For functions that don't need row context, delegate to constant version
            _ => self.evaluate_constant_function(func),
        }
    }

    fn get_join_expr_value(
        &self,
        expr: &Expr,
        row: &[Value],
        tables: &[(String, &Table)],
        table_aliases: &std::collections::HashMap<String, String>,
    ) -> crate::Result<Value> {
        match expr {
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    let table_ref = &parts[0].value;
                    let column_name = &parts[1].value;

                    // Resolve table alias if needed
                    let actual_table_name = table_aliases.get(table_ref).unwrap_or(table_ref);

                    // Find table index
                    let mut col_offset = 0;
                    for (table_name, table) in tables.iter() {
                        if table_name == actual_table_name || table_ref == table_name {
                            // Find column in this table
                            if let Some(col_idx) = table.get_column_index(column_name) {
                                return Ok(row[col_offset + col_idx].clone());
                            }
                            return Err(YamlBaseError::Database {
                                message: format!(
                                    "Column '{}.{}' not found",
                                    table_ref, column_name
                                ),
                            });
                        }
                        col_offset += table.columns.len();
                    }

                    Err(YamlBaseError::Database {
                        message: format!("Table '{}' not found in join", table_ref),
                    })
                } else {
                    Err(YamlBaseError::NotImplemented(
                        "Complex identifiers are not supported".to_string(),
                    ))
                }
            }
            Expr::Identifier(ident) => {
                // Search for column in all tables
                let mut col_offset = 0;
                for (_, table) in tables {
                    if let Some(col_idx) = table.get_column_index(&ident.value) {
                        return Ok(row[col_offset + col_idx].clone());
                    }
                    col_offset += table.columns.len();
                }

                Err(YamlBaseError::Database {
                    message: format!("Column '{}' not found in any table", ident.value),
                })
            }
            Expr::Value(val) => self.sql_value_to_db_value(val),
            Expr::Function(func) => {
                // Evaluate functions in JOIN conditions with row context
                self.evaluate_function_with_join_row(func, row, tables, table_aliases)
            }
            Expr::Extract { field, expr, .. } => {
                // Handle EXTRACT in JOIN conditions
                let date_val = self.get_join_expr_value(expr, row, tables, table_aliases)?;

                let date_str = match &date_val {
                    Value::Text(s) => s,
                    Value::Date(d) => {
                        let formatted = d.format("%Y-%m-%d").to_string();
                        return self.evaluate_extract_from_date(field, &formatted);
                    }
                    _ => {
                        return Err(YamlBaseError::Database {
                            message: "EXTRACT requires date argument".to_string(),
                        });
                    }
                };

                self.evaluate_extract_from_date(field, date_str)
            }
            Expr::Trim { expr, .. } => {
                // Handle TRIM expression
                let inner_val = self.get_join_expr_value(expr, row, tables, table_aliases)?;
                match &inner_val {
                    Value::Text(s) => Ok(Value::Text(s.trim().to_string())),
                    Value::Null => Ok(Value::Null),
                    _ => Err(YamlBaseError::Database {
                        message: "TRIM requires string argument".to_string(),
                    }),
                }
            }
            _ => Err(YamlBaseError::NotImplemented(
                "Expression type not supported in JOIN conditions".to_string(),
            )),
        }
    }

    fn extract_columns_for_join(
        &self,
        select: &Select,
        tables: &[(String, &Table)],
        table_aliases: &std::collections::HashMap<String, String>,
    ) -> crate::Result<Vec<JoinedColumn>> {
        let mut columns = Vec::new();
        let mut column_counter = 1;

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    match expr {
                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                            let table_ref = &parts[0].value;
                            let column_name = &parts[1].value;

                            // Resolve table alias if needed
                            let actual_table_name =
                                table_aliases.get(table_ref).unwrap_or(table_ref);

                            // Find table and column indices
                            for (table_idx, (table_name, table)) in tables.iter().enumerate() {
                                if table_name == actual_table_name || table_ref == table_name {
                                    if let Some(col_idx) = table.get_column_index(column_name) {
                                        let display_name = format!("{}.{}", table_ref, column_name);
                                        columns.push(JoinedColumn::TableColumn(
                                            display_name,
                                            table_idx,
                                            col_idx,
                                        ));
                                        break;
                                    }
                                }
                            }
                        }
                        Expr::Identifier(ident) => {
                            // Search for column in all tables
                            let mut found = false;
                            for (table_idx, (_, table)) in tables.iter().enumerate() {
                                if let Some(col_idx) = table.get_column_index(&ident.value) {
                                    columns.push(JoinedColumn::TableColumn(
                                        ident.value.clone(),
                                        table_idx,
                                        col_idx,
                                    ));
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                return Err(YamlBaseError::Database {
                                    message: format!("Column '{}' not found", ident.value),
                                });
                            }
                        }
                        _ => {
                            // Constant expression
                            let value = self.evaluate_constant_expr(expr)?;
                            let col_name = format!("column_{}", column_counter);
                            column_counter += 1;
                            columns.push(JoinedColumn::Constant(col_name, value));
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    match expr {
                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                            let table_ref = &parts[0].value;
                            let column_name = &parts[1].value;

                            // Resolve table alias if needed
                            let actual_table_name =
                                table_aliases.get(table_ref).unwrap_or(table_ref);

                            // Find table and column indices
                            for (table_idx, (table_name, table)) in tables.iter().enumerate() {
                                if table_name == actual_table_name || table_ref == table_name {
                                    if let Some(col_idx) = table.get_column_index(column_name) {
                                        columns.push(JoinedColumn::TableColumn(
                                            alias.value.clone(),
                                            table_idx,
                                            col_idx,
                                        ));
                                        break;
                                    }
                                }
                            }
                        }
                        Expr::Identifier(ident) => {
                            // Search for column in all tables
                            let mut found = false;
                            for (table_idx, (_, table)) in tables.iter().enumerate() {
                                if let Some(col_idx) = table.get_column_index(&ident.value) {
                                    columns.push(JoinedColumn::TableColumn(
                                        alias.value.clone(),
                                        table_idx,
                                        col_idx,
                                    ));
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                return Err(YamlBaseError::Database {
                                    message: format!("Column '{}' not found", ident.value),
                                });
                            }
                        }
                        _ => {
                            // Constant expression
                            let value = self.evaluate_constant_expr(expr)?;
                            columns.push(JoinedColumn::Constant(alias.value.clone(), value));
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    for (table_idx, (table_name, table)) in tables.iter().enumerate() {
                        for (col_idx, col) in table.columns.iter().enumerate() {
                            let display_name = if tables.len() > 1 {
                                format!("{}.{}", table_name, col.name)
                            } else {
                                col.name.clone()
                            };
                            columns.push(JoinedColumn::TableColumn(
                                display_name,
                                table_idx,
                                col_idx,
                            ));
                        }
                    }
                }
                _ => {
                    return Err(YamlBaseError::NotImplemented(
                        "Complex projections are not yet supported".to_string(),
                    ));
                }
            }
        }

        Ok(columns)
    }

    fn filter_joined_rows(
        &self,
        rows: &[Vec<Value>],
        selection: &Option<Expr>,
        tables: &[(String, &Table)],
        table_aliases: &std::collections::HashMap<String, String>,
    ) -> crate::Result<Vec<Vec<Value>>> {
        if let Some(where_expr) = selection {
            let mut result = Vec::new();
            for row in rows {
                if self.evaluate_join_condition(where_expr, row, tables, table_aliases)? {
                    result.push(row.clone());
                }
            }
            Ok(result)
        } else {
            Ok(rows.to_vec())
        }
    }

    fn project_joined_columns(
        &self,
        rows: &[Vec<Value>],
        columns: &[JoinedColumn],
        tables: &[(String, &Table)],
    ) -> crate::Result<Vec<Vec<Value>>> {
        let mut projected_rows = Vec::new();

        // Calculate cumulative offsets for each table
        let mut table_offsets = vec![0];
        let mut cumulative_offset = 0;
        for (_, table) in tables.iter() {
            cumulative_offset += table.columns.len();
            table_offsets.push(cumulative_offset);
        }

        // For each row, extract only the requested columns
        for row in rows {
            let mut projected_row = Vec::new();

            for column in columns {
                match column {
                    JoinedColumn::TableColumn(_, table_idx, col_idx) => {
                        // Calculate the actual position in the joined row
                        let position = table_offsets[*table_idx] + col_idx;

                        if let Some(value) = row.get(position) {
                            projected_row.push(value.clone());
                        } else {
                            return Err(YamlBaseError::Database {
                                message: format!(
                                    "Column index out of bounds: table_idx={}, col_idx={}, position={}",
                                    table_idx, col_idx, position
                                ),
                            });
                        }
                    }
                    JoinedColumn::Constant(_, value) => {
                        projected_row.push(value.clone());
                    }
                }
            }

            projected_rows.push(projected_row);
        }

        Ok(projected_rows)
    }

    fn sort_joined_rows(
        &self,
        rows: Vec<Vec<Value>>,
        _order_exprs: &[OrderByExpr],
        _columns: &[JoinedColumn],
    ) -> crate::Result<Vec<Vec<Value>>> {
        // For now, just return unsorted rows
        // Full implementation would require mapping order expressions to column indices
        Ok(rows)
    }

    async fn execute_aggregate_with_joined_rows(
        &self,
        _db: &Database,
        _select: &Select,
        _query: &Query,
        _joined_rows: &[Vec<Value>],
        _tables: &[(String, &Table)],
    ) -> crate::Result<QueryResult> {
        // Simplified aggregate implementation for joins
        // Would need full implementation with proper column mapping
        Err(YamlBaseError::NotImplemented(
            "Aggregate queries with JOINs are not yet fully implemented".to_string(),
        ))
    }

    // CTE (Common Table Expression) support
    async fn execute_query_with_ctes(
        &self,
        db: &Database,
        query: &Query,
        with: &With,
    ) -> crate::Result<QueryResult> {
        debug!("Executing query with CTEs");

        // Store CTE results in a temporary map
        let cte_results: std::collections::HashMap<String, QueryResult> =
            std::collections::HashMap::new();

        // Check if there are any CTEs to execute
        if !with.cte_tables.is_empty() {
            // For now, we don't support CTEs
            // This would require boxing the future to avoid infinite size for recursive CTEs
            return Err(YamlBaseError::NotImplemented(
                "CTE execution is not yet fully implemented".to_string(),
            ));
        }

        // Now execute the main query with CTE results available
        // For now, we'll return an error as full CTE support requires
        // modifying the entire execution pipeline to handle CTE references
        match &query.body.as_ref() {
            SetExpr::Select(select) => {
                // Check if the SELECT references any CTEs
                for table_with_joins in &select.from {
                    if let TableFactor::Table { name, .. } = &table_with_joins.relation {
                        let table_name = name
                            .0
                            .first()
                            .map(|ident| ident.value.clone())
                            .unwrap_or_else(String::new);

                        if cte_results.contains_key(&table_name) {
                            // This table is a CTE reference
                            return Err(YamlBaseError::NotImplemented(
                                "CTE references in FROM clause are not yet fully implemented"
                                    .to_string(),
                            ));
                        }
                    }
                }

                // If no CTE references, execute normally
                self.execute_select(db, select, query).await
            }
            _ => Err(YamlBaseError::NotImplemented(
                "Only SELECT queries are supported in CTEs".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{Column, Database, Storage as DbStorage, Table, Value};
    use chrono::NaiveDate;
    use rust_decimal::Decimal;
    use sqlparser::ast::Statement;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    // Helper function to create a test executor with storage
    async fn create_test_executor_from_arc(db: Arc<RwLock<Database>>) -> QueryExecutor {
        let db_owned = {
            let db_read = db.read().await;
            db_read.clone()
        };
        let storage = Arc::new(DbStorage::new(db_owned));
        // Wait a bit for the async index building to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        QueryExecutor::new(storage)
    }

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

    fn create_column(
        name: &str,
        sql_type: crate::yaml::schema::SqlType,
        primary_key: bool,
    ) -> Column {
        Column {
            name: name.to_string(),
            sql_type,
            primary_key,
            nullable: false,
            unique: primary_key,
            default: None,
            references: None,
        }
    }

    #[tokio::test]
    async fn test_select_without_from_simple() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

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
        let executor = create_test_executor_from_arc(db).await;

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
        let executor = create_test_executor_from_arc(db).await;

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
        let executor = create_test_executor_from_arc(db).await;

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
        let executor = create_test_executor_from_arc(db).await;

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
        let executor = create_test_executor_from_arc(db).await;

        let stmt = parse_statement("SELECT -5");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(-5));

        let stmt = parse_statement("SELECT -3.5");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(-3.5));
    }

    #[tokio::test]
    async fn test_select_without_from_division_by_zero() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        let stmt = parse_statement("SELECT 1 / 0");
        let result = executor.execute(&stmt).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Division by zero"));
    }

    #[tokio::test]
    async fn test_select_with_from_still_works() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

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

    #[tokio::test]
    async fn test_not_in_operator() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
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
                    name: "status".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Varchar(50),
                    primary_key: false,
                    nullable: false,
                    unique: false,
                    default: None,
                    references: None,
                },
            ];

            let mut table = Table::new("projects".to_string(), columns);

            table
                .insert_row(vec![Value::Integer(1), Value::Text("Active".to_string())])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(2), Value::Text("Pending".to_string())])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(3),
                    Value::Text("Cancelled".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(4), Value::Text("Closed".to_string())])
                .unwrap();

            db_write.add_table(table).unwrap();
        }

        let executor = create_test_executor_from_arc(db).await;
        let stmt = parse_statement(
            "SELECT id, status FROM projects WHERE status NOT IN ('Cancelled', 'Closed')",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][1], Value::Text("Active".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("Pending".to_string()));
    }

    #[tokio::test]
    async fn test_in_operator() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                create_column("type", crate::yaml::schema::SqlType::Varchar(50), false),
            ];

            let mut table = Table::new("tasks".to_string(), columns);

            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Text("Development".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(2), Value::Text("Research".to_string())])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(3), Value::Text("Support".to_string())])
                .unwrap();

            db_write.add_table(table).unwrap();
        }

        let executor = create_test_executor_from_arc(db).await;
        let stmt =
            parse_statement("SELECT id, type FROM tasks WHERE type IN ('Development', 'Research')");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][1], Value::Text("Development".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("Research".to_string()));
    }

    #[tokio::test]
    async fn test_like_operator_with_percent() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                Column {
                    name: "name".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Varchar(100),
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
            ];

            let mut table = Table::new("classifications".to_string(), columns);

            table
                .insert_row(vec![Value::Integer(1), Value::Text("NS-High".to_string())])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("NS-Medium".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(3), Value::Text("Public".to_string())])
                .unwrap();

            db_write.add_table(table).unwrap();
        }

        let executor = create_test_executor_from_arc(db).await;

        // Test with % at end
        let stmt = parse_statement("SELECT id, name FROM classifications WHERE name LIKE 'NS%'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2);

        // Test with % at beginning
        let stmt = parse_statement("SELECT id, name FROM classifications WHERE name LIKE '%High'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Value::Text("NS-High".to_string()));
    }

    #[tokio::test]
    async fn test_like_operator_with_underscore() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                create_column("code", crate::yaml::schema::SqlType::Varchar(10), false),
            ];

            let mut table = Table::new("codes".to_string(), columns);

            table
                .insert_row(vec![Value::Integer(1), Value::Text("A1B".to_string())])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(2), Value::Text("A2B".to_string())])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(3), Value::Text("A12B".to_string())])
                .unwrap();

            db_write.add_table(table).unwrap();
        }

        let executor = create_test_executor_from_arc(db).await;
        let stmt = parse_statement("SELECT id, code FROM codes WHERE code LIKE 'A_B'");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][1], Value::Text("A1B".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("A2B".to_string()));
    }

    #[tokio::test]
    async fn test_not_equals_operators() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                create_column("flag", crate::yaml::schema::SqlType::Varchar(1), false),
            ];

            let mut table = Table::new("flags".to_string(), columns);

            table
                .insert_row(vec![Value::Integer(1), Value::Text("Y".to_string())])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(2), Value::Text("N".to_string())])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(3), Value::Text("Y".to_string())])
                .unwrap();

            db_write.add_table(table).unwrap();
        }

        let executor = create_test_executor_from_arc(db).await;

        // Test <> operator
        let stmt = parse_statement("SELECT id FROM flags WHERE flag <> 'Y'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(2));

        // Test != operator
        let stmt = parse_statement("SELECT id FROM flags WHERE flag != 'Y'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(2));
    }

    #[tokio::test]
    async fn test_date_literal_comparisons() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                create_column("start_date", crate::yaml::schema::SqlType::Date, false),
            ];

            let mut table = Table::new("events".to_string(), columns);

            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 2, 1).unwrap()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(3),
                    Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 6, 1).unwrap()),
                ])
                .unwrap();

            db_write.add_table(table).unwrap();
        }

        let executor = create_test_executor_from_arc(db).await;
        let stmt = parse_statement("SELECT id FROM events WHERE start_date > DATE '2025-01-01'");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Integer(2));
        assert_eq!(result.rows[1][0], Value::Integer(3));
    }

    #[tokio::test]
    async fn test_complex_nested_conditions() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                create_column("status", crate::yaml::schema::SqlType::Varchar(20), false),
                create_column("type", crate::yaml::schema::SqlType::Varchar(50), false),
                create_column("priority", crate::yaml::schema::SqlType::Integer, false),
            ];

            let mut table = Table::new("items".to_string(), columns);

            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Text("Active".to_string()),
                    Value::Text("Development".to_string()),
                    Value::Integer(1),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("Pending".to_string()),
                    Value::Text("Research".to_string()),
                    Value::Integer(2),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(3),
                    Value::Text("Active".to_string()),
                    Value::Text("Support".to_string()),
                    Value::Integer(3),
                ])
                .unwrap();

            db_write.add_table(table).unwrap();
        }

        let executor = create_test_executor_from_arc(db).await;
        let stmt = parse_statement(
            "SELECT id FROM items WHERE (status = 'Active' OR status = 'Pending') AND type IN ('Development', 'Research') AND priority < 3",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[1][0], Value::Integer(2));
    }

    #[tokio::test]
    async fn test_like_with_special_regex_chars() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                create_column("pattern", crate::yaml::schema::SqlType::Varchar(50), false),
            ];

            let mut table = Table::new("patterns".to_string(), columns);

            table
                .insert_row(vec![Value::Integer(1), Value::Text("test.com".to_string())])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("test[123]".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(3), Value::Text("test^abc".to_string())])
                .unwrap();

            db_write.add_table(table).unwrap();
        }

        let executor = create_test_executor_from_arc(db).await;

        // Dots should be treated as literals, not regex wildcards
        let stmt = parse_statement("SELECT id FROM patterns WHERE pattern LIKE 'test.com'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(1));
    }

    #[tokio::test]
    async fn test_not_like_operator() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                create_column(
                    "category",
                    crate::yaml::schema::SqlType::Varchar(100),
                    false,
                ),
            ];

            let mut table = Table::new("items".to_string(), columns);

            table
                .insert_row(vec![Value::Integer(1), Value::Text("NS-High".to_string())])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("NS-Medium".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(3), Value::Text("NS-Low".to_string())])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(4), Value::Text("Public".to_string())])
                .unwrap();

            db_write.add_table(table).unwrap();
        }

        let executor = create_test_executor_from_arc(db).await;

        // Test NOT LIKE
        let stmt = parse_statement("SELECT id, category FROM items WHERE category NOT LIKE 'NS%'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(4));
        assert_eq!(result.rows[0][1], Value::Text("Public".to_string()));
    }

    #[tokio::test]
    async fn test_complex_enterprise_query() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column(
                    "PROJECT_ID",
                    crate::yaml::schema::SqlType::Varchar(255),
                    true,
                ),
                create_column(
                    "PROJECT_NAME",
                    crate::yaml::schema::SqlType::Varchar(255),
                    false,
                ),
                create_column(
                    "VERSION_CODE",
                    crate::yaml::schema::SqlType::Varchar(50),
                    false,
                ),
                create_column(
                    "STATUS_CODE",
                    crate::yaml::schema::SqlType::Varchar(50),
                    false,
                ),
                create_column(
                    "ACTIVE_FLAG",
                    crate::yaml::schema::SqlType::Varchar(1),
                    false,
                ),
                create_column(
                    "CLOSED_FOR_TIME_ENTRY",
                    crate::yaml::schema::SqlType::Varchar(1),
                    false,
                ),
                create_column(
                    "SECURITY_CLASSIFICATION",
                    crate::yaml::schema::SqlType::Varchar(100),
                    false,
                ),
                create_column(
                    "PROJECT_STRUCTURE",
                    crate::yaml::schema::SqlType::Varchar(100),
                    false,
                ),
                create_column("START_DATE", crate::yaml::schema::SqlType::Date, false),
                create_column(
                    "DEPARTMENT",
                    crate::yaml::schema::SqlType::Varchar(255),
                    false,
                ),
                create_column(
                    "PROJECT_CLASS",
                    crate::yaml::schema::SqlType::Varchar(255),
                    false,
                ),
                create_column(
                    "PROJECT_CATEGORY",
                    crate::yaml::schema::SqlType::Varchar(255),
                    false,
                ),
            ];

            let mut table = Table::new("PROJECTS".to_string(), columns);

            // Add test data that should match
            table
                .insert_row(vec![
                    Value::Text("PR-2025-001".to_string()),
                    Value::Text("5G Development".to_string()),
                    Value::Text("Published".to_string()),
                    Value::Text("Active".to_string()),
                    Value::Text("Y".to_string()),
                    Value::Text("N".to_string()),
                    Value::Text("NS-High".to_string()),
                    Value::Text("Project".to_string()),
                    Value::Date(NaiveDate::from_ymd_opt(2025, 2, 1).unwrap()),
                    Value::Text("Automotive".to_string()),
                    Value::Text("Product Development".to_string()),
                    Value::Text("PROD DEV".to_string()),
                ])
                .unwrap();

            // Add test data that should NOT match (closed status)
            table
                .insert_row(vec![
                    Value::Text("PR-2024-999".to_string()),
                    Value::Text("Legacy System".to_string()),
                    Value::Text("Published".to_string()),
                    Value::Text("Closed".to_string()),
                    Value::Text("Y".to_string()),
                    Value::Text("N".to_string()),
                    Value::Text("NS-Low".to_string()),
                    Value::Text("Project".to_string()),
                    Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
                    Value::Text("Support IT".to_string()),
                    Value::Text("Product Development".to_string()),
                    Value::Text("PROD DEV".to_string()),
                ])
                .unwrap();

            db_write.add_table(table).unwrap();
        }

        let executor = create_test_executor_from_arc(db).await;

        // Test complex enterprise query with multiple conditions
        let stmt = parse_statement(
            "SELECT PROJECT_ID, PROJECT_NAME FROM PROJECTS WHERE VERSION_CODE = 'Published' \
             AND STATUS_CODE NOT IN ('Cancelled', 'Closed') AND ACTIVE_FLAG = 'Y' \
             AND CLOSED_FOR_TIME_ENTRY <> 'Y' AND SECURITY_CLASSIFICATION LIKE 'NS%' \
             AND PROJECT_STRUCTURE = 'Project' AND START_DATE > DATE '2025-01-01' \
             AND DEPARTMENT NOT IN ('Support IT', 'The Support IT', 'The Demo Portfolio', 'The Archive') \
             AND PROJECT_CLASS IN ('Product Development', 'Technology & Research Development') \
             AND PROJECT_CATEGORY IN ('PROD DEV', 'TECH & RESEARCH DEV')",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("PR-2025-001".to_string()));
        assert_eq!(result.rows[0][1], Value::Text("5G Development".to_string()));
    }

    #[tokio::test]
    async fn test_like_escape_sequences() {
        // Create a custom database for this test
        let mut db = Database::new("test_db".to_string());

        // Create test table with needed columns
        let columns = vec![
            create_column("id", crate::yaml::schema::SqlType::Integer, true),
            create_column("name", crate::yaml::schema::SqlType::Varchar(255), false),
        ];

        let mut table = Table::new("test_table".to_string(), columns);
        // Row with literal %
        table
            .insert_row(vec![Value::Integer(10), Value::Text("100%".to_string())])
            .unwrap();
        // Row with literal _
        table
            .insert_row(vec![
                Value::Integer(11),
                Value::Text("user_name".to_string()),
            ])
            .unwrap();
        // Row with literal \\
        table
            .insert_row(vec![
                Value::Integer(12),
                Value::Text("C:\\path\\file".to_string()),
            ])
            .unwrap();

        db.add_table(table).unwrap();
        let db = Arc::new(RwLock::new(db));

        let executor = create_test_executor_from_arc(db).await;

        // Test escaped % (should match literal %)
        let stmt = parse_statement("SELECT id FROM test_table WHERE name LIKE '100\\%'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(10));

        // Test escaped _ (should match literal _)
        let stmt = parse_statement("SELECT id FROM test_table WHERE name LIKE 'user\\_name'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(11));

        // Test escaped \\ (should match literal \\)
        let stmt =
            parse_statement("SELECT id FROM test_table WHERE name LIKE 'C:\\\\path\\\\file'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(12));

        // Test unescaped % as wildcard
        let stmt = parse_statement("SELECT id FROM test_table WHERE name LIKE '%name'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1); // Should match user_name
        assert_eq!(result.rows[0][0], Value::Integer(11));

        // Test unescaped _ as single character wildcard
        let stmt = parse_statement("SELECT id FROM test_table WHERE name LIKE '10_%'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1); // Should match 100%
        assert_eq!(result.rows[0][0], Value::Integer(10));
    }

    #[tokio::test]
    async fn test_select_constant_from_table() {
        // Create database with test table already included
        let mut db = Database::new("test_db".to_string());

        let columns = vec![
            create_column("id", crate::yaml::schema::SqlType::Integer, true),
            create_column("name", crate::yaml::schema::SqlType::Varchar(100), false),
        ];

        let mut table = Table::new("test_table".to_string(), columns);

        table
            .insert_row(vec![Value::Integer(1), Value::Text("Alice".to_string())])
            .unwrap();
        table
            .insert_row(vec![Value::Integer(2), Value::Text("Bob".to_string())])
            .unwrap();
        table
            .insert_row(vec![Value::Integer(3), Value::Text("Charlie".to_string())])
            .unwrap();

        db.add_table(table).unwrap();

        // Now create Storage with the complete database
        let storage = Arc::new(DbStorage::new(db));

        // Wait a bit for the async index building to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let executor = QueryExecutor::new(storage);

        // Test 1: SELECT 1 FROM test_table
        let stmt = parse_statement("SELECT 1 FROM test_table");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0], "column_1");
        assert_eq!(result.rows.len(), 3); // Should have 3 rows
        for row in &result.rows {
            assert_eq!(row[0], Value::Integer(1));
        }

        // Test 2: SELECT 1 AS constant_value FROM test_table
        let stmt = parse_statement("SELECT 1 AS constant_value FROM test_table");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.columns[0], "constant_value");
        assert_eq!(result.rows.len(), 3);
        for row in &result.rows {
            assert_eq!(row[0], Value::Integer(1));
        }

        // Test 3: SELECT id, 1 AS flag, name FROM test_table
        let stmt = parse_statement("SELECT id, 1 AS flag, name FROM test_table ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0], "id");
        assert_eq!(result.columns[1], "flag");
        assert_eq!(result.columns[2], "name");
        assert_eq!(result.rows.len(), 3);

        // Check first row
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[0][1], Value::Integer(1)); // constant
        assert_eq!(result.rows[0][2], Value::Text("Alice".to_string()));

        // Test 4: SELECT 'hello' FROM test_table
        let stmt = parse_statement("SELECT 'hello' FROM test_table");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 3);
        for row in &result.rows {
            assert_eq!(row[0], Value::Text("hello".to_string()));
        }

        // Test 5: SELECT 1 FROM test_table WHERE id = 2
        let stmt = parse_statement("SELECT 1 FROM test_table WHERE id = 2");

        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 1); // Only one row should match
        assert_eq!(result.rows[0][0], Value::Integer(1));

        // Test 6: SELECT 1 FROM test_table LIMIT 1
        let stmt = parse_statement("SELECT 1 FROM test_table LIMIT 1");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 1); // Limited to 1 row
        assert_eq!(result.rows[0][0], Value::Integer(1));
    }

    #[tokio::test]
    async fn test_sqlalchemy_compatibility() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // Test 1: SELECT VERSION()
        let stmt = parse_statement("SELECT VERSION()");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        assert!(matches!(result.rows[0][0], Value::Text(ref s) if s.contains("8.0.35-yamlbase")));

        // Test 2: SELECT CURRENT_DATE
        let stmt = parse_statement("SELECT CURRENT_DATE");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        // Check format YYYY-MM-DD
        if let Value::Text(date_str) = &result.rows[0][0] {
            assert_eq!(date_str.len(), 10);
            assert!(date_str.chars().nth(4).unwrap() == '-');
            assert!(date_str.chars().nth(7).unwrap() == '-');
        } else {
            panic!("Expected text value for CURRENT_DATE");
        }

        // Test 3: SELECT NOW()
        let stmt = parse_statement("SELECT NOW()");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        // Check format YYYY-MM-DD HH:MM:SS
        if let Value::Text(datetime_str) = &result.rows[0][0] {
            assert_eq!(datetime_str.len(), 19);
            assert!(datetime_str.chars().nth(10).unwrap() == ' ');
        } else {
            panic!("Expected text value for NOW()");
        }
    }

    #[tokio::test]
    async fn test_transaction_commands() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // Test 1: START TRANSACTION
        let stmt = parse_statement("START TRANSACTION");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 0);
        assert_eq!(result.rows.len(), 0);

        // Test 2: COMMIT
        let stmt = parse_statement("COMMIT");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 0);
        assert_eq!(result.rows.len(), 0);

        // Test 3: ROLLBACK
        let stmt = parse_statement("ROLLBACK");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 0);
        assert_eq!(result.rows.len(), 0);
    }

    #[tokio::test]
    async fn test_join_queries() {
        let mut db = Database::new("test_db".to_string());

        // Create first table
        let columns1 = vec![
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
        let mut users = Table::new("users".to_string(), columns1);
        users
            .insert_row(vec![Value::Integer(1), Value::Text("Alice".to_string())])
            .unwrap();
        users
            .insert_row(vec![Value::Integer(2), Value::Text("Bob".to_string())])
            .unwrap();
        db.add_table(users).unwrap();

        // Create second table
        let columns2 = vec![
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
                name: "user_id".to_string(),
                sql_type: crate::yaml::schema::SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "amount".to_string(),
                sql_type: crate::yaml::schema::SqlType::Decimal(10, 2),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ];
        let mut orders = Table::new("orders".to_string(), columns2);
        orders
            .insert_row(vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Decimal(Decimal::from_str("100.50").unwrap()),
            ])
            .unwrap();
        orders
            .insert_row(vec![
                Value::Integer(2),
                Value::Integer(1),
                Value::Decimal(Decimal::from_str("200.75").unwrap()),
            ])
            .unwrap();
        orders
            .insert_row(vec![
                Value::Integer(3),
                Value::Integer(2),
                Value::Decimal(Decimal::from_str("50.25").unwrap()),
            ])
            .unwrap();
        db.add_table(orders).unwrap();

        let db_arc = Arc::new(RwLock::new(db));
        let executor = create_test_executor_from_arc(db_arc).await;

        // Test 1: INNER JOIN
        let stmt = parse_statement(
            "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 3); // Alice has 2 orders, Bob has 1

        // Test 2: LEFT JOIN
        let stmt = parse_statement(
            "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.id = 2",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 1); // Bob has 1 order

        // Test 3: CROSS JOIN
        let stmt = parse_statement("SELECT * FROM users CROSS JOIN orders");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 6); // 2 users × 3 orders = 6 rows
    }

    #[tokio::test]
    async fn test_current_date_and_timestamp() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;
        
        // Test 1: CURRENT_DATE returns Date value
        let stmt = parse_statement("SELECT CURRENT_DATE");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        // Result should be a Date value
        assert!(matches!(result.rows[0][0], Value::Date(_)));

        // Test 2: CURRENT_TIMESTAMP returns datetime string
        let stmt = parse_statement("SELECT CURRENT_TIMESTAMP");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        // Check format YYYY-MM-DD HH:MM:SS
        if let Value::Text(datetime_str) = &result.rows[0][0] {
            assert_eq!(datetime_str.len(), 19);
            assert!(datetime_str.chars().nth(4).unwrap() == '-');
            assert!(datetime_str.chars().nth(7).unwrap() == '-');
            assert!(datetime_str.chars().nth(10).unwrap() == ' ');
            assert!(datetime_str.chars().nth(13).unwrap() == ':');
            assert!(datetime_str.chars().nth(16).unwrap() == ':');
        } else {
            panic!("Expected text value for CURRENT_TIMESTAMP");
        }

        // Test 3: CURRENT_DATE in table query
        let stmt = parse_statement("SELECT id, name, CURRENT_DATE FROM users");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0], "id");
        assert_eq!(result.columns[1], "name");
        assert_eq!(result.columns[2], "column_1"); // Generated column name
        assert_eq!(result.rows.len(), 2); // Two users in test db
        // All rows should have the same current date
        assert!(matches!(result.rows[0][2], Value::Date(_)));
        assert!(matches!(result.rows[1][2], Value::Date(_)));
        assert_eq!(result.rows[0][2], result.rows[1][2]);

        // Test 4: CURRENT_TIMESTAMP with table data
        let stmt = parse_statement("SELECT name, CURRENT_TIMESTAMP FROM users WHERE id = 1");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));
        assert!(matches!(result.rows[0][1], Value::Text(_)));

        // Test 5: With aliases
        let stmt = parse_statement("SELECT CURRENT_DATE AS today, CURRENT_TIMESTAMP AS now");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0], "today");
        assert_eq!(result.columns[1], "now");
        assert!(matches!(result.rows[0][0], Value::Date(_)));
        assert!(matches!(result.rows[0][1], Value::Text(_)));
    }

    #[tokio::test]
    async fn test_date_format_function() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;
        
        // Test 1: Basic date formatting
        let stmt = parse_statement("SELECT DATE_FORMAT(DATE '2025-07-15', '%Y-%m-%d')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("2025-07-15".to_string()));

        // Test 2: Month name
        let stmt = parse_statement("SELECT DATE_FORMAT(DATE '2025-07-15', '%M %Y')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("July 2025".to_string()));

        // Test 3: Day and abbreviated month
        let stmt = parse_statement("SELECT DATE_FORMAT(DATE '2025-07-15', '%d %b %Y')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("15 Jul 2025".to_string()));

        // Test 4: Weekday name
        let stmt = parse_statement("SELECT DATE_FORMAT(DATE '2025-07-15', '%W, %d %M %Y')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        // July 15, 2025 is a Tuesday
        assert_eq!(result.rows[0][0], Value::Text("Tuesday, 15 July 2025".to_string()));

        // Test 5: With CURRENT_DATE
        let stmt = parse_statement("SELECT DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        // Should return today's date in YYYY-MM-DD format
        assert!(matches!(result.rows[0][0], Value::Text(_)));
    }

    #[tokio::test]
    async fn test_date_functions() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // Test 1: ADD_MONTHS
        let stmt = parse_statement("SELECT ADD_MONTHS(CURRENT_DATE, 3)");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        // Result should be a date string 3 months from now
        if let Value::Text(date_str) = &result.rows[0][0] {
            assert_eq!(date_str.len(), 10); // YYYY-MM-DD format
        } else {
            panic!("Expected text value for ADD_MONTHS");
        }

        // Test 2: EXTRACT from literal date
        let stmt = parse_statement("SELECT EXTRACT(MONTH FROM DATE '2025-07-15')");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(7));

        // Test 3: LAST_DAY
        let stmt = parse_statement("SELECT LAST_DAY(DATE '2025-02-15')");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("2025-02-28".to_string()));

        // Test 4: Complex date expression
        let _stmt = parse_statement(
            "SELECT ADD_MONTHS(CURRENT_DATE, 0) - EXTRACT(DAY FROM CURRENT_DATE) + 1",
        );
        // This should give us the first day of the current month
        // Note: This complex arithmetic isn't fully implemented, but the individual functions work
    }

    #[tokio::test]
    async fn test_aggregate_functions_enhanced() {
        let mut db = Database::new("test_db".to_string());

        // Create a table with numeric data
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
                name: "department".to_string(),
                sql_type: crate::yaml::schema::SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "salary".to_string(),
                sql_type: crate::yaml::schema::SqlType::Decimal(10, 2),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "bonus".to_string(),
                sql_type: crate::yaml::schema::SqlType::Integer,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ];

        let mut employees = Table::new("employees".to_string(), columns);
        employees
            .insert_row(vec![
                Value::Integer(1),
                Value::Text("Engineering".to_string()),
                Value::Decimal(Decimal::from_str("75000.00").unwrap()),
                Value::Integer(5000),
            ])
            .unwrap();
        employees
            .insert_row(vec![
                Value::Integer(2),
                Value::Text("Engineering".to_string()),
                Value::Decimal(Decimal::from_str("85000.00").unwrap()),
                Value::Integer(7000),
            ])
            .unwrap();
        employees
            .insert_row(vec![
                Value::Integer(3),
                Value::Text("Sales".to_string()),
                Value::Decimal(Decimal::from_str("65000.00").unwrap()),
                Value::Integer(10000),
            ])
            .unwrap();
        employees
            .insert_row(vec![
                Value::Integer(4),
                Value::Text("Sales".to_string()),
                Value::Decimal(Decimal::from_str("70000.00").unwrap()),
                Value::Null,
            ])
            .unwrap();
        db.add_table(employees).unwrap();

        let db_arc = Arc::new(RwLock::new(db));
        let executor = create_test_executor_from_arc(db_arc).await;

        // Test 1: COUNT DISTINCT
        let stmt = parse_statement("SELECT COUNT(DISTINCT department) FROM employees");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(2)); // 2 unique departments

        // Test 2: AVG with NULL handling
        let stmt = parse_statement("SELECT AVG(bonus) FROM employees");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        if let Value::Double(avg) = &result.rows[0][0] {
            assert_eq!(*avg, 7333.333333333333); // (5000 + 7000 + 10000) / 3
        } else {
            panic!("Expected Double value for AVG");
        }

        // Test 3: MIN
        let stmt = parse_statement("SELECT MIN(salary) FROM employees");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0][0],
            Value::Decimal(Decimal::from_str("65000.00").unwrap())
        );

        // Test 4: MAX
        let stmt = parse_statement("SELECT MAX(salary) FROM employees");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0][0],
            Value::Decimal(Decimal::from_str("85000.00").unwrap())
        );

        // Test 5: Multiple aggregates in one query
        let stmt =
            parse_statement("SELECT COUNT(*), AVG(salary), MIN(bonus), MAX(bonus) FROM employees");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(4)); // COUNT(*)
        // AVG(salary) should be (75000 + 85000 + 65000 + 70000) / 4 = 73750
        if let Value::Double(avg) = &result.rows[0][1] {
            assert_eq!(*avg, 73750.0);
        }
        assert_eq!(result.rows[0][2], Value::Integer(5000)); // MIN(bonus)
        assert_eq!(result.rows[0][3], Value::Integer(10000)); // MAX(bonus)
    }

    #[tokio::test]
    async fn test_cte_basic() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // Test basic CTE parsing (execution not fully implemented)
        let stmt = parse_statement(
            "WITH user_cte AS (
                SELECT id, name FROM users WHERE id = 1
            )
            SELECT * FROM users",
        );
        let result = executor.execute(&stmt).await;

        // CTE execution is not fully implemented
        assert!(result.is_err());
        if let Err(YamlBaseError::NotImplemented(msg)) = result {
            assert!(msg.contains("CTE execution"));
        }

        // Test CTE reference (will also fail with CTE execution not implemented)
        let stmt = parse_statement(
            "WITH user_cte AS (
                SELECT id, name FROM users WHERE id = 1
            )
            SELECT * FROM user_cte",
        );
        let result = executor.execute(&stmt).await;

        assert!(result.is_err());
        if let Err(YamlBaseError::NotImplemented(msg)) = result {
            assert!(msg.contains("CTE execution"));
        }
    }

    #[tokio::test]
    async fn test_complex_joins_with_functions() {
        let mut db = Database::new("test_db".to_string());

        // Create tables
        let user_columns = vec![
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
            Column {
                name: "created_date".to_string(),
                sql_type: crate::yaml::schema::SqlType::Date,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ];

        let mut users = Table::new("users".to_string(), user_columns);
        users
            .insert_row(vec![
                Value::Integer(1),
                Value::Text("Alice".to_string()),
                Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()),
            ])
            .unwrap();
        users
            .insert_row(vec![
                Value::Integer(2),
                Value::Text("Bob".to_string()),
                Value::Date(NaiveDate::from_ymd_opt(2024, 2, 20).unwrap()),
            ])
            .unwrap();
        db.add_table(users).unwrap();

        let activity_columns = vec![
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
                name: "user_id".to_string(),
                sql_type: crate::yaml::schema::SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "activity_date".to_string(),
                sql_type: crate::yaml::schema::SqlType::Date,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ];

        let mut activities = Table::new("activities".to_string(), activity_columns);
        activities
            .insert_row(vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Date(NaiveDate::from_ymd_opt(2024, 3, 1).unwrap()),
            ])
            .unwrap();
        activities
            .insert_row(vec![
                Value::Integer(2),
                Value::Integer(1),
                Value::Date(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()),
            ])
            .unwrap();
        db.add_table(activities).unwrap();

        let db_arc = Arc::new(RwLock::new(db));
        let executor = create_test_executor_from_arc(db_arc).await;

        // Test JOIN with date functions in WHERE
        let stmt = parse_statement(
            "SELECT u.name, a.activity_date 
             FROM users u 
             INNER JOIN activities a ON u.id = a.user_id 
             WHERE EXTRACT(MONTH FROM a.activity_date) = 3",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 2); // Both activities are in March
    }

    #[tokio::test]
    async fn test_upper_function() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                Column {
                    name: "name".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Varchar(100),
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
            ];
            let mut table = Table::new("test_table".to_string(), columns);
            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Text("hello world".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("ALREADY UPPER".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(3), Value::Null])
                .unwrap();
            db_write.add_table(table).unwrap();
        }
        let executor = create_test_executor_from_arc(db).await;

        // Test UPPER in SELECT
        let stmt = parse_statement("SELECT id, UPPER(name) FROM test_table ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0][1], Value::Text("HELLO WORLD".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("ALREADY UPPER".to_string()));
        assert_eq!(result.rows[2][1], Value::Null);

        // Test UPPER in WHERE
        let stmt = parse_statement("SELECT id FROM test_table WHERE UPPER(name) = 'HELLO WORLD'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(1));
    }

    #[tokio::test]
    async fn test_lower_function() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                Column {
                    name: "name".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Varchar(100),
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
            ];
            let mut table = Table::new("test_table".to_string(), columns);
            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Text("HELLO WORLD".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("already lower".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(3), Value::Null])
                .unwrap();
            db_write.add_table(table).unwrap();
        }
        let executor = create_test_executor_from_arc(db).await;

        // Test LOWER in SELECT
        let stmt = parse_statement("SELECT id, LOWER(name) FROM test_table ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0][1], Value::Text("hello world".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("already lower".to_string()));
        assert_eq!(result.rows[2][1], Value::Null);

        // Test LOWER in WHERE
        let stmt = parse_statement("SELECT id FROM test_table WHERE LOWER(name) = 'hello world'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(1));
    }

    #[tokio::test]
    async fn test_string_functions_basic() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // First, let's test simple SELECT to ensure basics work
        let stmt = parse_statement("SELECT 1");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(1));

        // Test with string literal
        let stmt = parse_statement("SELECT 'hello'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("hello".to_string()));

        // Test LENGTH
        let stmt = parse_statement("SELECT LENGTH('hello')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(5));

        // Test SUBSTRING with 2 args
        let stmt = parse_statement("SELECT SUBSTRING('Hello World', 7)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("World".to_string()));

        // Test SUBSTRING with 3 args
        let stmt = parse_statement("SELECT SUBSTRING('Hello World', 7, 5)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("World".to_string()));

        // Test CONCAT
        let stmt = parse_statement("SELECT CONCAT('Hello', ' ', 'World')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hello World".to_string()));

        // Test REPLACE
        let stmt = parse_statement("SELECT REPLACE('Hello World', 'World', 'Universe')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hello Universe".to_string()));
    }

    #[tokio::test]
    async fn test_string_functions_with_nulls() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // Test LENGTH with NULL
        let stmt = parse_statement("SELECT LENGTH(NULL)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);

        // Test SUBSTRING with NULL string
        let stmt = parse_statement("SELECT SUBSTRING(NULL, 1)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);

        // Test CONCAT with NULL
        let stmt = parse_statement("SELECT CONCAT('Hello', NULL)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);

        // Test REPLACE with NULL
        let stmt = parse_statement("SELECT REPLACE(NULL, 'a', 'b')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);
    }

    #[tokio::test]
    async fn test_string_functions_with_table() {
        let mut db = Database::new("test_db".to_string());

        // Create a table with string data
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
            Column {
                name: "description".to_string(),
                sql_type: crate::yaml::schema::SqlType::Text,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ];

        let mut table = Table::new("products".to_string(), columns);
        table
            .insert_row(vec![
                Value::Integer(1),
                Value::Text("Laptop".to_string()),
                Value::Text("High-performance laptop for professionals".to_string()),
            ])
            .unwrap();
        table
            .insert_row(vec![
                Value::Integer(2),
                Value::Text("Mouse".to_string()),
                Value::Null,
            ])
            .unwrap();

        db.add_table(table).unwrap();
        let db_arc = Arc::new(RwLock::new(db));
        let executor = create_test_executor_from_arc(db_arc).await;

        // Test LENGTH on column
        let stmt = parse_statement("SELECT LENGTH(name) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(6)); // "Laptop"
        assert_eq!(result.rows[1][0], Value::Integer(5)); // "Mouse"

        // Test SUBSTRING on column
        let stmt = parse_statement("SELECT SUBSTRING(description, 1, 10) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("High-perfo".to_string()));
        assert_eq!(result.rows[1][0], Value::Null);

        // Test CONCAT with columns
        let stmt = parse_statement("SELECT CONCAT('Product: ', name) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(
            result.rows[0][0],
            Value::Text("Product: Laptop".to_string())
        );
        assert_eq!(result.rows[1][0], Value::Text("Product: Mouse".to_string()));

        // Test REPLACE on column
        let stmt = parse_statement(
            "SELECT REPLACE(description, 'laptop', 'notebook') FROM products WHERE id = 1",
        );
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(
            result.rows[0][0],
            Value::Text("High-performance notebook for professionals".to_string())
        );
    }

    #[tokio::test]
    async fn test_string_functions_edge_cases() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // LENGTH edge cases
        let stmt = parse_statement("SELECT LENGTH('')"); // Empty string
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(0));

        // SUBSTRING edge cases
        // Start position beyond string length
        let stmt = parse_statement("SELECT SUBSTRING('Hello', 10)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("".to_string()));

        // Start position 0 (should be treated as 1)
        let stmt = parse_statement("SELECT SUBSTRING('Hello', 0)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hello".to_string()));

        // Negative start position
        let stmt = parse_statement("SELECT SUBSTRING('Hello', -2)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hello".to_string()));

        // Length longer than remaining string
        let stmt = parse_statement("SELECT SUBSTRING('Hello', 3, 10)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("llo".to_string()));

        // Zero length
        let stmt = parse_statement("SELECT SUBSTRING('Hello', 1, 0)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("".to_string()));

        // Negative length (treated as 0)
        let stmt = parse_statement("SELECT SUBSTRING('Hello', 1, -5)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("".to_string()));

        // CONCAT edge cases
        // Single argument
        let stmt = parse_statement("SELECT CONCAT('Hello')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hello".to_string()));

        // Mixed types
        let stmt = parse_statement(
            "SELECT CONCAT('Value: ', 123, ' Price: ', 45.67, ' Available: ', true)",
        );
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(
            result.rows[0][0],
            Value::Text("Value: 123 Price: 45.67 Available: true".to_string())
        );

        // Empty strings
        let stmt = parse_statement("SELECT CONCAT('', 'Hello', '', 'World', '')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("HelloWorld".to_string()));

        // REPLACE edge cases
        // Empty search string (should return original)
        let stmt = parse_statement("SELECT REPLACE('Hello World', '', 'X')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hello World".to_string()));

        // Empty replacement string (removes occurrences)
        let stmt = parse_statement("SELECT REPLACE('Hello World', 'o', '')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hell Wrld".to_string()));

        // No matches
        let stmt = parse_statement("SELECT REPLACE('Hello World', 'xyz', 'abc')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hello World".to_string()));

        // Multiple occurrences
        let stmt = parse_statement("SELECT REPLACE('abcabcabc', 'abc', 'X')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("XXX".to_string()));

        // Overlapping patterns
        let stmt = parse_statement("SELECT REPLACE('aaaa', 'aa', 'b')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("bb".to_string()));
    }

    #[tokio::test]
    async fn test_string_functions_unicode() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // Test with Unicode characters
        let stmt = parse_statement("SELECT LENGTH('Hello 世界')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(8)); // 6 ASCII + 2 Unicode

        let stmt = parse_statement("SELECT SUBSTRING('Hello 世界', 7)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("世界".to_string()));

        let stmt = parse_statement("SELECT SUBSTRING('Hello 世界', 7, 1)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("世".to_string()));

        let stmt = parse_statement("SELECT CONCAT('Hello ', '世界')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hello 世界".to_string()));

        let stmt = parse_statement("SELECT REPLACE('Hello 世界', '世界', 'World')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hello World".to_string()));

        // Test with emojis
        let stmt = parse_statement("SELECT LENGTH('Hello 👋 World 🌍')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(15)); // Counts characters, not bytes

        let stmt = parse_statement("SELECT SUBSTRING('👋🌍🎉', 2, 1)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("🌍".to_string()));
    }

    #[tokio::test]
    async fn test_string_functions_nested() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // Nested function calls
        let stmt = parse_statement("SELECT LENGTH(CONCAT('Hello', ' ', 'World'))");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(11));

        let stmt =
            parse_statement("SELECT SUBSTRING(REPLACE('Hello World', 'World', 'Universe'), 7)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Universe".to_string()));

        let stmt = parse_statement("SELECT REPLACE(SUBSTRING('Hello World', 1, 5), 'l', 'L')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("HeLLo".to_string()));

        let stmt = parse_statement(
            "SELECT CONCAT('Length: ', LENGTH('test'), ', Upper: ', UPPER('test'))",
        );
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(
            result.rows[0][0],
            Value::Text("Length: 4, Upper: TEST".to_string())
        );
    }

    #[tokio::test]
    async fn test_string_functions_with_expressions() {
        let mut db = Database::new("test_db".to_string());

        // Create a table with numeric data
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
                name: "text1".to_string(),
                sql_type: crate::yaml::schema::SqlType::Varchar(100),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "text2".to_string(),
                sql_type: crate::yaml::schema::SqlType::Varchar(100),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "num".to_string(),
                sql_type: crate::yaml::schema::SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ];

        let mut table = Table::new("test_data".to_string(), columns);
        table
            .insert_row(vec![
                Value::Integer(1),
                Value::Text("Hello".to_string()),
                Value::Text("World".to_string()),
                Value::Integer(3),
            ])
            .unwrap();

        db.add_table(table).unwrap();
        let db_arc = Arc::new(RwLock::new(db));
        let executor = create_test_executor_from_arc(db_arc).await;

        // Test string functions with column expressions
        let stmt = parse_statement("SELECT CONCAT(text1, ' ', text2) FROM test_data");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Hello World".to_string()));

        let stmt = parse_statement("SELECT SUBSTRING(text1, num) FROM test_data");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("llo".to_string()));

        let stmt = parse_statement("SELECT LENGTH(CONCAT(text1, text2)) FROM test_data");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(10));

        // Test in WHERE clause
        let stmt = parse_statement("SELECT id FROM test_data WHERE LENGTH(text1) = 5");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(1));
    }

    #[tokio::test]
    async fn test_trim_function() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                Column {
                    name: "name".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Varchar(100),
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
            ];
            let mut table = Table::new("test_table".to_string(), columns);
            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Text("  spaces around  ".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("no spaces".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(3),
                    Value::Text("\t\ttabs\t\t".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![Value::Integer(4), Value::Null])
                .unwrap();
            db_write.add_table(table).unwrap();
        }
        let executor = create_test_executor_from_arc(db).await;

        // Test TRIM in SELECT
        let stmt = parse_statement("SELECT id, TRIM(name) FROM test_table ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 4);
        assert_eq!(result.rows[0][1], Value::Text("spaces around".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("no spaces".to_string()));
        assert_eq!(result.rows[2][1], Value::Text("tabs".to_string()));
        assert_eq!(result.rows[3][1], Value::Null);

        // Test TRIM in WHERE
        let stmt = parse_statement("SELECT id FROM test_table WHERE TRIM(name) = 'spaces around'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(1));
    }

    #[tokio::test]
    async fn test_group_by_count() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                create_column(
                    "department",
                    crate::yaml::schema::SqlType::Varchar(50),
                    false,
                ),
                create_column("salary", crate::yaml::schema::SqlType::Integer, false),
            ];
            let mut table = Table::new("employees".to_string(), columns);

            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Text("Engineering".to_string()),
                    Value::Integer(80000),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("Engineering".to_string()),
                    Value::Integer(85000),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(3),
                    Value::Text("Sales".to_string()),
                    Value::Integer(60000),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(4),
                    Value::Text("Sales".to_string()),
                    Value::Integer(65000),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(5),
                    Value::Text("Engineering".to_string()),
                    Value::Integer(90000),
                ])
                .unwrap();

            db_write.add_table(table).unwrap();
        }
        let executor = create_test_executor_from_arc(db).await;

        // Test GROUP BY with COUNT
        let stmt =
            parse_statement("SELECT department, COUNT(*) FROM employees GROUP BY department");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns, vec!["department", "COUNT(*)"]);
        assert_eq!(result.rows.len(), 2);

        // Check results (order may vary)
        let mut found_engineering = false;
        let mut found_sales = false;

        for row in &result.rows {
            if let (Value::Text(dept), Value::Integer(count)) = (&row[0], &row[1]) {
                if dept == "Engineering" {
                    assert_eq!(count, &3);
                    found_engineering = true;
                } else if dept == "Sales" {
                    assert_eq!(count, &2);
                    found_sales = true;
                }
            }
        }

        assert!(found_engineering);
        assert!(found_sales);
    }

    #[tokio::test]
    async fn test_group_by_sum_avg() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                create_column(
                    "department",
                    crate::yaml::schema::SqlType::Varchar(50),
                    false,
                ),
                create_column("salary", crate::yaml::schema::SqlType::Integer, false),
            ];
            let mut table = Table::new("employees".to_string(), columns);

            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Text("Engineering".to_string()),
                    Value::Integer(80000),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("Engineering".to_string()),
                    Value::Integer(90000),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(3),
                    Value::Text("Sales".to_string()),
                    Value::Integer(60000),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(4),
                    Value::Text("Sales".to_string()),
                    Value::Integer(70000),
                ])
                .unwrap();

            db_write.add_table(table).unwrap();
        }
        let executor = create_test_executor_from_arc(db).await;

        // Test GROUP BY with SUM
        let stmt =
            parse_statement("SELECT department, SUM(salary) FROM employees GROUP BY department");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns, vec!["department", "SUM(salary)"]);
        assert_eq!(result.rows.len(), 2);

        for row in &result.rows {
            if let (Value::Text(dept), Value::Double(sum)) = (&row[0], &row[1]) {
                if dept == "Engineering" {
                    assert_eq!(sum, &170000.0);
                } else if dept == "Sales" {
                    assert_eq!(sum, &130000.0);
                }
            }
        }

        // Test GROUP BY with AVG
        let stmt =
            parse_statement("SELECT department, AVG(salary) FROM employees GROUP BY department");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns, vec!["department", "AVG(salary)"]);
        assert_eq!(result.rows.len(), 2);

        for row in &result.rows {
            if let (Value::Text(dept), Value::Double(avg)) = (&row[0], &row[1]) {
                if dept == "Engineering" {
                    assert_eq!(avg, &85000.0);
                } else if dept == "Sales" {
                    assert_eq!(avg, &65000.0);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_group_by_having() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                create_column(
                    "department",
                    crate::yaml::schema::SqlType::Varchar(50),
                    false,
                ),
                create_column("salary", crate::yaml::schema::SqlType::Integer, false),
            ];
            let mut table = Table::new("employees".to_string(), columns);

            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Text("Engineering".to_string()),
                    Value::Integer(80000),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("Engineering".to_string()),
                    Value::Integer(85000),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(3),
                    Value::Text("Sales".to_string()),
                    Value::Integer(60000),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(4),
                    Value::Text("HR".to_string()),
                    Value::Integer(55000),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(5),
                    Value::Text("Engineering".to_string()),
                    Value::Integer(90000),
                ])
                .unwrap();

            db_write.add_table(table).unwrap();
        }
        let executor = create_test_executor_from_arc(db).await;

        // Test GROUP BY with HAVING COUNT(*) > 1
        let stmt = parse_statement(
            "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 1",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns, vec!["department", "COUNT(*)"]);
        assert_eq!(result.rows.len(), 1); // Only Engineering has more than 1 employee

        assert_eq!(result.rows[0][0], Value::Text("Engineering".to_string()));
        assert_eq!(result.rows[0][1], Value::Integer(3));

        // Test GROUP BY with HAVING on AVG
        let stmt = parse_statement(
            "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING AVG(salary) > 70000",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns, vec!["department", "AVG(salary)"]);
        assert_eq!(result.rows.len(), 1); // Only Engineering has avg > 70000

        assert_eq!(result.rows[0][0], Value::Text("Engineering".to_string()));
        assert_eq!(result.rows[0][1], Value::Double(85000.0));
    }

    #[tokio::test]
    async fn test_case_when_expressions() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                create_column("age", crate::yaml::schema::SqlType::Integer, false),
                create_column("name", crate::yaml::schema::SqlType::Varchar(50), false),
            ];
            let mut table = Table::new("case_test_users".to_string(), columns);

            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Integer(25),
                    Value::Text("Alice".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Integer(15),
                    Value::Text("Bob".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(3),
                    Value::Integer(10),
                    Value::Text("Charlie".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(4),
                    Value::Integer(65),
                    Value::Text("David".to_string()),
                ])
                .unwrap();

            db_write.add_table(table).unwrap();
        }
        let executor = create_test_executor_from_arc(db).await;

        // Test searched CASE (CASE WHEN)
        let stmt = parse_statement(
            "SELECT name, CASE WHEN age >= 65 THEN 'senior' WHEN age >= 18 THEN 'adult' WHEN age >= 13 THEN 'teen' ELSE 'child' END as category FROM case_test_users",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns, vec!["name", "category"]);
        assert_eq!(result.rows.len(), 4);

        assert_eq!(result.rows[0][1], Value::Text("adult".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("teen".to_string()));
        assert_eq!(result.rows[2][1], Value::Text("child".to_string()));
        assert_eq!(result.rows[3][1], Value::Text("senior".to_string()));

        // Test simple CASE
        let stmt = parse_statement(
            "SELECT name, CASE age WHEN 25 THEN 'twenty-five' WHEN 15 THEN 'fifteen' ELSE 'other' END FROM case_test_users",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows[0][1], Value::Text("twenty-five".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("fifteen".to_string()));
        assert_eq!(result.rows[2][1], Value::Text("other".to_string()));
        assert_eq!(result.rows[3][1], Value::Text("other".to_string()));

        // Test CASE without ELSE (returns NULL)
        let stmt = parse_statement(
            "SELECT name, CASE WHEN age = 100 THEN 'centenarian' END FROM case_test_users",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows[0][1], Value::Null);
        assert_eq!(result.rows[1][1], Value::Null);
        assert_eq!(result.rows[2][1], Value::Null);
        assert_eq!(result.rows[3][1], Value::Null);
    }

    #[tokio::test]
    async fn test_case_when_without_from() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // Test searched CASE without FROM
        let stmt = parse_statement("SELECT CASE WHEN 1 > 0 THEN 'positive' ELSE 'negative' END");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("positive".to_string()));

        // Test simple CASE without FROM
        let stmt =
            parse_statement("SELECT CASE 5 WHEN 1 THEN 'one' WHEN 5 THEN 'five' ELSE 'other' END");
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows[0][0], Value::Text("five".to_string()));

        // Test nested CASE
        let stmt = parse_statement(
            "SELECT CASE WHEN 10 > 5 THEN CASE WHEN 20 > 10 THEN 'both true' ELSE 'first true' END ELSE 'false' END",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows[0][0], Value::Text("both true".to_string()));
    }

    #[tokio::test]
    async fn test_coalesce_and_nullif_functions() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                Column {
                    name: "name".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Varchar(50),
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
                Column {
                    name: "nickname".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Varchar(50),
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
                Column {
                    name: "status".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Varchar(20),
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
            ];
            let mut table = Table::new("users_with_nulls".to_string(), columns);

            // Insert test data with various NULL values
            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Text("Alice".to_string()),
                    Value::Null,
                    Value::Text("active".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("Bob".to_string()),
                    Value::Text("Bobby".to_string()),
                    Value::Null,
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(3),
                    Value::Null,
                    Value::Text("Chuck".to_string()),
                    Value::Text("inactive".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(4),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ])
                .unwrap();

            db_write.add_table(table).unwrap();
        }
        let executor = create_test_executor_from_arc(db).await;

        // Test COALESCE with table rows
        let stmt = parse_statement(
            "SELECT id, COALESCE(name, nickname, 'Unknown') as display_name FROM users_with_nulls",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns, vec!["id", "display_name"]);
        assert_eq!(result.rows.len(), 4);

        assert_eq!(result.rows[0][1], Value::Text("Alice".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("Bob".to_string()));
        assert_eq!(result.rows[2][1], Value::Text("Chuck".to_string()));
        assert_eq!(result.rows[3][1], Value::Text("Unknown".to_string()));

        // Test COALESCE without FROM
        let stmt = parse_statement("SELECT COALESCE(NULL, NULL, 'default', 'other')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Text("default".to_string()));

        // Test NULLIF with table rows
        let stmt = parse_statement(
            "SELECT id, NULLIF(status, 'inactive') as active_status FROM users_with_nulls",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.columns, vec!["id", "active_status"]);
        assert_eq!(result.rows.len(), 4);

        assert_eq!(result.rows[0][1], Value::Text("active".to_string()));
        assert_eq!(result.rows[1][1], Value::Null);
        assert_eq!(result.rows[2][1], Value::Null); // "inactive" becomes NULL
        assert_eq!(result.rows[3][1], Value::Null);

        // Test NULLIF without FROM
        let stmt = parse_statement("SELECT NULLIF(5, 5)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);

        let stmt = parse_statement("SELECT NULLIF(5, 3)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(5));

        // Test COALESCE with NULLIF
        let stmt = parse_statement(
            "SELECT id, COALESCE(NULLIF(nickname, ''), name, 'Guest') as display FROM users_with_nulls",
        );
        let result = executor.execute(&stmt).await.unwrap();

        assert_eq!(result.rows[0][1], Value::Text("Alice".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("Bobby".to_string()));
        assert_eq!(result.rows[2][1], Value::Text("Chuck".to_string()));
        assert_eq!(result.rows[3][1], Value::Text("Guest".to_string()));
    }

    #[tokio::test]
    async fn test_nested_string_functions() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                Column {
                    name: "name".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Varchar(100),
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
            ];
            let mut table = Table::new("test_table".to_string(), columns);
            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Text("  hello world  ".to_string()),
                ])
                .unwrap();
            db_write.add_table(table).unwrap();
        }
        let executor = create_test_executor_from_arc(db).await;

        // Test nested functions: UPPER(TRIM(name))
        let stmt = parse_statement("SELECT UPPER(TRIM(name)) FROM test_table");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("HELLO WORLD".to_string()));

        // Test nested functions: LOWER(UPPER(name))
        let stmt = parse_statement("SELECT LOWER(UPPER(name)) FROM test_table");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0][0],
            Value::Text("  hello world  ".to_string())
        );
    }

    #[tokio::test]
    async fn test_functions_in_join_conditions() {
        let mut db = Database::new("test_db".to_string());

        // Create employees table
        let emp_columns = vec![
            create_column("emp_id", crate::yaml::schema::SqlType::Varchar(10), true),
            Column {
                name: "emp_name".to_string(),
                sql_type: crate::yaml::schema::SqlType::Varchar(100),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ];
        let mut employees = Table::new("employees".to_string(), emp_columns);
        employees
            .insert_row(vec![
                Value::Text("e001".to_string()),
                Value::Text("Alice".to_string()),
            ])
            .unwrap();
        employees
            .insert_row(vec![
                Value::Text("E002".to_string()), // Uppercase
                Value::Text("Bob".to_string()),
            ])
            .unwrap();
        db.add_table(employees).unwrap();

        // Create assignments table
        let assign_columns = vec![
            create_column("assign_id", crate::yaml::schema::SqlType::Integer, true),
            Column {
                name: "emp_id".to_string(),
                sql_type: crate::yaml::schema::SqlType::Varchar(10),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "project".to_string(),
                sql_type: crate::yaml::schema::SqlType::Varchar(100),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ];
        let mut assignments = Table::new("assignments".to_string(), assign_columns);
        assignments
            .insert_row(vec![
                Value::Integer(1),
                Value::Text("E001".to_string()), // Uppercase
                Value::Text("Project A".to_string()),
            ])
            .unwrap();
        assignments
            .insert_row(vec![
                Value::Integer(2),
                Value::Text("e002".to_string()), // Lowercase
                Value::Text("Project B".to_string()),
            ])
            .unwrap();
        db.add_table(assignments).unwrap();

        let db_arc = Arc::new(RwLock::new(db));
        let executor = create_test_executor_from_arc(db_arc).await;

        // Test JOIN with UPPER function - should match case-insensitively
        let stmt = parse_statement(
            "SELECT e.emp_name, a.project 
             FROM employees e 
             JOIN assignments a ON UPPER(e.emp_id) = UPPER(a.emp_id)",
        );
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));
        assert_eq!(result.rows[0][1], Value::Text("Project A".to_string()));
        assert_eq!(result.rows[1][0], Value::Text("Bob".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("Project B".to_string()));

        // Test LEFT JOIN with TRIM function
        let stmt = parse_statement(
            "SELECT e.emp_name, a.project 
             FROM employees e 
             LEFT JOIN assignments a ON TRIM(UPPER(e.emp_id)) = TRIM(UPPER(a.emp_id))
             ORDER BY e.emp_name",
        );
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_char_type_support() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;

            // Create table with CHAR columns
            let columns = vec![
                create_column("id", crate::yaml::schema::SqlType::Integer, true),
                Column {
                    name: "flag".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Char(1),
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
                Column {
                    name: "code".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Char(3),
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
                Column {
                    name: "fixed_id".to_string(),
                    sql_type: crate::yaml::schema::SqlType::Char(10),
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
            ];
            let mut table = Table::new("test_char".to_string(), columns);
            table
                .insert_row(vec![
                    Value::Integer(1),
                    Value::Text("Y".to_string()),
                    Value::Text("ABC".to_string()),
                    Value::Text("ID12345678".to_string()),
                ])
                .unwrap();
            table
                .insert_row(vec![
                    Value::Integer(2),
                    Value::Text("N".to_string()),
                    Value::Text("XY".to_string()), // Less than 3 chars
                    Value::Text("SHORT".to_string()), // Less than 10 chars
                ])
                .unwrap();
            db_write.add_table(table).unwrap();
        }

        let executor = create_test_executor_from_arc(db).await;

        // Test selecting CHAR columns
        let stmt = parse_statement("SELECT * FROM test_char ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2);
        // Debug: Print the rows to see what we have
        eprintln!("Rows in test_char table:");
        for (i, row) in result.rows.iter().enumerate() {
            eprintln!("Row {}: {:?}", i, row);
        }

        // Test WHERE clause with CHAR column
        let stmt = parse_statement("SELECT id FROM test_char WHERE flag = 'Y'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(1));

        // Test basic SELECT first - without WHERE
        let stmt = parse_statement("SELECT id, code FROM test_char");
        let result = executor.execute(&stmt).await.unwrap();
        eprintln!("SELECT id, code results:");
        for row in &result.rows {
            eprintln!("  {:?}", row);
        }

        // Now test with WHERE clause
        let stmt = parse_statement("SELECT code FROM test_char WHERE id = 2");
        let result = executor.execute(&stmt).await.unwrap();
        eprintln!(
            "SELECT code WHERE id = 2 returned {} rows",
            result.rows.len()
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("XY".to_string()));

        // Test functions with CHAR columns
        let stmt = parse_statement("SELECT UPPER(code) FROM test_char WHERE id = 2");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("XY".to_string()));
    }

    #[tokio::test]
    async fn test_math_functions() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // Test ROUND - simpler case first
        let stmt = parse_statement("SELECT ROUND(3.24)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(3.0));

        let stmt = parse_statement("SELECT ROUND(3.456, 2)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(3.46));

        let stmt = parse_statement("SELECT ROUND(3.456789, 4)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(3.4568));

        let stmt = parse_statement("SELECT ROUND(3.789, 0)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(4.0));

        // Test FLOOR
        let stmt = parse_statement("SELECT FLOOR(3.7)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(3.0));

        let stmt = parse_statement("SELECT FLOOR(-3.7)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(-4.0));

        let stmt = parse_statement("SELECT FLOOR(5)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(5));

        // Test CEIL
        let stmt = parse_statement("SELECT CEIL(3.2)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(4.0));

        let stmt = parse_statement("SELECT CEIL(-3.2)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(-3.0));

        let stmt = parse_statement("SELECT CEIL(5)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(5));

        // Test ABS
        let stmt = parse_statement("SELECT ABS(-5)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(5));

        let stmt = parse_statement("SELECT ABS(5)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(5));

        let stmt = parse_statement("SELECT ABS(-3.5)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(3.5));

        // Test MOD
        let stmt = parse_statement("SELECT MOD(10, 3)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(1));

        let stmt = parse_statement("SELECT MOD(10, -3)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(1));

        let stmt = parse_statement("SELECT MOD(-10, 3)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(-1));

        let stmt = parse_statement("SELECT MOD(10.5, 3.0)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(1.5));
    }

    #[tokio::test]
    async fn test_math_functions_debug() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // Test if VERSION works (it should)
        let stmt = parse_statement("SELECT VERSION()");
        let result = executor.execute(&stmt).await.unwrap();
        assert!(matches!(result.rows[0][0], Value::Text(ref s) if s.contains("yamlbase")));

        // Test if LENGTH works (it should)
        let stmt = parse_statement("SELECT LENGTH('test')");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(4));

        // Now test ROUND
        let stmt = parse_statement("SELECT ROUND(3.14)");
        let result = executor.execute(&stmt).await;

        match result {
            Ok(res) => {
                assert_eq!(res.rows[0][0], Value::Double(3.0));
            }
            Err(e) => {
                panic!("ROUND failed with error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_math_functions_null_handling() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // First test a simple case
        let stmt = parse_statement("SELECT ROUND(3.14)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(3.0));

        // Test NULL handling for each function
        let stmt = parse_statement("SELECT ROUND(NULL)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);

        let stmt = parse_statement("SELECT ROUND(3.14, NULL)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);

        let stmt = parse_statement("SELECT FLOOR(NULL)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);

        let stmt = parse_statement("SELECT CEIL(NULL)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);

        let stmt = parse_statement("SELECT ABS(NULL)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);

        let stmt = parse_statement("SELECT MOD(NULL, 3)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);

        let stmt = parse_statement("SELECT MOD(10, NULL)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Null);
    }

    #[tokio::test]
    async fn test_math_functions_edge_cases() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;

        // ROUND edge cases
        let stmt = parse_statement("SELECT ROUND(2.5)"); // Banker's rounding
        let result = executor.execute(&stmt).await.unwrap();
        // Note: Rust uses "round half away from zero", so 2.5 -> 3.0
        assert_eq!(result.rows[0][0], Value::Double(3.0));

        let stmt = parse_statement("SELECT ROUND(-2.5)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(-3.0));

        // Negative precision (not common in SQL, but let's test the behavior)
        let stmt = parse_statement("SELECT ROUND(123.456, -1)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(120.0));

        let stmt = parse_statement("SELECT ROUND(155.456, -2)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(200.0));

        // Very small numbers
        let stmt = parse_statement("SELECT FLOOR(0.0000001)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(0.0));

        let stmt = parse_statement("SELECT CEIL(0.0000001)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows[0][0], Value::Double(1.0));

        // MOD with zero divisor (should error)
        let stmt = parse_statement("SELECT MOD(10, 0)");
        let result = executor.execute(&stmt).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Division by zero"));

        // MOD with floating point zero
        let stmt = parse_statement("SELECT MOD(10.0, 0.0)");
        let result = executor.execute(&stmt).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Division by zero"));

        // ABS with minimum integer (potential overflow in some systems, but Rust handles it)
        // Note: We can't directly use -9223372036854775808 because the positive part is too large
        // Instead, we'll use -9223372036854775807 - 1
        let stmt = parse_statement("SELECT ABS(-9223372036854775807 - 1)"); // i64::MIN
        let result = executor.execute(&stmt).await.unwrap();
        // In Rust, i64::MIN.abs() would panic in debug mode, but we handle it with wrapping_abs
        assert_eq!(result.rows[0][0], Value::Integer(i64::MIN.wrapping_abs()));
    }

    #[tokio::test]
    async fn test_distinct() {
        let mut db = Database::new("test_db".to_string());

        // Create test table
        let columns = vec![
            Column {
                name: "id".to_string(),
                sql_type: crate::yaml::schema::SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "name".to_string(),
                sql_type: crate::yaml::schema::SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "department".to_string(),
                sql_type: crate::yaml::schema::SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ];

        let mut table = Table::new("test_table".to_string(), columns);

        // Add test data with duplicates
        table
            .insert_row(vec![
                Value::Integer(1),
                Value::Text("Alice".to_string()),
                Value::Text("Sales".to_string()),
            ])
            .unwrap();
        table
            .insert_row(vec![
                Value::Integer(2),
                Value::Text("Bob".to_string()),
                Value::Text("Engineering".to_string()),
            ])
            .unwrap();
        table
            .insert_row(vec![
                Value::Integer(3),
                Value::Text("Charlie".to_string()),
                Value::Text("Sales".to_string()),
            ])
            .unwrap();
        table
            .insert_row(vec![
                Value::Integer(4),
                Value::Text("David".to_string()),
                Value::Text("Engineering".to_string()),
            ])
            .unwrap();
        table
            .insert_row(vec![
                Value::Integer(5),
                Value::Text("Eve".to_string()),
                Value::Text("Sales".to_string()),
            ])
            .unwrap();

        db.add_table(table).unwrap();
        let db = Arc::new(RwLock::new(db));
        let executor = create_test_executor_from_arc(db).await;

        // Test DISTINCT on single column
        let stmt = parse_statement("SELECT DISTINCT department FROM test_table");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2); // Only Sales and Engineering

        let departments: Vec<String> = result
            .rows
            .iter()
            .map(|row| match &row[0] {
                Value::Text(s) => s.clone(),
                _ => panic!("Expected text value"),
            })
            .collect();
        assert!(departments.contains(&"Sales".to_string()));
        assert!(departments.contains(&"Engineering".to_string()));

        // Test DISTINCT on all columns
        let stmt = parse_statement("SELECT DISTINCT * FROM test_table");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 5); // All rows are unique

        // Test DISTINCT with ORDER BY
        let stmt =
            parse_statement("SELECT DISTINCT department FROM test_table ORDER BY department");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Text("Engineering".to_string()));
        assert_eq!(result.rows[1][0], Value::Text("Sales".to_string()));

        // Test DISTINCT with WHERE
        let stmt =
            parse_statement("SELECT DISTINCT name FROM test_table WHERE department = 'Sales'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 3); // Alice, Charlie, Eve
    }

    #[tokio::test]
    async fn test_distinct_with_multiple_columns() {
        let mut db = Database::new("test_db".to_string());

        // Create orders table
        let columns = vec![
            Column {
                name: "id".to_string(),
                sql_type: crate::yaml::schema::SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "customer".to_string(),
                sql_type: crate::yaml::schema::SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "product".to_string(),
                sql_type: crate::yaml::schema::SqlType::Text,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "quantity".to_string(),
                sql_type: crate::yaml::schema::SqlType::Integer,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ];

        let mut table = Table::new("orders".to_string(), columns);

        // Add test data with duplicates
        table
            .insert_row(vec![
                Value::Integer(1),
                Value::Text("Alice".to_string()),
                Value::Text("Widget".to_string()),
                Value::Integer(5),
            ])
            .unwrap();
        table
            .insert_row(vec![
                Value::Integer(2),
                Value::Text("Alice".to_string()),
                Value::Text("Widget".to_string()),
                Value::Integer(5),
            ])
            .unwrap();
        table
            .insert_row(vec![
                Value::Integer(3),
                Value::Text("Alice".to_string()),
                Value::Text("Gadget".to_string()),
                Value::Integer(3),
            ])
            .unwrap();
        table
            .insert_row(vec![
                Value::Integer(4),
                Value::Text("Bob".to_string()),
                Value::Text("Widget".to_string()),
                Value::Integer(5),
            ])
            .unwrap();
        table
            .insert_row(vec![
                Value::Integer(5),
                Value::Text("Bob".to_string()),
                Value::Text("Widget".to_string()),
                Value::Integer(2),
            ])
            .unwrap();

        db.add_table(table).unwrap();
        let db = Arc::new(RwLock::new(db));
        let executor = create_test_executor_from_arc(db).await;

        // Test DISTINCT on multiple columns
        let stmt = parse_statement("SELECT DISTINCT customer, product FROM orders");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 3); // (Alice, Widget), (Alice, Gadget), (Bob, Widget)

        // Test DISTINCT with all columns having duplicates
        let stmt = parse_statement("SELECT DISTINCT customer, product, quantity FROM orders");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 4); // One duplicate row (Alice, Widget, 5)
    }

    #[tokio::test]
    async fn test_is_null_is_not_null_operators() {
        let mut db = Database::new("test_db".to_string());
        
        // Create test table with nullable columns
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
                sql_type: crate::yaml::schema::SqlType::Varchar(50),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "email".to_string(),
                sql_type: crate::yaml::schema::SqlType::Varchar(100),
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "age".to_string(),
                sql_type: crate::yaml::schema::SqlType::Integer,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "created_at".to_string(),
                sql_type: crate::yaml::schema::SqlType::Date,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ];

        let mut table = Table::new("users".to_string(), columns);
        
        // Add test data with various NULL values
        table.insert_row(vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Text("alice@example.com".to_string()),
            Value::Integer(25),
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
        ]).unwrap();
        
        table.insert_row(vec![
            Value::Integer(2),
            Value::Text("Bob".to_string()),
            Value::Null,  // NULL email
            Value::Integer(30),
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()),
        ]).unwrap();
        
        table.insert_row(vec![
            Value::Integer(3),
            Value::Null,  // NULL name
            Value::Text("charlie@example.com".to_string()),
            Value::Null,  // NULL age
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()),
        ]).unwrap();
        
        table.insert_row(vec![
            Value::Integer(4),
            Value::Text("David".to_string()),
            Value::Text("david@example.com".to_string()),
            Value::Integer(35),
            Value::Null,  // NULL created_at
        ]).unwrap();
        
        table.insert_row(vec![
            Value::Integer(5),
            Value::Null,  // NULL name
            Value::Null,  // NULL email
            Value::Null,  // NULL age
            Value::Null,  // NULL created_at
        ]).unwrap();

        db.add_table(table).unwrap();
        let db = Arc::new(RwLock::new(db));
        let executor = create_test_executor_from_arc(db).await;

        // Test IS NULL
        let stmt = parse_statement("SELECT * FROM users WHERE email IS NULL ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2); // IDs 2, 5
        assert_eq!(result.rows[0][0], Value::Integer(2));
        assert_eq!(result.rows[1][0], Value::Integer(5));

        // Test IS NOT NULL
        let stmt = parse_statement("SELECT * FROM users WHERE email IS NOT NULL ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 3); // IDs 1, 3, 4
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[1][0], Value::Integer(3));
        assert_eq!(result.rows[2][0], Value::Integer(4));

        // Test multiple NULL checks
        // Both ID 3 and ID 5 have NULL name and NULL age
        let stmt = parse_statement("SELECT * FROM users WHERE name IS NULL AND age IS NULL ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2); // IDs 3, 5
        assert_eq!(result.rows[0][0], Value::Integer(3));
        assert_eq!(result.rows[1][0], Value::Integer(5));

        // Test IS NULL with OR
        let stmt = parse_statement("SELECT * FROM users WHERE name IS NULL OR age IS NULL ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2); // IDs 3, 5
        assert_eq!(result.rows[0][0], Value::Integer(3));
        assert_eq!(result.rows[1][0], Value::Integer(5));

        // Test combining IS NULL with other conditions
        // Note: NULL > 25 evaluates to false, so Charlie (ID 3) with NULL age won't match
        let stmt = parse_statement("SELECT * FROM users WHERE email IS NOT NULL AND age > 25 ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1); // Only ID 4 (David, age 35)
        assert_eq!(result.rows[0][0], Value::Integer(4));
        
        // Test with OR to include NULL ages
        let stmt = parse_statement("SELECT * FROM users WHERE email IS NOT NULL AND (age > 29 OR age IS NULL) ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2); // IDs 3, 4
        
        // Test COUNT with IS NULL
        let stmt = parse_statement("SELECT COUNT(*) FROM users WHERE created_at IS NULL");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(2)); // IDs 4, 5

        // Test with SELECT specific columns
        let stmt = parse_statement("SELECT id, name FROM users WHERE name IS NOT NULL ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 3); // IDs 1, 2, 4
        assert_eq!(result.rows[0][1], Value::Text("Alice".to_string()));
        assert_eq!(result.rows[1][1], Value::Text("Bob".to_string()));
        assert_eq!(result.rows[2][1], Value::Text("David".to_string()));
    }

    #[tokio::test]
    async fn test_min_max_aggregate_functions() {
        let mut db = Database::new("test_db".to_string());
        
        // Create test table
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
                name: "product".to_string(),
                sql_type: crate::yaml::schema::SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "price".to_string(),
                sql_type: crate::yaml::schema::SqlType::Double,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "quantity".to_string(),
                sql_type: crate::yaml::schema::SqlType::Integer,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "category".to_string(),
                sql_type: crate::yaml::schema::SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "created_date".to_string(),
                sql_type: crate::yaml::schema::SqlType::Date,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
        ];

        let mut table = Table::new("products".to_string(), columns);
        
        // Add test data
        table.insert_row(vec![
            Value::Integer(1),
            Value::Text("Laptop".to_string()),
            Value::Double(999.99),
            Value::Integer(10),
            Value::Text("Electronics".to_string()),
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()),
        ]).unwrap();
        
        table.insert_row(vec![
            Value::Integer(2),
            Value::Text("Mouse".to_string()),
            Value::Double(29.99),
            Value::Integer(50),
            Value::Text("Electronics".to_string()),
            Value::Date(NaiveDate::from_ymd_opt(2024, 1, 20).unwrap()),
        ]).unwrap();
        
        table.insert_row(vec![
            Value::Integer(3),
            Value::Text("Desk".to_string()),
            Value::Double(299.99),
            Value::Integer(5),
            Value::Text("Furniture".to_string()),
            Value::Date(NaiveDate::from_ymd_opt(2024, 2, 1).unwrap()),
        ]).unwrap();
        
        table.insert_row(vec![
            Value::Integer(4),
            Value::Text("Chair".to_string()),
            Value::Double(149.99),
            Value::Null,  // NULL quantity
            Value::Text("Furniture".to_string()),
            Value::Date(NaiveDate::from_ymd_opt(2024, 2, 10).unwrap()),
        ]).unwrap();
        
        table.insert_row(vec![
            Value::Integer(5),
            Value::Text("Monitor".to_string()),
            Value::Null,  // NULL price
            Value::Integer(15),
            Value::Text("Electronics".to_string()),
            Value::Null,  // NULL created_date
        ]).unwrap();

        db.add_table(table).unwrap();
        let db = Arc::new(RwLock::new(db));
        let executor = create_test_executor_from_arc(db).await;

        // Test MIN on numeric column
        let stmt = parse_statement("SELECT MIN(price) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Double(29.99));

        // Test MAX on numeric column
        let stmt = parse_statement("SELECT MAX(price) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Double(999.99));

        // Test MIN on integer column with NULL
        let stmt = parse_statement("SELECT MIN(quantity) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(5));

        // Test MAX on integer column with NULL
        let stmt = parse_statement("SELECT MAX(quantity) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(50));

        // Test MIN on text column
        let stmt = parse_statement("SELECT MIN(product) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("Chair".to_string()));

        // Test MAX on text column
        let stmt = parse_statement("SELECT MAX(product) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("Mouse".to_string()));

        // Test MIN on date column with NULL
        let stmt = parse_statement("SELECT MIN(created_date) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()));

        // Test MAX on date column with NULL
        let stmt = parse_statement("SELECT MAX(created_date) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Date(NaiveDate::from_ymd_opt(2024, 2, 10).unwrap()));

        // Test MIN/MAX with GROUP BY
        let stmt = parse_statement("SELECT category, MIN(price), MAX(price) FROM products GROUP BY category ORDER BY category");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2);
        
        // Electronics: MIN=29.99, MAX=999.99 (NULL price excluded)
        assert_eq!(result.rows[0][0], Value::Text("Electronics".to_string()));
        assert_eq!(result.rows[0][1], Value::Double(29.99));
        assert_eq!(result.rows[0][2], Value::Double(999.99));
        
        // Furniture: MIN=149.99, MAX=299.99
        assert_eq!(result.rows[1][0], Value::Text("Furniture".to_string()));
        assert_eq!(result.rows[1][1], Value::Double(149.99));
        assert_eq!(result.rows[1][2], Value::Double(299.99));

        // Test MIN/MAX on column with all NULLs
        let stmt = parse_statement("SELECT MIN(price) FROM products WHERE price IS NULL");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Null);

        // Test multiple aggregates together
        let stmt = parse_statement("SELECT COUNT(*), MIN(price), MAX(price), AVG(price), SUM(price) FROM products");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(5)); // COUNT(*)
        assert_eq!(result.rows[0][1], Value::Double(29.99)); // MIN(price)
        assert_eq!(result.rows[0][2], Value::Double(999.99)); // MAX(price)
        // AVG and SUM will exclude the NULL price
    }

    #[tokio::test]
    async fn test_between_operator() {
        let mut db = Database::new("test_db".to_string());

        // Create test table
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
                name: "value".to_string(),
                sql_type: crate::yaml::schema::SqlType::Integer,
                primary_key: false,
                nullable: true,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "price".to_string(),
                sql_type: crate::yaml::schema::SqlType::Double,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "name".to_string(),
                sql_type: crate::yaml::schema::SqlType::Varchar(50),
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
            Column {
                name: "created_date".to_string(),
                sql_type: crate::yaml::schema::SqlType::Date,
                primary_key: false,
                nullable: false,
                unique: false,
                default: None,
                references: None,
            },
        ];

        let mut table = Table::new("test_data".to_string(), columns);

        // Add test data
        table
            .insert_row(vec![
                Value::Integer(1),
                Value::Integer(10),
                Value::Double(99.99),
                Value::Text("apple".to_string()),
                Value::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            ])
            .unwrap();

        table
            .insert_row(vec![
                Value::Integer(2),
                Value::Integer(20),
                Value::Double(149.99),
                Value::Text("banana".to_string()),
                Value::Date(NaiveDate::from_ymd_opt(2024, 2, 15).unwrap()),
            ])
            .unwrap();

        table
            .insert_row(vec![
                Value::Integer(3),
                Value::Integer(30),
                Value::Double(199.99),
                Value::Text("cherry".to_string()),
                Value::Date(NaiveDate::from_ymd_opt(2024, 3, 20).unwrap()),
            ])
            .unwrap();

        table
            .insert_row(vec![
                Value::Integer(4),
                Value::Integer(40),
                Value::Double(249.99),
                Value::Text("date".to_string()),
                Value::Date(NaiveDate::from_ymd_opt(2024, 4, 10).unwrap()),
            ])
            .unwrap();

        table
            .insert_row(vec![
                Value::Integer(5),
                Value::Integer(50),
                Value::Double(299.99),
                Value::Text("elderberry".to_string()),
                Value::Date(NaiveDate::from_ymd_opt(2024, 5, 5).unwrap()),
            ])
            .unwrap();

        table
            .insert_row(vec![
                Value::Integer(6),
                Value::Null,
                Value::Double(399.99),
                Value::Text("fig".to_string()),
                Value::Date(NaiveDate::from_ymd_opt(2024, 6, 1).unwrap()),
            ])
            .unwrap();

        db.add_table(table).unwrap();
        let db = Arc::new(RwLock::new(db));
        let executor = create_test_executor_from_arc(db).await;

        // Test basic integer BETWEEN
        let stmt =
            parse_statement("SELECT * FROM test_data WHERE value BETWEEN 20 AND 40 ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 3); // IDs 2, 3, 4
        assert_eq!(result.rows[0][0], Value::Integer(2));
        assert_eq!(result.rows[1][0], Value::Integer(3));
        assert_eq!(result.rows[2][0], Value::Integer(4));

        // Test NOT BETWEEN
        let stmt = parse_statement(
            "SELECT * FROM test_data WHERE value NOT BETWEEN 20 AND 40 ORDER BY id",
        );
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2); // IDs 1, 5 (not 6 because it's NULL)
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[1][0], Value::Integer(5));

        // Test BETWEEN with double values
        let stmt = parse_statement(
            "SELECT * FROM test_data WHERE price BETWEEN 150.0 AND 250.0 ORDER BY id",
        );
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2); // IDs 2, 3 (249.99 < 250.0)

        // Test BETWEEN with double values - inclusive upper bound
        let stmt = parse_statement(
            "SELECT * FROM test_data WHERE price BETWEEN 150.0 AND 249.99 ORDER BY id",
        );
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2); // IDs 3, 4 (149.99 < 150.0)

        // Test text BETWEEN
        let stmt =
            parse_statement("SELECT * FROM test_data WHERE name BETWEEN 'b' AND 'd' ORDER BY id");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 2); // banana, cherry
        assert_eq!(result.rows[0][3], Value::Text("banana".to_string()));
        assert_eq!(result.rows[1][3], Value::Text("cherry".to_string()));

        // Test date BETWEEN
        let stmt = parse_statement(
            "SELECT * FROM test_data WHERE created_date BETWEEN '2024-02-01' AND '2024-04-30' ORDER BY id",
        );
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 3); // IDs 2, 3, 4

        // Test NULL handling - NULL values should not match
        let stmt = parse_statement("SELECT * FROM test_data WHERE value BETWEEN 0 AND 100");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 5); // Should not include row with NULL value

        // Test boundary inclusiveness
        let stmt = parse_statement("SELECT * FROM test_data WHERE value BETWEEN 20 AND 20");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1); // ID 2
        assert_eq!(result.rows[0][1], Value::Integer(20));
    }

    #[tokio::test]
    async fn test_cast_function() {
        let db = create_test_database().await;
        let executor = create_test_executor_from_arc(db).await;
        
        // Test 1: Cast integer to text
        let stmt = parse_statement("SELECT CAST(123 AS VARCHAR)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("123".to_string()));
        
        // Test 2: Cast text to integer
        let stmt = parse_statement("SELECT CAST('456' AS INTEGER)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(456));
        
        // Test 3: Cast float to integer (truncates)
        let stmt = parse_statement("SELECT CAST(123.789 AS INTEGER)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(123));
        
        // Test 4: Cast integer to float
        let stmt = parse_statement("SELECT CAST(123 AS FLOAT)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Float(123.0));
        
        // Test 5: Cast text to double
        let stmt = parse_statement("SELECT CAST('123.456' AS DOUBLE)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Double(123.456));
        
        // Test 6: Cast text to date
        let stmt = parse_statement("SELECT CAST('2025-07-15' AS DATE)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Date(NaiveDate::from_ymd_opt(2025, 7, 15).unwrap()));
        
        // Test 7: Cast boolean to integer
        let stmt = parse_statement("SELECT CAST(TRUE AS INTEGER)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(1));
        
        // Test 8: Cast integer to boolean
        let stmt = parse_statement("SELECT CAST(1 AS BOOLEAN)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Boolean(true));
        
        // Test 9: Cast NULL
        let stmt = parse_statement("SELECT CAST(NULL AS INTEGER)");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Null);
        
        // Test 10: Cast in WHERE clause
        let stmt = parse_statement("SELECT name FROM users WHERE CAST(id AS VARCHAR) = '1'");
        let result = executor.execute(&stmt).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));
    }
}
