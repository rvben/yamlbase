use chrono;
use regex::Regex;
use sqlparser::ast::*;
use std::sync::Arc;
use tracing::debug;

use crate::database::{Database, Storage, Table, Value};
use crate::YamlBaseError;

pub struct QueryExecutor {
    storage: Arc<Storage>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub column_types: Vec<crate::yaml::schema::SqlType>,
    pub rows: Vec<Vec<Value>>,
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
            _ => Err(YamlBaseError::NotImplemented(
                "Only SELECT queries are supported".to_string(),
            )),
        }
    }

    async fn execute_query(&self, query: &Query) -> crate::Result<QueryResult> {
        let db_arc = self.storage.database();
        let db = db_arc.read().await;

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

        // Check if this is an aggregate query
        if self.is_aggregate_query(select) {
            return self.execute_aggregate_select(db, select, query, table, &table_name).await;
        }

        // Get column names for projection
        let columns = self.extract_columns(select, table)?;

        // Filter rows based on WHERE clause
        let filtered_rows = self.filter_rows(table, &table_name, &select.selection).await?;

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

        // Get column types from the table
        let column_types = columns.iter().map(|(_, idx)| {
            table.columns[*idx].sql_type.clone()
        }).collect();

        Ok(QueryResult {
            columns: columns.iter().map(|c| c.0.clone()).collect(),
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
                    ))
                }
            }
        }

        // Infer types from the values
        let column_types = row_values.iter().map(|value| {
            match value {
                Value::Integer(i) => {
                    // Use BigInt for values that might be larger than i32
                    if *i > i32::MAX as i64 || *i < i32::MIN as i64 {
                        crate::yaml::schema::SqlType::BigInt
                    } else {
                        crate::yaml::schema::SqlType::Integer
                    }
                },
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
        }).collect();

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
                // Handle system variables (@@variable_name)
                if ident.value.starts_with("@@") {
                    self.get_system_variable(&ident.value)
                } else {
                    Err(YamlBaseError::NotImplemented(
                        format!("Identifier '{}' not supported in SELECT without FROM", ident.value),
                    ))
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
            if let Some(row) = self.storage.find_by_primary_key(table_name, &pk_value).await {
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
            _ => Err(YamlBaseError::NotImplemented(format!(
                "Expression type not supported: {:?}",
                expr
            ))),
        }
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
                })
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
                })
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

    fn get_system_variable(&self, var_name: &str) -> crate::Result<Value> {
        // Remove @@ prefix and handle session/global prefixes
        let name = if var_name.starts_with("@@") {
            &var_name[2..]
        } else {
            var_name
        };
        
        // Handle session. and global. prefixes
        let name = if name.starts_with("session.") {
            &name[8..]
        } else if name.starts_with("SESSION.") {
            &name[8..]
        } else if name.starts_with("global.") {
            &name[7..]
        } else if name.starts_with("GLOBAL.") {
            &name[7..]
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
                let func_name = func.name.0.first()
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
        let filtered_rows = self.filter_rows(table, table_name, &select.selection).await?;

        // Process aggregate functions
        let mut columns = Vec::new();
        let mut row_values = Vec::new();

        for (idx, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (col_name, value) = self.evaluate_aggregate_expr(expr, &filtered_rows, table, idx)?;
                    columns.push(col_name);
                    row_values.push(value);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (_, value) = self.evaluate_aggregate_expr(expr, &filtered_rows, table, idx)?;
                    columns.push(alias.value.clone());
                    row_values.push(value);
                }
                _ => {
                    return Err(YamlBaseError::NotImplemented(
                        "Complex projections in aggregate queries are not supported".to_string(),
                    ))
                }
            }
        }

        // Determine column types for aggregate results
        let column_types = select.projection.iter().map(|item| {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    self.get_aggregate_result_type(expr)
                }
                _ => crate::yaml::schema::SqlType::Text,
            }
        }).collect();

        Ok(QueryResult {
            columns,
            column_types,
            rows: vec![row_values],
        })
    }

    fn get_aggregate_result_type(&self, expr: &Expr) -> crate::yaml::schema::SqlType {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.0.first()
                    .map(|ident| ident.value.to_uppercase())
                    .unwrap_or_default();
                
                match func_name.as_str() {
                    "COUNT" => crate::yaml::schema::SqlType::BigInt, // COUNT returns i64
                    "SUM" => crate::yaml::schema::SqlType::Text, // We return as formatted text for monetary values
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
                let func_name = func.name.0.first()
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
                                if args.args.is_empty() {
                                    // COUNT() - should be an error but treat as COUNT(*)
                                    rows.len() as i64
                                } else if args.args.len() == 1 {
                                    match &args.args[0] {
                                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                                            // COUNT(*)
                                            rows.len() as i64
                                        }
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                                            // COUNT(column)
                                            let mut count = 0i64;
                                            for row in rows {
                                                let value = self.get_expr_value(expr, row, table)?;
                                                if !matches!(value, Value::Null) {
                                                    count += 1;
                                                }
                                            }
                                            count
                                        }
                                        _ => {
                                            return Err(YamlBaseError::NotImplemented(
                                                "Unsupported COUNT argument".to_string(),
                                            ))
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
                                ))
                            }
                        };
                        Ok((func_name.clone(), Value::Integer(count)))
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
                                                Value::Decimal(d) => sum += d.to_string().parse::<f64>().unwrap_or(0.0),
                                                Value::Null => {} // Skip NULL values
                                                _ => {
                                                    return Err(YamlBaseError::Database {
                                                        message: "Cannot sum non-numeric values".to_string(),
                                                    })
                                                }
                                            }
                                        }
                                        // Return as string with 2 decimal places for monetary values
                                        Ok((func_name.clone(), Value::Text(format!("{:.2}", sum))))
                                    }
                                    _ => {
                                        return Err(YamlBaseError::NotImplemented(
                                            "Unsupported SUM argument".to_string(),
                                        ))
                                    }
                                }
                            }
                            _ => {
                                return Err(YamlBaseError::Database {
                                    message: "SUM requires exactly one argument".to_string(),
                                });
                            }
                        }
                    }
                    _ => Err(YamlBaseError::NotImplemented(
                        format!("Aggregate function {} not supported", func_name),
                    )),
                }
            }
            _ => Err(YamlBaseError::NotImplemented(
                "Only aggregate functions are supported in aggregate queries".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{Column, Database, Storage as DbStorage, Table, Value};
    use chrono::NaiveDate;
    use sqlparser::ast::Statement;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    
    // Helper function to create a test executor with storage
    async fn create_test_executor_from_arc(db: Arc<RwLock<Database>>) -> QueryExecutor {
        let db_owned = {
            let db_read = db.read().await;
            db_read.clone()
        };
        let storage = Arc::new(DbStorage::new(db_owned));
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
                create_column("name", crate::yaml::schema::SqlType::Varchar(100), false),
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
            "SELECT id FROM items WHERE (status = 'Active' OR status = 'Pending') AND type IN ('Development', 'Research') AND priority < 3"
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
    async fn test_complex_sciforma_query() {
        let db = create_test_database().await;
        {
            let mut db_write = db.write().await;
            let columns = vec![
                create_column(
                    "SAP_PROJECT_ID",
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
                    "PF_PRODUCT_GROUP_NAME",
                    crate::yaml::schema::SqlType::Varchar(255),
                    false,
                ),
                create_column(
                    "PROJECT_CLASS",
                    crate::yaml::schema::SqlType::Varchar(255),
                    false,
                ),
                create_column(
                    "IFRS_TYPE",
                    crate::yaml::schema::SqlType::Varchar(255),
                    false,
                ),
            ];

            let mut table = Table::new("SF_PROJECT_V2".to_string(), columns);

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

        // Test the full Sciforma query
        let stmt = parse_statement(
            "SELECT SAP_PROJECT_ID, PROJECT_NAME FROM SF_PROJECT_V2 WHERE VERSION_CODE = 'Published' \
             AND STATUS_CODE NOT IN ('Cancelled', 'Closed') AND ACTIVE_FLAG = 'Y' \
             AND CLOSED_FOR_TIME_ENTRY <> 'Y' AND SECURITY_CLASSIFICATION LIKE 'NS%' \
             AND PROJECT_STRUCTURE = 'Project' AND START_DATE > DATE '2025-01-01' \
             AND PF_PRODUCT_GROUP_NAME NOT IN ('Support IT', 'The Support IT', 'The Demo Portfolio', 'The Archive') \
             AND PROJECT_CLASS IN ('Product Development', 'Technology & Research Development') \
             AND IFRS_TYPE IN ('PROD DEV', 'TECH & RESEARCH DEV')"
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
        table.insert_row(vec![
            Value::Integer(10),
            Value::Text("100%".to_string()),
        ]).unwrap();
        // Row with literal _
        table.insert_row(vec![
            Value::Integer(11),
            Value::Text("user_name".to_string()),
        ]).unwrap();
        // Row with literal \\
        table.insert_row(vec![
            Value::Integer(12),
            Value::Text("C:\\path\\file".to_string()),
        ]).unwrap();
        
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
        let stmt = parse_statement("SELECT id FROM test_table WHERE name LIKE 'C:\\\\path\\\\file'");
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
}
