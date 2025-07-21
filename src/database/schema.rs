use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use indexmap::IndexMap;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::yaml::schema::SqlType;

#[derive(Debug, Clone)]
pub struct Database {
    pub name: String,
    pub tables: IndexMap<String, Table>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub column_index: IndexMap<String, usize>,
    pub rows: Vec<Vec<Value>>,
    pub primary_key_index: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub sql_type: SqlType,
    pub primary_key: bool,
    pub nullable: bool,
    pub unique: bool,
    pub default: Option<String>,
    pub references: Option<(String, String)>, // (table, column)
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f32),
    Double(f64),
    Decimal(Decimal),
    Text(String),
    Boolean(bool),
    Timestamp(NaiveDateTime),
    Date(NaiveDate),
    Time(NaiveTime),
    Uuid(Uuid),
    Json(JsonValue),
}

// Implement Eq manually, treating NaN values as equal
impl Eq for Value {}

// Implement Hash manually, handling float types
impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0u8.hash(state),
            Value::Integer(i) => {
                1u8.hash(state);
                i.hash(state);
            }
            Value::Float(f) => {
                2u8.hash(state);
                f.to_bits().hash(state);
            }
            Value::Double(d) => {
                3u8.hash(state);
                d.to_bits().hash(state);
            }
            Value::Decimal(d) => {
                4u8.hash(state);
                d.hash(state);
            }
            Value::Text(s) => {
                5u8.hash(state);
                s.hash(state);
            }
            Value::Boolean(b) => {
                6u8.hash(state);
                b.hash(state);
            }
            Value::Timestamp(ts) => {
                7u8.hash(state);
                ts.hash(state);
            }
            Value::Date(d) => {
                8u8.hash(state);
                d.hash(state);
            }
            Value::Time(t) => {
                9u8.hash(state);
                t.hash(state);
            }
            Value::Uuid(u) => {
                10u8.hash(state);
                u.hash(state);
            }
            Value::Json(j) => {
                11u8.hash(state);
                j.to_string().hash(state);
            }
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Double(d) => write!(f, "{}", d),
            Value::Decimal(d) => write!(f, "{}", d),
            Value::Text(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Timestamp(ts) => write!(f, "{}", ts.format("%Y-%m-%d %H:%M:%S")),
            Value::Date(d) => write!(f, "{}", d.format("%Y-%m-%d")),
            Value::Time(t) => write!(f, "{}", t.format("%H:%M:%S")),
            Value::Uuid(u) => write!(f, "{}", u),
            Value::Json(j) => write!(f, "{}", j),
        }
    }
}

impl Database {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tables: IndexMap::new(),
        }
    }

    pub fn add_table(&mut self, table: Table) -> crate::Result<()> {
        if self.tables.contains_key(&table.name) {
            return Err(crate::YamlBaseError::Database {
                message: format!("Table '{}' already exists", table.name),
            });
        }
        self.tables.insert(table.name.clone(), table);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        // First try exact match
        if let Some(table) = self.tables.get(name) {
            return Some(table);
        }

        // Fall back to case-insensitive search
        let name_lower = name.to_lowercase();
        for (table_name, table) in &self.tables {
            if table_name.to_lowercase() == name_lower {
                return Some(table);
            }
        }
        None
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        // First try exact match
        if self.tables.contains_key(name) {
            return self.tables.get_mut(name);
        }

        // Fall back to case-insensitive search
        let name_lower = name.to_lowercase();
        for (table_name, _) in self.tables.iter() {
            if table_name.to_lowercase() == name_lower {
                // Need to clone the key to avoid borrow checker issues
                let key = table_name.clone();
                return self.tables.get_mut(&key);
            }
        }
        None
    }
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        let mut column_index = IndexMap::new();
        let mut primary_key_index = None;

        for (idx, col) in columns.iter().enumerate() {
            column_index.insert(col.name.clone(), idx);
            if col.primary_key {
                primary_key_index = Some(idx);
            }
        }

        Self {
            name,
            columns,
            column_index,
            rows: Vec::new(),
            primary_key_index,
        }
    }

    pub fn insert_row(&mut self, row: Vec<Value>) -> crate::Result<()> {
        if row.len() != self.columns.len() {
            return Err(crate::YamlBaseError::Database {
                message: format!(
                    "Row has {} values but table has {} columns",
                    row.len(),
                    self.columns.len()
                ),
            });
        }

        // Validate data types
        for (value, column) in row.iter().zip(self.columns.iter()) {
            if !value.is_compatible_with(&column.sql_type) {
                return Err(crate::YamlBaseError::TypeConversion(format!(
                    "Value {:?} is not compatible with column '{}' of type {:?}",
                    value, column.name, column.sql_type
                )));
            }

            if !column.nullable && matches!(value, Value::Null) {
                return Err(crate::YamlBaseError::Database {
                    message: format!("Column '{}' cannot be NULL", column.name),
                });
            }
        }

        self.rows.push(row);
        Ok(())
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        // First try exact match
        if let Some(&index) = self.column_index.get(name) {
            return Some(index);
        }

        // Fall back to case-insensitive search
        let name_lower = name.to_lowercase();
        for (col_name, &index) in &self.column_index {
            if col_name.to_lowercase() == name_lower {
                return Some(index);
            }
        }
        None
    }
}

impl Value {
    pub fn is_compatible_with(&self, sql_type: &SqlType) -> bool {
        matches!(
            (self, sql_type),
            (Value::Null, _)
                | (Value::Integer(_), SqlType::Integer)
                | (Value::Float(_), SqlType::Float)
                | (Value::Double(_), SqlType::Double)
                | (Value::Decimal(_), SqlType::Decimal(_, _))
                | (
                    Value::Text(_),
                    SqlType::Char(_) | SqlType::Varchar(_) | SqlType::Text
                )
                | (Value::Boolean(_), SqlType::Boolean)
                | (Value::Timestamp(_), SqlType::Timestamp)
                | (Value::Date(_), SqlType::Date)
                | (Value::Time(_), SqlType::Time)
                | (Value::Uuid(_), SqlType::Uuid)
                | (Value::Json(_), SqlType::Json)
        )
    }

    pub fn compare(&self, other: &Value) -> Option<std::cmp::Ordering> {
        use rust_decimal::prelude::*;
        use std::cmp::Ordering;

        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),

            (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Double(a), Value::Double(b)) => a.partial_cmp(b),
            (Value::Decimal(a), Value::Decimal(b)) => Some(a.cmp(b)),
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
            (Value::Date(a), Value::Date(b)) => Some(a.cmp(b)),
            (Value::Time(a), Value::Time(b)) => Some(a.cmp(b)),
            (Value::Uuid(a), Value::Uuid(b)) => Some(a.cmp(b)),

            // Handle cross-type numeric comparisons
            (Value::Integer(a), Value::Double(b)) => (*a as f64).partial_cmp(b),
            (Value::Double(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Integer(a), Value::Float(b)) => (*a as f32).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f32)),
            (Value::Float(a), Value::Double(b)) => (*a as f64).partial_cmp(b),
            (Value::Double(a), Value::Float(b)) => a.partial_cmp(&(*b as f64)),

            // Handle Decimal comparisons with other numeric types
            (Value::Decimal(a), Value::Integer(b)) => {
                Decimal::from_i64(*b).map(|b_decimal| a.cmp(&b_decimal))
            }
            (Value::Integer(a), Value::Decimal(b)) => {
                Decimal::from_i64(*a).map(|a_decimal| a_decimal.cmp(b))
            }
            (Value::Decimal(a), Value::Double(b)) => {
                // Convert double to decimal for comparison
                Decimal::from_f64(*b).map(|b_decimal| a.cmp(&b_decimal))
            }
            (Value::Double(a), Value::Decimal(b)) => {
                // Convert double to decimal for comparison
                Decimal::from_f64(*a).map(|a_decimal| a_decimal.cmp(b))
            }
            (Value::Decimal(a), Value::Float(b)) => {
                // Convert float to decimal for comparison
                Decimal::from_f32(*b).map(|b_decimal| a.cmp(&b_decimal))
            }
            (Value::Float(a), Value::Decimal(b)) => {
                // Convert float to decimal for comparison
                Decimal::from_f32(*a).map(|a_decimal| a_decimal.cmp(b))
            }

            _ => None,
        }
    }
}
