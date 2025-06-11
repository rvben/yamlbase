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
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
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
        self.column_index.get(name).copied()
    }
}

impl Value {
    pub fn is_compatible_with(&self, sql_type: &SqlType) -> bool {
        match (self, sql_type) {
            (Value::Null, _) => true,
            (Value::Integer(_), SqlType::Integer) => true,
            (Value::Float(_), SqlType::Float) => true,
            (Value::Double(_), SqlType::Double) => true,
            (Value::Decimal(_), SqlType::Decimal(_, _)) => true,
            (Value::Text(_), SqlType::Varchar(_) | SqlType::Text) => true,
            (Value::Boolean(_), SqlType::Boolean) => true,
            (Value::Timestamp(_), SqlType::Timestamp) => true,
            (Value::Date(_), SqlType::Date) => true,
            (Value::Time(_), SqlType::Time) => true,
            (Value::Uuid(_), SqlType::Uuid) => true,
            (Value::Json(_), SqlType::Json) => true,
            _ => false,
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Double(d) => d.to_string(),
            Value::Decimal(d) => d.to_string(),
            Value::Text(s) => s.clone(),
            Value::Boolean(b) => b.to_string(),
            Value::Timestamp(ts) => ts.format("%Y-%m-%d %H:%M:%S").to_string(),
            Value::Date(d) => d.format("%Y-%m-%d").to_string(),
            Value::Time(t) => t.format("%H:%M:%S").to_string(),
            Value::Uuid(u) => u.to_string(),
            Value::Json(j) => j.to_string(),
        }
    }

    pub fn compare(&self, other: &Value) -> Option<std::cmp::Ordering> {
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
            
            _ => None,
        }
    }
}