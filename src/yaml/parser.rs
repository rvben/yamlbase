use indexmap::IndexMap;
use std::path::Path;
use tracing::{debug, info};

use crate::database::{Column, Database, Table, Value as DbValue};
use crate::yaml::schema::{AuthConfig, SqlType, YamlColumn, YamlDatabase};

pub async fn parse_yaml_database(path: &Path) -> crate::Result<(Database, Option<AuthConfig>)> {
    info!("Parsing YAML database from: {}", path.display());

    let content = tokio::fs::read_to_string(path).await?;
    let yaml_db: YamlDatabase = serde_yaml::from_str(&content)?;

    let auth_config = yaml_db.database.auth.clone();
    let mut database = Database::new(yaml_db.database.name.clone());

    for (table_name, yaml_table) in yaml_db.tables {
        debug!("Parsing table: {}", table_name);

        let mut columns = Vec::new();
        let mut column_map = IndexMap::new();

        for (col_name, type_def) in &yaml_table.columns {
            let yaml_column = YamlColumn::parse(col_name.clone(), type_def)?;
            let sql_type = yaml_column.get_base_type()?;

            let column = Column {
                name: yaml_column.name.clone(),
                sql_type,
                primary_key: yaml_column.is_primary_key,
                nullable: yaml_column.is_nullable,
                unique: yaml_column.is_unique,
                default: yaml_column.default_value,
                references: yaml_column.references.map(|r| (r.table, r.column)),
            };

            column_map.insert(yaml_column.name.clone(), columns.len());
            columns.push(column);
        }

        let mut table = Table::new(table_name.clone(), columns);

        // Parse and insert data
        for row_data in yaml_table.data {
            let mut row = Vec::new();

            for column in &table.columns {
                let value = if let Some(yaml_value) = row_data.get(&column.name) {
                    parse_value(yaml_value, &column.sql_type)?
                } else if column.nullable {
                    DbValue::Null
                } else if let Some(default) = &column.default {
                    parse_default_value(default, &column.sql_type)?
                } else {
                    return Err(crate::YamlBaseError::Database {
                        message: format!(
                            "Non-nullable column '{}' has no value and no default",
                            column.name
                        ),
                    });
                };
                row.push(value);
            }

            table.insert_row(row)?;
        }

        database.add_table(table)?;
    }

    info!(
        "Successfully parsed database with {} tables",
        database.tables.len()
    );
    Ok((database, auth_config))
}

fn parse_value(yaml_value: &serde_yaml::Value, sql_type: &SqlType) -> crate::Result<DbValue> {
    use serde_yaml::Value;

    match (yaml_value, sql_type) {
        (Value::Null, _) => Ok(DbValue::Null),

        (Value::Bool(b), SqlType::Boolean) => Ok(DbValue::Boolean(*b)),

        (Value::Number(n), SqlType::Integer) => {
            if let Some(i) = n.as_i64() {
                Ok(DbValue::Integer(i))
            } else {
                Err(crate::YamlBaseError::TypeConversion(format!(
                    "Cannot convert {:?} to integer",
                    n
                )))
            }
        }

        (Value::Number(n), SqlType::Float) => {
            if let Some(f) = n.as_f64() {
                Ok(DbValue::Float(f as f32))
            } else {
                Err(crate::YamlBaseError::TypeConversion(format!(
                    "Cannot convert {:?} to float",
                    n
                )))
            }
        }

        (Value::Number(n), SqlType::Double) => {
            if let Some(f) = n.as_f64() {
                Ok(DbValue::Double(f))
            } else {
                Err(crate::YamlBaseError::TypeConversion(format!(
                    "Cannot convert {:?} to double",
                    n
                )))
            }
        }

        (Value::Number(n), SqlType::Decimal(_, _)) => {
            let s = n.to_string();
            match s.parse::<rust_decimal::Decimal>() {
                Ok(d) => Ok(DbValue::Decimal(d)),
                Err(_) => Err(crate::YamlBaseError::TypeConversion(format!(
                    "Cannot convert {:?} to decimal",
                    n
                ))),
            }
        }

        (Value::String(s), SqlType::Char(_) | SqlType::Varchar(_) | SqlType::Text) => Ok(DbValue::Text(s.clone())),

        (Value::String(s), SqlType::Timestamp) => {
            match chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                Ok(dt) => Ok(DbValue::Timestamp(dt)),
                Err(_) => {
                    // Try ISO format
                    match chrono::DateTime::parse_from_rfc3339(s) {
                        Ok(dt) => Ok(DbValue::Timestamp(dt.naive_local())),
                        Err(_) => Err(crate::YamlBaseError::TypeConversion(format!(
                            "Cannot parse timestamp: {}",
                            s
                        ))),
                    }
                }
            }
        }

        (Value::String(s), SqlType::Date) => {
            match chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                Ok(d) => Ok(DbValue::Date(d)),
                Err(_) => Err(crate::YamlBaseError::TypeConversion(format!(
                    "Cannot parse date: {}",
                    s
                ))),
            }
        }

        (Value::String(s), SqlType::Time) => {
            match chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                Ok(t) => Ok(DbValue::Time(t)),
                Err(_) => Err(crate::YamlBaseError::TypeConversion(format!(
                    "Cannot parse time: {}",
                    s
                ))),
            }
        }

        (Value::String(s), SqlType::Uuid) => match uuid::Uuid::parse_str(s) {
            Ok(u) => Ok(DbValue::Uuid(u)),
            Err(_) => Err(crate::YamlBaseError::TypeConversion(format!(
                "Cannot parse UUID: {}",
                s
            ))),
        },

        (Value::String(s), SqlType::Integer) => match s.parse::<i64>() {
            Ok(i) => Ok(DbValue::Integer(i)),
            Err(_) => Err(crate::YamlBaseError::TypeConversion(format!(
                "Cannot parse integer: {}",
                s
            ))),
        },

        (Value::Mapping(_) | Value::Sequence(_), SqlType::Json) => {
            let json_str = serde_json::to_string(yaml_value).map_err(|e| {
                crate::YamlBaseError::TypeConversion(format!("Cannot convert to JSON: {}", e))
            })?;
            Ok(DbValue::Json(serde_json::from_str(&json_str).unwrap()))
        }

        _ => Err(crate::YamlBaseError::TypeConversion(format!(
            "Cannot convert {:?} to {:?}",
            yaml_value, sql_type
        ))),
    }
}

fn parse_default_value(default: &str, sql_type: &SqlType) -> crate::Result<DbValue> {
    match default.to_uppercase().as_str() {
        "NULL" => Ok(DbValue::Null),
        "TRUE" => Ok(DbValue::Boolean(true)),
        "FALSE" => Ok(DbValue::Boolean(false)),
        "CURRENT_TIMESTAMP" => Ok(DbValue::Timestamp(chrono::Local::now().naive_local())),
        _ => {
            // Try to parse as the specific type
            let yaml_value: serde_yaml::Value = match sql_type {
                SqlType::Boolean => serde_yaml::Value::Bool(default.parse().map_err(|_| {
                    crate::YamlBaseError::TypeConversion(format!("Invalid boolean: {}", default))
                })?),
                SqlType::Integer => serde_yaml::Value::Number(serde_yaml::Number::from(
                    default.parse::<i64>().map_err(|_| {
                        crate::YamlBaseError::TypeConversion(format!(
                            "Invalid integer: {}",
                            default
                        ))
                    })?,
                )),
                _ => serde_yaml::Value::String(default.to_string()),
            };
            parse_value(&yaml_value, sql_type)
        }
    }
}
