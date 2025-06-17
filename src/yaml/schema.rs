use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YamlDatabase {
    pub database: DatabaseInfo,
    pub tables: IndexMap<String, YamlTable>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YamlTable {
    pub columns: IndexMap<String, String>,
    #[serde(default)]
    pub data: Vec<IndexMap<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct YamlColumn {
    pub name: String,
    pub type_def: String,
    pub is_primary_key: bool,
    pub is_nullable: bool,
    pub is_unique: bool,
    pub default_value: Option<String>,
    pub references: Option<ForeignKeyRef>,
}

#[derive(Debug, Clone)]
pub struct ForeignKeyRef {
    pub table: String,
    pub column: String,
}

impl YamlColumn {
    pub fn parse(name: String, type_def: &str) -> crate::Result<Self> {
        let type_def_upper = type_def.to_uppercase();
        let parts: Vec<&str> = type_def_upper.split_whitespace().collect();

        let mut column = YamlColumn {
            name,
            type_def: type_def.to_string(),
            is_primary_key: false,
            is_nullable: true,
            is_unique: false,
            default_value: None,
            references: None,
        };

        let mut i = 0;
        while i < parts.len() {
            match parts[i] {
                "PRIMARY" if i + 1 < parts.len() && parts[i + 1] == "KEY" => {
                    column.is_primary_key = true;
                    column.is_nullable = false;
                    i += 2;
                }
                "NOT" if i + 1 < parts.len() && parts[i + 1] == "NULL" => {
                    column.is_nullable = false;
                    i += 2;
                }
                "NULL" => {
                    column.is_nullable = true;
                    i += 1;
                }
                "UNIQUE" => {
                    column.is_unique = true;
                    i += 1;
                }
                "DEFAULT" if i + 1 < parts.len() => {
                    if i + 2 < parts.len() && parts[i + 1] == "CURRENT_TIMESTAMP" {
                        column.default_value = Some("CURRENT_TIMESTAMP".to_string());
                        i += 2;
                    } else {
                        column.default_value = Some(parts[i + 1].to_string());
                        i += 2;
                    }
                }
                "REFERENCES" if i + 1 < parts.len() => {
                    let ref_str = parts[i + 1];
                    if let Some(open_paren) = ref_str.find('(') {
                        if let Some(close_paren) = ref_str.find(')') {
                            let table = ref_str[..open_paren].to_string();
                            let col = ref_str[open_paren + 1..close_paren].to_string();
                            column.references = Some(ForeignKeyRef { table, column: col });
                        }
                    }
                    i += 2;
                }
                _ => i += 1,
            }
        }

        Ok(column)
    }

    pub fn get_base_type(&self) -> crate::Result<SqlType> {
        let type_upper = self.type_def.to_uppercase();
        let base_type = type_upper.split_whitespace().next().unwrap_or("");

        Ok(match base_type {
            "INTEGER" | "INT" | "BIGINT" | "SMALLINT" => SqlType::Integer,
            s if s.starts_with("VARCHAR") => {
                let size = extract_size(s).unwrap_or(255);
                SqlType::Varchar(size)
            }
            "TEXT" | "CLOB" => SqlType::Text,
            "TIMESTAMP" | "DATETIME" => SqlType::Timestamp,
            "DATE" => SqlType::Date,
            "TIME" => SqlType::Time,
            "BOOLEAN" | "BOOL" => SqlType::Boolean,
            s if s.starts_with("DECIMAL") || s.starts_with("NUMERIC") => {
                let (precision, scale) = extract_decimal_params(s).unwrap_or((10, 2));
                SqlType::Decimal(precision, scale)
            }
            "FLOAT" | "REAL" => SqlType::Float,
            "DOUBLE" => SqlType::Double,
            "UUID" => SqlType::Uuid,
            "JSON" | "JSONB" => SqlType::Json,
            _ => {
                return Err(crate::YamlBaseError::TypeConversion(format!(
                    "Unknown SQL type: {}",
                    base_type
                )))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlType {
    Integer,
    BigInt, // For i64 values like COUNT(*)
    Varchar(usize),
    Text,
    Timestamp,
    Date,
    Time,
    Boolean,
    Decimal(u32, u32), // precision, scale
    Float,
    Double,
    Uuid,
    Json,
}

#[cfg(test)]
pub(super) fn extract_size(type_str: &str) -> Option<usize> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.find(')') {
            return type_str[start + 1..end].parse().ok();
        }
    }
    None
}

#[cfg(not(test))]
fn extract_size(type_str: &str) -> Option<usize> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.find(')') {
            return type_str[start + 1..end].parse().ok();
        }
    }
    None
}

fn extract_decimal_params(type_str: &str) -> Option<(u32, u32)> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.find(')') {
            let params = &type_str[start + 1..end];
            let parts: Vec<&str> = params.split(',').collect();
            if parts.len() == 2 {
                if let (Ok(p), Ok(s)) = (parts[0].trim().parse(), parts[1].trim().parse()) {
                    return Some((p, s));
                }
            } else if parts.len() == 1 {
                if let Ok(p) = parts[0].trim().parse() {
                    return Some((p, 0));
                }
            }
        }
    }
    None
}
