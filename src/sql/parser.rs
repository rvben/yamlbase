use sqlparser::ast::{Query, Statement};
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
use tracing::debug;

#[derive(Debug, Clone, Copy, Default)]
pub enum SqlDialect {
    #[default]
    PostgreSQL,
    MySQL,
    Generic,
}

pub fn parse_sql(sql: &str) -> crate::Result<Vec<Statement>> {
    parse_sql_with_dialect(sql, SqlDialect::default())
}

pub fn parse_sql_with_dialect(sql: &str, dialect: SqlDialect) -> crate::Result<Vec<Statement>> {
    debug!("Parsing SQL with dialect {:?}: {}", dialect, sql);

    let statements = match dialect {
        SqlDialect::PostgreSQL => {
            let dialect = PostgreSqlDialect {};
            Parser::parse_sql(&dialect, sql)?
        }
        SqlDialect::MySQL | SqlDialect::Generic => {
            // Use GenericDialect for MySQL and generic SQL
            let dialect = GenericDialect {};
            Parser::parse_sql(&dialect, sql)?
        }
    };

    Ok(statements)
}

pub fn is_select_query(statement: &Statement) -> Option<&Query> {
    match statement {
        Statement::Query(query) => Some(query),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgresql_dialect_parsing() {
        let sql = "SELECT * FROM users LIMIT 5";
        let result = parse_sql_with_dialect(sql, SqlDialect::PostgreSQL);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn test_generic_dialect_parsing() {
        let sql = "SELECT * FROM users LIMIT 5";
        let result = parse_sql_with_dialect(sql, SqlDialect::Generic);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn test_string_concatenation_parsing() {
        let sql = "SELECT first_name || ' ' || last_name AS full_name FROM users";
        let result = parse_sql_with_dialect(sql, SqlDialect::Generic);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn test_date_literal_parsing() {
        let sql = "SELECT * FROM projects WHERE start_date >= DATE '2024-01-01'";
        let result = parse_sql_with_dialect(sql, SqlDialect::Generic);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn test_cte_parsing() {
        let sql = "WITH project_cte AS (SELECT id FROM projects) SELECT * FROM project_cte";
        let result = parse_sql_with_dialect(sql, SqlDialect::Generic);
        assert!(result.is_ok());
        let statements = result.unwrap();
        assert_eq!(statements.len(), 1);
    }
}
