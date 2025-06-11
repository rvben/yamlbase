use sqlparser::ast::{Statement, Query};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tracing::debug;

pub fn parse_sql(sql: &str) -> crate::Result<Vec<Statement>> {
    debug!("Parsing SQL: {}", sql);
    
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    
    Ok(statements)
}

pub fn is_select_query(statement: &Statement) -> Option<&Box<Query>> {
    match statement {
        Statement::Query(query) => Some(query),
        _ => None,
    }
}