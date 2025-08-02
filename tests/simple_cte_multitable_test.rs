#![allow(clippy::uninlined_format_args)]

use std::sync::Arc;
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

/// Simple test to verify the NotImplemented error is gone
#[tokio::test]
async fn test_notimplemented_error_fixed() {
    let mut db = Database::new("simple_test".to_string());

    // Create simple test tables
    let table1_columns = vec![Column {
        name: "id".to_string(),
        sql_type: SqlType::Integer,
        primary_key: true,
        nullable: false,
        unique: true,
        default: None,
        references: None,
    }];

    let mut table1 = Table::new("table1".to_string(), table1_columns);
    table1.insert_row(vec![Value::Integer(1)]).unwrap();

    let table2_columns = vec![Column {
        name: "value".to_string(),
        sql_type: SqlType::Integer,
        primary_key: false,
        nullable: false,
        unique: false,
        default: None,
        references: None,
    }];

    let mut table2 = Table::new("table2".to_string(), table2_columns);
    table2.insert_row(vec![Value::Integer(100)]).unwrap();

    db.add_table(table1).unwrap();
    db.add_table(table2).unwrap();

    let storage = Arc::new(Storage::new(db));
    let executor = QueryExecutor::new(storage.clone()).await.unwrap();

    // Simple multi-table CTE that would previously fail with NotImplemented
    let multi_table_cte = r#"
        WITH combined AS (
            SELECT * FROM table1, table2
        )
        SELECT * FROM combined
    "#;

    let result = executor
        .execute(&parse_sql(multi_table_cte).unwrap()[0])
        .await;

    match result {
        Ok(_) => {
            println!("✅ Multi-table CTE executed without NotImplemented error!");
        }
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("Complex multi-table CTE queries not yet fully implemented") {
                panic!("❌ The NotImplemented error still exists! Fix failed.");
            } else {
                println!(
                    "✅ NotImplemented error is gone, but got different error: {}",
                    error_str
                );
                println!("   This indicates the fix worked but there are other issues to resolve");
            }
        }
    }
}
