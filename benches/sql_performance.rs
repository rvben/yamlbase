#![allow(clippy::uninlined_format_args)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use tokio::runtime::Runtime;

use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::sql::{QueryExecutor, parse_sql};
use yamlbase::yaml::schema::SqlType;

fn create_test_database() -> Database {
    let mut db = Database::new("bench_db".to_string());

    // Create users table
    let columns = vec![
        Column {
            name: "id".to_string(),
            sql_type: SqlType::Integer,
            primary_key: true,
            nullable: false,
            unique: true,
            default: None,
            references: None,
        },
        Column {
            name: "name".to_string(),
            sql_type: SqlType::Varchar(100),
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "age".to_string(),
            sql_type: SqlType::Integer,
            primary_key: false,
            nullable: true,
            unique: false,
            default: None,
            references: None,
        },
        Column {
            name: "active".to_string(),
            sql_type: SqlType::Boolean,
            primary_key: false,
            nullable: false,
            unique: false,
            default: Some("true".to_string()),
            references: None,
        },
    ];

    let mut table = Table::new("users".to_string(), columns);

    // Insert test data
    for i in 1..=1000 {
        let row = vec![
            Value::Integer(i),
            Value::Text(format!("User {}", i)),
            Value::Integer(20 + (i % 50)),
            Value::Boolean(i % 2 == 0),
        ];
        table.insert_row(row).unwrap();
    }

    db.add_table(table).unwrap();
    db
}

fn benchmark_simple_select(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = create_test_database();
    let storage = Arc::new(Storage::new(db));
    let executor = rt.block_on(QueryExecutor::new(storage)).unwrap();

    c.bench_function("simple_select", |b| {
        b.iter(|| {
            rt.block_on(async {
                let sql = "SELECT * FROM users";
                let statements = parse_sql(sql).unwrap();
                let result = executor.execute(&statements[0]).await.unwrap();
                black_box(result);
            });
        });
    });
}

fn benchmark_where_clause(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = create_test_database();
    let storage = Arc::new(Storage::new(db));
    let executor = rt.block_on(QueryExecutor::new(storage)).unwrap();

    c.bench_function("where_clause", |b| {
        b.iter(|| {
            rt.block_on(async {
                let sql = "SELECT * FROM users WHERE age > 30 AND active = true";
                let statements = parse_sql(sql).unwrap();
                let result = executor.execute(&statements[0]).await.unwrap();
                black_box(result);
            });
        });
    });
}

fn benchmark_order_by(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = create_test_database();
    let storage = Arc::new(Storage::new(db));
    let executor = rt.block_on(QueryExecutor::new(storage)).unwrap();

    c.bench_function("order_by", |b| {
        b.iter(|| {
            rt.block_on(async {
                let sql = "SELECT * FROM users ORDER BY age DESC, name ASC";
                let statements = parse_sql(sql).unwrap();
                let result = executor.execute(&statements[0]).await.unwrap();
                black_box(result);
            });
        });
    });
}

fn benchmark_limit(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = create_test_database();
    let storage = Arc::new(Storage::new(db));
    let executor = rt.block_on(QueryExecutor::new(storage)).unwrap();

    c.bench_function("limit", |b| {
        b.iter(|| {
            rt.block_on(async {
                let sql = "SELECT * FROM users WHERE active = true ORDER BY id LIMIT 50";
                let statements = parse_sql(sql).unwrap();
                let result = executor.execute(&statements[0]).await.unwrap();
                black_box(result);
            });
        });
    });
}

fn benchmark_cte(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = create_test_database();
    let storage = Arc::new(Storage::new(db));
    let executor = rt.block_on(QueryExecutor::new(storage)).unwrap();

    c.bench_function("cte", |b| {
        b.iter(|| {
            rt.block_on(async {
                let sql = "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users WHERE age > 30 ORDER BY id LIMIT 10";
                let statements = parse_sql(sql).unwrap();
                let result = executor.execute(&statements[0]).await.unwrap();
                black_box(result);
            });
        });
    });
}

fn benchmark_sql_parsing(c: &mut Criterion) {
    c.bench_function("sql_parsing", |b| {
        b.iter(|| {
            let sql = "SELECT id, name, age FROM users WHERE age > 25 AND active = true ORDER BY age DESC LIMIT 100";
            let statements = parse_sql(sql).unwrap();
            black_box(statements);
        });
    });
}

criterion_group!(
    benches,
    benchmark_simple_select,
    benchmark_where_clause,
    benchmark_order_by,
    benchmark_limit,
    benchmark_cte,
    benchmark_sql_parsing
);
criterion_main!(benches);
