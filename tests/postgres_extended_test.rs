use tokio_postgres::{NoTls, Config};
use yamlbase::database::{Column, Database, Table, Value};
use yamlbase::yaml::schema::SqlType;

mod common;
use common::TestServer;

#[tokio::test]
async fn test_postgres_extended_query_protocol() {
    let mut db = Database::new("test_db".to_string());
    
    // Create a test table
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
            sql_type: SqlType::Text,
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
    ];
    
    let mut table = Table::new("users".to_string(), columns);
    
    // Insert test data
    table.insert_row(vec![
        Value::Integer(1),
        Value::Text("Alice".to_string()),
        Value::Integer(30),
    ]).unwrap();
    
    table.insert_row(vec![
        Value::Integer(2),
        Value::Text("Bob".to_string()),
        Value::Integer(25),
    ]).unwrap();
    
    table.insert_row(vec![
        Value::Integer(3),
        Value::Text("Charlie".to_string()),
        Value::Null,
    ]).unwrap();
    
    db.add_table(table).unwrap();
    
    // Start server
    let test_server = TestServer::new_postgres(db).await;
    
    // Connect using tokio-postgres which uses extended query protocol
    let pg_config = Config::new()
        .host("127.0.0.1")
        .port(test_server.port)
        .user("yamlbase")
        .password("password")
        .dbname("test_db")
        .to_owned();
    
    let (client, connection) = pg_config.connect(NoTls).await.unwrap();
    
    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Test prepared statement with parameters
    let stmt = client.prepare("SELECT * FROM users WHERE id = $1").await.unwrap();
    let rows = client.query(&stmt, &[&2i32]).await.unwrap();
    
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.get::<_, i32>(0), 2);
    assert_eq!(row.get::<_, &str>(1), "Bob");
    assert_eq!(row.get::<_, i32>(2), 25);
    
    // Test prepared statement with multiple parameters
    let stmt2 = client.prepare("SELECT * FROM users WHERE age > $1 AND name != $2").await.unwrap();
    let rows2 = client.query(&stmt2, &[&20i32, &"Alice".to_string()]).await.unwrap();
    
    assert_eq!(rows2.len(), 1);
    let row2 = &rows2[0];
    assert_eq!(row2.get::<_, &str>(1), "Bob");
    
    // Test NULL handling
    let stmt3 = client.prepare("SELECT * FROM users WHERE age IS NULL").await.unwrap();
    let rows3 = client.query(&stmt3, &[]).await.unwrap();
    
    assert_eq!(rows3.len(), 1);
    assert_eq!(rows3[0].get::<_, &str>(1), "Charlie");
    
    // Test describe functionality
    let stmt4 = client.prepare("SELECT id, name FROM users").await.unwrap();
    assert_eq!(stmt4.columns().len(), 2);
    assert_eq!(stmt4.columns()[0].name(), "id");
    assert_eq!(stmt4.columns()[1].name(), "name");
}

#[tokio::test]
async fn test_postgres_prepared_statement_reuse() {
    let mut db = Database::new("test_db".to_string());
    
    let columns = vec![
        Column {
            name: "value".to_string(),
            sql_type: SqlType::Integer,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];
    
    let mut table = Table::new("numbers".to_string(), columns);
    for i in 1..=100 {
        table.insert_row(vec![Value::Integer(i)]).unwrap();
    }
    
    db.add_table(table).unwrap();
    
    let test_server = TestServer::new_postgres(db).await;
    
    let pg_config = Config::new()
        .host("127.0.0.1")
        .port(test_server.port)
        .user("yamlbase")
        .password("password")
        .dbname("test_db")
        .to_owned();
    
    let (client, connection) = pg_config.connect(NoTls).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Prepare once, execute multiple times
    let stmt = client.prepare("SELECT * FROM numbers WHERE value = $1").await.unwrap();
    
    for i in [10i32, 20, 30, 40, 50] {
        let rows = client.query(&stmt, &[&i]).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i32>(0), i);
    }
}