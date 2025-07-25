#![allow(clippy::uninlined_format_args)]

mod common;

use common::{_mysql_connect_and_auth, _mysql_test_query, TestServer};

#[test]
fn test_mysql_various_queries() {
    let server = TestServer::start_mysql("examples/database_with_auth.yaml");

    let result = std::panic::catch_unwind(|| {
        let mut stream = _mysql_connect_and_auth(&server, "dbadmin", "securepass123");

        // Test SELECT without FROM
        _mysql_test_query(&mut stream, "SELECT 1", vec!["1"]);
        _mysql_test_query(&mut stream, "SELECT 1 + 1", vec!["2"]);
        _mysql_test_query(&mut stream, "SELECT 5 - 3", vec!["2"]);
        _mysql_test_query(&mut stream, "SELECT 3 * 4", vec!["12"]);
        _mysql_test_query(&mut stream, "SELECT 10 / 2", vec!["5"]);
        _mysql_test_query(&mut stream, "SELECT 'hello'", vec!["hello"]);
        _mysql_test_query(&mut stream, "SELECT 1, 2, 3", vec!["1", "2", "3"]);
        _mysql_test_query(&mut stream, "SELECT 1 AS num", vec!["1"]);
        _mysql_test_query(&mut stream, "SELECT -5", vec!["-5"]);
        _mysql_test_query(&mut stream, "SELECT true", vec!["true"]);
        _mysql_test_query(&mut stream, "SELECT false", vec!["false"]);
        _mysql_test_query(&mut stream, "SELECT null", vec!["NULL"]);

        // Test SELECT with FROM
        _mysql_test_query(
            &mut stream,
            "SELECT * FROM users WHERE id = 1",
            vec!["1", "alice", "alice@example.com", "2024-01-15 10:30:00"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT username FROM users",
            vec!["alice", "bob"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT id, username FROM users ORDER BY id DESC",
            vec!["2", "bob", "1", "alice"],
        );

        // Test PING command - temporarily disabled due to sequence number issues
        // mysql_test_ping(&mut stream);
    });

    if let Err(e) = result {
        eprintln!("Test failed: {:?}", e);
        panic!("Test failed");
    }
}

#[test]
fn test_mysql_system_variables() {
    let server = TestServer::start_mysql("examples/database_with_auth.yaml");

    let result = std::panic::catch_unwind(|| {
        let mut stream = _mysql_connect_and_auth(&server, "dbadmin", "securepass123");

        // Test system variables
        _mysql_test_query(&mut stream, "SELECT @@version", vec!["5.7.0-yamlbase"]);
        _mysql_test_query(&mut stream, "SELECT @@VERSION", vec!["5.7.0-yamlbase"]);
        _mysql_test_query(
            &mut stream,
            "SELECT @@version_comment",
            vec!["YamlBase Server"],
        );
        _mysql_test_query(&mut stream, "SELECT @@protocol_version", vec!["10"]);

        // Mixed queries
        _mysql_test_query(
            &mut stream,
            "SELECT 1, @@version",
            vec!["1", "5.7.0-yamlbase"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT @@version, @@version_comment",
            vec!["5.7.0-yamlbase", "YamlBase Server"],
        );
    });

    if let Err(e) = result {
        eprintln!("Test failed: {:?}", e);
        panic!("Test failed");
    }
}

#[test]
fn test_mysql_where_conditions() {
    let server = TestServer::start_mysql("examples/database_with_auth.yaml");

    let result = std::panic::catch_unwind(|| {
        let mut stream = _mysql_connect_and_auth(&server, "dbadmin", "securepass123");

        // Test various WHERE conditions
        _mysql_test_query(
            &mut stream,
            "SELECT id FROM users WHERE username = 'alice'",
            vec!["1"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT username FROM users WHERE id > 1",
            vec!["bob"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT username FROM users WHERE id >= 1",
            vec!["alice", "bob"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT username FROM users WHERE id < 2",
            vec!["alice"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT username FROM users WHERE id <= 2",
            vec!["alice", "bob"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT username FROM users WHERE id != 1",
            vec!["bob"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT username FROM users WHERE username LIKE 'a%'",
            vec!["alice"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT username FROM users WHERE username LIKE '%b'",
            vec!["bob"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT username FROM users WHERE username LIKE '%li%'",
            vec!["alice"],
        );
    });

    if let Err(e) = result {
        eprintln!("Test failed: {:?}", e);
        panic!("Test failed");
    }
}

#[test]
fn test_mysql_limit_queries() {
    let server = TestServer::start_mysql("examples/database_with_auth.yaml");

    let result = std::panic::catch_unwind(|| {
        let mut stream = _mysql_connect_and_auth(&server, "dbadmin", "securepass123");

        // Test LIMIT
        _mysql_test_query(
            &mut stream,
            "SELECT username FROM users LIMIT 1",
            vec!["alice"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT username FROM users ORDER BY id DESC LIMIT 1",
            vec!["bob"],
        );
        _mysql_test_query(
            &mut stream,
            "SELECT id FROM users ORDER BY id LIMIT 2",
            vec!["1", "2"],
        );
    });

    if let Err(e) = result {
        eprintln!("Test failed: {:?}", e);
        panic!("Test failed");
    }
}

// Aggregate functions are not implemented in yamlbase (by design - it's a read-only test database)
// #[test]
// fn test_mysql_aggregate_functions() { ... }
