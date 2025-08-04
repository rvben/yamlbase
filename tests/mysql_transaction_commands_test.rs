use yamlbase::sql::parser::parse_sql;

#[test]
fn test_transaction_command_parsing() {
    // Ensure we can parse all transaction commands correctly
    let commands = vec![
        "BEGIN",
        "BEGIN WORK",
        "START TRANSACTION",
        "COMMIT",
        "COMMIT WORK",
        "ROLLBACK",
        "ROLLBACK WORK",
    ];

    for cmd in commands {
        let result = parse_sql(cmd);
        assert!(result.is_ok(), "Failed to parse '{cmd}': {result:?}");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "{cmd} should parse to one statement");

        // Check that it's a transaction statement
        match &statements[0] {
            sqlparser::ast::Statement::StartTransaction { .. } => {
                assert!(
                    cmd.contains("BEGIN") || cmd.contains("START"),
                    "Unexpected START TRANSACTION for: {cmd}"
                );
            }
            sqlparser::ast::Statement::Commit { .. } => {
                assert!(cmd.contains("COMMIT"), "Unexpected COMMIT for: {cmd}");
            }
            sqlparser::ast::Statement::Rollback { .. } => {
                assert!(cmd.contains("ROLLBACK"), "Unexpected ROLLBACK for: {cmd}");
            }
            _ => panic!(
                "'{cmd}' parsed to unexpected statement type: {:?}",
                statements[0]
            ),
        }
    }
}

#[test]
fn test_mysql_handler_identifies_transaction_commands() {
    // Test the transaction command detection logic
    let commands = vec![
        ("BEGIN", true),
        ("BEGIN WORK", true),
        ("START TRANSACTION", true),
        ("COMMIT", true),
        ("ROLLBACK", true),
        ("SELECT 1", false),
        ("INSERT INTO test VALUES (1)", false),
        ("UPDATE test SET id = 1", false),
        ("DELETE FROM test", false),
    ];

    for (cmd, expected_is_transaction) in commands {
        let statements = parse_sql(cmd).expect("Failed to parse");
        let statement = &statements[0];

        let is_transaction = matches!(
            statement,
            sqlparser::ast::Statement::StartTransaction { .. }
                | sqlparser::ast::Statement::Commit { .. }
                | sqlparser::ast::Statement::Rollback { .. }
        );

        assert_eq!(
            is_transaction, expected_is_transaction,
            "Command '{cmd}' transaction detection mismatch"
        );
    }
}
