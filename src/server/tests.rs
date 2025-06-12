#[cfg(test)]
mod tests {
    use crate::server::Server;
    use crate::config::{Config, Protocol};
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[tokio::test]
    async fn test_server_uses_yaml_auth() {
        let yaml_content = r#"
database:
  name: "test_db"
  auth:
    username: "yaml_user"
    password: "yaml_pass"

tables:
  test:
    columns:
      id: "INTEGER PRIMARY KEY"
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = Config {
            file: temp_file.path().to_path_buf(),
            port: None,
            bind_address: "127.0.0.1".to_string(),
            protocol: Protocol::Postgres,
            username: "cli_user".to_string(),  // These should be overridden
            password: "cli_pass".to_string(),  // by YAML auth
            hot_reload: false,
            verbose: false,
            log_level: "error".to_string(),
            database: None,
        };

        let server = Server::new(config).await.unwrap();
        
        // Verify that the server's config has been updated with YAML auth
        assert_eq!(server.config.username, "yaml_user");
        assert_eq!(server.config.password, "yaml_pass");
    }

    #[tokio::test]
    async fn test_server_without_yaml_auth() {
        let yaml_content = r#"
database:
  name: "test_db"

tables:
  test:
    columns:
      id: "INTEGER PRIMARY KEY"
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = Config {
            file: temp_file.path().to_path_buf(),
            port: None,
            bind_address: "127.0.0.1".to_string(),
            protocol: Protocol::Postgres,
            username: "cli_user".to_string(),
            password: "cli_pass".to_string(),
            hot_reload: false,
            verbose: false,
            log_level: "error".to_string(),
            database: None,
        };

        let server = Server::new(config).await.unwrap();
        
        // Verify that the server's config keeps CLI auth when YAML has none
        assert_eq!(server.config.username, "cli_user");
        assert_eq!(server.config.password, "cli_pass");
    }
}