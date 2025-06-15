#[cfg(test)]
use crate::yaml::schema::{AuthConfig, DatabaseInfo};
use std::io::Write;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_parse_yaml_with_auth() {
    let yaml_content = r#"
database:
  name: "test_db"
  auth:
    username: "testuser"
    password: "testpass"

tables:
  users:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "VARCHAR(100)"
    data:
      - id: 1
        name: "Test"
"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(yaml_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let (database, auth_config) = crate::yaml::parse_yaml_database(temp_file.path())
        .await
        .unwrap();

    assert_eq!(database.name, "test_db");
    assert!(auth_config.is_some());

    let auth = auth_config.unwrap();
    assert_eq!(auth.username, "testuser");
    assert_eq!(auth.password, "testpass");
}

#[tokio::test]
async fn test_parse_yaml_without_auth() {
    let yaml_content = r#"
database:
  name: "test_db"

tables:
  users:
    columns:
      id: "INTEGER PRIMARY KEY"
"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(yaml_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let (database, auth_config) = crate::yaml::parse_yaml_database(temp_file.path())
        .await
        .unwrap();

    assert_eq!(database.name, "test_db");
    assert!(auth_config.is_none());
}

#[test]
fn test_auth_config_serialization() {
    let auth = AuthConfig {
        username: "user".to_string(),
        password: "pass".to_string(),
    };

    let serialized = serde_yaml::to_string(&auth).unwrap();
    assert!(serialized.contains("username: user"));
    assert!(serialized.contains("password: pass"));

    let deserialized: AuthConfig = serde_yaml::from_str(&serialized).unwrap();
    assert_eq!(deserialized.username, "user");
    assert_eq!(deserialized.password, "pass");
}

#[test]
fn test_database_info_with_optional_auth() {
    // Test with auth
    let yaml_with_auth = r#"
name: "db1"
auth:
  username: "user1"
  password: "pass1"
"#;

    let db_info: DatabaseInfo = serde_yaml::from_str(yaml_with_auth).unwrap();
    assert_eq!(db_info.name, "db1");
    assert!(db_info.auth.is_some());
    assert_eq!(db_info.auth.as_ref().unwrap().username, "user1");

    // Test without auth
    let yaml_without_auth = r#"
name: "db2"
"#;

    let db_info: DatabaseInfo = serde_yaml::from_str(yaml_without_auth).unwrap();
    assert_eq!(db_info.name, "db2");
    assert!(db_info.auth.is_none());
}

#[test]
fn test_auth_override_behavior() {
    // This tests the expected behavior that YAML auth should override CLI args
    // The actual override is done in server/mod.rs, but we can test the data structure
    let db_info = DatabaseInfo {
        name: "test_db".to_string(),
        auth: Some(AuthConfig {
            username: "yaml_user".to_string(),
            password: "yaml_pass".to_string(),
        }),
    };

    // Verify auth is properly stored
    assert!(db_info.auth.is_some());
    let auth = db_info.auth.unwrap();
    assert_eq!(auth.username, "yaml_user");
    assert_eq!(auth.password, "yaml_pass");
}
