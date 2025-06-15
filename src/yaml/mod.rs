pub mod parser;
pub mod schema;
pub mod watcher;

#[cfg(test)]
mod tests;

pub use parser::parse_yaml_database;
pub use schema::{AuthConfig, YamlColumn, YamlDatabase, YamlTable};
pub use watcher::FileWatcher;

// For fuzzing
pub fn parse_yaml_string(yaml_str: &str) -> Result<YamlDatabase, serde_yaml::Error> {
    serde_yaml::from_str(yaml_str)
}
