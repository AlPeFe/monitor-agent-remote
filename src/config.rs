use std::fs;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub listen: SocketAddr,
    #[serde(default = "default_db_path")]
    pub db_path: String,
    /// Token opcional: si está definido, /api/ingest exige `Authorization: Bearer <token>`.
    #[serde(default)]
    pub token: Option<String>,
}

fn default_db_path() -> String {
    "monitor.db".to_string()
}

pub fn load_config(path: &str) -> Config {
    let content = fs::read_to_string(path).expect("cannot read config file");
    toml::from_str(&content).expect("invalid toml format")
}
