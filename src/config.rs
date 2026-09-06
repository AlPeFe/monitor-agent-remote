use std::fs;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

pub const DEFAULT_LISTEN: &str = "127.0.0.1:9600";
pub const DEFAULT_DB_PATH: &str = "monitor.db";

/// Configuración de arranque (OPCIONAL). El fichero `config.toml` solo existe
/// para elegir la ruta de la base de datos antes del primer arranque; el resto
/// de opciones (listen, token) se gestionan desde el panel de ajustes de la UI
/// y se persisten en SQLite.
#[derive(Debug, Clone, Deserialize)]
pub struct Bootstrap {
    #[serde(default = "default_db_path")]
    pub db_path: String,
    /// Semillas opcionales para el primer arranque (se guardan en SQLite y
    /// después se pueden cambiar desde la web).
    #[serde(default)]
    pub listen: Option<SocketAddr>,
    #[serde(default)]
    pub token: Option<String>,
}

fn default_db_path() -> String {
    DEFAULT_DB_PATH.to_string()
}

/// Lee `config.toml` si existe. Sin fichero, usa valores por defecto.
pub fn load_bootstrap(path: &str) -> Bootstrap {
    match fs::read_to_string(path) {
        Ok(content) => toml::from_str(&content).unwrap_or_else(|e| {
            eprintln!("monitor-agent-remote: invalid {path}: {e}");
            std::process::exit(1);
        }),
        Err(_) => Bootstrap {
            db_path: default_db_path(),
            listen: None,
            token: None,
        },
    }
}

/// Configuración editable y persistida en SQLite (tabla `settings`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub listen: SocketAddr,
    pub token: Option<String>,
    /// Política de retención de datos (días de eventos, horas de incidencias,
    /// minutos de offline). Se persiste en `settings` y la aplica el worker.
    #[serde(default)]
    pub retention: crate::db::Retention,
}
