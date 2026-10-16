//! Logging ligero de la central: escribe a stderr (visible si se lanza a mano)
//! y a `monitor-agent-remote.log` junto a la configuración. Sin esto, cuando la
//! central corre como tarea programada/servicio no queda NINGUNA traza de lo
//! que pasa (stderr no lo recoge nadie).

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub const LOG_FILE: &str = "monitor-agent-remote.log";

static FILE: Mutex<Option<std::fs::File>> = Mutex::new(None);

/// Abre (o crea) el fichero de log en modo append. Sin ruta, solo stderr.
pub fn init(log_path: Option<&Path>) {
    let file = log_path.and_then(|p| {
        if let Some(parent) = p.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
            .map_err(|e| eprintln!("monitor-agent-remote: no se pudo abrir el log {}: {e}", p.display()))
            .ok()
    });
    *FILE.lock().unwrap_or_else(|p| p.into_inner()) = file;
}

/// Ruta por defecto del log: junto a la configuración (CWD del proceso).
pub fn default_log_path() -> PathBuf {
    std::env::current_dir()
        .map(|dir| dir.join(LOG_FILE))
        .unwrap_or_else(|_| PathBuf::from(LOG_FILE))
}

fn now_stamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    format!("{secs}")
}

/// Escribe una línea a stderr y (si está abierto) al fichero.
pub fn write(level: &str, msg: &str) {
    let line = format!("[{}] monitor-agent-remote {level}: {msg}", now_stamp());
    eprintln!("{line}");
    if let Ok(mut guard) = FILE.lock()
        && let Some(f) = guard.as_mut()
    {
        let _ = writeln!(f, "{line}");
    }
}

#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => { $crate::logging::write("INFO", &format!($($arg)*)) };
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => { $crate::logging::write("ERROR", &format!($($arg)*)) };
}
