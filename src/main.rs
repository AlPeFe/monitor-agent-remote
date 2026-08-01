mod api;
mod config;
mod db;

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::config::load_config;
use crate::db::Database;

#[tokio::main]
async fn main() {
    let config = load_config("config.toml");
    let db = Arc::new(
        Mutex::new(
            Database::open(&config.db_path).expect("could not open sqlite database"),
        ),
    );
    let app = api::router(db, config.token.clone());

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .unwrap_or_else(|e| {
            eprintln!("monitor-agent-remote: could not bind {}: {e}", config.listen);
            std::process::exit(1);
        });
    eprintln!("monitor-agent-remote: listening on {}", config.listen);
    eprintln!(
        "monitor-agent-remote: sqlite at {}",
        config.db_path
    );

    axum::serve(listener, app).await.expect("server error");
}
