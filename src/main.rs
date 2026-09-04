mod api;
mod config;
mod db;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tokio::sync::{Mutex, RwLock, watch};

use crate::config::{Settings, DEFAULT_LISTEN, load_bootstrap};
use crate::db::Database;

#[tokio::main]
async fn main() {
    let bootstrap = load_bootstrap("config.toml");
    let db = Arc::new(Mutex::new(
        Database::open(&bootstrap.db_path).expect("could not open sqlite database"),
    ));

    // Lee la configuración persistida; si es la primera vez, la siembra
    // desde el bootstrap (o desde los valores por defecto).
    let (listen, token) = {
        let mut db = db.lock().await;
        match db.get_settings().expect("read settings") {
            Some((listen, token)) => {
                let addr: SocketAddr = listen.parse().expect("stored listen is invalid");
                (addr, token)
            }
            None => {
                let addr = bootstrap
                    .listen
                    .unwrap_or_else(|| DEFAULT_LISTEN.parse().expect("default listen"));
                let token = bootstrap.token.clone();
                db.set_settings(&addr.to_string(), token.as_deref())
                    .expect("seed settings");
                (addr, token)
            }
        }
    };
    eprintln!("monitor-agent-remote: sqlite at {}", bootstrap.db_path);

    let settings = Arc::new(RwLock::new(Settings { listen, token }));

    let (listen_tx, listen_rx) = watch::channel(listen);
    let (stop_tx, stop_rx) = watch::channel(false);

    let app = api::router(db, settings, bootstrap.db_path, listen_tx);

    tokio::spawn(serve_loop(app, listen_rx, stop_rx));

    tokio::signal::ctrl_c().await.expect("ctrl_c handler");
    eprintln!("monitor-agent-remote: shutting down");
    let _ = stop_tx.send(true);
}

/// Sirve la app rebindeando cuando cambia `listen` desde la UI (sin reiniciar).
async fn serve_loop(
    app: Router,
    mut listen_rx: watch::Receiver<SocketAddr>,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut first_bind = true;
    loop {
        let addr = *listen_rx.borrow();
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("monitor-agent-remote: could not bind {addr}: {e}");
                if first_bind {
                    std::process::exit(1);
                }
                // Espera a un cambio de listen para reintentar.
                let _ = listen_rx.changed().await;
                continue;
            }
        };
        first_bind = false;
        eprintln!("monitor-agent-remote: listening on {addr}");

        // No se clonan los receivers aquí: un clon recién creado devuelve
        // `changed()` de inmediato (busy-loop). Se mueven dentro del select.
        tokio::select! {
            _ = axum::serve(listener, app.clone()) => {}
            _ = stop_rx.changed() => {}
            _ = listen_rx.changed() => {}
        }

        if *stop_rx.borrow() {
            eprintln!("monitor-agent-remote: shutting down");
            return;
        }
        // listen cambió o el server murió: rebind en el siguiente ciclo.
    }
}
