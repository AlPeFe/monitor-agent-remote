mod alert;
mod api;
mod config;
mod db;
mod logging;
mod watchdog;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tokio::sync::{Mutex, RwLock, watch};

use crate::config::{Settings, DEFAULT_LISTEN, load_bootstrap};
use crate::db::Database;

#[tokio::main]
async fn main() {
    // Log a fichero desde el primer momento: como tarea programada o servicio,
    // stderr no lo recoge nadie y sin esto no queda traza de nada.
    logging::init(Some(&logging::default_log_path()));

    let bootstrap = load_bootstrap("config.toml");
    let db = Arc::new(Mutex::new(
        Database::open(&bootstrap.db_path).expect("could not open sqlite database"),
    ));

    // Lee la configuración persistida; si es la primera vez, la siembra
    // desde el bootstrap (o desde los valores por defecto).
    let (listen, token, retention, alert) = {
        let mut db = db.lock().await;
        match db.get_settings().expect("read settings") {
            Some((listen, token)) => {
                let addr: SocketAddr = listen.parse().expect("stored listen is invalid");
                let retention = db.get_retention();
                let alert = db.get_alert_settings();
                (addr, token, retention, alert)
            }
            None => {
                let addr = bootstrap
                    .listen
                    .unwrap_or_else(|| DEFAULT_LISTEN.parse().expect("default listen"));
                let token = bootstrap.token.clone();
                db.set_settings(&addr.to_string(), token.as_deref())
                    .expect("seed settings");
                let retention = db.get_retention();
                let alert = db.get_alert_settings();
                (addr, token, retention, alert)
            }
        }
    };
    crate::log_info!("sqlite at {}", bootstrap.db_path);

    let settings = Arc::new(RwLock::new(Settings {
        listen,
        token,
        retention,
        alert,
    }));

    let (listen_tx, listen_rx) = watch::channel(listen);
    let (stop_tx, stop_rx) = watch::channel(false);

    let app = api::router(db.clone(), settings.clone(), bootstrap.db_path, listen_tx);

    tokio::spawn(serve_loop(app, listen_rx, stop_rx));
    tokio::spawn(retention_worker(db.clone(), settings.clone()));
    // Watchdog de servidores sin señales (avisos por correo).
    tokio::spawn(watchdog::run(db, settings));

    tokio::signal::ctrl_c().await.expect("ctrl_c handler");
    crate::log_info!("shutting down");
    let _ = stop_tx.send(true);
}

/// Worker de retención: cada hora purga los datos viejos según la política
/// configurada (días de eventos, horas de incidencias activas y checks en ok
/// inactivos). Coge la política actual de `settings` (se puede cambiar en vivo
/// desde la UI) y, si algo queda fuera de rango tras un cambio, lo aplica en
/// el siguiente ciclo.
async fn retention_worker(db: Arc<Mutex<Database>>, settings: Arc<RwLock<Settings>>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
    // Primera purga al arrancar (sin esperar la primera hora).
    interval.tick().await;
    loop {
        interval.tick().await;
        let retention = settings.read().await.retention;
        let mut db = db.lock().await;
        match db.purge_expired(&retention) {
            Ok(n) => {
                if n > 0 {
                    crate::log_info!("retención: purgadas {n} filas");
                }
            }
            Err(e) => crate::log_error!("retención falló: {e}"),
        }
    }
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
                crate::log_error!("could not bind {addr}: {e}");
                if first_bind {
                    std::process::exit(1);
                }
                // Espera a un cambio de listen para reintentar.
                let _ = listen_rx.changed().await;
                continue;
            }
        };
        first_bind = false;
        crate::log_info!("listening on {addr}");

        // No se clonan los receivers aquí: un clon recién creado devuelve
        // `changed()` de inmediato (busy-loop). Se mueven dentro del select.
        tokio::select! {
            _ = axum::serve(listener, app.clone()) => {}
            _ = stop_rx.changed() => {}
            _ = listen_rx.changed() => {}
        }

        if *stop_rx.borrow() {
            crate::log_info!("shutting down");
            return;
        }
        // listen cambió o el server murió: rebind en el siguiente ciclo.
    }
}
