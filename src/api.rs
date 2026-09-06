use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, RwLock, watch};

use crate::config::Settings;
use crate::db::{AgentCheck, AgentRow, Database, EventRow, IngestPayload, Retention};

const INDEX_HTML: &str = include_str!("../web/index.html");

#[derive(Clone)]
pub struct ApiState {
    db: Arc<Mutex<Database>>,
    settings: Arc<RwLock<Settings>>,
    db_path: String,
    listen_tx: watch::Sender<SocketAddr>,
}

#[derive(Debug, Deserialize)]
pub struct LimitQuery {
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SettingsUpdate {
    pub listen: String,
    /// Cadena vacía = desactivar la autenticación.
    #[serde(default)]
    pub token: String,
    /// Retención opcional: si se envía, se persiste y aplica.
    #[serde(default)]
    pub retention: Option<Retention>,
}

#[derive(Debug, Serialize)]
struct IngestResponse {
    ok: bool,
    stored: i64,
    agent_id: String,
}

pub fn router(
    db: Arc<Mutex<Database>>,
    settings: Arc<RwLock<Settings>>,
    db_path: String,
    listen_tx: watch::Sender<SocketAddr>,
) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/api/ingest", post(ingest_handler))
        .route("/api/agents", get(agents_handler))
        .route("/api/agents/{agent_id}/events", get(agent_events_handler))
        .route("/api/agents/{agent_id}/checks", get(agent_checks_handler))
        .route("/api/alerts", get(alerts_handler))
        .route("/api/settings", get(get_settings_handler).put(update_settings_handler))
        .with_state(ApiState {
            db,
            settings,
            db_path,
            listen_tx,
        })
}

async fn index_handler() -> impl IntoResponse {
    // no-store: el panel cambia con cada deploy; evitar HTML cacheado.
    ([("Cache-Control", "no-store")], Html(INDEX_HTML))
}

/// POST /api/ingest — contrato del webhook de monitor-agent.
async fn ingest_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<IngestPayload>,
) -> (StatusCode, Json<Value>) {
    let expected = state.settings.read().await.token.clone();
    if let Some(expected) = expected {
        let provided = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if provided != format!("Bearer {expected}") {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "ok": false, "error": "unauthorized" })),
            );
        }
    }
    if payload.agent_id.trim().is_empty() || payload.event.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "agent_id and event are required" })),
        );
    }

    let mut db = state.db.lock().await;
    match db.ingest(&payload) {
        Ok(stored) => (
            StatusCode::OK,
            Json(
                serde_json::to_value(IngestResponse {
                    ok: true,
                    stored,
                    agent_id: payload.agent_id,
                })
                .unwrap_or(json!({ "ok": true })),
            ),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        ),
    }
}

/// GET /api/agents — estado agregado de todos los servidores.
async fn agents_handler(State(state): State<ApiState>) -> Result<Json<Vec<AgentRow>>, StatusCode> {
    let db = state.db.lock().await;
    db.list_agents().map(Json).map_err(|e| {
        eprintln!("monitor-agent-remote: list agents failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

/// GET /api/agents/{agent_id}/checks?limit=50 — historial de un servidor.
async fn agent_events_handler(
    State(state): State<ApiState>,
    Path(agent_id): Path<String>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<Vec<EventRow>>, StatusCode> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let db = state.db.lock().await;
    db.agent_events(&agent_id, limit).map(Json).map_err(|e| {
        eprintln!("monitor-agent-remote: agent events failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

/// GET /api/agents/{id}/checks — estado actual por check (uptime-kuma style).
async fn agent_checks_handler(
    State(state): State<ApiState>,
    Path(agent_id): Path<String>,
) -> Result<Json<Vec<AgentCheck>>, StatusCode> {
    let db = state.db.lock().await;
    db.list_agent_checks(&agent_id).map(Json).map_err(|e| {
        eprintln!("monitor-agent-remote: agent checks failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

/// GET /api/alerts — avisos activos globales (fail sin recover posterior).
async fn alerts_handler(State(state): State<ApiState>) -> Result<Json<Vec<EventRow>>, StatusCode> {
    let db = state.db.lock().await;
    db.active_alerts().map(Json).map_err(|e| {
        eprintln!("monitor-agent-remote: active alerts failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

/// GET /api/settings — configuración actual (persistida en SQLite).
async fn get_settings_handler(State(state): State<ApiState>) -> Json<Value> {
    let s = state.settings.read().await;
    Json(json!({
        "listen": s.listen.to_string(),
        "token": s.token.clone().unwrap_or_default(),
        "db_path": state.db_path,
        "ingest_url": format!("http://{}/api/ingest", s.listen),
        "retention": s.retention,
    }))
}

/// PUT /api/settings — guarda listen y token; aplica en vivo.
async fn update_settings_handler(
    State(state): State<ApiState>,
    Json(update): Json<SettingsUpdate>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let addr: SocketAddr = update
        .listen
        .trim()
        .parse()
        .map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": "invalid listen address" })),
            )
        })?;
    let token = if update.token.is_empty() {
        None
    } else {
        Some(update.token)
    };

    {
        let mut db = state.db.lock().await;
        db.set_settings(&addr.to_string(), token.as_deref()).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "ok": false, "error": e.to_string() })),
            )
        })?;
        if let Some(retention) = update.retention {
            db.set_retention(&retention).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "ok": false, "error": e.to_string() })),
                )
            })?;
        }
    }
    {
        let mut s = state.settings.write().await;
        s.listen = addr;
        s.token = token.clone();
        if let Some(retention) = update.retention {
            s.retention = retention;
        }
    }

    // Solo avisa al supervisor si el puerto/address cambió de verdad.
    let rebound = state.listen_tx.send_if_modified(|cur| {
        if *cur != addr {
            *cur = addr;
            true
        } else {
            false
        }
    });

    let s = state.settings.read().await;
    Ok(Json(json!({
        "ok": true,
        "listen": addr.to_string(),
        "token": token.unwrap_or_default(),
        "retention": s.retention,
        "rebound": rebound,
    })))
}
