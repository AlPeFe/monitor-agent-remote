use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router, response::Html};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;

use crate::db::{AgentRow, Database, EventRow, IngestPayload};

const INDEX_HTML: &str = include_str!("../web/index.html");

#[derive(Clone)]
pub struct ApiState {
    db: Arc<Mutex<Database>>,
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LimitQuery {
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct IngestResponse {
    ok: bool,
    stored: i64,
    agent_id: String,
}

pub fn router(db: Arc<Mutex<Database>>, token: Option<String>) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/api/ingest", post(ingest_handler))
        .route("/api/agents", get(agents_handler))
        .route("/api/agents/{agent_id}/events", get(agent_events_handler))
        .route("/api/alerts", get(alerts_handler))
        .with_state(ApiState { db, token })
}

async fn index_handler() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// POST /api/ingest — contrato del webhook de monitor-agent.
async fn ingest_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(payload): Json<IngestPayload>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Some(expected) = &state.token {
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

/// GET /api/agents/{agent_id}/events?limit=50 — historial de un servidor.
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

/// GET /api/alerts — avisos activos globales (fail sin recover posterior).
async fn alerts_handler(State(state): State<ApiState>) -> Result<Json<Vec<EventRow>>, StatusCode> {
    let db = state.db.lock().await;
    db.active_alerts().map(Json).map_err(|e| {
        eprintln!("monitor-agent-remote: active alerts failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}
