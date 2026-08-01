use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

/// Contrato del webhook enviado por monitor-agent (POST /api/ingest).
#[derive(Debug, Clone, Deserialize)]
pub struct IngestPayload {
    /// Identificador del servidor emisor ([exporter].agent_id en el agente).
    pub agent_id: String,
    pub agent_host: Option<String>,
    /// "fail" | "recover" | "heartbeat" (cualquier otro se almacena sin tocar el estado agregado).
    pub event: String,
    #[serde(default)]
    pub check_type: String,
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub message: String,
    pub value: Option<f64>,
    pub timestamp: Option<i64>,
    pub ok_count: Option<i64>,
    pub warn_count: Option<i64>,
    pub critical_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentRow {
    pub agent_id: String,
    pub agent_host: String,
    pub status: String,
    pub ok_count: i64,
    pub warn_count: i64,
    pub critical_count: i64,
    pub last_seen: i64,
    pub first_seen: i64,
    pub active_alerts: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventRow {
    pub id: i64,
    pub agent_id: String,
    pub event: String,
    pub check_type: String,
    pub target: String,
    pub status: String,
    pub message: String,
    pub value: Option<f64>,
    pub ts: i64,
    pub received_at: i64,
}

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;

             CREATE TABLE IF NOT EXISTS agents (
               agent_id       TEXT PRIMARY KEY,
               agent_host     TEXT NOT NULL DEFAULT '',
               status         TEXT NOT NULL DEFAULT 'unknown',
               ok_count       INTEGER NOT NULL DEFAULT 0,
               warn_count     INTEGER NOT NULL DEFAULT 0,
               critical_count INTEGER NOT NULL DEFAULT 0,
               last_seen      INTEGER NOT NULL DEFAULT 0,
               first_seen     INTEGER NOT NULL DEFAULT 0
             );

             CREATE TABLE IF NOT EXISTS events (
               id          INTEGER PRIMARY KEY AUTOINCREMENT,
               agent_id    TEXT NOT NULL,
               event       TEXT NOT NULL,
               check_type  TEXT NOT NULL,
               target      TEXT NOT NULL,
               status      TEXT NOT NULL,
               message     TEXT NOT NULL DEFAULT '',
               value       REAL,
               ts          INTEGER NOT NULL,
               received_at INTEGER NOT NULL
             );

             CREATE INDEX IF NOT EXISTS idx_events_agent_id ON events(agent_id, id DESC);
             CREATE INDEX IF NOT EXISTS idx_events_recent   ON events(id DESC);
             CREATE INDEX IF NOT EXISTS idx_events_active   ON events(agent_id, event, check_type, target);",
        )?;
        Ok(Self { conn })
    }

    /// Inserta el evento y actualiza el estado agregado del agente.
    pub fn ingest(&mut self, p: &IngestPayload) -> rusqlite::Result<i64> {
        let now = now_unix();
        let ts = p.timestamp.unwrap_or(now);
        let status = if p.status.trim().is_empty() {
            "unknown"
        } else {
            p.status.trim()
        };

        self.conn.execute(
            "INSERT INTO events (agent_id, event, check_type, target, status, message, value, ts, received_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                p.agent_id,
                p.event,
                p.check_type,
                p.target,
                status,
                p.message,
                p.value,
                ts,
                now
            ],
        )?;
        let id = self.conn.last_insert_rowid();

        let host = p.agent_host.as_deref().unwrap_or("");
        self.conn.execute(
            "INSERT INTO agents (agent_id, agent_host, status, last_seen, first_seen)
             VALUES (?1, ?2, 'unknown', ?3, ?3)
             ON CONFLICT(agent_id) DO UPDATE SET
               agent_host = CASE WHEN excluded.agent_host <> '' THEN excluded.agent_host ELSE agents.agent_host END,
               last_seen  = excluded.last_seen",
            params![p.agent_id, host, now],
        )?;

        match p.event.as_str() {
            "heartbeat" => {
                self.conn.execute(
                    "UPDATE agents SET status = ?1, ok_count = ?2, warn_count = ?3, critical_count = ?4
                     WHERE agent_id = ?5",
                    params![
                        status,
                        p.ok_count.unwrap_or(0),
                        p.warn_count.unwrap_or(0),
                        p.critical_count.unwrap_or(0),
                        p.agent_id
                    ],
                )?;
            }
            "fail" => {
                // Sube el nivel de gravedad si hace falta, nunca lo baja.
                self.conn.execute(
                    "UPDATE agents SET status = CASE
                       WHEN ?1 = 'critical' THEN 'critical'
                       WHEN ?1 = 'warn' AND status <> 'critical' THEN 'warn'
                       ELSE status
                     END
                     WHERE agent_id = ?2",
                    params![status, p.agent_id],
                )?;
            }
            "recover" => {
                // Si ya no quedan avisos activos para el agente, vuelve a ok.
                let active = self.active_alerts_for_agent(&p.agent_id)?;
                if active == 0 {
                    self.conn.execute(
                        "UPDATE agents SET status = 'ok' WHERE agent_id = ?1",
                        params![p.agent_id],
                    )?;
                }
            }
            _ => {} // otros eventos (p.ej. "ok") solo se almacenan
        }

        Ok(id)
    }

    fn active_alerts_for_agent(&self, agent_id: &str) -> rusqlite::Result<i64> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM (
                   SELECT 1 FROM events e
                   WHERE e.agent_id = ?1 AND e.event = 'fail'
                     AND NOT EXISTS (
                       SELECT 1 FROM events r
                       WHERE r.agent_id = e.agent_id
                         AND r.check_type = e.check_type
                         AND r.target = e.target
                         AND r.event = 'recover'
                         AND r.id > e.id
                     )
                   GROUP BY e.check_type, e.target
                 )",
                params![agent_id],
                |row| row.get(0),
            )
            .optional()
            .map(|v| v.unwrap_or(0))
    }

    pub fn list_agents(&self) -> rusqlite::Result<Vec<AgentRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT a.agent_id, a.agent_host, a.status, a.ok_count, a.warn_count, a.critical_count,
                    a.last_seen, a.first_seen,
                    (SELECT COUNT(*) FROM (
                       SELECT 1 FROM events e
                       WHERE e.agent_id = a.agent_id AND e.event = 'fail'
                         AND NOT EXISTS (
                           SELECT 1 FROM events r
                           WHERE r.agent_id = e.agent_id
                             AND r.check_type = e.check_type
                             AND r.target = e.target
                             AND r.event = 'recover'
                             AND r.id > e.id
                         )
                       GROUP BY e.check_type, e.target
                     )) AS active_alerts
             FROM agents a
             ORDER BY a.last_seen DESC, a.agent_id ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(AgentRow {
                    agent_id: row.get(0)?,
                    agent_host: row.get(1)?,
                    status: row.get(2)?,
                    ok_count: row.get(3)?,
                    warn_count: row.get(4)?,
                    critical_count: row.get(5)?,
                    last_seen: row.get(6)?,
                    first_seen: row.get(7)?,
                    active_alerts: row.get(8)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn agent_events(&self, agent_id: &str, limit: i64) -> rusqlite::Result<Vec<EventRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, agent_id, event, check_type, target, status, message, value, ts, received_at
             FROM events WHERE agent_id = ?1
             ORDER BY id DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![agent_id, limit], map_event_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Avisos activos globales: último "fail" de cada (agente, check, target)
    /// que no tiene un "recover" posterior.
    pub fn active_alerts(&self) -> rusqlite::Result<Vec<EventRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT e.id, e.agent_id, e.event, e.check_type, e.target, e.status, e.message,
                    e.value, e.ts, e.received_at
             FROM events e
             WHERE e.event = 'fail'
               AND NOT EXISTS (
                 SELECT 1 FROM events r
                 WHERE r.agent_id = e.agent_id
                   AND r.check_type = e.check_type
                   AND r.target = e.target
                   AND r.event = 'recover'
                   AND r.id > e.id
               )
               AND e.id = (SELECT MAX(e2.id) FROM events e2
                           WHERE e2.agent_id = e.agent_id
                             AND e2.check_type = e.check_type
                             AND e2.target = e.target
                             AND e2.event = 'fail')
             ORDER BY e.ts DESC",
        )?;
        let rows = stmt
            .query_map([], map_event_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}

fn map_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventRow> {
    Ok(EventRow {
        id: row.get(0)?,
        agent_id: row.get(1)?,
        event: row.get(2)?,
        check_type: row.get(3)?,
        target: row.get(4)?,
        status: row.get(5)?,
        message: row.get(6)?,
        value: row.get(7)?,
        ts: row.get(8)?,
        received_at: row.get(9)?,
    })
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(agent: &str, event: &str, check: &str, target: &str, status: &str) -> IngestPayload {
        IngestPayload {
            agent_id: agent.to_string(),
            agent_host: Some("host.example".to_string()),
            event: event.to_string(),
            check_type: check.to_string(),
            target: target.to_string(),
            status: status.to_string(),
            message: "test message".to_string(),
            value: None,
            timestamp: Some(1_700_000_000),
            ok_count: None,
            warn_count: None,
            critical_count: None,
        }
    }

    #[test]
    fn ingest_fail_recover_cycles() {
        let mut db = Database::open(":memory:").expect("open memory db");

        db.ingest(&payload("srv-a", "fail", "process", "app.exe", "critical"))
            .expect("fail");
        db.ingest(&payload("srv-b", "fail", "disk", "C", "warn"))
            .expect("fail b");

        let agents = db.list_agents().expect("agents");
        assert_eq!(agents.len(), 2);
        let srv_a = agents.iter().find(|a| a.agent_id == "srv-a").unwrap();
        let srv_b = agents.iter().find(|a| a.agent_id == "srv-b").unwrap();
        assert_eq!(srv_a.active_alerts, 1);
        assert_eq!(srv_b.active_alerts, 1);
        assert_eq!(srv_a.status, "critical");
        assert_eq!(srv_b.status, "warn");

        // recover en srv-a -> sin avisos activos -> status vuelve a ok
        db.ingest(&payload("srv-a", "recover", "process", "app.exe", "ok"))
            .expect("recover a");
        let agents = db.list_agents().expect("agents");
        let srv_a = agents.iter().find(|a| a.agent_id == "srv-a").unwrap();
        assert_eq!(srv_a.active_alerts, 0);
        assert_eq!(srv_a.status, "ok");

        // los avisos activos globales solo tienen srv-b
        let alerts = db.active_alerts().expect("alerts");
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].agent_id, "srv-b");
        assert_eq!(alerts[0].check_type, "disk");
    }

    #[test]
    fn heartbeat_updates_summary() {
        let mut db = Database::open(":memory:").expect("open memory db");
        db.ingest(&payload("srv-a", "heartbeat", "summary", "server", "warn"))
            .expect("heartbeat");
        // el test payload no lleva counts; comprobar que el agente se crea con last_seen
        let agents = db.list_agents().expect("agents");
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].last_seen, now_unix());
        assert!(agents[0].first_seen > 0);
    }

    #[test]
    fn active_alerts_dedup_repeated_fails() {
        let mut db = Database::open(":memory:").expect("open memory db");
        // re-envíos por cooldown: mismo check, mismo target, tres fails
        db.ingest(&payload("srv-a", "fail", "http", "https://x/health", "critical"))
            .expect("fail 1");
        db.ingest(&payload("srv-a", "fail", "http", "https://x/health", "critical"))
            .expect("fail 2");
        db.ingest(&payload("srv-a", "fail", "http", "https://x/health", "critical"))
            .expect("fail 3");

        let alerts = db.active_alerts().expect("alerts");
        assert_eq!(alerts.len(), 1, "repeated fails must dedupe to one active alert");
        let agents = db.list_agents().expect("agents");
        assert_eq!(agents[0].active_alerts, 1);
    }
}
