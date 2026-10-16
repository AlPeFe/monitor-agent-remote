use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::alert::AlertSettings;

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
    #[serde(default)]
    pub value: Option<f64>,
    pub timestamp: Option<i64>,
    /// recover con gone=true: el check desapareció de la config del agente;
    /// la central debe borrarlo de agent_checks (no dejarlo en ok fantasma).
    #[serde(default)]
    pub gone: Option<bool>,
    pub ok_count: Option<i64>,
    pub warn_count: Option<i64>,
    pub critical_count: Option<i64>,
    /// Inventario de checks que manda el heartbeat: permite pintar TODOS los
    /// checks del servidor (también los que están OK) y borrar los retirados.
    /// Los agentes anteriores no lo mandan (queda vacío y no se toca nada).
    #[serde(default)]
    pub checks: Vec<CheckSnapshot>,
}

/// Una comprobación dentro del inventario del heartbeat.
#[derive(Debug, Clone, Deserialize)]
pub struct CheckSnapshot {
    pub check_type: String,
    pub target: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub value: Option<f64>,
}

/// Estado persistido del aviso de "servidor sin señales".
#[derive(Debug, Clone, PartialEq)]
pub struct OfflineAlertRow {
    pub agent_id: String,
    /// Cuándo se detectó la caída (para el mensaje del correo).
    pub since: i64,
    /// Último aviso ENVIADO con éxito (0 = pendiente).
    pub last_sent_at: i64,
    /// Backoff tras un fallo de envío.
    pub retry_after: i64,
}

/// Qué hacer con cada servidor en un ciclo del watchdog.
#[derive(Debug, Clone, PartialEq)]
pub enum OfflineAction {
    /// Primera detección de la caída o reintento de un aviso que no salió.
    NotifyDown { agent_id: String, since: i64 },
    /// Recordatorio mientras sigue caído.
    RenotifyDown { agent_id: String, since: i64 },
    /// El servidor ha vuelto.
    NotifyUp { agent_id: String, since: i64 },
}

/// Decide los avisos de caída/recuperación de servidores.
///
/// Función pura (sin BD ni SMTP) para poder probarla: recibe el último
/// `last_seen` de cada agente y el estado persistido de los avisos.
pub fn decide_offline_alerts(
    agents: &[(String, i64)],
    state: &[OfflineAlertRow],
    now: i64,
    offline_seconds: i64,
    cooldown_seconds: i64,
) -> Vec<OfflineAction> {
    let mut acciones = Vec::new();
    for (agent_id, last_seen) in agents {
        let row = state.iter().find(|r| &r.agent_id == agent_id);
        // offline_minutes = 0 significa "no vigilar caídas por falta de señales".
        let offline =
            *last_seen > 0 && offline_seconds > 0 && now.saturating_sub(*last_seen) > offline_seconds;
        match (offline, row) {
            (true, None) => acciones.push(OfflineAction::NotifyDown {
                agent_id: agent_id.clone(),
                since: now,
            }),
            (true, Some(r)) => {
                if now < r.retry_after {
                    continue; // backoff: el envío anterior falló hace poco
                }
                if r.last_sent_at == 0 {
                    // Aviso pendiente (falló el envío o el proceso murió antes).
                    acciones.push(OfflineAction::NotifyDown {
                        agent_id: agent_id.clone(),
                        since: r.since,
                    });
                } else if cooldown_seconds > 0
                    && now.saturating_sub(r.last_sent_at) >= cooldown_seconds
                {
                    acciones.push(OfflineAction::RenotifyDown {
                        agent_id: agent_id.clone(),
                        since: r.since,
                    });
                }
            }
            (false, Some(r)) => acciones.push(OfflineAction::NotifyUp {
                agent_id: agent_id.clone(),
                since: r.since,
            }),
            (false, None) => {}
        }
    }
    acciones
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

#[derive(Debug, Clone, Serialize)]
pub struct AgentCheck {
    pub agent_id: String,
    pub check_type: String,
    pub target: String,
    pub status: String,
    pub message: String,
    pub value: Option<f64>,
    pub last_event: String,
    pub ts: i64,
}

/// Política de retención de datos de la central. Se persiste en `settings`
/// y se aplica de forma periódica por el worker de purga.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Retention {
    /// Días que se conservan los eventos (fails/recovers) en `events`.
    /// 0 = conservar indefinidamente.
    #[serde(default)]
    pub events_days: i64,
    /// Horas que se conserva la última incidencia activa sin recover en
    /// `events`. 0 = conservar indefinidamente.
    #[serde(default)]
    pub alerts_hours: i64,
    /// Minutos de inactividad para considerar a un agente "offline" en el
    /// panel (por ejemplo 5 minutos con el heartbeat a 60 s). 0 = nunca.
    #[serde(default)]
    pub offline_minutes: i64,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            events_days: 30,
            alerts_hours: 24 * 7, // 7 días de incidencias sin recuperar
            offline_minutes: 5,
        }
    }
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
             CREATE INDEX IF NOT EXISTS idx_events_active   ON events(agent_id, event, check_type, target);

             CREATE TABLE IF NOT EXISTS agent_checks (
               agent_id   TEXT NOT NULL,
               check_type TEXT NOT NULL,
               target     TEXT NOT NULL,
               status     TEXT NOT NULL DEFAULT 'unknown',
               message    TEXT NOT NULL DEFAULT '',
               value      REAL,
               last_event TEXT NOT NULL DEFAULT '',
               ts         INTEGER NOT NULL,
               PRIMARY KEY (agent_id, check_type, target)
             );

             CREATE TABLE IF NOT EXISTS settings (
               key    TEXT PRIMARY KEY,
               listen TEXT NOT NULL,
               token  TEXT,
               retention_events_days    INTEGER NOT NULL DEFAULT 30,
               retention_alerts_hours   INTEGER NOT NULL DEFAULT 168,
               retention_offline_minutes INTEGER NOT NULL DEFAULT 5,
               alert_json TEXT NOT NULL DEFAULT ''
             );

             -- Estado del aviso de servidor-sin-senales: que servidores estan
             -- caidos y a quien se le aviso ya (sobrevive a reinicios).
             CREATE TABLE IF NOT EXISTS offline_alerts (
               agent_id     TEXT PRIMARY KEY,
               since        INTEGER NOT NULL,
               last_sent_at INTEGER NOT NULL DEFAULT 0,
               retry_after  INTEGER NOT NULL DEFAULT 0
             );",
        )?;

        // Migración idempotente: las BDs creadas por versiones anteriores
        // (0.1.x / primeras 0.2.x) no tienen las columnas de retención. Se
        // añaden SOLO si faltan; antes este bloque corría incondicionalmente
        // en cada arranque y la segunda ejecución panikeaba con
        // "duplicate column name: retention_events_days" (borrar la DB
        // "arreglaba" el arranque porque la tabla se recreaba desde cero).
        let retention_cols: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('settings') WHERE name LIKE 'retention_%'",
            [],
            |r| r.get(0),
        )?;
        if retention_cols < 3 {
            conn.execute_batch(
                "ALTER TABLE settings ADD COLUMN retention_events_days INTEGER NOT NULL DEFAULT 30;
                 ALTER TABLE settings ADD COLUMN retention_alerts_hours INTEGER NOT NULL DEFAULT 168;
                 ALTER TABLE settings ADD COLUMN retention_offline_minutes INTEGER NOT NULL DEFAULT 5;",
            )?;
        }
        // Migración idempotente de la columna de alertas (0.2.5+).
        let alert_col: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('settings') WHERE name = 'alert_json'",
            [],
            |r| r.get(0),
        )?;
        if alert_col == 0 {
            conn.execute(
                "ALTER TABLE settings ADD COLUMN alert_json TEXT NOT NULL DEFAULT ''",
                [],
            )?;
        }
        Ok(Self { conn })
    }

    /// Lee la configuración persistida (fila única 'default').
    pub fn get_settings(&self) -> rusqlite::Result<Option<(String, Option<String>)>> {
        self.conn
            .query_row(
                "SELECT listen, token FROM settings WHERE key = 'default'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
    }

    /// Guarda la configuración. `token = None` desactiva la autenticación.
    pub fn set_settings(&mut self, listen: &str, token: Option<&str>) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO settings (key, listen, token) VALUES ('default', ?1, ?2)
             ON CONFLICT(key) DO UPDATE SET
               listen = excluded.listen,
               token  = excluded.token",
            params![listen, token],
        )?;
        Ok(())
    }

    /// Lee la política de retención persistida (si la fila aún no existe o
    /// las columnas no se han migrado, devuelve los valores por defecto).
    pub fn get_retention(&self) -> Retention {
        self.conn
            .query_row(
                "SELECT retention_events_days, retention_alerts_hours, retention_offline_minutes
                 FROM settings WHERE key = 'default'",
                [],
                |r| {
                    Ok(Retention {
                        events_days: r.get(0)?,
                        alerts_hours: r.get(1)?,
                        offline_minutes: r.get(2)?,
                    })
                },
            )
            .unwrap_or_else(|_| Retention::default())
    }

    /// Lee la configuración de alertas por correo (JSON en `settings`).
    pub fn get_alert_settings(&self) -> AlertSettings {
        self.conn
            .query_row(
                "SELECT alert_json FROM settings WHERE key = 'default'",
                [],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .and_then(|json| serde_json::from_str::<AlertSettings>(&json).ok())
            .unwrap_or_default()
    }

    /// Guarda la configuración de alertas por correo.
    pub fn set_alert_settings(&mut self, alert: &AlertSettings) -> rusqlite::Result<()> {
        let json = serde_json::to_string(alert)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        self.conn.execute(
            "INSERT INTO settings (key, listen, token, alert_json) VALUES ('default', '', NULL, ?1)
             ON CONFLICT(key) DO UPDATE SET alert_json = excluded.alert_json",
            params![json],
        )?;
        Ok(())
    }

    // ------------------------------------------------ avisos de servidor caído

    pub fn list_offline_alerts(&self) -> rusqlite::Result<Vec<OfflineAlertRow>> {
        let mut stmt = self
            .conn
            .prepare("SELECT agent_id, since, last_sent_at, retry_after FROM offline_alerts")?;
        let rows = stmt
            .query_map([], |r| {
                Ok(OfflineAlertRow {
                    agent_id: r.get(0)?,
                    since: r.get(1)?,
                    last_sent_at: r.get(2)?,
                    retry_after: r.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Marca un servidor como "caído y pendiente de avisar" (sin tocar el
    /// estado si ya lo estaba).
    pub fn insert_offline_alert(&mut self, agent_id: &str, since: i64) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO offline_alerts (agent_id, since, last_sent_at, retry_after)
             VALUES (?1, ?2, 0, 0)",
            params![agent_id, since],
        )?;
        Ok(())
    }

    /// El aviso salió: guarda cuándo (para el cooldown) y limpia el backoff.
    pub fn mark_offline_alert_sent(&mut self, agent_id: &str, ts: i64) -> rusqlite::Result<()> {
        self.conn.execute(
            "UPDATE offline_alerts SET last_sent_at = ?2, retry_after = 0 WHERE agent_id = ?1",
            params![agent_id, ts],
        )?;
        Ok(())
    }

    /// No se pudo enviar: no se reintenta antes de `retry_after`.
    pub fn mark_offline_alert_error(
        &mut self,
        agent_id: &str,
        retry_after: i64,
    ) -> rusqlite::Result<()> {
        self.conn.execute(
            "UPDATE offline_alerts SET retry_after = ?2 WHERE agent_id = ?1",
            params![agent_id, retry_after],
        )?;
        Ok(())
    }

    /// El servidor ha vuelto: olvida su aviso de caída.
    pub fn delete_offline_alert(&mut self, agent_id: &str) -> rusqlite::Result<()> {
        self.conn.execute(
            "DELETE FROM offline_alerts WHERE agent_id = ?1",
            params![agent_id],
        )?;
        Ok(())
    }

    /// Guarda la política de retención (UPSERT sobre la fila 'default').
    pub fn set_retention(&mut self, retention: &Retention) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO settings (key, listen, token, retention_events_days, retention_alerts_hours, retention_offline_minutes)
             VALUES ('default', '', NULL, ?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET
               retention_events_days       = excluded.retention_events_days,
               retention_alerts_hours      = excluded.retention_alerts_hours,
               retention_offline_minutes   = excluded.retention_offline_minutes",
            params![
                retention.events_days,
                retention.alerts_hours,
                retention.offline_minutes
            ],
        )?;
        Ok(())
    }

    /// Borra los datos viejos según la política de retención.
    /// Devuelve el número de filas borradas en total.
    pub fn purge_expired(&mut self, retention: &Retention) -> rusqlite::Result<i64> {
        let now = now_unix();
        let mut deleted = 0usize;

        // 1) Eventos (fails/recovers) más antiguos que events_days.
        if retention.events_days > 0 {
            let cutoff = now - retention.events_days * 86_400;
            deleted += self.conn.execute(
                "DELETE FROM events WHERE ts < ?1 AND event <> 'heartbeat'",
                params![cutoff],
            )?;
        }

        // 2) Incidencias activas (último fail sin recover) abiertas más de
        //    alerts_hours. Solo borra el fail si no hay ningún recover posterior
        //    (si lo hay, ya lo habrá borrado la regla 1 por antigüedad).
        let mut purged_agents: Vec<String> = Vec::new();
        if retention.alerts_hours > 0 {
            let cutoff = now - retention.alerts_hours * 3600;
            // Averigua qué agentes pierden incidencias antes de borrar.
            let mut stmt = self.conn.prepare(
                "SELECT DISTINCT agent_id FROM events
                 WHERE event = 'fail'
                   AND ts < ?1
                   AND NOT EXISTS (
                     SELECT 1 FROM events r
                     WHERE r.agent_id = events.agent_id
                       AND r.check_type = events.check_type
                       AND r.target = events.target
                       AND r.event = 'recover'
                       AND r.id > events.id
                   )",
            )?;
            let rows = stmt.query_map(params![cutoff], |row| row.get::<_, String>(0))?;
            for row in rows {
                purged_agents.push(row?);
            }
            deleted += self.conn.execute(
                "DELETE FROM events
                 WHERE id IN (
                   SELECT e.id FROM events e
                   WHERE e.event = 'fail'
                     AND e.ts < ?1
                     AND NOT EXISTS (
                       SELECT 1 FROM events r
                       WHERE r.agent_id = e.agent_id
                         AND r.check_type = e.check_type
                         AND r.target = e.target
                         AND r.event = 'recover'
                         AND r.id > e.id
                     )
                 )",
                params![cutoff],
            )?;
        }
        // Reconciliación: los agentes que perdieron TODAS sus incidencias
        // activas (o que nunca tuvieron recover) deben volver a ok, y sus
        // checks no deben quedar en fail fantasma (el fail ya se purgó).
        for agent in &purged_agents {
            // ¿Queda alguna incidencia activa para este agente?
            let active = self.active_alerts_for_agent(agent)?;
            if active == 0 {
                self.conn.execute(
                    "UPDATE agents SET status = 'ok' WHERE agent_id = ?1",
                    params![agent],
                )?;
            }
            // Limpia los agent_checks en fail que ya no tienen un fail activo
            // (el evento se purgó o el check se recuperó en el ínterin).
            self.conn.execute(
                "DELETE FROM agent_checks
                 WHERE agent_id = ?1
                   AND last_event = 'fail'
                   AND NOT EXISTS (
                     SELECT 1 FROM events
                     WHERE events.agent_id = agent_checks.agent_id
                       AND events.check_type = agent_checks.check_type
                       AND events.target = agent_checks.target
                       AND events.event = 'fail'
                   )",
                params![agent],
            )?;
        }

        // 3) agent_checks: estado por check. Se mantienen los checks vivos
        //    (status fail = incidencia abierta) aunque sean antiguos, pero se
        //    podan los checks en ok que llevan demasiado tiempo sin actividad.
        //    La vida real de un check en ok la marca el heartbeat del agente,
        //    así que usamos un umbral generoso: alerts_hours (o 7 días por
        //    defecto si no hay incidencias configuradas).
        let check_cutoff_hours = if retention.alerts_hours > 0 {
            retention.alerts_hours
        } else {
            24 * 7
        };
        let check_cutoff = now - check_cutoff_hours * 3600;
        deleted += self.conn.execute(
            "DELETE FROM agent_checks
             WHERE status = 'ok' AND ts < ?1",
            params![check_cutoff],
        )?;

        Ok(deleted as i64)
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

        // Retención: los heartbeats NO se guardan como eventos (solo actualizan
        // el estado agregado del agente). La tabla events conserva fails y
        // recovers, que es donde está la historia que importa.
        let is_heartbeat = p.event == "heartbeat";
        if !is_heartbeat {
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
        }
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
                // El heartbeat refleja el resumen agregado que el agente cree
                // tener. Pero no debe ENMASCARAR una incidencia activa: si la
                // central aún tiene fails sin recover para este agente, su
                // estado global sigue siendo crítico/peor, y el heartbeat no
                // puede bajarlo a ok (el recover llegará por su propio canal).
                let active = self.active_alerts_for_agent(&p.agent_id)?;
                let hb_status = if active > 0 {
                    // conserva el estado previo del agente (crítico si algo
                    // sigue abierto); solo sube si el heartbeat es más grave.
                    let prev: String = self
                        .conn
                        .query_row(
                            "SELECT status FROM agents WHERE agent_id = ?1",
                            params![p.agent_id],
                            |r| r.get(0),
                        )
                        .unwrap_or_else(|_| "unknown".to_string());
                    if severity(status) > severity(&prev) {
                        status.to_string()
                    } else {
                        prev
                    }
                } else {
                    status.to_string()
                };
                self.conn.execute(
                    "UPDATE agents SET status = ?1, ok_count = ?2, warn_count = ?3, critical_count = ?4
                     WHERE agent_id = ?5",
                    params![
                        hb_status,
                        p.ok_count.unwrap_or(0),
                        p.warn_count.unwrap_or(0),
                        p.critical_count.unwrap_or(0),
                        p.agent_id
                    ],
                )?;
                // Inventario: estado actual de TODOS los checks del servidor,
                // aunque nunca hayan fallado (y limpieza de los retirados).
                self.apply_check_inventory(&p.agent_id, &p.checks, ts)?;
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
                // Estado actual por check (vista uptime-kuma).
                self.conn.execute(
                    "INSERT INTO agent_checks (agent_id, check_type, target, status, message, value, last_event, ts)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'fail', ?7)
                     ON CONFLICT(agent_id, check_type, target) DO UPDATE SET
                       status = excluded.status,
                       message = excluded.message,
                       value = excluded.value,
                       last_event = 'fail',
                       ts = excluded.ts",
                    params![p.agent_id, p.check_type, p.target, status, p.message, p.value, ts],
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
                // recover con gone=true: el check desapareció de la config del
                // agente -> se borra de agent_checks (sin ok fantasma).
                let gone = p.gone.unwrap_or(false);
                if gone {
                    self.conn.execute(
                        "DELETE FROM agent_checks
                         WHERE agent_id = ?1 AND check_type = ?2 AND target = ?3",
                        params![p.agent_id, p.check_type, p.target],
                    )?;
                } else {
                    // El check recuperado queda en ok (estado actual por check).
                    self.conn.execute(
                        "INSERT INTO agent_checks (agent_id, check_type, target, status, message, value, last_event, ts)
                         VALUES (?1, ?2, ?3, 'ok', ?4, ?5, 'recover', ?6)
                         ON CONFLICT(agent_id, check_type, target) DO UPDATE SET
                           status = 'ok',
                           message = excluded.message,
                           value = excluded.value,
                           last_event = 'recover',
                           ts = excluded.ts",
                        params![p.agent_id, p.check_type, p.target, p.message, p.value, ts],
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

    /// Aplica el inventario de checks del heartbeat sobre `agent_checks`.
    ///
    /// - Refresca estado/mensaje/valor/fecha de cada check que manda el agente
    ///   (también los que están OK: antes la central no sabía ni que existían).
    /// - Borra los que ya no vienen: el agente los ha quitado de su config.
    /// - No toca `last_event`, que es de la historia fail/recover.
    /// - Inventario vacío (agente antiguo) = no se toca nada.
    fn apply_check_inventory(
        &mut self,
        agent_id: &str,
        checks: &[CheckSnapshot],
        ts: i64,
    ) -> rusqlite::Result<()> {
        if checks.is_empty() {
            return Ok(());
        }
        for c in checks {
            self.conn.execute(
                "INSERT INTO agent_checks (agent_id, check_type, target, status, message, value, last_event, ts)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'heartbeat', ?7)
                 ON CONFLICT(agent_id, check_type, target) DO UPDATE SET
                   status  = excluded.status,
                   message = excluded.message,
                   value   = excluded.value,
                   ts      = excluded.ts",
                params![
                    agent_id,
                    c.check_type,
                    c.target,
                    c.status,
                    c.message,
                    c.value,
                    ts
                ],
            )?;
        }

        let mut stmt = self
            .conn
            .prepare("SELECT check_type, target FROM agent_checks WHERE agent_id = ?1")?;
        let existentes = stmt
            .query_map(params![agent_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);

        let vigentes: std::collections::HashSet<(&str, &str)> = checks
            .iter()
            .map(|c| (c.check_type.as_str(), c.target.as_str()))
            .collect();
        for (check_type, target) in existentes {
            if !vigentes.contains(&(check_type.as_str(), target.as_str())) {
                self.conn.execute(
                    "DELETE FROM agent_checks WHERE agent_id = ?1 AND check_type = ?2 AND target = ?3",
                    params![agent_id, check_type, target],
                )?;
            }
        }
        Ok(())
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
            // Por defecto se omiten los heartbeats (ruido): el historial útil son
            // fails/recovers. La retención de eventos ya no guarda heartbeats.
            "SELECT id, agent_id, event, check_type, target, status, message, value, ts, received_at
             FROM events WHERE agent_id = ?1 AND event <> 'heartbeat'
             ORDER BY id DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![agent_id, limit], |row| {
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
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Estado actual por check del agente (vista uptime-kuma: una fila por
    /// monitor con su último estado, sin el ruido de eventos repetidos).
    pub fn list_agent_checks(&self, agent_id: &str) -> rusqlite::Result<Vec<AgentCheck>> {
        let mut stmt = self.conn.prepare(
            "SELECT agent_id, check_type, target, status, message, value, last_event, ts
             FROM agent_checks WHERE agent_id = ?1
             ORDER BY check_type, target",
        )?;
        let rows = stmt
            .query_map(params![agent_id], |row| {
                Ok(AgentCheck {
                    agent_id: row.get(0)?,
                    check_type: row.get(1)?,
                    target: row.get(2)?,
                    status: row.get(3)?,
                    message: row.get(4)?,
                    value: row.get(5)?,
                    last_event: row.get(6)?,
                    ts: row.get(7)?,
                })
            })?
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

/// Orden de severidad para combinar estados: ok < warn < critical.
/// Cualquier valor desconocido se trata como warn (ni mejor ni peor).
fn severity(status: &str) -> u8 {
    match status {
        "critical" => 3,
        "warn" => 2,
        "ok" => 1,
        _ => 2,
    }
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
            gone: None,
            ok_count: None,
            warn_count: None,
            critical_count: None,
            checks: vec![],
        }
    }

    fn snapshot(check: &str, target: &str, status: &str) -> CheckSnapshot {
        CheckSnapshot {
            check_type: check.to_string(),
            target: target.to_string(),
            status: status.to_string(),
            message: "snapshot".to_string(),
            value: Some(1.5),
        }
    }

    fn fila(agent: &str, since: i64, sent: i64, retry: i64) -> OfflineAlertRow {
        OfflineAlertRow {
            agent_id: agent.to_string(),
            since,
            last_sent_at: sent,
            retry_after: retry,
        }
    }

    #[test]
    fn avisa_cuando_un_servidor_deja_de_dar_senales() {
        let agents = vec![("srv-01".to_string(), 1_000)];
        // 5 minutos sin señales (300s), comprobado 400s después.
        let acciones = decide_offline_alerts(&agents, &[], 1_400, 300, 900);
        assert_eq!(
            acciones,
            vec![OfflineAction::NotifyDown { agent_id: "srv-01".to_string(), since: 1_400 }]
        );
        // Justo en el límite todavía no es "caído".
        assert!(decide_offline_alerts(&agents, &[], 1_300, 300, 900).is_empty());
    }

    #[test]
    fn reavisa_por_cooldown_y_respeta_el_backoff() {
        let agents = vec![("srv-01".to_string(), 1_000)];
        let estado = vec![fila("srv-01", 1_400, 1_500, 0)];
        // 800s después del primer aviso, cooldown 900 -> aún no.
        assert!(decide_offline_alerts(&agents, &estado, 2_300, 300, 900).is_empty());
        // Cooldown vencido -> recordatorio.
        assert_eq!(
            decide_offline_alerts(&agents, &estado, 2_400, 300, 900),
            vec![OfflineAction::RenotifyDown { agent_id: "srv-01".to_string(), since: 1_400 }]
        );
        // Cooldown 0 = sólo el primer correo.
        assert!(decide_offline_alerts(&agents, &estado, 99_999, 300, 0).is_empty());
        // Tras un fallo de envío se respeta el backoff.
        let con_backoff = vec![fila("srv-01", 1_400, 0, 2_000)];
        assert!(decide_offline_alerts(&agents, &con_backoff, 1_900, 300, 900).is_empty());
        assert_eq!(
            decide_offline_alerts(&agents, &con_backoff, 2_100, 300, 900),
            vec![OfflineAction::NotifyDown { agent_id: "srv-01".to_string(), since: 1_400 }],
            "el aviso pendiente se reintenta cuando vence el backoff"
        );
    }

    #[test]
    fn avisa_de_la_recuperacion_y_no_duplica() {
        let estado = vec![fila("srv-01", 1_400, 1_500, 0)];
        let vivo = vec![("srv-01".to_string(), 9_000)];
        assert_eq!(
            decide_offline_alerts(&vivo, &estado, 9_000, 300, 900),
            vec![OfflineAction::NotifyUp { agent_id: "srv-01".to_string(), since: 1_400 }]
        );
        // Sin estado previo, un servidor sano no genera nada.
        assert!(decide_offline_alerts(&vivo, &[], 9_000, 300, 900).is_empty());
        // Y estando caído, mientras esté dentro del cooldown, tampoco.
        let caido = vec![("srv-01".to_string(), 1_000)];
        assert!(decide_offline_alerts(&caido, &estado, 1_600, 300, 900).is_empty());
        // offline_minutes = 0 -> nunca se considera caído (y se "recupera").
        assert_eq!(
            decide_offline_alerts(&caido, &estado, 9_999, 0, 900),
            vec![OfflineAction::NotifyUp { agent_id: "srv-01".to_string(), since: 1_400 }],
            "con 0 desactivado, cualquier estado previo se cierra"
        );
    }

    #[test]
    fn las_alertas_se_persisten_y_sobreviven_al_reinicio() {
        let dir = std::env::temp_dir().join(format!(
            "mar_alertas_test_{}.db",
            std::process::id()
        ));
        let path = dir.to_string_lossy().to_string();
        let _ = std::fs::remove_file(&path);
        {
            let mut db = Database::open(&path).expect("open");
            db.set_settings("127.0.0.1:9600", None).expect("settings");
            let cfg = AlertSettings {
                enabled: true,
                cooldown_minutes: 0,
                recovery_notifications: false,
                to: vec!["ops@empresa.com".to_string()],
                smtp: crate::alert::SmtpSettings {
                    host: "smtp.empresa.com".to_string(),
                    port: 465,
                    tls: crate::alert::AlertTls::Implicit,
                    username: String::new(),
                    password: String::new(),
                    from: "alertas@empresa.com".to_string(),
                },
            };
            db.set_alert_settings(&cfg).expect("guardar alertas");
            db.insert_offline_alert("srv-01", 1_000).expect("marca");
            db.mark_offline_alert_sent("srv-01", 1_100).expect("enviado");
        }
        // "Reinicio": se vuelve a abrir la misma base.
        let db = Database::open(&path).expect("reopen");
        let cfg = db.get_alert_settings();
        assert!(cfg.enabled);
        assert_eq!(cfg.cooldown_minutes, 0);
        assert!(!cfg.recovery_notifications);
        assert_eq!(cfg.smtp.tls, crate::alert::AlertTls::Implicit);
        assert_eq!(cfg.smtp.port, 465);
        let estado = db.list_offline_alerts().expect("estado");
        assert_eq!(estado.len(), 1);
        assert_eq!(estado[0].last_sent_at, 1_100, "no se re-avisa tras reiniciar");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
    }

    #[test]
    fn purge_expired_respects_retention() {
        let mut db = Database::open(":memory:").expect("open memory db");
        let retention = Retention {
            events_days: 30,
            alerts_hours: 24,
            offline_minutes: 5,
        };
        let now = now_unix();

        // Evento viejo (hace 40 días): debe purgarse.
        db.conn
            .execute(
                "INSERT INTO events (agent_id, event, check_type, target, status, message, value, ts, received_at)
                 VALUES ('srv-a', 'fail', 'process', 'old.exe', 'critical', 'x', NULL, ?1, ?1)",
                params![now - 40 * 86_400],
            )
            .expect("insert old event");
        // Evento reciente (hace 1 día): se conserva.
        db.conn
            .execute(
                "INSERT INTO events (agent_id, event, check_type, target, status, message, value, ts, received_at)
                 VALUES ('srv-a', 'fail', 'process', 'recent.exe', 'critical', 'x', NULL, ?1, ?1)",
                params![now - 86_400],
            )
            .expect("insert recent event");

        let purged = db.purge_expired(&retention).expect("purge");
        assert!(purged >= 1, "debe borrar al menos el evento viejo");
        let remaining: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
            .expect("count");
        assert_eq!(remaining, 1, "solo debe quedar el evento reciente");
    }

    #[test]
    fn recover_gone_removes_agent_check() {
        let mut db = Database::open(":memory:").expect("open memory db");

        // fail -> agent_checks guarda el check
        db.ingest(&payload("srv-a", "fail", "process", "app.exe", "critical"))
            .expect("fail");
        assert_eq!(db.list_agent_checks("srv-a").expect("checks").len(), 1);

        // recover normal -> el check queda ok
        db.ingest(&payload("srv-a", "recover", "process", "app.exe", "ok"))
            .expect("recover ok");
        let checks = db.list_agent_checks("srv-a").expect("checks");
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, "ok");

        // fail otra vez + recover gone=true -> el check se borra (no fantasma)
        db.ingest(&payload("srv-a", "fail", "process", "app.exe", "critical"))
            .expect("fail 2");
        let mut gone = payload("srv-a", "recover", "process", "app.exe", "ok");
        gone.gone = Some(true);
        db.ingest(&gone).expect("recover gone");
        assert!(
            db.list_agent_checks("srv-a").expect("checks").is_empty(),
            "check desaparecido debe borrarse de agent_checks"
        );
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

    /// El heartbeat lleva el inventario completo: la central debe conocer los
    /// checks sanos (que nunca han fallado) y borrar los que ya no existen.
    #[test]
    fn heartbeat_inventory_creates_and_prunes_checks() {
        let mut db = Database::open(":memory:").expect("open memory db");
        let mut p = payload("srv-a", "heartbeat", "summary", "server", "ok");
        p.checks = vec![
            snapshot("process", "app.exe", "ok"),
            snapshot("http_endpoint", "http://x/health", "critical"),
            snapshot("disk", "C", "warn"),
        ];
        db.ingest(&p).expect("heartbeat con inventario");

        let checks = db.list_agent_checks("srv-a").expect("checks");
        assert_eq!(checks.len(), 3, "todos los checks, también los sanos");
        assert_eq!(
            checks.iter().filter(|c| c.status == "ok").count(),
            1,
            "el check OK aparece en el panel"
        );

        // El agente retira dos checks de su configuración.
        p.checks = vec![snapshot("process", "app.exe", "ok")];
        db.ingest(&p).expect("heartbeat sin los retirados");
        let checks = db.list_agent_checks("srv-a").expect("checks");
        assert_eq!(checks.len(), 1, "los retirados se borran");
        assert_eq!(checks[0].target, "app.exe");
    }

    /// Un agente antiguo (sin inventario en el heartbeat) no debe vaciar el
    /// panel de checks.
    #[test]
    fn heartbeat_without_inventory_keeps_checks() {
        let mut db = Database::open(":memory:").expect("open memory db");
        db.ingest(&payload("srv-a", "fail", "process", "app.exe", "critical"))
            .expect("fail");
        let antes = db.list_agent_checks("srv-a").expect("checks").len();
        assert_eq!(antes, 1);

        db.ingest(&payload("srv-a", "heartbeat", "summary", "server", "ok"))
            .expect("heartbeat sin inventario");
        assert_eq!(
            db.list_agent_checks("srv-a").expect("checks").len(),
            1,
            "sin inventario no se borra nada"
        );
    }

    /// Regresión: abrir la misma BD de fichero varias veces (reinicios del
    /// proceso) debe funcionar. Antes, la migración ALTER corría sin guarda y
    /// el segundo arranque panikeaba con "duplicate column name".
    #[test]
    fn open_is_idempotent_across_restarts() {
        let path = std::env::temp_dir().join(format!(
            "mar_restart_test_{}_{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0)
        ));
        let path_str = path.to_string_lossy().to_string();
        let _ = std::fs::remove_file(&path);

        for _ in 0..3 {
            let db = Database::open(&path_str).expect("open must not fail on restart");
            let _ = db;
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path_str}-wal"));
        let _ = std::fs::remove_file(format!("{path_str}-shm"));
    }

    /// Regresión: una BD creada por una versión anterior (settings sin las
    /// columnas de retención) debe migrarse al abrirla.
    #[test]
    fn open_migrates_legacy_settings_table() {
        let path = std::env::temp_dir().join(format!(
            "mar_legacy_test_{}_{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0)
        ));
        let path_str = path.to_string_lossy().to_string();
        let _ = std::fs::remove_file(&path);

        // Esquema legacy: settings con SOLO key/listen/token.
        {
            let conn = rusqlite::Connection::open(&path).expect("open legacy db");
            conn.execute_batch(
                "CREATE TABLE settings (
                   key    TEXT PRIMARY KEY,
                   listen TEXT NOT NULL,
                   token  TEXT
                 );
                 INSERT INTO settings (key, listen, token) VALUES ('default', '127.0.0.1:9600', NULL);",
            )
            .expect("create legacy schema");
        }

        let db = Database::open(&path_str).expect("legacy db must migrate");
        let (listen, _) = db.get_settings().expect("read").expect("seeded row");
        assert_eq!(listen, "127.0.0.1:9600");
        // La retención migrada usa los valores por defecto de la nueva versión.
        assert_eq!(db.get_retention().events_days, 30);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path_str}-wal"));
        let _ = std::fs::remove_file(format!("{path_str}-shm"));
    }

    #[test]
    fn settings_roundtrip() {
        let mut db = Database::open(":memory:").expect("open memory db");
        assert!(db.get_settings().expect("read").is_none());

        db.set_settings("0.0.0.0:9600", Some("s3cret"))
            .expect("set");
        let (listen, token) = db.get_settings().expect("read").unwrap();
        assert_eq!(listen, "0.0.0.0:9600");
        assert_eq!(token.as_deref(), Some("s3cret"));

        // actualizar: token a None (desactiva auth), listen cambia
        db.set_settings("127.0.0.1:9601", None).expect("update");
        let (listen, token) = db.get_settings().expect("read").unwrap();
        assert_eq!(listen, "127.0.0.1:9601");
        assert_eq!(token, None);
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

    /// La purga de una incidencia activa caducada (fail sin recover más viejo
    /// que alerts_hours) debe dejar el estado consistente: el agente vuelve a
    /// ok y el check no queda en fail fantasma. Antes, `purge_expired` borraba
    /// el evento fail pero dejaba `agents.status='critical'` y el
    /// `agent_checks` en `fail` para siempre, mostrando en el panel un
    /// servidor crítico con 0 incidencias.
    #[test]
    fn purge_of_active_alert_reconciles_agent_state() {
        let mut db = Database::open(":memory:").expect("open memory db");
        let now = now_unix();

        // incidencia activa (fail sin recover) de hace 10 días
        db.ingest(&payload("srv-a", "fail", "process", "app.exe", "critical"))
            .expect("fail");
        db.conn
            .execute(
                "UPDATE events SET ts = ?1 WHERE event = 'fail'",
                params![now - 10 * 86_400],
            )
            .expect("age the fail");

        // agente queda en critical con 1 incidencia antes de purgar
        let agents = db.list_agents().expect("agents");
        assert_eq!(agents[0].status, "critical");
        assert_eq!(agents[0].active_alerts, 1);

        let retention = Retention {
            events_days: 0,
            alerts_hours: 24, // 24h: la incidencia de 10 días caduca
            offline_minutes: 5,
        };
        let purged = db.purge_expired(&retention).expect("purge");
        assert!(purged >= 1, "debe purgar el fail caducado");

        let agents = db.list_agents().expect("agents");
        assert_eq!(agents[0].active_alerts, 0, "sin incidencias activas tras la purga");
        assert_eq!(
            agents[0].status, "ok",
            "agente sin incidencias activas debe volver a ok (no quedar critical fantasma)"
        );
        let checks = db.list_agent_checks("srv-a").expect("checks");
        assert!(
            checks.is_empty() || checks.iter().all(|c| c.status == "ok"),
            "no deben quedar checks en fail fantasma tras purgar la incidencia, got {:?}",
            checks
        );
    }



    #[test]
    fn heartbeat_does_not_hide_active_alert() {
        let mut db = Database::open(":memory:").expect("open memory db");
        // incidencia activa
        db.ingest(&payload("srv-a", "fail", "process", "app.exe", "critical")).expect("fail");
        // heartbeat con todo ok (pero la incidencia no se ha recuperado)
        let mut hb = payload("srv-a", "heartbeat", "summary", "server", "ok");
        hb.ok_count = Some(5);
        hb.critical_count = Some(0);
        db.ingest(&hb).expect("heartbeat");
        let agents = db.list_agents().expect("agents");
        // La incidencia sigue activa: el agente debe seguir critical, no ok
        assert_eq!(agents[0].active_alerts, 1);
        assert_eq!(agents[0].status, "critical", "heartbeat no debe enmascarar una incidencia activa");
    }

}
