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
    /// Identificador único del evento generado por el agente (idempotencia).
    /// Si llega dos veces el mismo (agent_id, event_id) el segundo no se
    /// procesa. Los agentes antiguos no lo mandan: se comportan como siempre.
    #[serde(default)]
    pub event_id: Option<String>,
}

/// Resultado de ingerir un evento.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IngestOutcome {
    /// Id de la fila en `events` (0 si no se guarda como evento o es duplicado).
    pub id: i64,
    /// Ya se había ingerido un evento con el mismo (agent_id, event_id).
    pub duplicate: bool,
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

/// Uptime de un check en un rango (24h, 7d...): ok/total sobre los cubos
/// horarios. `pct` es None si no hay ninguna muestra en el rango.
#[derive(Debug, Clone, Serialize, Default)]
pub struct UptimeRange {
    pub pct: Option<f64>,
    pub total: i64,
    pub ok: i64,
    /// Horas del rango en las que el agente no mandó ningún heartbeat
    /// (servidor caído o sin red). No cuentan en `pct`.
    pub no_data_hours: i64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct UptimeRanges {
    #[serde(rename = "24h")]
    pub h24: UptimeRange,
    #[serde(rename = "7d")]
    pub d7: UptimeRange,
    #[serde(rename = "30d")]
    pub d30: UptimeRange,
    #[serde(rename = "365d")]
    pub d365: UptimeRange,
}

/// Un cubo de una hora para la barra de estado (total = 0: sin muestras).
#[derive(Debug, Clone, Serialize)]
pub struct HourBucket {
    pub hour_ts: i64,
    pub total: i64,
    pub ok: i64,
    pub worst: Option<String>,
    /// Hora sin ningún heartbeat del agente desde que hay historial.
    pub no_data: bool,
}

/// Incidencia fail -> recover de un check (end = None: sigue abierta).
#[derive(Debug, Clone, Serialize)]
pub struct Incident {
    pub start: i64,
    pub end: Option<i64>,
    pub duration_seconds: i64,
    /// Peor estado visto durante la incidencia.
    pub status: String,
    /// Mensaje del primer fail.
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckUptime {
    pub check_type: String,
    pub target: String,
    pub uptime: UptimeRanges,
    /// Últimas 48 horas, de la más antigua a la actual.
    pub hours: Vec<HourBucket>,
    /// Últimas 50 incidencias, la más reciente primero.
    pub incidents: Vec<Incident>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentUptime {
    pub agent_id: String,
    pub now: i64,
    /// Inicio de la hora actual (último cubo de `hours`).
    pub hour_ts: i64,
    /// Primera hora desde la que se cuentan las horas sin datos (None: el
    /// agente aún no tiene historial).
    pub history_start: Option<i64>,
    /// Horas sin heartbeat del agente por rango (las mismas que en cada check).
    pub no_data_hours: NoDataHours,
    pub checks: Vec<CheckUptime>,
}

#[derive(Debug, Clone, Copy, Serialize, Default)]
pub struct NoDataHours {
    #[serde(rename = "24h")]
    pub h24: i64,
    #[serde(rename = "7d")]
    pub d7: i64,
    #[serde(rename = "30d")]
    pub d30: i64,
    #[serde(rename = "365d")]
    pub d365: i64,
}

/// Horas de la barra de estado.
pub const UPTIME_BAR_HOURS: i64 = 48;
/// Incidencias devueltas por check.
pub const UPTIME_MAX_INCIDENTS: usize = 50;

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
    /// Días de historial de uptime por hora (check_hourly / agent_hourly).
    /// 0 = conservar indefinidamente. Si no viene (UI antigua), 400.
    #[serde(default = "default_history_days")]
    pub history_days: i64,
}

fn default_history_days() -> i64 {
    400
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            events_days: 30,
            alerts_hours: 24 * 7, // 7 días de incidencias sin recuperar
            offline_minutes: 5,
            history_days: default_history_days(),
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
               first_seen     INTEGER NOT NULL DEFAULT 0,
               last_hb_ts     INTEGER NOT NULL DEFAULT 0
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
               received_at INTEGER NOT NULL,
               event_id    TEXT
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
               alert_json TEXT NOT NULL DEFAULT '',
               retention_history_days INTEGER NOT NULL DEFAULT 400
             );

             -- Estado del aviso de servidor-sin-senales: que servidores estan
             -- caidos y a quien se le aviso ya (sobrevive a reinicios).
             CREATE TABLE IF NOT EXISTS offline_alerts (
               agent_id     TEXT PRIMARY KEY,
               since        INTEGER NOT NULL,
               last_sent_at INTEGER NOT NULL DEFAULT 0,
               retry_after  INTEGER NOT NULL DEFAULT 0
             );

             -- Idempotencia de los heartbeats (no se guardan en events): ids
             -- ya vistos, se purgan a los 7 dias.
             CREATE TABLE IF NOT EXISTS seen_events (
               agent_id TEXT NOT NULL,
               event_id TEXT NOT NULL,
               ts       INTEGER NOT NULL,
               PRIMARY KEY (agent_id, event_id)
             ) WITHOUT ROWID;

             -- Historial de estado por check en cubos de una hora (uptime).
             -- Cada heartbeat suma 1 a total (y a ok si el check estaba ok)
             -- en el cubo de SU timestamp. worst = peor estado visto.
             CREATE TABLE IF NOT EXISTS check_hourly (
               agent_id   TEXT NOT NULL,
               check_type TEXT NOT NULL,
               target     TEXT NOT NULL,
               hour_ts    INTEGER NOT NULL,
               total      INTEGER NOT NULL DEFAULT 0,
               ok         INTEGER NOT NULL DEFAULT 0,
               worst      TEXT NOT NULL DEFAULT 'ok',
               PRIMARY KEY (agent_id, check_type, target, hour_ts)
             ) WITHOUT ROWID;

             -- Heartbeats recibidos por agente y hora: una hora sin fila es
             -- una hora sin datos (servidor caido o sin red).
             CREATE TABLE IF NOT EXISTS agent_hourly (
               agent_id   TEXT NOT NULL,
               hour_ts    INTEGER NOT NULL,
               heartbeats INTEGER NOT NULL DEFAULT 0,
               PRIMARY KEY (agent_id, hour_ts)
             ) WITHOUT ROWID;",
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
        // Migraciones idempotentes de la ingesta idempotente: event_id en
        // events y último heartbeat aplicado por agente.
        if !has_column(&conn, "events", "event_id")? {
            conn.execute("ALTER TABLE events ADD COLUMN event_id TEXT", [])?;
        }
        if !has_column(&conn, "agents", "last_hb_ts")? {
            conn.execute(
                "ALTER TABLE agents ADD COLUMN last_hb_ts INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !has_column(&conn, "settings", "retention_history_days")? {
            conn.execute(
                "ALTER TABLE settings ADD COLUMN retention_history_days INTEGER NOT NULL DEFAULT 400",
                [],
            )?;
        }
        // El índice va después de la migración: necesita la columna.
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id
             ON events(agent_id, event_id) WHERE event_id IS NOT NULL",
            [],
        )?;
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
                "SELECT retention_events_days, retention_alerts_hours, retention_offline_minutes,
                        retention_history_days
                 FROM settings WHERE key = 'default'",
                [],
                |r| {
                    Ok(Retention {
                        events_days: r.get(0)?,
                        alerts_hours: r.get(1)?,
                        offline_minutes: r.get(2)?,
                        history_days: r.get(3)?,
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
            "INSERT INTO settings (key, listen, token, retention_events_days, retention_alerts_hours, retention_offline_minutes, retention_history_days)
             VALUES ('default', '', NULL, ?1, ?2, ?3, ?4)
             ON CONFLICT(key) DO UPDATE SET
               retention_events_days       = excluded.retention_events_days,
               retention_alerts_hours      = excluded.retention_alerts_hours,
               retention_offline_minutes   = excluded.retention_offline_minutes,
               retention_history_days      = excluded.retention_history_days",
            params![
                retention.events_days,
                retention.alerts_hours,
                retention.offline_minutes,
                retention.history_days
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

        // 4) Historial de uptime por hora.
        if retention.history_days > 0 {
            let cutoff = now - retention.history_days * 86_400;
            deleted += self.conn.execute(
                "DELETE FROM check_hourly WHERE hour_ts < ?1",
                params![cutoff],
            )?;
            deleted += self.conn.execute(
                "DELETE FROM agent_hourly WHERE hour_ts < ?1",
                params![cutoff],
            )?;
        }

        // 5) Ids de heartbeats ya vistos: basta con una semana (margen de
        //    sobra para que un agente vacíe su backlog).
        deleted += self.conn.execute(
            "DELETE FROM seen_events WHERE ts < ?1",
            params![now - 7 * 86_400],
        )?;

        Ok(deleted as i64)
    }

    /// Ingiere un evento dentro de su propia transacción.
    pub fn ingest(&mut self, p: &IngestPayload) -> rusqlite::Result<IngestOutcome> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        match self.ingest_inner(p) {
            Ok(out) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(out)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Ingiere un lote en orden y en una sola transacción (backlog que el
    /// agente reenvía al recuperar la conexión). Si algo falla no se aplica
    /// nada. Devuelve (guardados, duplicados).
    pub fn ingest_batch(&mut self, payloads: &[IngestPayload]) -> rusqlite::Result<(i64, i64)> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let mut stored = 0;
        let mut duplicates = 0;
        for p in payloads {
            match self.ingest_inner(p) {
                Ok(out) if out.duplicate => duplicates += 1,
                Ok(_) => stored += 1,
                Err(e) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(e);
                }
            }
        }
        self.conn.execute_batch("COMMIT")?;
        Ok((stored, duplicates))
    }

    /// ¿Ya se ingirió este (agent_id, event_id)? Mira events (fail/recover)
    /// y seen_events (heartbeats).
    fn is_duplicate(&self, agent_id: &str, event_id: &str) -> rusqlite::Result<bool> {
        let n: i64 = self.conn.query_row(
            "SELECT (SELECT COUNT(*) FROM events WHERE agent_id = ?1 AND event_id = ?2)
                  + (SELECT COUNT(*) FROM seen_events WHERE agent_id = ?1 AND event_id = ?2)",
            params![agent_id, event_id],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    /// Inserta el evento y actualiza el estado agregado del agente.
    fn ingest_inner(&mut self, p: &IngestPayload) -> rusqlite::Result<IngestOutcome> {
        let now = now_unix();
        // Hora real del evento (la del agente): es la que alimenta el
        // historial. last_seen sigue siendo la hora de llegada.
        let ts = p.timestamp.unwrap_or(now);
        let status = if p.status.trim().is_empty() {
            "unknown"
        } else {
            p.status.trim()
        };
        let event_id = p
            .event_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());

        // Idempotencia: un reenvío del mismo evento no reprocesa nada.
        if let Some(eid) = event_id
            && self.is_duplicate(&p.agent_id, eid)?
        {
            return Ok(IngestOutcome {
                id: 0,
                duplicate: true,
            });
        }

        // Retención: los heartbeats NO se guardan como eventos (solo actualizan
        // el estado agregado del agente). La tabla events conserva fails y
        // recovers, que es donde está la historia que importa.
        let is_heartbeat = p.event == "heartbeat";
        let mut id = 0;
        if !is_heartbeat {
            self.conn.execute(
                "INSERT INTO events (agent_id, event, check_type, target, status, message, value, ts, received_at, event_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    p.agent_id,
                    p.event,
                    p.check_type,
                    p.target,
                    status,
                    p.message,
                    p.value,
                    ts,
                    now,
                    event_id
                ],
            )?;
            id = self.conn.last_insert_rowid();
        } else if let Some(eid) = event_id {
            self.conn.execute(
                "INSERT OR IGNORE INTO seen_events (agent_id, event_id, ts) VALUES (?1, ?2, ?3)",
                params![p.agent_id, eid, now],
            )?;
        }

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
                // Historial de uptime: cuenta en el cubo de la hora real del
                // heartbeat, también si es un reenvío atrasado.
                self.record_history(&p.agent_id, ts, &p.checks)?;
                // Un heartbeat reenviado (backlog) más viejo que el último
                // aplicado no puede pisar el estado actual con datos antiguos.
                let last_hb_ts: i64 = self.conn.query_row(
                    "SELECT last_hb_ts FROM agents WHERE agent_id = ?1",
                    params![p.agent_id],
                    |r| r.get(0),
                )?;
                if ts < last_hb_ts {
                    return Ok(IngestOutcome {
                        id,
                        duplicate: false,
                    });
                }
                // Acotado a "ahora": un reloj del agente adelantado no debe
                // dejar fuera los heartbeats siguientes.
                self.conn.execute(
                    "UPDATE agents SET last_hb_ts = ?1 WHERE agent_id = ?2",
                    params![ts.min(now), p.agent_id],
                )?;
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

        Ok(IngestOutcome {
            id,
            duplicate: false,
        })
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

    /// Suma el heartbeat a los cubos horarios del agente y de cada check del
    /// inventario (hora = timestamp del heartbeat redondeado a la hora).
    fn record_history(
        &mut self,
        agent_id: &str,
        ts: i64,
        checks: &[CheckSnapshot],
    ) -> rusqlite::Result<()> {
        let hour = hour_floor(ts);
        self.conn.execute(
            "INSERT INTO agent_hourly (agent_id, hour_ts, heartbeats) VALUES (?1, ?2, 1)
             ON CONFLICT(agent_id, hour_ts) DO UPDATE SET heartbeats = heartbeats + 1",
            params![agent_id, hour],
        )?;
        let mut stmt = self.conn.prepare_cached(
            "INSERT INTO check_hourly (agent_id, check_type, target, hour_ts, total, ok, worst)
             VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6)
             ON CONFLICT(agent_id, check_type, target, hour_ts) DO UPDATE SET
               total = total + 1,
               ok    = ok + excluded.ok,
               worst = CASE
                 WHEN (CASE excluded.worst WHEN 'critical' THEN 3 WHEN 'ok' THEN 1 ELSE 2 END)
                    > (CASE check_hourly.worst WHEN 'critical' THEN 3 WHEN 'ok' THEN 1 ELSE 2 END)
                 THEN excluded.worst ELSE check_hourly.worst END",
        )?;
        for c in checks {
            let status = if c.status.trim().is_empty() {
                "unknown"
            } else {
                c.status.trim()
            };
            let ok = i64::from(status == "ok");
            stmt.execute(params![agent_id, c.check_type, c.target, hour, ok, status])?;
        }
        Ok(())
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

    /// Uptime por check de un agente: porcentajes 24h/7d/30d/365d, barra de
    /// las últimas 48 horas e incidencias fail -> recover. None si el agente
    /// no existe.
    pub fn agent_uptime(&self, agent_id: &str, now: i64) -> rusqlite::Result<Option<AgentUptime>> {
        let Some(first_seen) = self
            .conn
            .query_row(
                "SELECT first_seen FROM agents WHERE agent_id = ?1",
                params![agent_id],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
        else {
            return Ok(None);
        };

        let cur = hour_floor(now);
        // Inicio de cada rango: N cubos contando la hora actual (parcial).
        let start = |hours: i64| cur - (hours - 1) * 3600;
        let (s24, s7, s30, s365) = (start(24), start(24 * 7), start(24 * 30), start(24 * 365));
        let bar_start = start(UPTIME_BAR_HOURS);

        // Horas sin datos: horas sin fila en agent_hourly desde que el agente
        // tiene historial. Se cuenta desde su primera hora con heartbeats (o
        // desde first_seen si aún no tiene ninguna): en una central que se
        // actualiza, los agentes ya existentes tienen first_seen de hace
        // meses pero historial desde hoy, y no serían "horas sin datos".
        let first_hour: Option<i64> = self.conn.query_row(
            "SELECT MIN(hour_ts) FROM agent_hourly WHERE agent_id = ?1",
            params![agent_id],
            |r| r.get(0),
        )?;
        let history_start = first_hour.or(if first_seen > 0 {
            Some(hour_floor(first_seen))
        } else {
            None
        });
        // Horas del agente con heartbeat en los últimos 365 días.
        let present: std::collections::HashSet<i64> = {
            let mut stmt = self.conn.prepare(
                "SELECT hour_ts FROM agent_hourly
                 WHERE agent_id = ?1 AND hour_ts >= ?2 AND hour_ts <= ?3",
            )?;
            stmt.query_map(params![agent_id, s365, cur], |r| r.get::<_, i64>(0))?
                .collect::<rusqlite::Result<_>>()?
        };
        let no_data_since = |range_start: i64| -> i64 {
            let Some(hs) = history_start else { return 0 };
            let from = range_start.max(hs);
            if from > cur {
                return 0;
            }
            let expected = (cur - from) / 3600 + 1;
            let seen = present.iter().filter(|h| **h >= from).count() as i64;
            (expected - seen).max(0)
        };
        let no_data_hours = NoDataHours {
            h24: no_data_since(s24),
            d7: no_data_since(s7),
            d30: no_data_since(s30),
            d365: no_data_since(s365),
        };
        let is_no_data =
            |hour_ts: i64| history_start.is_some_and(|hs| hour_ts >= hs) && !present.contains(&hour_ts);

        type Key = (String, String);
        let mut checks: std::collections::BTreeMap<Key, CheckUptime> = Default::default();
        let entry = |checks: &mut std::collections::BTreeMap<Key, CheckUptime>,
                     check_type: String,
                     target: String| {
            checks
                .entry((check_type.clone(), target.clone()))
                .or_insert_with(|| CheckUptime {
                    check_type,
                    target,
                    uptime: UptimeRanges {
                        h24: UptimeRange {
                            no_data_hours: no_data_hours.h24,
                            ..Default::default()
                        },
                        d7: UptimeRange {
                            no_data_hours: no_data_hours.d7,
                            ..Default::default()
                        },
                        d30: UptimeRange {
                            no_data_hours: no_data_hours.d30,
                            ..Default::default()
                        },
                        d365: UptimeRange {
                            no_data_hours: no_data_hours.d365,
                            ..Default::default()
                        },
                    },
                    hours: (0..UPTIME_BAR_HOURS)
                        .map(|i| {
                            let hour_ts = bar_start + i * 3600;
                            HourBucket {
                                hour_ts,
                                total: 0,
                                ok: 0,
                                worst: None,
                                no_data: is_no_data(hour_ts),
                            }
                        })
                        .collect(),
                    incidents: Vec::new(),
                });
        };

        // Checks actuales (aunque aún no tengan historial).
        {
            let mut stmt = self
                .conn
                .prepare("SELECT check_type, target FROM agent_checks WHERE agent_id = ?1")?;
            let rows = stmt.query_map(params![agent_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (ct, t) = row?;
                entry(&mut checks, ct, t);
            }
        }

        // Porcentajes: sumas por rango en una sola pasada. Las horas sin
        // datos no tienen filas en check_hourly, así que ya quedan fuera del
        // denominador.
        {
            let mut stmt = self.conn.prepare(
                "SELECT check_type, target,
                        SUM(CASE WHEN hour_ts >= ?2 THEN total ELSE 0 END),
                        SUM(CASE WHEN hour_ts >= ?2 THEN ok    ELSE 0 END),
                        SUM(CASE WHEN hour_ts >= ?3 THEN total ELSE 0 END),
                        SUM(CASE WHEN hour_ts >= ?3 THEN ok    ELSE 0 END),
                        SUM(CASE WHEN hour_ts >= ?4 THEN total ELSE 0 END),
                        SUM(CASE WHEN hour_ts >= ?4 THEN ok    ELSE 0 END),
                        SUM(total), SUM(ok)
                 FROM check_hourly
                 WHERE agent_id = ?1 AND hour_ts >= ?5 AND hour_ts <= ?6
                 GROUP BY check_type, target",
            )?;
            let rows = stmt.query_map(params![agent_id, s24, s7, s30, s365, cur], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    [
                        (r.get::<_, i64>(2)?, r.get::<_, i64>(3)?),
                        (r.get::<_, i64>(4)?, r.get::<_, i64>(5)?),
                        (r.get::<_, i64>(6)?, r.get::<_, i64>(7)?),
                        (r.get::<_, i64>(8)?, r.get::<_, i64>(9)?),
                    ],
                ))
            })?;
            for row in rows {
                let (ct, t, sums) = row?;
                entry(&mut checks, ct.clone(), t.clone());
                let c = checks.get_mut(&(ct, t)).expect("entrada recién creada");
                let r = |(total, ok): (i64, i64), no_data_hours: i64| UptimeRange {
                    pct: uptime_pct(ok, total),
                    total,
                    ok,
                    no_data_hours,
                };
                c.uptime = UptimeRanges {
                    h24: r(sums[0], no_data_hours.h24),
                    d7: r(sums[1], no_data_hours.d7),
                    d30: r(sums[2], no_data_hours.d30),
                    d365: r(sums[3], no_data_hours.d365),
                };
            }
        }

        // Barra: últimas 48 horas.
        {
            let mut stmt = self.conn.prepare(
                "SELECT check_type, target, hour_ts, total, ok, worst
                 FROM check_hourly
                 WHERE agent_id = ?1 AND hour_ts >= ?2 AND hour_ts <= ?3",
            )?;
            let rows = stmt.query_map(params![agent_id, bar_start, cur], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, String>(5)?,
                ))
            })?;
            for row in rows {
                let (ct, t, hour_ts, total, ok, worst) = row?;
                let Some(c) = checks.get_mut(&(ct, t)) else {
                    continue;
                };
                let idx = ((hour_ts - bar_start) / 3600) as usize;
                if let Some(b) = c.hours.get_mut(idx) {
                    b.total = total;
                    b.ok = ok;
                    b.worst = Some(worst);
                }
            }
        }

        // Incidencias: emparejar fail -> recover por check, en orden de la
        // hora real del evento. Varios fails seguidos (p.ej. warn -> critical)
        // son la misma incidencia.
        {
            let mut stmt = self.conn.prepare(
                "SELECT check_type, target, event, status, message, ts
                 FROM events
                 WHERE agent_id = ?1 AND event IN ('fail', 'recover')
                 ORDER BY ts ASC, id ASC",
            )?;
            let rows = stmt.query_map(params![agent_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, i64>(5)?,
                ))
            })?;
            let mut open: std::collections::HashMap<Key, Incident> = Default::default();
            for row in rows {
                let (ct, t, event, status, message, ts) = row?;
                let key = (ct, t);
                if event == "fail" {
                    match open.get_mut(&key) {
                        Some(inc) => {
                            if severity(&status) > severity(&inc.status) {
                                inc.status = status;
                            }
                        }
                        None => {
                            open.insert(
                                key,
                                Incident {
                                    start: ts,
                                    end: None,
                                    duration_seconds: 0,
                                    status,
                                    message,
                                },
                            );
                        }
                    }
                } else if let Some(mut inc) = open.remove(&key) {
                    inc.end = Some(ts);
                    inc.duration_seconds = (ts - inc.start).max(0);
                    if let Some(c) = checks.get_mut(&key) {
                        c.incidents.push(inc);
                    }
                }
            }
            for (key, mut inc) in open {
                inc.duration_seconds = (now - inc.start).max(0);
                if let Some(c) = checks.get_mut(&key) {
                    c.incidents.push(inc);
                }
            }
            for c in checks.values_mut() {
                c.incidents.sort_by(|a, b| b.start.cmp(&a.start));
                c.incidents.truncate(UPTIME_MAX_INCIDENTS);
            }
        }

        Ok(Some(AgentUptime {
            agent_id: agent_id.to_string(),
            now,
            hour_ts: cur,
            history_start,
            no_data_hours,
            checks: checks.into_values().collect(),
        }))
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

/// Porcentaje ok/total truncado a 2 decimales (truncado, no redondeado: un
/// 99.996% no debe mostrarse como 100%). None sin muestras.
fn uptime_pct(ok: i64, total: i64) -> Option<f64> {
    if total <= 0 {
        return None;
    }
    Some((ok.saturating_mul(10_000) / total) as f64 / 100.0)
}

/// Inicio de la hora (unix) que contiene `ts`.
pub fn hour_floor(ts: i64) -> i64 {
    ts - ts.rem_euclid(3600)
}

/// ¿Existe la columna en la tabla? (para migraciones idempotentes)
fn has_column(conn: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM pragma_table_info(?1) WHERE name = ?2",
        params![table, column],
        |r| r.get(0),
    )?;
    Ok(n > 0)
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
            event_id: None,
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
            history_days: 400,
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
            history_days: 400,
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
