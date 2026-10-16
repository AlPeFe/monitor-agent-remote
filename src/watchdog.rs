//! Watchdog de servidores sin señales.
//!
//! La central es la única que puede avisar de que un **servidor entero** se ha
//! caído: si se apaga la máquina (o el servicio del agente), el propio agente ya
//! no puede mandar su correo. Este worker mira cada 30s qué agentes llevan más
//! de `retention.offline_minutes` sin enviar heartbeats y manda el aviso,
//! además del correo de recuperación cuando vuelven.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};

use crate::alert::{self, AlertSettings, RETRY_BACKOFF_SECONDS};
use crate::config::Settings;
use crate::db::{Database, OfflineAction, decide_offline_alerts};
use crate::log_error;
use crate::log_info;

/// Cada cuánto se comprueba si algún servidor ha dejado de dar señales. Es
/// independiente de `offline_minutes`: cuanto más corto, antes se detecta.
const CHECK_INTERVAL: Duration = Duration::from_secs(30);

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

pub async fn run(db: Arc<Mutex<Database>>, settings: Arc<RwLock<Settings>>) {
    let mut interval = tokio::time::interval(CHECK_INTERVAL);
    loop {
        interval.tick().await; // el primer tick es inmediato
        if let Err(e) = ciclo(&db, &settings).await {
            log_error!("watchdog: {e}");
        }
    }
}

async fn ciclo(
    db: &Arc<Mutex<Database>>,
    settings: &Arc<RwLock<Settings>>,
) -> Result<(), String> {
    let (alerta, offline_minutes) = {
        let s = settings.read().await;
        (s.alert.clone(), s.retention.offline_minutes)
    };
    if !alerta.enabled || !alerta.is_configured() {
        return Ok(());
    }

    let now = now_unix();
    let cooldown_seconds = alerta.cooldown_minutes.saturating_mul(60) as i64;
    let offline_seconds = offline_minutes.max(0).saturating_mul(60);

    // 1) Decidir con la BD bloqueada sólo lo justo, y dejar ya marcado el
    //    estado "caído" para que un hipo al enviar no genere duplicados.
    let (acciones, info) = {
        let mut db = db.lock().await;
        let agentes = db.list_agents().map_err(|e| e.to_string())?;
        let estado = db.list_offline_alerts().map_err(|e| e.to_string())?;
        let pares: Vec<(String, i64)> = agentes
            .iter()
            .map(|a| (a.agent_id.clone(), a.last_seen))
            .collect();
        let info: HashMap<String, (String, i64)> = agentes
            .iter()
            .map(|a| (a.agent_id.clone(), (a.agent_host.clone(), a.last_seen)))
            .collect();
        let acciones = decide_offline_alerts(&pares, &estado, now, offline_seconds, cooldown_seconds);
        for accion in &acciones {
            if let OfflineAction::NotifyDown { agent_id, since } = accion {
                db.insert_offline_alert(agent_id, *since)
                    .map_err(|e| e.to_string())?;
            }
        }
        (acciones, info)
    };

    // 2) Enviar fuera del lock y registrar el resultado.
    for accion in acciones {
        let (agent_id, since, es_caida) = match &accion {
            OfflineAction::NotifyDown { agent_id, since } => (agent_id.clone(), *since, true),
            OfflineAction::RenotifyDown { agent_id, since } => (agent_id.clone(), *since, true),
            OfflineAction::NotifyUp { agent_id, since } => (agent_id.clone(), *since, false),
        };
        let (host, last_seen) = info
            .get(&agent_id)
            .cloned()
            .unwrap_or_else(|| (String::new(), 0));

        if !es_caida && !alerta.recovery_notifications {
            // No se avisa de las recuperaciones: basta con olvidar el estado.
            let mut db = db.lock().await;
            db.delete_offline_alert(&agent_id).map_err(|e| e.to_string())?;
            continue;
        }

        let (subject, body) = if es_caida {
            (
                alert::subject_down(&agent_id),
                alert::body_down(&agent_id, &host, since, last_seen, offline_minutes),
            )
        } else {
            (
                alert::subject_up(&agent_id),
                alert::body_up(&agent_id, &host, since, now),
            )
        };

        let resultado = alert::send_mail(
            &alerta.smtp,
            &alerta.to,
            &subject,
            &body,
            alert::SMTP_TIMEOUT,
        )
        .await;

        let mut db = db.lock().await;
        match resultado {
            Ok(()) => {
                if es_caida {
                    db.mark_offline_alert_sent(&agent_id, now)
                        .map_err(|e| e.to_string())?;
                    log_info!("watchdog: aviso enviado, servidor {agent_id} sin señales");
                } else {
                    db.delete_offline_alert(&agent_id).map_err(|e| e.to_string())?;
                    log_info!("watchdog: recuperación avisada, servidor {agent_id} de vuelta");
                }
            }
            Err(e) => {
                // No se pierde: si es una caída queda pendiente de reintento;
                // si es una recuperación se reintenta en el ciclo siguiente.
                if es_caida {
                    db.mark_offline_alert_error(&agent_id, now + RETRY_BACKOFF_SECONDS)
                        .map_err(|e| e.to_string())?;
                }
                log_error!(
                    "watchdog: no se pudo avisar de {agent_id} ({e}); se reintentará"
                );
            }
        }
    }

    Ok(())
}

/// Envía un correo de prueba con la configuración dada (botón del panel).
pub async fn send_test(smtp: &alert::SmtpSettings, to: &[String]) -> Result<(), String> {
    let body = "Si recibes este correo, las alertas de la central (monitor-agent-remote) están bien configuradas.\n\
                Avisarán cuando un servidor deje de enviar señales y cuando vuelva.\n";
    alert::send_mail(
        smtp,
        to,
        "[monitor-central] Prueba de alertas",
        body,
        alert::SMTP_TEST_TIMEOUT,
    )
    .await
}
