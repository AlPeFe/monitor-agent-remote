//! Alertas por correo de la central.
//!
//! La central es la única que puede avisar de que un **servidor entero** se ha
//! caído: si se apaga la máquina (o el agente), el propio agente ya no puede
//! mandar su correo. Aquí vive el envío SMTP y la configuración editable desde
//! el panel (se persiste en `settings.alert_json`).

use std::time::Duration;

use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use serde::{Deserialize, Serialize};

use crate::log_error;

/// Timeout de todo el envío. Sin esto, un servidor que acepta la conexión y no
/// contesta (firewall que blackholea el 587) dejaría colgado al watchdog.
pub const SMTP_TIMEOUT: Duration = Duration::from_secs(15);

/// El botón de prueba del panel espera de forma interactiva: más margen.
pub const SMTP_TEST_TIMEOUT: Duration = Duration::from_secs(40);

/// Backoff tras un aviso que no se pudo enviar.
pub const RETRY_BACKOFF_SECONDS: i64 = 60;

fn default_true() -> bool {
    true
}

fn default_cooldown() -> u64 {
    15
}

fn default_port() -> u16 {
    587
}

fn default_tls() -> AlertTls {
    AlertTls::Starttls
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AlertTls {
    #[default]
    Starttls,
    Implicit,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmtpSettings {
    #[serde(default)]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_tls")]
    pub tls: AlertTls,
    /// Usuario SMTP. **Vacío = sin autenticación**: hay relays internos y
    /// servidores de pruebas que no anuncian AUTH, y mandar credenciales ahí
    /// hace fallar el envío entero.
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub from: String,
}

impl Default for SmtpSettings {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: default_port(),
            tls: AlertTls::default(),
            username: String::new(),
            password: String::new(),
            from: String::new(),
        }
    }
}

impl SmtpSettings {
    pub fn uses_auth(&self) -> bool {
        !self.username.trim().is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertSettings {
    #[serde(default)]
    pub enabled: bool,
    /// Re-aviso mientras el servidor siga caído. 0 = sólo el primer correo.
    #[serde(default = "default_cooldown")]
    pub cooldown_minutes: u64,
    /// Avisar también cuando el servidor vuelve.
    #[serde(default = "default_true")]
    pub recovery_notifications: bool,
    #[serde(default)]
    pub to: Vec<String>,
    #[serde(default)]
    pub smtp: SmtpSettings,
}

impl Default for AlertSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            cooldown_minutes: default_cooldown(),
            recovery_notifications: true,
            to: Vec::new(),
            smtp: SmtpSettings::default(),
        }
    }
}

impl AlertSettings {
    /// ¿Hay todo lo necesario para intentar enviar?
    pub fn is_configured(&self) -> bool {
        !self.to.is_empty() && !self.smtp.host.trim().is_empty() && !self.smtp.from.trim().is_empty()
    }

    /// Errores de configuración visibles para el usuario (el panel los muestra
    /// al guardar), en el mismo estilo que la validación del agente.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if self.to.is_empty() {
            return Err("alertas de la central: falta al menos un destinatario".to_string());
        }
        if self.smtp.host.trim().is_empty() {
            return Err("alertas de la central: falta el host SMTP".to_string());
        }
        if self.smtp.port == 0 {
            return Err("alertas de la central: el puerto SMTP no puede ser 0".to_string());
        }
        if self.smtp.from.trim().is_empty() {
            return Err("alertas de la central: falta el remitente (from)".to_string());
        }
        if self.smtp.username.trim().is_empty() && !self.smtp.password.is_empty() {
            return Err(
                "alertas de la central: hay contraseña pero no usuario (deja ambos vacíos para un SMTP sin autenticación)"
                    .to_string(),
            );
        }
        Ok(())
    }
}

fn resolve_password(raw: &str) -> Option<String> {
    if let Some(var_name) = raw.strip_prefix("env:") {
        std::env::var(var_name).ok()
    } else {
        Some(raw.to_string())
    }
}

/// Envía un correo por SMTP. `timeout` acota TODO el envío (el `timeout` del
/// builder de lettre sólo cubre el connect TCP).
pub async fn send_mail(
    smtp: &SmtpSettings,
    recipients: &[String],
    subject: &str,
    body: &str,
    timeout: Duration,
) -> Result<(), String> {
    let from = smtp
        .from
        .parse::<Mailbox>()
        .map_err(|e| format!("remitente inválido '{}': {e}", smtp.from))?;
    let mut builder = Message::builder().from(from).subject(subject);
    for recipient in recipients {
        let mailbox = recipient
            .parse::<Mailbox>()
            .map_err(|e| format!("destinatario inválido '{recipient}': {e}"))?;
        builder = builder.to(mailbox);
    }
    let message = builder
        .body(body.to_string())
        .map_err(|e| format!("no se pudo construir el correo: {e}"))?;

    let mut transport = match smtp.tls {
        AlertTls::Starttls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&smtp.host)
            .map_err(|e| e.to_string())?,
        AlertTls::Implicit => {
            AsyncSmtpTransport::<Tokio1Executor>::relay(&smtp.host).map_err(|e| e.to_string())?
        }
        AlertTls::None => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&smtp.host),
    };
    transport = transport.port(smtp.port).timeout(Some(SMTP_TIMEOUT));
    if smtp.uses_auth() {
        let password = resolve_password(&smtp.password).ok_or_else(|| {
            "la variable de entorno de la contraseña SMTP no está definida".to_string()
        })?;
        transport = transport.credentials(Credentials::new(smtp.username.clone(), password));
    }
    let mailer = transport.build();

    match tokio::time::timeout(timeout, mailer.send(message)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!(
            "tiempo de espera agotado ({}s) hablando con {}:{}",
            timeout.as_secs(),
            smtp.host,
            smtp.port
        )),
    }
}

// ------------------------------------------------------------------ correos

/// Asunto del aviso de caída. **Sólo ASCII a propósito**: los asuntos con
/// acentos viajan como encoded-words de RFC 2047 y algunos servidores/clientes
/// los destrozan (visto con Mailpit: "SEÑALES" llegaba como "SEÑAL").
pub fn subject_down(agent_id: &str) -> String {
    format!("[monitor-central] SERVIDOR CAIDO: {agent_id}")
}

/// Asunto del aviso de recuperación (ASCII, mismo motivo).
pub fn subject_up(agent_id: &str) -> String {
    format!("[monitor-central] SERVIDOR RECUPERADO: {agent_id}")
}

pub fn body_down(agent_id: &str, host: &str, since: i64, last_seen: i64, offline_minutes: i64) -> String {
    // El cuerpo sí puede llevar acentos: va en quoted-printable y llega bien.
    let unidad = if offline_minutes == 1 { "minuto" } else { "minutos" };
    format!(
        "El servidor {agent_id} lleva más de {offline_minutes} {unidad} sin enviar señales.\n\
         \n\
         agente:            {agent_id}\n\
         host:              {host}\n\
         sin señales desde: {last_seen}\n\
         detectado:         {since}\n\
         \n\
         Puede estar apagado, con el servicio monitor-agent parado o sin red.\n\
         Último estado conocido en el panel de la central.\n"
    )
}

pub fn body_up(agent_id: &str, host: &str, down_since: i64, now: i64) -> String {
    format!(
        "El servidor {agent_id} vuelve a enviar señales.\n\
         \n\
         agente:      {agent_id}\n\
         host:        {host}\n\
         caído desde: {down_since}\n\
         recuperado:  {now}\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sin_usuario_no_hay_auth() {
        let mut smtp = SmtpSettings::default();
        assert!(!smtp.uses_auth());
        smtp.username = "  ".to_string();
        assert!(!smtp.uses_auth(), "espacios no cuentan como usuario");
        smtp.username = "alertas@empresa.com".to_string();
        assert!(smtp.uses_auth());
    }

    #[test]
    fn validacion_de_alertas() {
        let mut a = AlertSettings::default();
        assert!(a.validate().is_ok(), "desactivadas: nada que validar");

        a.enabled = true;
        assert!(a.validate().is_err(), "sin destinatarios ni smtp");
        a.to = vec!["ops@empresa.com".to_string()];
        assert!(a.validate().is_err(), "sin host");
        a.smtp.host = "smtp.empresa.com".to_string();
        assert!(a.validate().is_err(), "sin from");
        a.smtp.from = "alertas@empresa.com".to_string();
        assert!(a.validate().is_ok(), "completo y sin auth");
        assert!(a.is_configured());

        a.smtp.password = "secreto".to_string();
        assert!(a.validate().is_err(), "contraseña sin usuario no tiene sentido");
        a.smtp.username = "alertas@empresa.com".to_string();
        assert!(a.validate().is_ok());
    }

    #[test]
    fn json_antiguo_o_parcial_no_revienta() {
        // JSON de una versión anterior (sin campos nuevos) debe cargar.
        let a: AlertSettings = serde_json::from_str(r#"{"enabled":true,"to":["x@y.z"]}"#).unwrap();
        assert!(a.enabled);
        assert_eq!(a.cooldown_minutes, 15, "default del cooldown");
        assert!(a.recovery_notifications);
        assert_eq!(a.smtp.port, 587);
        assert_eq!(a.smtp.tls, AlertTls::Starttls);
    }

    #[test]
    fn cuerpos_mencionan_el_servidor() {
        let b = body_down("srv-01", "srv01", 100, 50, 5);
        assert!(b.contains("srv-01") && b.contains("srv01") && b.contains("5 minutos"));
        let r = body_up("srv-01", "srv01", 100, 200);
        assert!(r.contains("srv-01") && r.contains("caído desde: 100"));
    }

    /// Los asuntos van en ASCII: con acentos, el encoded-word RFC 2047 se
    /// destroza por el camino (visto con Mailpit).
    #[test]
    fn asuntos_en_ascii() {
        assert!(subject_down("srv-01").is_ascii());
        assert!(subject_up("srv-01").is_ascii());
        assert!(subject_down("srv-01").contains("SERVIDOR CAIDO: srv-01"));
        assert!(subject_up("srv-01").contains("SERVIDOR RECUPERADO: srv-01"));
    }

    #[test]
    fn el_cuerpo_usa_minuto_en_singular() {
        assert!(body_down("a", "h", 1, 0, 1).contains("1 minuto sin"));
        assert!(body_down("a", "h", 1, 0, 7).contains("7 minutos sin"));
    }
}
