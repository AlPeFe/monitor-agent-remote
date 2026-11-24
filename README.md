<p align="center">
  <img src="docs/logo.svg" alt="monitor-agent-remote" width="180" />
</p>

<h1 align="center">monitor-agent-remote</h1>

<p align="center">
  Receptor central de avisos de <a href="https://github.com/AlPeFe/monitor-agent">monitor-agent</a>.
  Recibe los eventos de N servidores y los muestra en un panel web con SQLite.
</p>

<p align="center">
  <img alt="Rust" src="https://img.shields.io/badge/Rust-1.96+-deA584?logo=rust&logoColor=white" />
  <img alt="Axum" src="https://img.shields.io/badge/Axum-0.8-000000" />
  <img alt="SQLite" src="https://img.shields.io/badge/SQLite-bundled-FFB7CE" />
</p>

---

## Que es

Cada servidor con **monitor-agent** puede exportar sus avisos (fallos y
recuperaciones) a este servicio mediante un simple POST. **monitor-agent-remote**
los guarda en SQLite y muestra el estado de todos los servidores en un panel web
con el mismo estilo rosa pastel que el agente.

> Guía completa de instalación y uso (agente + central) en el repo del agente:
> [`docs/GUIA-USO-INSTALACION.md`](https://github.com/AlPeFe/monitor-agent/blob/master/docs/GUIA-USO-INSTALACION.md)

## Uso rapido

Requisitos: **Rust 1.96+**.

```powershell
cargo run --release
```

Abre `http://127.0.0.1:9600`. La configuración (dirección de escucha, token y
URL de ingest) se gestiona desde la **rueda de ajustes** de la web y se guarda
en SQLite: no hace falta tocar ficheros de configuración. Para que los agentes
de la red local puedan avisar, cambia `listen` a `0.0.0.0:9600` en el panel de
ajustes (se aplica al instante, sin reiniciar).

## Configuracion

Toda la configuración se hace desde la UI (panel de ajustes) y se persiste en
SQLite, así que el `config.toml` es **opcional** y solo sirve para fijar la
ruta de la base de datos antes del primer arranque:

```toml
#db_path = "monitor.db"   # ruta SQLite (solo se lee la primera vez)
#listen  = "0.0.0.0:9600" # semilla inicial opcional
#token   = "CAMBIAME"     # semilla inicial opcional
```

Campos gestionables desde la web:

| Campo         | Descripcion                                                                 |
|---------------|-----------------------------------------------------------------------------|
| `listen`      | Dirección del panel + API. Se aplica en vivo (rebind) sin reiniciar.        |
| `token`       | Opcional. Si se define, `/api/ingest` exige `Authorization: Bearer <token>`.|
| `db_path`     | Solo lectura (elegida antes del primer arranque).                           |
| `ingest_url`  | URL que deben apuntar los `[exporter]` de los agentes.                      |
| `retention`   | Retención de datos: días de eventos (`events_days`), horas de incidencias activas (`alerts_hours`), minutos para marcar un servidor offline (`offline_minutes`) y días de historial de uptime por hora (`history_days`, 400 por defecto). La purga se aplica cada hora; el estado offline, en vivo (0 = indefinido). |
| `alert`       | Alertas por correo de la central: avisa cuando un servidor deja de enviar señales (y cuando vuelve). Incluye `enabled`, `to[]`, `cooldown_minutes` (0 = sólo el primer correo), `recovery_notifications` y el bloque `smtp` (host, port, tls, username, password, from). Con `username` vacío no se envía AUTH. El botón "enviar correo de prueba" del panel valida la configuración. |

## Contrato del webhook

Cada **monitor-agent** hace `POST /api/ingest` con `Content-Type: application/json`:

| Campo            | Tipo            | Descripcion                                                     |
|------------------|-----------------|-----------------------------------------------------------------|
| `agent_id`       | string          | Identificador del servidor emisor (`[exporter].agent_id`).       |
| `agent_host`     | string?         | Hostname del emisor.                                             |
| `event`          | string          | `fail`, `recover` o `heartbeat`.                                 |
| `check_type`     | string          | Tipo de comprobacion (`process`, `disk`, `http_endpoint`, ...).  |
| `target`         | string          | Recurso comprobado (ruta, URL, letra de disco...).               |
| `status`         | string          | `ok`, `warn`, `critical` o `unknown`.                            |
| `message`        | string          | Mensaje legible de la comprobacion.                              |
| `value`          | number?         | Valor numerico opcional.                                         |
| `timestamp`      | number          | Unix seconds del evento (hora real del agente). Es la que usa el historial; `last_seen` es la hora de llegada. |
| `event_id`       | string?         | Id único del evento generado por el agente. Si la central ya recibió ese `(agent_id, event_id)`, responde `200` con `"duplicate": true` y no procesa nada. Sin él, todo funciona como antes. |
| `ok_count`       | number?         | Solo heartbeat: comprobaciones OK.                               |
| `warn_count`     | number?         | Solo heartbeat: comprobaciones en Warning.                       |
| `critical_count` | number?         | Solo heartbeat: comprobaciones en Critical.                      |
| `checks`         | array?          | Solo heartbeat: inventario de TODAS las comprobaciones del servidor (aunque estén OK), con `check_type`, `target`, `status`, `message` y `value`. La central refresca con él su vista "estado actual" y borra los checks que el agente ya no vigila. |

Ejemplo:

```json
{
  "agent_id": "server-web-01",
  "agent_host": "web01",
  "event": "fail",
  "check_type": "http_endpoint",
  "target": "https://example.com/health",
  "status": "critical",
  "message": "HTTP 500 (expected 200)",
  "value": 500,
  "timestamp": 1754123456
}
```

Respuesta: `200 {"ok": true, "stored": 42, "agent_id": "server-web-01", "duplicate": false}`.

Un heartbeat con `timestamp` anterior al último heartbeat aplicado de ese
agente (un reenvío atrasado) no pisa el estado actual ni el inventario de
checks: solo cuenta para el historial.

### Lotes

`POST /api/ingest/batch` recibe un array JSON con los mismos objetos que
`/api/ingest` (máximo 500) y los aplica en orden en una sola transacción. Lo
usa el agente para reenviar su backlog tras un corte. Exige el mismo token.
Si algún elemento no trae `agent_id` o `event`, se rechaza el lote entero
(`400`); con más de 500 elementos, `413`.

Respuesta: `200 {"ok": true, "stored": 120, "duplicates": 3}`.

## API

| Metodo | Ruta                          | Descripcion                              |
|--------|-------------------------------|------------------------------------------|
| POST   | `/api/ingest`                 | Recibe un evento de un agente.           |
| POST   | `/api/ingest/batch`           | Recibe un lote de eventos (máx. 500) en una transacción. |
| POST   | `/api/alert/test`             | Envía un correo de prueba con la config SMTP dada (exige token si lo hay). |
| GET    | `/api/agents`                 | Estado agregado de todos los servidores. Un agente sin heartbeat dentro de `retention.offline_minutes` aparece con `status: "offline"`. |
| GET    | `/api/agents/{id}/events`     | Historial de eventos de un servidor.     |
| GET    | `/api/agents/{id}/uptime`     | Uptime por check. Ver abajo.             |
| GET    | `/api/alerts`                 | Avisos activos (fail sin recover).       |
| GET    | `/api/settings`               | Config actual (listen, token, db_path, ingest_url). Con token definido exige `Authorization: Bearer`. |
| PUT    | `/api/settings`               | Guarda `listen` y `token` (aplica en vivo). |

Si defines `token` en la configuracion, `/api/ingest` **y** `/api/settings`
(`GET`/`PUT`) exigen la cabecera `Authorization: Bearer <token>`. El resto del
panel (estado, agentes, alertas) sigue siendo de solo lectura y sin
autenticación para la LAN. El navegador guarda el token de acceso en su
`localStorage` (campo *access token* del panel de ajustes).

### Uptime

La central guarda, por cada check del inventario del heartbeat, cuántas
muestras llegaron en cada hora y cuántas estaban en `ok` (hora del
`timestamp` del heartbeat). Se conservan `retention.history_days` días.

`GET /api/agents/{id}/uptime` devuelve `404` si el agente no existe y, si no,
`{agent_id, now, hour_ts, checks: [...]}`. Cada check trae:

- `uptime`: objetos `24h`, `7d`, `30d` y `365d` con `pct` (ok/total en %,
  truncado a 2 decimales; `null` si no hay muestras), `total` y `ok`. Cada
  rango cuenta la hora actual, aunque esté a medias.
- `hours`: las últimas 48 horas, de la más antigua a la actual, con
  `hour_ts`, `total`, `ok` y `worst` (peor estado de la hora, `null` sin
  muestras).
- `incidents`: hasta 50 pares fail → recover sacados de `events`, la más
  reciente primero, con `start`, `end` (`null` si sigue abierta),
  `duration_seconds` (hasta ahora si sigue abierta), `status` y `message`.
  Solo cubren lo que queda en `events` (`retention.events_days`).

## Log

La central escribe su traza en `monitor-agent-remote.log` (junto a la
configuración / `monitor.db`) además de stderr: imprescindible cuando corre
como tarea programada o servicio, donde stderr no lo recoge nadie.

## Compilar

```powershell
cargo test
cargo build --release
```

El ejecutable queda en `target/release/monitor-agent-remote.exe`.
