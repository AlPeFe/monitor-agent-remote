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
| `retention`   | Retención de datos: días de eventos, horas de incidencias activas y minutos para marcar un servidor offline. La purga se aplica cada hora; el estado offline, en vivo (0 = indefinido). |

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
| `timestamp`      | number          | Unix seconds del evento.                                         |
| `ok_count`       | number?         | Solo heartbeat: comprobaciones OK.                               |
| `warn_count`     | number?         | Solo heartbeat: comprobaciones en Warning.                       |
| `critical_count` | number?         | Solo heartbeat: comprobaciones en Critical.                      |

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

Respuesta: `200 {"ok": true, "stored": 42, "agent_id": "server-web-01"}`.

## API

| Metodo | Ruta                          | Descripcion                              |
|--------|-------------------------------|------------------------------------------|
| POST   | `/api/ingest`                 | Recibe un evento de un agente.           |
| GET    | `/api/agents`                 | Estado agregado de todos los servidores. Un agente sin heartbeat dentro de `retention.offline_minutes` aparece con `status: "offline"`. |
| GET    | `/api/agents/{id}/events`     | Historial de eventos de un servidor.     |
| GET    | `/api/alerts`                 | Avisos activos (fail sin recover).       |
| GET    | `/api/settings`               | Config actual (listen, token, db_path, ingest_url). Con token definido exige `Authorization: Bearer`. |
| PUT    | `/api/settings`               | Guarda `listen` y `token` (aplica en vivo). |

Si defines `token` en la configuracion, `/api/ingest` **y** `/api/settings`
(`GET`/`PUT`) exigen la cabecera `Authorization: Bearer <token>`. El resto del
panel (estado, agentes, alertas) sigue siendo de solo lectura y sin
autenticación para la LAN. El navegador guarda el token de acceso en su
`localStorage` (campo *access token* del panel de ajustes).

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
