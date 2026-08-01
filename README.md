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

## Uso rapido

Requisitos: **Rust 1.96+**.

```powershell
Copy-Item config.example.toml config.toml
cargo run --release
```

Abre `http://127.0.0.1:9600` (cambia `listen` a `0.0.0.0:9600` para acceder desde
la red). El primer aviso que reciba crea automáticamente el servidor en la vista.

## Configuracion

```toml
listen = "127.0.0.1:9600"   # panel + API (0.0.0.0 para red local)
db_path = "monitor.db"       # base SQLite
#token = "CAMBIAME"          # opcional: exige Authorization: Bearer <token>
```

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
| GET    | `/api/agents`                 | Estado agregado de todos los servidores. |
| GET    | `/api/agents/{id}/events`     | Historial de eventos de un servidor.     |
| GET    | `/api/alerts`                 | Avisos activos (fail sin recover).       |

Si defines `token` en la configuracion, `/api/ingest` exige la cabecera
`Authorization: Bearer <token>`.

## Compilar

```powershell
cargo test
cargo build --release
```

El ejecutable queda en `target/release/monitor-agent-remote.exe`.
