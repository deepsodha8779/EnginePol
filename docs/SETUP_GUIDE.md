# Evoture Tadpole Engine - Setup Guide


The `TADPOLES_COLLECTION` and `METRICS_COLLECTION` are reserved for future use. `ACTIONS_COLLECTION` is used by ActionBuilder when creating tasks from FAIL decisions.

## Build & Run

```bash
# From project root
cd tadpole-engine
cargo build

# Run the HTTP gateway (Actix web server on port 8083)
cargo run -p http-gateway
```

**Rust Edition:** The workspace uses `edition = "2024"` (requires Rust 1.85+). If you see edition-related errors, change `Cargo.toml` to `edition = "2021"`.

## Verify Setup

1. **Start MongoDB** (if local):
   ```bash
   # Windows: start mongod
   # Linux/Mac: mongod
   ```

2. **Start the gateway:**
   ```bash
   cargo run -p http-gateway
   ```

3. **Send a test event** (PowerShell):
   ```powershell
   Invoke-RestMethod -Uri "http://127.0.0.1:8083/intake" -Method POST -ContentType "application/json" -Body (Get-Content .\schema\event-envelope.example.json -Raw)
   ```

4. **Check diagnostics:**
   ```powershell
   Invoke-RestMethod -Uri "http://127.0.0.1:8083/diagnostics" -Method GET
   ```

## Event Name Mismatch

Your **dummy event** uses:
```json
"event_name": "InvoiceReceivedForExternal"
```

The **playbooks.json** matches:
```json
"event_name == \"InvoiceReceivedForExternalDependency\""
```

To have your dummy event match a playbook, either:
- Change the event to `"InvoiceReceivedForExternalDependency"`, or
- Add a playbook that matches `"InvoiceReceivedForExternal"`

The `schema/event-envelope.example.json` uses `InvoiceReceivedForExternalDependency` and will match.

## RabbitMQ consumer

The gateway can consume events from RabbitMQ in addition to HTTP POST `/intake`. If `RABBITMQ_URL` is set, a background consumer starts automatically.

### Required env vars

| Variable | Description | Example |
|----------|-------------|---------|
| **RABBITMQ_URL** | Full AMQP connection URL including credentials. Use **AMQP** (not the Management UI URL). | See below |
| **RABBITMQ_QUEUE** | Queue name to consume from | `events_intake` (default) |

### URL format

- **Management UI** (browser): `https://mq.evotu.re/#/channels` — used only for the web UI and HTTP API.
- **AMQP** (engine connection): use one of:
  - **TLS:** `amqps://USERNAME:PASSWORD@mq.evotu.re:5671/%2F`
  - **Non-TLS:** `amqp://USERNAME:PASSWORD@mq.evotu.re:5672/%2F`

Use your RabbitMQ username and password in place of `USERNAME` and `PASSWORD`. The `%2F` is the URL-encoded virtual host `/`. If your vhost is different, encode it (e.g. `%2Fmyvhost`).

### Example .env

```env
# Enable RabbitMQ consumer (omit or leave empty to disable)
RABBITMQ_URL=amqps://your_user:your_password@mq.evotu.re:5671/%2F
RABBITMQ_QUEUE=events_intake
```

If `RABBITMQ_URL` is not set or empty, the consumer does not start and the gateway runs with HTTP intake only.

## Troubleshooting

**"found invalid metadata files for crate" / "corrupt metadata"**  
The build cache can become inconsistent (e.g. after an interrupted build or when switching toolchains). Fix it by cleaning and rebuilding:

```bash
cargo clean
cargo build -p http-gateway
```

**RabbitMQ: "received corrupt message of type InvalidContentType"**  
This usually means a protocol mismatch between client and broker:

- **Try non-TLS first:** If you use `amqps://`, switch to `amqp://` and port **5672** (if your broker allows plain AMQP). Some hosted brokers (e.g. CloudAMQP) use TLS on a different port or require specific TLS settings.
- **Try TLS if you use plain:** If you use `amqp://` and the broker expects TLS, use `amqps://` and port **5671**.
- **Check broker docs** for the exact AMQP URL (scheme, port, vhost). The Management UI URL (`https://...`) is not the AMQP URL.

If the error persists, run without RabbitMQ (unset `RABBITMQ_URL`) and use HTTP POST `/intake` to test the pipeline.

## WorkRouter (optional)

When ActionBuilder creates an action, WorkRouter resolves **assignee** (user/team/queue) and **notification channels**. If no config is set, all actions go to a fallback queue.

| Variable | Description | Default |
|----------|-------------|---------|
| **WORK_ROUTER_FALLBACK_QUEUE** | Queue ID when no role matches | `default-governance-queue` |
| **WORK_ROUTER_ROLES_CONFIG** | Path to JSON file mapping role keys to assignees | (none) |

Example JSON at `config/work_router_roles.example.json`. Keys can be `tenant_id:task_type`, `task_type`, or `playbook_id`. Each value: `type` (user|team|queue), `id`, optional `display_name`, `notification_channels` (email, slack, teams).
