# Dynatrace Extension 2.0 for Apache Airflow (`custom:apache-airflow`)

| Property | Value |
|---|---|
| **Extension Name** | `custom:apache-airflow` |
| **Extension Version** | `1.0.0` |
| **Minimum Dynatrace Version** | `1.285+` (Extensions 2.0 Python SDK `dt-extensions-sdk >= 1.11.0`) |
| **Supported Airflow Versions** | **Apache Airflow 2.10+ / 2.11** (`/api/v1`) and **Apache Airflow 3.0 – 3.4+** (`/api/v2`) |
| **Execution Modes** | Remote (Dynatrace ActiveGate) and Local (Dynatrace OneAgent) |
| **License** | Apache-2.0 |

Enterprise-grade **Dynatrace Extension 2.0** written in Python using `dt-extensions-sdk` to monitor self-hosted and managed **Apache Airflow** deployments (Kubernetes, VMs, AWS MWAA, Google Cloud Composer, Astronomer). It collects **19 low-cardinality golden-signal metrics**, extracts **3 custom Smartscape topology entity types**, streams **structured Grail log events** for DAG run and failed task completions (plus optional audit logs), and raises **Davis problem events** on control-plane outages or broken DAG deployments.

> Companion OpenTelemetry Collector receiver: [`npcomplete777/airflowreceiver`](https://github.com/npcomplete777/airflowreceiver) — shares identical metric keys and dimension names so Smartscape topology rules in `custom:apache-airflow` work seamlessly with both ActiveGate polling and OTLP ingestion.

---

## Key Capabilities

1. **Zero-Config Airflow 2 & Airflow 3 Auto-Detection**:
   - Automatically probes `/api/v2/monitor/health` (Airflow 3.x) and falls back to `/api/v1/health` (Airflow 2.10+/2.11).
   - Supports **Airflow 3 JWT authentication** (`POST /auth/token` with in-memory token expiry caching, proactive 60-second pre-expiry refresh, and automatic re-authentication on `401`/`403` after `jwt_secret` rotation), **Airflow 2 HTTP Basic Auth**, and **static Bearer tokens** (`username` left empty, token placed in `password`).
2. **O(1) Cross-DAG Batch Polling & Exact Watermarks**:
   - Eliminates `O(DAGs × Runs)` N+1 HTTP fan-out by querying Airflow's cross-DAG `~/dagRuns` and `~/taskInstances` endpoints (`GET /api/v2/dags/~/dagRuns/~/taskInstances` on Airflow 3; `POST /api/v1/dags/~/dagRuns/~/taskInstances/list` on Airflow 2) with bounded pagination (`page_size`, `max_pages`) and an LRU-bounded watermark deduplicator (`BoundedSet`).
3. **Strict Cardinality Governance**:
   - Metrics never attach high-cardinality execution IDs (`dag_run_id`, `try_number`, `map_index`, worker `hostname`). Instead, per-run and failed/upstream-failed task attempts are emitted as structured **Grail Log Events** (`report_log_events`).
4. **Native Dynatrace MINT Semantics**:
   - Emits `gauge` state signals, `count,delta` (`MetricType.DELTA`) completion counters, and `SummaryStat` (`min`/`max`/`sum`/`count`) latency distributions for `airflow.dag_run.duration`, `airflow.dag_run.schedule_delay`, and `airflow.task_instance.duration`.

---

## Repository Layout

```text
dynatrace-airflow-extension/
├── airflow_extension/
│   ├── __init__.py
│   ├── __main__.py              # Extension 2.0 entrypoint (fastcheck, schedule, metrics, logs, Davis events)
│   └── client.py                # Dual-version Airflow v1/v2 REST client, JWT caching, pagination & dedup
├── extension/
│   ├── extension.yaml           # Extension 2.0 manifest: 19 metric definitions + Smartscape topology
│   └── activationSchema.json    # Dynatrace Hub / Settings UI configuration schema
├── scripts/
│   └── deploy_and_verify.py     # Automated Vault CA helper, bundle uploader & Grail DQL verifier
├── tests/
│   └── test_extension.py        # Unit tests (JWT caching, v2 auto-detection, BoundedSet eviction)
├── activation.json              # Sample local activation config referencing {{secret}} placeholders
├── secrets.example.json         # Template for local secrets.json (gitignored)
├── Makefile                     # Developer & CI automation targets
├── ruff.toml                    # Ruff formatter & linter configuration
└── setup.py                     # Python package definition
```

---

## Prerequisites

- **Python** `3.10+` (`3.12` recommended for local development)
- **Dynatrace Environment** `1.285+` (SaaS `https://<ENV_ID>.apps.dynatrace.com` or Managed `https://<HOST>/e/<ENV_ID>`)
- **Dynatrace ActiveGate** (`1.285+`, Linux `x86_64`) with Extensions 2.0 execution enabled and network access to your Airflow Webserver / API Server URL (for `pythonRemote` execution)
- **Dynatrace Platform Token (`dt0s16.*`)** or **API Token (`dt0c01.*`)** for uploading & activating the extension:
  - Platform Token scopes (`https://<ENV_ID>.apps.dynatrace.com`):
    - `extensions:definitions:write` & `extensions:configurations:write` (upload & activate extension)
    - `environment-api:credentials:write` *(optional — only needed if using `scripts/deploy_and_verify.py` to upload `certs/ca.pem` via API instead of the UI)*
    - `storage:metrics:read`, `storage:logs:read`, `storage:entities:read` *(optional — used by `scripts/deploy_and_verify.py --verify-only` to run Grail DQL checks)*

---

## Quick Start — Local Development & Testing

```bash
# 1. Create virtualenv and install package + dt-extensions-sdk[cli]
make install

# 2. Lint and run unit tests
make lint
make test

# 3. Copy secrets template and edit activation.json + secrets.json for your Airflow URL(s)
cp secrets.example.json secrets.json

# 4. Verify connectivity & authentication against live Airflow (--fastcheck)
make fastcheck
# Or directly:
# .venv/bin/dt-sdk run . --activation-config activation.json --secrets secrets.json --fastcheck

# 5. Run continuous local collection (prints MINT metric lines, log events & Davis events to stdout)
make run
```

> **Note on `secrets.json` Placeholders**: `dt-extensions-sdk` substitutes `{{secret_key}}` (double curly braces, **no** leading dot) in `activation.json` with matching keys from `secrets.json`. Never commit `secrets.json` to version control.

---

## Build, Sign, Upload & Activate in Dynatrace

Dynatrace Extensions 2.0 require every custom extension zip bundle to be cryptographically signed by a developer keypair whose **Root CA public certificate (`ca.pem`)** is registered in your Dynatrace **Credential Vault**.

### Step 1: Generate Signing Certificates (Once per Organization/Developer)

```bash
make gencerts
# Equivalent to:
# mkdir -p certs && .venv/bin/dt-sdk gencerts -o certs --ca_pass "" --dev_pass "" -f
```

This creates:
- `certs/ca.pem` — **Public Root CA certificate** (upload this to Dynatrace Credential Vault)
- `certs/ca.key` & `certs/developer.pem` — **Private keys** (gitignored; keep secure and never commit)

### Step 2: Add `certs/ca.pem` to Dynatrace Credential Vault

1. Open your Dynatrace environment and navigate to **Credential Vault** (Settings → Preferences → Limit extension execution, or search for **Credentials** app).
2. Click **Add credential** → choose **Public Certificate**.
3. Upload `certs/ca.pem`, set a descriptive name (e.g., `Apache Airflow Extension Root CA`), and enable **Extension validation** under credential scope.
4. Save. *(If running on a self-hosted ActiveGate with strict local CA verification, also place `ca.pem` under `/var/lib/dynatrace/remotepluginmodule/agent/conf/certificates/` on Linux ActiveGate hosts as documented by Dynatrace).*

### Step 3: Build & Sign the Extension Bundle (`dist/custom_apache-airflow-1.0.0.zip`)

`make build-bundle` packages pure-Python wheels plus `manylinux2014_x86_64` wheels for **both Python 3.10 and Python 3.14** ActiveGate runtimes (works cleanly even when building from macOS Apple Silicon `arm64`):

```bash
make build-bundle
```

<details>
<summary>Manual <code>dt-sdk</code> commands equivalent to <code>make build-bundle</code></summary>

```bash
# If building natively on Linux x86_64 for a single ActiveGate Python version:
.venv/bin/dt-sdk build . -k certs/developer.pem -t dist

# Cross-platform build (e.g. from macOS arm64) supporting both Python 3.10 and Python 3.14 Linux ActiveGates:
rm -rf extension/lib dist
.venv/bin/dt-sdk wheel . -e manylinux2014_x86_64 -p 3.10 -o
.venv/bin/python -m pip download -d extension/lib --only-binary=:all: \
  --python-version 314 --platform manylinux2014_x86_64 charset_normalizer==3.5.2
mkdir -p dist
.venv/bin/dt-sdk assemble . -o dist/extension.zip
.venv/bin/dt-sdk sign dist/extension.zip -c certs/developer.pem -o dist/custom_apache-airflow-1.0.0.zip -f
rm -f dist/extension.zip
```
</details>

### Step 4: Upload & Activate in Dynatrace

```bash
export DT_ENV_URL="https://<ENV_ID>.apps.dynatrace.com"
export DT_PLATFORM_TOKEN="dt0s16.YOUR_PLATFORM_TOKEN"

# Upload and activate version 1.0.0:
make upload
# Or directly:
# .venv/bin/dt-sdk upload dist/custom_apache-airflow-1.0.0.zip -u "$DT_ENV_URL" -p "$DT_PLATFORM_TOKEN" --active
```

### Step 5: Configure Monitoring in Dynatrace Hub

1. In Dynatrace, open **Extensions** → select **Apache Airflow Monitoring** (`custom:apache-airflow`).
2. Click **Add monitoring configuration**.
3. Select the **ActiveGate group** that has network reachability to your Airflow Webserver / API Server.
4. Click **Add Airflow Endpoint** and populate:
   - **Airflow Instance Name**: e.g., `prod-finance-us-east-1`
   - **Airflow Base URL**: e.g., `https://airflow.company.internal:8080`
   - **REST API Version**: `auto` (or force `v1` / `v2`)
   - **Authentication Mode**: `auto` (or `basic` / `token` / `none`)
   - **Username** & **Password / Bearer Token**
5. Save — Dynatrace automatically executes `fastcheck()` on the ActiveGate and begins scheduled polling.

### Step 6: Verify Ingested Data in Grail (DQL)

Run the included verifier script:

```bash
.venv/bin/python scripts/deploy_and_verify.py --env-url "$DT_ENV_URL" --verify-only
```

Or run these DQL queries in **Dynatrace Notebooks / Dashboards**:

```dql
// 1. Control-plane availability & component heartbeat staleness
timeseries {
  api_up = avg(airflow.api.up),
  max_heartbeat_age_s = max(airflow.component.heartbeat.age)
}, by: { airflow.instance.name }
```

```dql
// 2. Smartscape custom entities extracted automatically from metrics
fetch `dt.entity.airflow:instance`
| fields id, entity.name, server.address, server.port, airflow.version
```

```dql
// 3. Structured DAG run & failed task instance completion events in Grail Logs
fetch logs, from: now() - 1h
| filter matchesValue(event.name, "airflow.*")
| fields timestamp, loglevel, event.name, airflow.instance.name, airflow.dag.id, airflow.dag_run.id, content
| sort timestamp desc
```

---

## Least-Privilege Airflow Service Account Setup

Create a dedicated read-only monitoring user rather than using `admin` credentials.

### Apache Airflow 2.10+ / 2.11 (FAB Auth Manager)
```bash
airflow users create \
  --username dt_monitor \
  --firstname Dynatrace \
  --lastname Monitor \
  --role Viewer \
  --email dt-monitor@company.internal \
  --password "${AIRFLOW_MONITOR_PASSWORD}"
```
- The built-in `Viewer` role grants read access to `/api/v1/health`, `/version`, `/dags`, `/importErrors`, `/pools`, `/dags/~/dagRuns`, and `POST /dags/~/dagRuns/~/taskInstances/list`.
- If you enable **Emit Airflow Audit Log Events (`/eventLogs`)**, also grant `can read on Audit Logs` to the role.

### Apache Airflow 3.x (JWT `/auth/token`)
- With `auth_mode: auto`, the extension exchanges `username` + `password` at `POST /auth/token` and caches the Bearer JWT until 60 seconds before its `exp` claim.
- **Static Bearer / IAM Token**: Leave `username` blank, set `auth_mode: token`, and paste the Bearer token into `password`.
- **Airflow 3 `importErrors` RBAC Note**: In Airflow 3's FastAPI server, `GET /api/v2/importErrors` scopes results to DAGs the caller is authorized to view, which hides unparsed file-level syntax errors from the basic `viewer` role. To alert on broken unparsed DAG files in Airflow 3, grant `can read on ImportError` + global `can read on DAGs` (FAB Auth Manager) or use an `op` / `admin` monitoring role.

---

## Configuration Reference (`activationSchema.json`)

| Field | Key | Default | Description |
|---|---|---|---|
| **Airflow Instance Name** | `instance_name` | `airflow-prod` | Logical identifier emitted as `airflow.instance.name` on all metrics, entities, logs, and events. |
| **Airflow Base URL** | `url` | `http://localhost:8080` | Base `http://` or `https://` URL of the Airflow Webserver (2.x) or API Server (3.x). |
| **REST API Version** | `api_version` | `auto` | `auto` (probes `/api/v2/monitor/health` then `/api/v1/health`), `v1`, or `v2`. |
| **Authentication Mode** | `auth_mode` | `auto` | `auto` (`basic` on `v1`, `/auth/token` JWT on `v2`), `basic`, `token`, or `none`. |
| **Username** | `username` | `""` | Airflow service-account username (leave empty when passing a static Bearer token in `password`). |
| **Password / Bearer Token** | `password` | `""` | Masked secret field storing the user password or static Bearer token. |
| **Verify TLS Certificates** | `verify_ssl` | `true` | Enforce TLS certificate verification on HTTPS endpoints. |
| **Collection Interval (seconds)** | `collection_interval_seconds` | `30` | Scrape cadence per endpoint (minimum `10` seconds). |
| **Initial Completion Lookback (minutes)** | `initial_lookback_minutes` | `15` | How far back the first poll after startup looks for newly completed DAG runs and task attempts. |
| **Emit DAG Run & Failed Task Completion Log Events** | `emit_completion_logs` | `true` | Emits `airflow.dag_run.completed` and failed/upstream-failed `airflow.task_instance.completed` Grail log events. |
| **Emit Airflow Audit Log Events (`/eventLogs`)** | `emit_audit_logs` | `false` | Incrementally tails `/eventLogs` into Grail as `airflow.audit_log` events. |
| **Emit Davis Problem Events** | `emit_davis_events` | `true` | Sends `AVAILABILITY_EVENT` when API/component health fails and `ERROR_EVENT` when `import_errors > 0`. |

---

## Emitted Telemetry Reference

### 1. Metrics (19 Metrics)

All metrics carry base dimensions `airflow.instance.name`, `server.address`, `server.port`, and `airflow.version` (once `/version` is resolved), plus metric-specific dimensions below:

| Metric Key | MINT Type | Unit | Additional Dimensions | Description |
|---|---|---|---|---|
| `airflow.api.up` | `gauge` | `Count` | — | `1` when authenticated Airflow API scrape succeeds; `0` when unreachable or failing. |
| `airflow.component.health` | `gauge` | `Count` | `airflow.component` (`metadatabase`, `scheduler`, `triggerer`, `dag_processor`) | `1` = healthy, `0` = unhealthy. |
| `airflow.component.heartbeat.age` | `gauge` | `Second` | `airflow.component` | Seconds since latest heartbeat (`scheduler`, `triggerer`, `dag_processor`). |
| `airflow.component.instances.alive` | `gauge` | `Count` | `airflow.component` | Live control-plane job instances (`GET /api/v2/jobs?is_alive=true` on Airflow 3). |
| `airflow.dag.count` | `gauge` | `Count` | `airflow.dag.is_active`, `airflow.dag.is_paused` | Non-stale DAG definitions partitioned by active and paused state. |
| `airflow.dag.import_errors` | `gauge` | `Count` | — | Current count of DAG files failing to parse/import. |
| `airflow.dag_processing.total_parse_time` | `gauge` | `Second` | `airflow.dag.id`, `airflow.dag.file_token` | Duration of most recent parse per DAG file (Airflow 3 `last_parse_duration`). |
| `airflow.dag_run.active` | `gauge` | `Count` | `airflow.dag.id`, `airflow.dag_run.state` (`queued`, `running`) | Active non-terminal DAG runs per DAG. |
| `airflow.dag_run.running.duration.max` | `gauge` | `Second` | `airflow.dag.id` | Elapsed age of the longest-running active DAG run per DAG. |
| `airflow.dag_run.completed` | `count,delta` | `Count` | `airflow.dag.id`, `airflow.dag_run.state`, `airflow.dag_run.type` | Newly completed terminal DAG runs (`success`, `failed`). |
| `airflow.dag_run.duration` | `gauge` (`SummaryStat`) | `Second` | `airflow.dag.id`, `airflow.dag_run.state`, `airflow.dag_run.type` | `min`/`max`/`sum`/`count` duration distribution of completed DAG runs. |
| `airflow.dag_run.schedule_delay` | `gauge` (`SummaryStat`) | `Second` | `airflow.dag.id` | `min`/`max`/`sum`/`count` start delay (`start_date - data_interval_end`) for scheduled runs. |
| `airflow.task_instance.active` | `gauge` | `Count` | `airflow.dag.id`, `airflow.task.state` | Active non-terminal task instances per DAG and state. |
| `airflow.task_instance.queued.duration.max` | `gauge` | `Second` | `airflow.dag.id` | Wait time of the oldest task instance currently stuck in `queued` state. |
| `airflow.task_instance.completed` | `count,delta` | `Count` | `airflow.dag.id`, `airflow.task.id`, `airflow.task.state`, `airflow.task.operator` | Newly finished task instance attempts (`success`, `failed`, `skipped`, `upstream_failed`). |
| `airflow.task_instance.duration` | `gauge` (`SummaryStat`) | `Second` | `airflow.dag.id`, `airflow.task.id`, `airflow.task.state`, `airflow.task.operator` | `min`/`max`/`sum`/`count` execution duration distribution of completed task attempts. |
| `airflow.pool.slots` | `gauge` | `Count` | `airflow.pool.name`, `airflow.slot.state` (`running`, `queued`, `scheduled`, `deferred`, `open`) | Pool slot allocation breakdown. |
| `airflow.pool.slots.limit` | `gauge` | `Count` | `airflow.pool.name` | Total configured slot capacity of the pool. |
| `airflow.pool.utilization` | `gauge` | `Ratio` | `airflow.pool.name` | Pool saturation ratio `(slots - open_slots) / slots` (`0.0 .. 1.0`). |

### 2. Smartscape Topology (`extension.yaml`)

| Entity Type | Display Name | ID Pattern | Relationships |
|---|---|---|---|
| `airflow:instance` | Apache Airflow Instance | `airflow_instance_{airflow.instance.name}` | Parent of `airflow:dag` and `airflow:pool` |
| `airflow:dag` | Apache Airflow DAG | `airflow_dag_{airflow.instance.name}_{airflow.dag.id}` | `CHILD_OF` → `airflow:instance` |
| `airflow:pool` | Apache Airflow Pool | `airflow_pool_{airflow.instance.name}_{airflow.pool.name}` | `CHILD_OF` → `airflow:instance` |

### 3. Grail Log Events & Davis Events

- **Log Events (`report_log_events`)**:
  - `event.name = "airflow.dag_run.completed"` (`INFO` on `success`, `ERROR` on `failed`) with `airflow.dag.id`, `airflow.dag_run.id`, `airflow.dag_run.state`, `airflow.dag_run.type`, `event.duration`.
  - `event.name = "airflow.task_instance.completed"` (`ERROR` on `failed` / `upstream_failed`) with `airflow.dag.id`, `airflow.task.id`, `airflow.dag_run.id`, `airflow.task.operator`, `event.try_number`, `event.pool`.
  - `event.name = "airflow.audit_log"` (`INFO`, opt-in) with `airflow.audit.event`, `airflow.dag.id`, `airflow.task.id`, `airflow.dag_run.id`, `airflow.audit.owner`.
- **Davis Events (`report_dt_event`)**:
  - `AVAILABILITY_EVENT`: Emitted when the Airflow API is unreachable or any control-plane component (`metadatabase`, `scheduler`, `triggerer`, `dag_processor`) reports unhealthy.
  - `ERROR_EVENT`: Emitted when `airflow.dag.import_errors > 0`.
