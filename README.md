# Delfos ETL Pipeline

A containerised ETL pipeline that reads wind turbine sensor data from a source PostgreSQL database via a FastAPI connector, aggregates it in 10-minute intervals, and writes the results to a target PostgreSQL database. The pipeline is orchestrated with Dagster.

---

## Architecture

```
source_db (Postgres) ──► source_api (FastAPI :8000)
                                │
                                ▼
                          etl logic (pandas)
                                │
                                ▼
                         target_db (Postgres)
                                ▲
                         Dagster (:3000) ─── daily schedule @ 01:00 UTC
```

---

## Services

| Service            | Port (host) | Description                          |
|--------------------|-------------|--------------------------------------|
| `source_db`        | 5433        | Source PostgreSQL (wind turbine data)|
| `target_db`        | 5434        | Target PostgreSQL (aggregated data)  |
| `source_api`       | 8000        | FastAPI connector for source data    |
| `dagster_webserver`| 3000        | Dagster UI                           |
| `dagster_daemon`   | —           | Dagster scheduler + run launcher     |
| `seed`             | —           | One-shot seed job (exits when done)  |

---

## Quick Start

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env if you need custom credentials
```

### 2. Build and start all services

```bash
docker compose up --build
```

The `seed` service will automatically populate the source DB with 10 days of 1-minute resolution wind turbine data (2024-01-01 → 2024-01-10) on first run. It is idempotent and exits cleanly.

### 3. Verify the API

```bash
curl "http://localhost:8000/health"
curl "http://localhost:8000/data?start=2024-01-05T00:00:00Z&end=2024-01-06T00:00:00Z&variables=wind_speed&variables=power"
```

### 4. Open Dagster UI

Navigate to [http://localhost:3000](http://localhost:3000).

- Go to **Assets** → `etl_asset`
- Materialise a partition (e.g. `2024-01-05`) manually, or let the daily schedule run at 01:00 UTC.

---

## Running the ETL manually (without Docker)

```bash
cd etl
pip install -r requirements.txt

export SOURCE_DB_URL=postgresql://user:password@localhost:5433/source
export TARGET_DB_URL=postgresql://user:password@localhost:5434/target
export API_URL=http://localhost:8000

python etl.py 2024-01-05
```

---

## Data Model

### Source DB — `source` database

**`data`** table:

| Column               | Type             |
|----------------------|------------------|
| `timestamp`          | TIMESTAMPTZ (PK) |
| `wind_speed`         | FLOAT            |
| `power`              | FLOAT            |
| `ambient_temperature`| FLOAT            |

### Target DB — `target` database

**`signal`** table:

| Column | Type         |
|--------|--------------|
| `id`   | SERIAL (PK)  |
| `name` | VARCHAR(255) |

Pre-populated signals: `wind_speed_mean`, `wind_speed_min`, `wind_speed_max`, `wind_speed_std`, `power_mean`, `power_min`, `power_max`, `power_std`.

**`data`** table:

| Column      | Type         |
|-------------|--------------|
| `id`        | BIGSERIAL PK |
| `timestamp` | TIMESTAMPTZ  |
| `signal_id` | INTEGER (FK → signal.id) |
| `value`     | DOUBLE PRECISION |

Unique constraint on `(timestamp, signal_id)` — safe to re-run.

---

## ETL Aggregation

For each day, the ETL:

1. Calls `GET /data` on the source API for `wind_speed` and `power`.
2. Resamples to **10-minute intervals** computing `mean`, `min`, `max`, `std` per variable.
3. Upserts results into the target `data` table keyed by `(timestamp, signal_id)`.

---

## Dagster Pipeline

- **Asset**: `etl_asset` — daily partitioned, one partition per calendar day.
- **Job**: `etl_job` — materialises `etl_asset`.
- **Schedule**: `etl_daily_1am_utc` — runs `etl_job` at `0 1 * * *` UTC.
