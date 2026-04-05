"""
ETL script: fetches wind_speed and power for a given date from the Source API,
aggregates into 10-minute intervals, and writes results to the Target DB.

Usage:
    python etl.py 2024-01-05
"""
import sys
import os
import logging
from datetime import datetime, timezone, timedelta

import httpx
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

VARIABLES = ["wind_speed", "power"]


def parse_date(date_str: str) -> datetime:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.replace(tzinfo=timezone.utc)


def fetch_data(api_url: str, start: datetime, end: datetime) -> pd.DataFrame:
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "variables": VARIABLES,
    }
    log.info("Fetching data from %s/data for %s → %s", api_url, start.date(), end.date())
    with httpx.Client(timeout=60.0) as client:
        response = client.get(f"{api_url}/data", params=params)
        response.raise_for_status()

    records = response.json()
    if not records:
        log.warning("No records returned from API.")
        return pd.DataFrame(columns=["timestamp"] + VARIABLES)

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp")
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """Resample to 10-minute intervals, computing mean/min/max/std per variable."""
    agg_frames = []
    for var in VARIABLES:
        if var not in df.columns:
            log.warning("Variable %s not found in data, skipping.", var)
            continue
        resampled = df[var].resample("10min").agg(["mean", "min", "max", "std"])
        resampled.columns = [f"{var}_{stat}" for stat in resampled.columns]
        agg_frames.append(resampled)

    if not agg_frames:
        return pd.DataFrame()

    result = pd.concat(agg_frames, axis=1).reset_index()
    result.rename(columns={"timestamp": "interval_start"}, inplace=True)
    return result


def ensure_target_schema(engine):
    """Create target tables if they do not exist and pre-populate signals."""
    ddl = """
    CREATE TABLE IF NOT EXISTS signal (
        id   SERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS data (
        id             BIGSERIAL PRIMARY KEY,
        timestamp      TIMESTAMPTZ NOT NULL,
        signal_id      INTEGER NOT NULL REFERENCES signal(id),
        value          DOUBLE PRECISION,
        UNIQUE (timestamp, signal_id)
    );
    """
    seed_signals = """
    INSERT INTO signal (name) VALUES ('wind_speed'), ('power')
    ON CONFLICT (name) DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text(seed_signals))


def load_signal_map(engine) -> dict:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name FROM signal")).fetchall()
    return {name: sid for sid, name in rows}


def save_aggregated(engine, agg_df: pd.DataFrame, signal_map: dict):
    """Flatten aggregated DataFrame into (timestamp, signal_id, value) rows."""
    rows = []
    stat_cols = [c for c in agg_df.columns if c != "interval_start"]

    for _, row in agg_df.iterrows():
        ts = row["interval_start"]
        for col in stat_cols:
            # col format: {variable}_{stat}  e.g. wind_speed_mean
            parts = col.rsplit("_", 1)
            if len(parts) != 2:
                continue
            var_name, stat = parts
            # signal name stored as e.g. "wind_speed_mean"
            signal_name = col
            if signal_name not in signal_map:
                continue
            rows.append({"timestamp": ts, "signal_id": signal_map[signal_name], "value": row[col]})

    if not rows:
        log.warning("No rows to insert after aggregation.")
        return

    insert_df = pd.DataFrame(rows)
    # Use a temp table + upsert pattern for idempotency
    with engine.begin() as conn:
        insert_df.to_sql("data_tmp", conn, if_exists="replace", index=False)
        conn.execute(text("""
            INSERT INTO data (timestamp, signal_id, value)
            SELECT timestamp, signal_id, value FROM data_tmp
            ON CONFLICT (timestamp, signal_id) DO UPDATE SET value = EXCLUDED.value;
        """))
        conn.execute(text("DROP TABLE IF EXISTS data_tmp;"))

    log.info("Saved %d aggregated rows to target DB.", len(rows))


def run_etl(date_str: str):
    api_url = os.environ.get("API_URL", "http://source_api:8000").rstrip("/")
    target_db_url = os.environ["TARGET_DB_URL"]

    start = parse_date(date_str)
    end = start + timedelta(days=1)

    # Fetch
    raw_df = fetch_data(api_url, start, end)
    if raw_df.empty:
        log.info("Nothing to process for %s.", date_str)
        return

    # Aggregate
    agg_df = aggregate(raw_df)
    if agg_df.empty:
        log.info("Aggregation produced no results for %s.", date_str)
        return

    log.info("Aggregated into %d 10-minute intervals.", len(agg_df))

    # Load
    engine = create_engine(target_db_url)
    ensure_target_schema(engine)

    # Build signal map: we store aggregated stats as separate signals
    # e.g. wind_speed_mean, wind_speed_min, etc.
    stat_signal_names = [c for c in agg_df.columns if c != "interval_start"]
    with engine.begin() as conn:
        for name in stat_signal_names:
            conn.execute(
                text("INSERT INTO signal (name) VALUES (:name) ON CONFLICT (name) DO NOTHING"),
                {"name": name},
            )

    signal_map = load_signal_map(engine)
    save_aggregated(engine, agg_df, signal_map)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python etl.py YYYY-MM-DD")
        sys.exit(1)
    run_etl(sys.argv[1])
