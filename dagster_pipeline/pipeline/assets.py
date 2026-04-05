import os
import logging
from datetime import datetime, timezone, timedelta

import httpx
import pandas as pd
from dagster import asset, DailyPartitionsDefinition, OpExecutionContext
from sqlalchemy import text

from .resources import PostgresResource

log = logging.getLogger(__name__)

VARIABLES = ["wind_speed", "power"]

daily_partitions = DailyPartitionsDefinition(
    start_date="2024-01-01",
    timezone="UTC",
)


def _fetch_data(api_url: str, start: datetime, end: datetime) -> pd.DataFrame:
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "variables": VARIABLES,
    }
    with httpx.Client(timeout=60.0) as client:
        response = client.get(f"{api_url}/data", params=params)
        response.raise_for_status()

    records = response.json()
    if not records:
        return pd.DataFrame(columns=["timestamp"] + VARIABLES)

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp")
    return df


def _aggregate(df: pd.DataFrame) -> pd.DataFrame:
    agg_frames = []
    for var in VARIABLES:
        if var not in df.columns:
            continue
        resampled = df[var].resample("10min").agg(["mean", "min", "max", "std"])
        resampled.columns = [f"{var}_{stat}" for stat in resampled.columns]
        agg_frames.append(resampled)

    if not agg_frames:
        return pd.DataFrame()

    result = pd.concat(agg_frames, axis=1).reset_index()
    result.rename(columns={"timestamp": "interval_start"}, inplace=True)
    return result


STAT_SIGNALS = [
    "wind_speed_mean", "wind_speed_min", "wind_speed_max", "wind_speed_std",
    "power_mean",      "power_min",      "power_max",      "power_std",
]


def _ensure_target_schema(engine):
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
    with engine.begin() as conn:
        conn.execute(text(ddl))
        for name in STAT_SIGNALS:
            conn.execute(
                text("INSERT INTO signal (name) VALUES (:name) ON CONFLICT (name) DO NOTHING"),
                {"name": name},
            )


def _load_signal_map(engine) -> dict:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name FROM signal")).fetchall()
    return {name: sid for sid, name in rows}


def _save_aggregated(engine, agg_df: pd.DataFrame, signal_map: dict) -> int:
    rows = []
    stat_cols = [c for c in agg_df.columns if c != "interval_start"]

    for _, row in agg_df.iterrows():
        ts = row["interval_start"]
        for col in stat_cols:
            if col not in signal_map:
                continue
            rows.append({
                "timestamp": ts,
                "signal_id": signal_map[col],
                "value": row[col],
            })

    if not rows:
        return 0

    insert_df = pd.DataFrame(rows)
    with engine.begin() as conn:
        insert_df.to_sql("data_tmp", conn, if_exists="replace", index=False)
        conn.execute(text("""
            INSERT INTO data (timestamp, signal_id, value)
            SELECT timestamp, signal_id, value FROM data_tmp
            ON CONFLICT (timestamp, signal_id) DO UPDATE SET value = EXCLUDED.value;
        """))
        conn.execute(text("DROP TABLE IF EXISTS data_tmp;"))

    return len(rows)


@asset(
    partitions_def=daily_partitions,
)
def etl_asset(context: OpExecutionContext, target_db: PostgresResource):
    """Daily ETL: fetch wind_speed and power from the Source API, aggregate to 10-minute
    intervals, and write results to the Target DB."""
    partition_date = context.partition_key  # "YYYY-MM-DD"
    start = datetime.strptime(partition_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    api_url = os.environ.get("API_URL", "http://source_api:8000").rstrip("/")

    context.log.info("Fetching data for %s from %s", partition_date, api_url)
    raw_df = _fetch_data(api_url, start, end)

    if raw_df.empty:
        context.log.warning("No data returned for %s — skipping.", partition_date)
        return

    agg_df = _aggregate(raw_df)
    if agg_df.empty:
        context.log.warning("Aggregation empty for %s — skipping.", partition_date)
        return

    context.log.info("Aggregated %d 10-minute intervals.", len(agg_df))

    engine = target_db.get_engine()
    _ensure_target_schema(engine)

    signal_map = _load_signal_map(engine)
    inserted = _save_aggregated(engine, agg_df, signal_map)
    context.log.info("Saved %d rows to target DB.", inserted)
