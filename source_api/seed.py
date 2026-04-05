"""
Seed script: inserts random wind turbine data at 1-minute frequency
from 2024-01-01 to 2024-01-10 (UTC). Safe to run multiple times.
"""
import os
import random
from datetime import datetime, timezone, timedelta

from sqlalchemy.orm import Session

from models import SourceData, get_engine, init_db


def generate_records():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 11, tzinfo=timezone.utc)  # exclusive
    current = start
    while current < end:
        yield SourceData(
            timestamp=current,
            wind_speed=round(random.uniform(0.0, 25.0), 4),
            power=round(random.uniform(0.0, 2000.0), 4),
            ambient_temperature=round(random.uniform(-5.0, 35.0), 4),
        )
        current += timedelta(minutes=1)


def seed():
    engine = get_engine()
    init_db(engine)

    with Session(engine) as session:
        existing = session.query(SourceData).count()
        if existing > 0:
            print(f"Database already has {existing} rows — skipping seed.")
            return

        records = list(generate_records())
        session.bulk_save_objects(records)
        session.commit()
        print(f"Inserted {len(records)} records.")


if __name__ == "__main__":
    seed()
