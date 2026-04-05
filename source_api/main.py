from datetime import datetime
from typing import List

from fastapi import FastAPI, Query, HTTPException
from sqlalchemy.orm import Session

from models import SourceData, get_engine, init_db

app = FastAPI(title="Source API")

_engine = None


def engine():
    global _engine
    if _engine is None:
        _engine = get_engine()
    return _engine


@app.on_event("startup")
def startup():
    init_db(engine())


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/data")
def get_data(
    start: datetime = Query(..., description="Start datetime (ISO 8601, UTC)"),
    end: datetime = Query(..., description="End datetime (ISO 8601, UTC)"),
    variables: List[str] = Query(..., description="Variables to return"),
):
    allowed = {"wind_speed", "power", "ambient_temperature"}
    invalid = set(variables) - allowed
    if invalid:
        raise HTTPException(status_code=400, detail=f"Unknown variables: {invalid}")

    with Session(engine()) as session:
        rows = (
            session.query(SourceData)
            .filter(SourceData.timestamp >= start, SourceData.timestamp < end)
            .order_by(SourceData.timestamp)
            .all()
        )

    result = []
    for row in rows:
        record = {"timestamp": row.timestamp.isoformat()}
        for var in variables:
            record[var] = getattr(row, var)
        result.append(record)

    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
