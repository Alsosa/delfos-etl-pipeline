from sqlalchemy import Column, DateTime, Float, create_engine, text
from sqlalchemy.orm import DeclarativeBase
import os


class Base(DeclarativeBase):
    pass


class SourceData(Base):
    __tablename__ = "data"

    timestamp = Column(DateTime(timezone=True), primary_key=True)
    wind_speed = Column(Float, nullable=True)
    power = Column(Float, nullable=True)
    ambient_temperature = Column(Float, nullable=True)


def get_engine():
    url = os.environ["SOURCE_DB_URL"]
    return create_engine(url)


def init_db(engine):
    Base.metadata.create_all(engine)
