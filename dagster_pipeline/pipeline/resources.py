import os
from dagster import ConfigurableResource
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class PostgresResource(ConfigurableResource):
    db_url: str

    def get_engine(self) -> Engine:
        return create_engine(self.db_url)


source_db_resource = PostgresResource(
    db_url=os.environ.get("SOURCE_DB_URL", "postgresql://user:password@source_db:5432/source")
)

target_db_resource = PostgresResource(
    db_url=os.environ.get("TARGET_DB_URL", "postgresql://user:password@target_db:5432/target")
)
