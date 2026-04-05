from dagster import Definitions, define_asset_job

from .assets import etl_asset, daily_partitions
from .resources import source_db_resource, target_db_resource
from .schedules import etl_daily_schedule

etl_job = define_asset_job(
    name="etl_job",
    selection=[etl_asset],
    partitions_def=daily_partitions,
)

defs = Definitions(
    assets=[etl_asset],
    resources={
        "source_db": source_db_resource,
        "target_db": target_db_resource,
    },
    jobs=[etl_job],
    schedules=[etl_daily_schedule],
)
