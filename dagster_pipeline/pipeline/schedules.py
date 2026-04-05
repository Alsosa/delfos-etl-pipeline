from dagster import ScheduleDefinition

# The job "etl_job" is registered in __init__.py alongside this schedule.
# Dagster resolves it by name from the same Definitions object.
etl_daily_schedule = ScheduleDefinition(
    name="etl_daily_1am_utc",
    job_name="etl_job",
    cron_schedule="0 1 * * *",
    execution_timezone="UTC",
)
