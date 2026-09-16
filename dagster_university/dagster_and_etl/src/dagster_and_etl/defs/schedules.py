import dagster as dg

import dagster_and_etl.defs.jobs as jobs

asset_partitioned_schedule = dg.build_schedule_from_partitioned_job(
    jobs.import_partition_job,
)

@dg.schedule(job=jobs.asteroid_job, cron_schedule="0 6 * * *")
def date_range_schedule(context):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")

    return dg.RunRequest(
        run_config={
            "ops": {
                "asteroids": {
                    "config": {
                        "date": scheduled_date,
                    },
                },
            },
        },
    )