import dagster as dg
import os
from pathlib import Path
from . import assets, resources


add_to_db = dg.define_asset_job(
    "add_to_db", selection=["*json_files", "*vec_embeddings"],op_retry_policy=dg.RetryPolicy(max_retries=0),
)


@dg.sensor(
    job=add_to_db,
    minimum_interval_seconds=5,
    default_status=dg.DefaultSensorStatus.RUNNING
)
def file_monitor(
    context: dg.SensorEvaluationContext,
    bucket: resources.BucketResource
) -> dg.SensorResult:
    last_mtime: float = float(context.cursor) if context.cursor else 0
    s3_conn = bucket.get_boto_conn()
    response = s3_conn.list_objects_v2(Bucket=bucket.bucket_path)
    objects = response.get("Contents", [])

    files_stats: list[tuple[str, float]] = [
        (data.get("Key", ""), data.get("LastModified").timestamp())
        for data in objects
    ]

    new_files: list[str] = [file for file, mtime in files_stats
                            if mtime > last_mtime]
    
    filekeys: list[str] = [str(filepath).split("test_bucket")[-1]
                           for filepath in new_files]


    try:
        max_new_mtime: float = max([
            mtime for _, mtime in files_stats
        ])
    except ValueError:
        max_new_mtime = 0
    
    max_mtime: float = max(last_mtime, max_new_mtime)
    context.update_cursor(str(max_mtime))

    run_reqs: list[dg.RunRequest] = [
        dg.RunRequest(
            partition_key=filekey,
            run_key=filekey,
            run_config={
                "ops": {
                    "binary_files": {
                        "config": {
                            "file_path": str(filepath)
                        }
                    }
                }
            }
        )
        for filepath, filekey in zip(new_files, filekeys)
    ]

    return dg.SensorResult(
        run_requests=run_reqs,
        dynamic_partitions_requests=[
            assets.files_partition_def.build_add_request(filekeys)
        ]
    )
