import dagster as dg
import os
from ..assets.binary_files import binary_files, files_partition_def
from pathlib import Path


add_to_db = dg.define_asset_job("add_to_db", selection=[binary_files])

@dg.sensor(
    job=add_to_db,
    minimum_interval_seconds=5,
    default_status=dg.DefaultSensorStatus.RUNNING
)
def file_monitor(context: dg.SensorEvaluationContext) \
    -> dg.SensorResult:
    last_mtime: float = float(context.cursor) if context.cursor else 0
    dirpath: Path = Path(os.getcwd().split("src")[0],
                         "test_bucket", "org", "usr", "files")
    
    files_stats: list[tuple[Path, os.stat_result]] = [
        (filepath, os.stat(filepath))
        for filepath in (
            dirpath / filename
            for filename in os.listdir(dirpath)
        )
        if os.path.isfile(filepath)
    ]

    new_files: list[Path] = [file for file, fstats in files_stats
                             if fstats.st_mtime > last_mtime]
    
    filekeys: list[str] = [str(filepath).split("test_bucket")[1]
                           for filepath in new_files]
    
    try:
        max_new_mtime: float = max([fstat.st_mtime for _, fstat in files_stats])
    except ValueError:
        max_new_mtime = 0
    
    max_mtime: float = max(last_mtime, max_new_mtime)
    context.update_cursor(str(max_mtime))

    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                run_key=filekey,
                # run_config=...,
                partition_key=filekey
            )
            for filekey in filekeys
        ],
        dynamic_partitions_requests=[
            files_partition_def.build_add_request(filekeys)
        ]
    )