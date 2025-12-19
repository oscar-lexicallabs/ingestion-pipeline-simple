import dagster as dg
import pytest
from unittest.mock import patch, Mock
from ingestion_pipeline_simple.defs import sensors
# from ..test_bucket import handle_files
from pathlib import Path
import os

# Need to test:
#   - If it triggers correctly
#   - If it triggers the right job
#   - If the cursor contains the right info

def test_sensor_skip(tmp_path: Path):
    mock_bucket = Mock()
    mock_bucket.bucket_path = tmp_path
    mock_bucket.org = "org"
    mock_bucket.usr = "usr"
    os.makedirs(
        Path(
            mock_bucket.bucket_path,
            mock_bucket.org,
            mock_bucket.usr
        ),
        exist_ok=True
    )
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(
        instance=instance,
        resources={
            "bucket": mock_bucket
        }
    )
    res = sensors.file_monitor(context)
    res = sensors.file_monitor(context)
    assert isinstance(res, dg.SensorResult)
    # assert not res
    assert res.run_requests is not None \
        and len(res.run_requests) == 0
    assert res.dynamic_partitions_requests is not None \
        and len(res.dynamic_partitions_requests[0].partition_keys) == 0
    assert res.cursor is not None \
        and float(res.cursor) == 0


def test_sensor_run(tmp_path: Path):
    mock_bucket = Mock()
    mock_bucket.bucket_path = tmp_path
    mock_bucket.org = "org"
    mock_bucket.usr = "usr"
    os.makedirs(
        Path(
            mock_bucket.bucket_path,
            mock_bucket.org,
            mock_bucket.usr
        ),
        exist_ok=True
    )
    test_bin = Path(
        mock_bucket.bucket_path,
        mock_bucket.org,
        mock_bucket.usr
    ) / "dummy.bin"
    test_bin.write_text("hello world", encoding="utf-8")
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(
        instance=instance,
        resources={
            "bucket": mock_bucket
        }
    )
    res = sensors.file_monitor(context)
    assert isinstance(res, dg.SensorResult)
    assert res.skip_reason is None
    assert res.run_requests is not None \
        and len(res.run_requests) == 1
    assert res.dynamic_partitions_requests is not None \
        and len(res.dynamic_partitions_requests[0].partition_keys) == 1
    assert res.cursor is not None \
        and float(res.cursor) > 0