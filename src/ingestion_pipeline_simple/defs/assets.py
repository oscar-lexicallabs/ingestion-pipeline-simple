import dagster as dg
# import sqlalchemy as alq
import sqlite3
import os
from pathlib import Path
from typing import Any

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="./logs/asset_logs.txt",
    encoding="utf-8",
    level=logging.INFO
)


files_partition_def = dg.DynamicPartitionsDefinition(name="files")

# TODO: Set up resource system
DB_PATH = Path(os.getcwd().split("src")[0], "database/dummy.db")

COMMON_ASSET_ARGS: dict[str, Any] = dict(
    partitions_def=files_partition_def,
    metadata={"partition_expr": "bucket_key"}
)


@dg.asset(
    **COMMON_ASSET_ARGS,
    config_schema={"file_path": str}
)
def binary_files(context: dg.AssetExecutionContext) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    bucket_key: str = context.partition_key

    logger.info("binary_files asset")
    logger.info(f"{bucket_key = }")
    logger.info(f"{context.run_config = }")
    logger.info(f"{context.op_config = }")

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        query = """
        INSERT INTO files (
            bucket_key, file_path, json_rep, plain_rep, chunks, vec_embeddings
        ) VALUES (?, ?, NULL, NULL, NULL, NULL)
        """
        logger.info(f"{query = }")
        cur.execute(query, (bucket_key, context.op_config["file_path"]))
        conn.commit()

    # engine = alq.create_engine(..., echo=True)
    # with engine.connect() as conn:
    #     query = "INSERT INTO files (file_name, bucket_key) VALUES (:name, :uri)"
    #     data = {"name": "example.csv", "uri": "s3://bucket/example.csv"}
    #     conn.execute(alq.text(query), data)


def _get_file_path(bucket_key: str, use_str = False) -> Path | str:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        query = """
        SELECT file_path FROM files
        WHERE bucket_key=?
        """
        file_path: str = cur.execute(query, (bucket_key,)).fetchone()[0]

    if not use_str:
        return Path(file_path)
    
    return file_path


@dg.asset(
    **COMMON_ASSET_ARGS,
    deps=[binary_files]
)
def json_files(context: dg.AssetExecutionContext) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    bucket_key: str = context.partition_key
    file_path = _get_file_path(bucket_key)

    logger.info("json_files asset")

    assert os.path.isfile(file_path)
    with open(file_path, mode="rt") as f:
        contents: str = f.read()

    import json
    strj: str = json.dumps({"contents": contents})
    
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        query = """
        UPDATE files
        SET json_rep=?
        WHERE bucket_key=?
        """
        cur.execute(query, (strj, bucket_key))
        conn.commit()


@dg.asset(
    **COMMON_ASSET_ARGS,
    deps=[binary_files]
)
def plain_files(context: dg.AssetExecutionContext) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    bucket_key: str = context.partition_key
    file_path = _get_file_path(bucket_key)

    logger.info("plain_files asset")

    assert os.path.isfile(file_path)
    with open(file_path, "rt", encoding="utf-8") as f:
        text: str = f.read()

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        query = """
        UPDATE files
        SET plain_rep=?
        WHERE bucket_key=?
        """
        cur.execute(query, (text, bucket_key))
        conn.commit()


CHUNK_SIZE = 32

@dg.asset(
    **COMMON_ASSET_ARGS,
    deps=[plain_files]
)
def chunks(context: dg.AssetExecutionContext) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    bucket_key: str = context.partition_key

    logger.info("chunks asset")

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        query = """
        SELECT plain_rep FROM files
        WHERE bucket_key=?
        """
        plain_text: str = cur.execute(query, (bucket_key,)).fetchone()[0]
        chunked_text: dict[int, str] = {
            idx: plain_text[pos:pos+CHUNK_SIZE]
            for idx, pos in enumerate(range(0,len(plain_text),CHUNK_SIZE))
        }

        import json
        chunks_str = json.dumps(chunked_text)

        query = """
        UPDATE files
        SET chunks=?
        WHERE bucket_key=?
        """
        cur.execute(query, (chunks_str, bucket_key))
        conn.commit()


@dg.asset(
    **COMMON_ASSET_ARGS,
    deps=[chunks]
)
def vec_embeddings(context: dg.AssetExecutionContext) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    bucket_key: str = context.partition_key

    logger.info("vec_embeddings asset")

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        query = """
        SELECT chunks FROM files
        WHERE bucket_key=?
        """
        chunked_text = cur.execute(query, (bucket_key,)).fetchone()[0]

        import random
        vecs: dict[int, float] = {
            idx: round(random.random(), 3)
            for idx, _ in enumerate(chunked_text)
        }

        import json
        vecs_str = json.dumps(vecs)
        query = """
        UPDATE files
        SET vec_embeddings=?
        WHERE bucket_key=?
        """
        cur.execute(query, (vecs_str, bucket_key))
        conn.commit()
        