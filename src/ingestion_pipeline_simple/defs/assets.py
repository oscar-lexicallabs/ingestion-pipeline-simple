import dagster as dg
import sqlite3
import os
from pathlib import Path
from typing import Any, Callable
from markitdown import MarkItDown
from . import resources

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="./logs/asset_logs.txt",
    encoding="utf-8",
    level=logging.INFO
)


files_partition_def = dg.DynamicPartitionsDefinition(name="files")

COMMON_ASSET_ARGS: dict[str, Any] = dict(
    partitions_def=files_partition_def,
    metadata={"partition_expr": "bucket_key"}
)
REGISTRY: dict[str, Callable] = {}

def register(file_type: str):
    def decorator(fn: Callable):
        REGISTRY[file_type] = fn
        return fn
    return decorator


@dg.asset(
    **COMMON_ASSET_ARGS,
    config_schema={"file_path": str}
)
def binary_files(
    context: dg.AssetExecutionContext,
    db: resources.SQLiteResource
) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    bucket_key: str = context.partition_key

    logger.info("binary_files asset")
    logger.info(f"{bucket_key = }")
    logger.info(f"{context.run_config = }")
    logger.info(f"{context.op_config = }")
    logger.info(f"{context.resources = }")

    db_path: str = db.db_path
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        query = """
        INSERT INTO files (bucket_key, file_path) VALUES (?, ?)
        """
        logger.info(f"{query = }")
        cur.execute(query, (bucket_key, context.op_config["file_path"]))
        conn.commit()


def _get_file_path(
    db_path: str | Path,
    bucket_key: str,
    use_str = False,
) -> Path | str:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        query = """
        SELECT file_path FROM files
        WHERE bucket_key=?
        """
        file_path: str = cur.execute(query, (bucket_key,)).fetchone()[0]

    if not use_str:
        return Path(file_path)
    
    return file_path


@register("docx")
def convert_docx(filepath: str | Path) -> ...:
    pass


@register("pdf")
def convert_pdf(filepath: str | Path) -> ...:
    pass


@dg.asset(
    **COMMON_ASSET_ARGS,
    deps=[binary_files]
)
def markdown_files(
    context: dg.AssetExecutionContext,
    db: resources.SQLiteResource
) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    bucket_key: str = context.partition_key
    db_path: str = db.db_path
    file_path = _get_file_path(
        db.db_path,
        bucket_key
    )

    logger.info("markdown_files asset")

    assert os.path.isfile(file_path)
    md = MarkItDown(enable_plugins=False)
    res = md.convert(file_path)
    
    with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            query = """
            UPDATE files
            SET md_rep=?
            WHERE bucket_key=?
            """
            cur.execute(query, (res.markdown, bucket_key))
            conn.commit()


@dg.asset(
    **COMMON_ASSET_ARGS,
    deps=[markdown_files]
)
def json_files(
    context: dg.AssetExecutionContext,
    db: resources.SQLiteResource
) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    bucket_key: str = context.partition_key
    logger.info("json_files asset")

    db_path: str = db.db_path
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        query = """
        SELECT md_rep FROM files
        WHERE bucket_key=?
        """
        md = cur.execute(query, (bucket_key,)).fetchone()

        import json
        json_rep: str = json.dumps({"contents": md})
    
        query = """
        UPDATE files
        SET json_rep=?
        WHERE bucket_key=?
        """
        cur.execute(query, (json_rep, bucket_key))
        conn.commit()


def md_to_plain(md_text: str) -> str:
    return md_text

@dg.asset(
    **COMMON_ASSET_ARGS,
    deps=[markdown_files]
)
def plain_files(
    context: dg.AssetExecutionContext,
    db: resources.SQLiteResource
) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    bucket_key: str = context.partition_key
    logger.info("plain_files asset")

    db_path: str = db.db_path
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        query = """
        SELECT md_rep FROM files
        WHERE bucket_key=?
        """
        md = cur.execute(query, (bucket_key,)).fetchone()[0]

        plain_text = md_to_plain(md)
        query = """
        UPDATE files
        SET plain_rep=?
        WHERE bucket_key=?
        """
        cur.execute(query, (plain_text, bucket_key))
        conn.commit()


CHUNK_SIZE = 32

@dg.asset(
    **COMMON_ASSET_ARGS,
    deps=[plain_files]
)
def chunks(
    context: dg.AssetExecutionContext,
    db: resources.SQLiteResource
) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    bucket_key: str = context.partition_key

    logger.info("chunks asset")

    db_path: str = db.db_path
    with sqlite3.connect(db_path) as conn:
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
def vec_embeddings(
    context: dg.AssetExecutionContext,
    db: resources.SQLiteResource
) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    bucket_key: str = context.partition_key

    logger.info("vec_embeddings asset")

    db_path: str = db.db_path
    with sqlite3.connect(db_path) as conn:
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
        