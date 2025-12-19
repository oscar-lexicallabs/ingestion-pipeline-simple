import dagster as dg
import pytest
import sqlite3
from unittest.mock import Mock, patch
import ingestion_pipeline_simple.defs
from ingestion_pipeline_simple.utils import handle_files
import os
from pathlib import Path
from ingestion_pipeline_simple.defs import assets
import json

# DB_FILE = "file::memory:?cache=shared&mode=memory"
DB_FILE = Path("tmp.db").resolve()
DB_CONN = sqlite3.connect(DB_FILE)
CUR = DB_CONN.cursor()
query = """
CREATE TABLE IF NOT EXISTS files (
    bucket_key      VARCHAR(200) PRIMARY KEY NOT NULL,
    file_path       VARCHAR(1000),
    md_rep          TEXT,
    json_rep        JSONB,
    plain_rep       TEXT,
    chunks          JSONB,
    vec_embeddings  JSONB
)
"""
CUR.execute(query)

query = """
CREATE TABLE IF NOT EXISTS relationships (
    doc_a_key       VARCHAR(200) NOT NULL,
    doc_b_key       VARCHAR(200) NOT NULL,
    relationship    TEXT CHECK(
                        relationship IN ('A','B','C')
                    ) NOT NULL DEFAULT 'A',
    PRIMARY KEY (doc_a_key, doc_b_key),
    FOREIGN KEY (doc_a_key) references files (doc_a_key),
    FOREIGN KEY (doc_b_key) references files (doc_b_key)
)
"""
CUR.execute(query)
DB_CONN.commit()

TMP_BUCKET = Path("./tmp-bucket/org/usr/files").resolve()
os.makedirs(TMP_BUCKET, exist_ok=True)
handle_files.make_new_doc(dir=str(TMP_BUCKET))
handle_files.convert_to_pdf(dir=str(TMP_BUCKET))
(TMP_BUCKET / "dummy.bin").write_text(
    "Hello World!",
    encoding="utf-8"
)

FILES: list[Path] = [
    Path(root, file)
    for root, _, files in os.walk(TMP_BUCKET)
    for file in files
]


@pytest.fixture
def defs():
    return dg.Definitions.merge(
        dg.components.load_defs(ingestion_pipeline_simple.defs) # type: ignore
    )


def test_defs():
    assert defs


def test_binary_files_asset():
    mock_db_resource = Mock()
    mock_db_resource.db_path = DB_FILE
    instance = dg.DagsterInstance.ephemeral()
    test_file = str(FILES[0])
    bucket_key = test_file.split("tmp-bucket")[-1]
    context = dg.build_asset_context(
        instance=instance,
        partition_key=bucket_key,
        asset_config={
            "file_path": test_file
        }
    )
    res: dg.Output = assets.binary_files(context, mock_db_resource)
    assert res.value is None
    filehash = str(hash(test_file) % (10 ** 10))
    hash_val = "-".join([bucket_key, filehash])
    assert res.data_version is not None \
         and res.data_version.value == hash_val
    query = """
        SELECT bucket_key, file_path FROM files
        WHERE bucket_key=?
    """
    q_key, q_path = CUR.execute(query, (bucket_key,)).fetchone()
    assert q_key == bucket_key
    assert q_path == test_file


def test_markdown_files_asset():
    mock_db_resource = Mock()
    mock_db_resource.db_path = DB_FILE
    instance = dg.DagsterInstance.ephemeral()
    test_file = str(FILES[0])
    bucket_key = test_file.split("tmp-bucket")[-1]
    context = dg.build_asset_context(
        instance=instance,
        partition_key=bucket_key
    )
    # Materialize upstream assets to enable isolated testing
    # upstream_res = dg.materialize_to_memory()
    res: dg.Output = assets.markdown_files(context, mock_db_resource)
    assert res.value is None
    query = """
        SELECT md_rep FROM files
        WHERE bucket_key=?
    """
    md_rep = CUR.execute(query, (bucket_key,)).fetchone()[0]
    assert isinstance(md_rep, str)
    filehash = str(hash(md_rep) % (10 ** 10))
    hash_val = "-".join([bucket_key, filehash])
    assert res.data_version is not None \
         and res.data_version.value == hash_val
    

def test_json_files_asset():
    mock_db_resource = Mock()
    mock_db_resource.db_path = DB_FILE
    instance = dg.DagsterInstance.ephemeral()
    test_file = str(FILES[0])
    bucket_key = test_file.split("tmp-bucket")[-1]
    context = dg.build_asset_context(
        instance=instance,
        partition_key=bucket_key
    )
    res: dg.Output = assets.json_files(context, mock_db_resource)
    assert res.value is None
    query = """
        SELECT json_rep FROM files
        WHERE bucket_key=?
    """
    json_rep = CUR.execute(query, (bucket_key,)).fetchone()[0]
    assert isinstance(json_rep, str)
    filehash = str(hash(json_rep) % (10 ** 10))
    hash_val = "-".join([bucket_key, filehash])
    assert res.data_version is not None \
         and res.data_version.value == hash_val
    try:
        json.loads(json_rep)
    except Exception:
        pytest.fail("Could not load stringified json.")
    

def test_plain_files_asset():
    mock_db_resource = Mock()
    mock_db_resource.db_path = DB_FILE
    instance = dg.DagsterInstance.ephemeral()
    test_file = str(FILES[0])
    bucket_key = test_file.split("tmp-bucket")[-1]
    context = dg.build_asset_context(
        instance=instance,
        partition_key=bucket_key
    )
    res: dg.Output = assets.plain_files(context, mock_db_resource)
    assert res.value is None
    query = """
        SELECT plain_rep FROM files
        WHERE bucket_key=?
    """
    plain_rep = CUR.execute(query, (bucket_key,)).fetchone()[0]
    assert isinstance(plain_rep, str)
    filehash = str(hash(plain_rep) % (10 ** 10))
    hash_val = "-".join([bucket_key, filehash])
    assert res.data_version is not None \
         and res.data_version.value == hash_val
    

def test_chunks_asset():
    mock_db_resource = Mock()
    mock_db_resource.db_path = DB_FILE
    instance = dg.DagsterInstance.ephemeral()
    test_file = str(FILES[0])
    bucket_key = test_file.split("tmp-bucket")[-1]
    context = dg.build_asset_context(
        instance=instance,
        partition_key=bucket_key
    )
    res: dg.Output = assets.chunks(context, mock_db_resource)
    assert res.value is None
    query = """
        SELECT chunks FROM files
        WHERE bucket_key=?
    """
    chunks = CUR.execute(query, (bucket_key,)).fetchone()[0]
    assert isinstance(chunks, str)
    filehash = str(hash(chunks) % (10 ** 10))
    hash_val = "-".join([bucket_key, filehash])
    assert res.data_version is not None \
         and res.data_version.value == hash_val
    try:
        chunks_dict = json.loads(chunks)
        assert len(chunks_dict) > 0
    except Exception:
        pytest.fail("Could not load stringified json.")
    

def test_vecs_asset():
    mock_db_resource = Mock()
    mock_db_resource.db_path = DB_FILE
    instance = dg.DagsterInstance.ephemeral()
    test_file = str(FILES[0])
    bucket_key = test_file.split("tmp-bucket")[-1]
    context = dg.build_asset_context(
        instance=instance,
        partition_key=bucket_key
    )
    res: dg.Output = assets.vec_embeddings(context, mock_db_resource)
    assert res.value is None
    query = """
        SELECT vec_embeddings FROM files
        WHERE bucket_key=?
    """
    vec_embeddings = CUR.execute(query, (bucket_key,)).fetchone()[0]
    assert isinstance(vec_embeddings, str)
    filehash = str(hash(vec_embeddings) % (10 ** 10))
    hash_val = "-".join([bucket_key, filehash])
    assert res.data_version is not None \
         and res.data_version.value == hash_val
    try:
        vecs_dict = json.loads(vec_embeddings)
        assert len(vecs_dict) > 0
    except Exception:
        pytest.fail("Could not load stringified json.")
    

def test_clean_up():
    DB_CONN.close()
    for file in FILES:
        os.remove(file)
    os.removedirs(TMP_BUCKET)
    if ":memory:" not in str(DB_FILE):
        os.remove(DB_FILE)
