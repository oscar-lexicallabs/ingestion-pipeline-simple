import sqlite3
import os
from pathlib import Path

DIR_PATH = Path(os.getcwd())

def init_db(db_name: str = "dummy.db") -> None:
    with sqlite3.connect(DIR_PATH / db_name) as conn:
        cur = conn.cursor()
        query = """
        CREATE TABLE IF NOT EXISTS files (
            bucket_key      VARCHAR(400) PRIMARY KEY NOT NULL,
            file_path       VARCHAR(100),
            json_rep        JSONB,
            plain_rep       TEXT,
            chunks          JSONB,
            vec_embeddings  JSONB
        )
        """
        cur.execute(query)
        
def check_db(db_name: str = "dummy.db") -> None:
    with sqlite3.connect(DIR_PATH / db_name) as conn:
        cur = conn.cursor()
        query = "SELECT tbl_name FROM sqlite_master"
        res = cur.execute(query).fetchall()
        print(set(res))
        
def main():
    init_db()
    check_db()

if __name__ == "__main__":
    main()