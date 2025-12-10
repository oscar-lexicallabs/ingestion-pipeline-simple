import sqlite3
import os
from pathlib import Path
import argparse

DIR_PATH = Path(os.getcwd())

def init_db(db_name: str = "dummy.db") -> None:
    with sqlite3.connect(DIR_PATH / db_name) as conn:
        cur = conn.cursor()
        query = """
        CREATE TABLE IF NOT EXISTS files (
            bucket_key      VARCHAR(200) PRIMARY KEY NOT NULL,
            file_path       VARCHAR(1000),
            json_rep        JSONB,
            plain_rep       TEXT,
            chunks          JSONB,
            vec_embeddings  JSONB
        )
        """
        cur.execute(query)
        conn.commit()
        
def check_db(db_name: str = "dummy.db") -> None:
    with sqlite3.connect(DIR_PATH / db_name) as conn:
        cur = conn.cursor()
        query = "SELECT tbl_name FROM sqlite_master"
        res = cur.execute(query).fetchall()
        print(set(res))

def check_table(db_name: str = "dummy.db",
                tbl_name: str = "files") -> None:
    with sqlite3.connect(DIR_PATH / db_name) as conn:
        cur = conn.cursor()
        query = f"SELECT * FROM {tbl_name}"
        res = cur.execute(query).fetchall()
        print(res)

def drop_val(key: str,
             primary_key: str = "bucket_key",
             db_name: str = "dummy.db",
             tbl_name: str = "files") -> None:
    with sqlite3.connect(DIR_PATH / db_name) as conn:
        cur = conn.cursor()
        query = f"DELETE FROM {tbl_name} WHERE {primary_key} = ?"
        cur.execute(query, (key,))
        conn.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--drop", type=str, default="")
    parser.add_argument("-i", "--init", action="store_true")
    parser.add_argument("-t", "--table", type=str, default="files")
    parser.add_argument("-n", "--name", type=str, default="dummy.db")
    opt = parser.parse_args()
    if opt.init is True:
        init_db(opt.name)
    check_db(opt.name)
    if opt.table:
        check_table(opt.name, opt.table)
    if opt.drop:
        drop_val("/org/usr/files/" + opt.drop,
                 db_name=opt.name,
                 tbl_name=opt.table)