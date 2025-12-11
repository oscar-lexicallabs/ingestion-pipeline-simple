import dagster as dg
import os

class SQLiteResource(dg.ConfigurableResource):
    db_path: str


class BucketResource(dg.ConfigurableResource):
    bucket_path: str
    org: str
    usr: str
    # dirs: str

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "db": SQLiteResource(
                db_path=os.path.join(
                    os.getcwd().split("src")[0],
                    "database/dummy.db"
                )
            ),
            "bucket": BucketResource(
                bucket_path=os.path.join(
                    os.getcwd().split("src")[0],
                    "test_bucket"
                ),
                org="org",
                usr="usr"
            )
        }
    )