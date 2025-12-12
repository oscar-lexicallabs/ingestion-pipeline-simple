import dagster as dg
import os


class SQLiteResource(dg.ConfigurableResource):
    db_path: str

class BucketResource(dg.ConfigurableResource):
    bucket_path: str
    org: str
    usr: str

class BucketIOManager(dg.ConfigurableIOManager):
    root: str

    def handle_output(self, context: dg.OutputContext, obj: dg.Any) -> None:
        pass

    def load_input(self, context: dg.InputContext) -> dg.Any:
        pass


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
            ),
            "bucket_io_manager": BucketIOManager(root="./tmp/")
        }
    )