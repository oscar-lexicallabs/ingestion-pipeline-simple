import dagster as dg
import os

class SQLiteResource(dg.ConfigurableResource):
    db_path: str = os.path.join(os.getcwd().split("src")[0],
                                "database/dummy.db")


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "db": SQLiteResource(
                db_path=os.path.join(
                    os.getcwd().split("src")[0],
                    "database/dummy.db"
                )
            )
        }
    )