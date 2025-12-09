import dagster as dg
import sqlalchemy as alq


files_partition_def = dg.DynamicPartitionsDefinition(name="files")

@dg.asset(partitions_def=files_partition_def)
def binary_files(context: dg.AssetExecutionContext) -> None:
    assert context.has_partition_key is True, "Error: No Partition Key"
    file_uri: str = context.partition_key
    engine = alq.create_engine(..., echo=True) # TODO: Set up resource system
    with engine.connect() as conn:
        query = "INSERT INTO files (file_name, file_uri) VALUES (:name, :uri)"
        data = {"name": "example.csv", "uri": "s3://bucket/example.csv"}
        conn.execute(alq.text(query), data)