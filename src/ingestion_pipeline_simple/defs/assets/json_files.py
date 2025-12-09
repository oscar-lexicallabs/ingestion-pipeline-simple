import dagster as dg


@dg.asset
def json_files(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
