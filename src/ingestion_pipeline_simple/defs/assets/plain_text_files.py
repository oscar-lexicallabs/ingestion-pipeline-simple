import dagster as dg


@dg.asset
def plain_text_files(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
