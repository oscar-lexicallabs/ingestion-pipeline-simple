import dagster as dg


@dg.asset
def document_chunks(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
