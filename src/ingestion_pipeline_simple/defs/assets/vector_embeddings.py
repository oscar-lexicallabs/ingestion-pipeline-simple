import dagster as dg


@dg.asset
def vector_embeddings(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
