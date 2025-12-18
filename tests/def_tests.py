import dagster as dg
import pytest

import ingestion_pipeline_simple.defs


@pytest.fixture
def defs():
    return dg.Definitions.merge(
        dg.components.load_defs(ingestion_pipeline_simple.defs)
    )


def test_defs():
    assert defs