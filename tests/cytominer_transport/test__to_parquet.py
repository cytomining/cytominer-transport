import tempfile

import cytominer_transport


def test_to_parquet(cell_painting):
    with tempfile.TemporaryDirectory() as destination:
        cell_painting["destination"] = destination

        cytominer_transport.to_parquet(**cell_painting)

    assert True
