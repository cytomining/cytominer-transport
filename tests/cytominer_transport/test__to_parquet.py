import cytominer_transport


def test_to_parquet(cell_painting):
    cytominer_transport.to_parquet(cell_painting["source"], ".")

    assert True
