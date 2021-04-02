import pytest


@pytest.fixture(params=["A01-1", "A01-2", "B01-1", "B01-2", "E17-4", "J21-2", "N23-5"])
def cell_painting(name):
    """
    Return configuration for a cell painting dataset.
    """
    yield {
        "experiment": None,
        "image": "Image.csv",
        "object": [
            "Cells.csv",
            "Cytoplasm.csv",
            "Nuclei.csv",
        ],
        "source": f"tests/data/cell_painting/${name}",
    }


@pytest.fixture(params=["1", "2"])
def htqc(name):
    """
    Return configuration for a high-throughput quality control (HTQC) dataset.
    """
    yield {
        "experiment": None,
        "image": "Image.csv",
        "object": "Object.csv",
        "source": f"tests/data/htqc/${name}",
    }


@pytest.fixture(params=["1", "2"])
def qc(name):
    """
    Return configuration for a quality control (QC) dataset.
    """
    yield {
        "experiment": None,
        "image": "Image.csv",
        "object": None,
        "source": f"tests/data/qc/${name}",
    }
