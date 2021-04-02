import glob
import os.path
import typing

import pytest


def params(name: str) -> typing.List[str]:
    directories = []

    for path in glob.glob(f"./tests/data/{name}/*"):
        if os.path.isdir(path):
            directories += [os.path.basename(path)]

    return directories


@pytest.fixture(params=params("cell_painting"))
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


@pytest.fixture(params=params("htqc"))
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


@pytest.fixture(params=params("qc"))
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
