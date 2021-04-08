import glob
import os
import os.path
import typing

import pytest


def params(name: str) -> typing.List[os.PathLike]:
    directories = []

    for path in glob.glob(f"./tests/data/{name}/*"):
        if os.path.isdir(path):
            directories += [os.path.basename(path)]

    return directories


@pytest.fixture(params=params("cell_painting"))
def cell_painting(request):
    """
    Return configuration for a cell painting dataset.
    """
    yield {
        "experiment": None,
        "objects": [
            "Cells.csv",
            "Cytoplasm.csv",
            "Nuclei.csv",
        ],
        "source": f"tests/data/cell_painting/{request.param}",
    }


@pytest.fixture(params=params("htqc"))
def htqc(request):
    """
    Return configuration for a high-throughput quality control (HTQC) dataset.
    """
    yield {
        "experiment": None,
        "objects": "Object.csv",
        "source": f"tests/data/htqc/{request.param}",
    }


@pytest.fixture(params=params("qc"))
def qc(request):
    """
    Return configuration for a quality control (QC) dataset.
    """
    yield {
        "experiment": None,
        "objects": None,
        "source": f"tests/data/qc/{request.param}",
    }
