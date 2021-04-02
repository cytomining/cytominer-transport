from setuptools import setup

setup(
    extras_require={
        "dotenv": [
            "python-dotenv>=0.16.0",
        ]
    },
    install_requires=[
        "click>=7.1.2",
        "numpy>=1.20.2",
        "pandas>=1.2.3",
        "pyarrow>=3.0.0",
        "dask>=2021.4.0",
    ],
    name="cytominer-transport",
)
