from setuptools import setup

setup(
    extras_require={
        "dotenv": [
            "python-dotenv>=0.16.0",
        ]
    },
    install_requires=[
        "click>=7.1.2",
        "dask>=2021.4.0",
        "s3fs>=0.6.0",
    ],
    name="cytominer-transport",
)
