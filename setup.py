from setuptools import setup

setup(
    extras_require={
        "dotenv": [
            "python-dotenv>=0.16.0",
        ]
    },
    install_requires=[
        "numpy>=1.20.2",
        "pandas>=1.2.3",
        "pyarrow>=3.0.0",
    ],
    name="cytominer-transport",
)
