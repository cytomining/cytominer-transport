import os
import os.path
import typing
import urllib.parse

import dask.dataframe
import pandas


def to_parquet(
    source: typing.Union[str, bytes, os.PathLike],
    destination: typing.Union[str, bytes, os.PathLike],
    experiment: typing.Optional[typing.Union[str, bytes, os.PathLike]] = None,
    image: typing.Optional[typing.Union[str, bytes, os.PathLike]] = "Image.csv",
    objects: typing.List[typing.Union[str, bytes, os.PathLike]] = [
        "Cells.csv",
        "Cytoplasm.csv",
        "Nuclei.csv",
    ],
    partition_on: typing.Optional[typing.List[str]] = ["Metadata_Well"],
    **kwargs,
):
    """
    source :
        Source directory for data. Prepend with a protocol (e.g. s3:// or
        hdfs://) for remote data.

    destination :
        Destination directory for data. Prepend with a protocol (e.g. s3:// or
        hdfs://) for remote data.

    experiment :
        CSV containing the run details needed to reproduce the image and
        objects CSVs.

    image :
        CSV containing data pertaining to images

    objects :
        One or more CSVs containing data pertaining to objects or regions of
        interest (e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv, etc.).

    partition_on : list, optional
        Construct directory-based partitioning by splitting on these fields'
        values. Each partition will result in one or more datafiles, there
        will be no global groupby.

    **kwargs :
        Extra options to be passed on to the specific backend.
    """
    # Open "Image.csv" as a Dask DataFrame:
    pathname = os.path.join(source, image)

    image = pandas.read_csv(pathname)

    image.set_index("ImageNumber", inplace=True)

    features = pandas.DataFrame()

    # Open object CSVs (e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv, etc.)
    # as Dask DataFrames:
    for object in objects:
        pathname = os.path.join(source, object)

        prefix, _ = os.path.splitext(object)

        object = pandas.read_csv(pathname)

        object = object.add_prefix(f"{prefix}_")

        object.rename(columns={f"{prefix}_ImageNumber": "ImageNumber"}, inplace=True)
        object.rename(columns={f"{prefix}_ObjectNumber": "ObjectNumber"}, inplace=True)

        object.set_index(["ImageNumber", "ObjectNumber"], drop=False, inplace=True)

        object[f"{prefix}_ImageNumber"] = object["ImageNumber"]
        object[f"{prefix}_ObjectNumber"] = object["ObjectNumber"]

        object.drop(["ImageNumber"], axis=1, inplace=True)
        object.drop(["ObjectNumber"], axis=1, inplace=True)

        features = pandas.concat([features, object], axis=1)

    image = image.merge(features, how="outer", left_index=True, right_index=True)

    image.reset_index(drop=False, inplace=True)

    npartitions = image[partition_on[0]].unique().size

    image = dask.dataframe.from_pandas(image, npartitions=npartitions)

    image.to_parquet(destination, partition_on=partition_on, **kwargs)
