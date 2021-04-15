import os
import os.path
import typing

import dask.delayed
import dask.dataframe
from ._generator import generator


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
    source = os.path.expanduser(source)

    directories = []

    for file in os.scandir(source):
        if file.is_dir():
            directories += [file.path]

    concatenated_records = list(generator(directories, image, objects, partition_on))

    concatenated_records = dask.dataframe.concat(concatenated_records)

    concatenated_records.to_parquet(destination, partition_on=partition_on, **kwargs)
