import os
import os.path
import typing
import urllib.parse

import dask.dataframe


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
    parsed = urllib.parse.urlparse(source)

    if not parsed.scheme:
        if not os.path.exists(source):
            raise FileNotFoundError(source)

        if not os.path.isdir(source):
            raise NotADirectoryError(source)

    # Open "Experiment.csv" as a Dask DataFrame:
    if experiment:
        pathname = os.path.join(source, experiment)
    else:
        pathname = os.path.join(source, "Experiment.csv")

    if os.path.exists(pathname):
        experiment = dask.dataframe.read_csv(pathname)

    # Open "Image.csv" as a Dask DataFrame:
    if image:
        pathname = os.path.join(source, image)
    else:
        pathname = os.path.join(source, "Image.csv")

    image = dask.dataframe.read_csv(pathname)

    image.set_index("ImageNumber")

    # Open object CSVs (e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv, etc.)
    # as Dask DataFrames:
    for object in objects:
        pathname = os.path.join(source, object)

        if os.path.exists(pathname):
            features = dask.dataframe.read_csv(pathname)

            features.set_index("ImageNumber")

            suffix, _ = os.path.splitext(object)

            image = image.merge(
                features,
                left_index=True,
                right_index=True,
                suffixes=(f"_{suffix}_Image", f"_{suffix}_{suffix}"),
            )

    # Create a destination directory if one doesn’t exist:
    if not os.path.exists(destination):
        os.mkdir(destination)

    if not os.path.isdir(destination):
        raise NotADirectoryError(destination)

    image.to_parquet(destination, partition_on=partition_on, **kwargs)
