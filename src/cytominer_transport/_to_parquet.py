import os
import os.path
import typing

import dask.dataframe


def to_parquet(
    source: typing.Union[str, bytes, os.PathLike],
    destination: typing.Union[str, bytes, os.PathLike],
    experiment: typing.Optional[typing.Union[str, bytes, os.PathLike]] = None,
    image: typing.Optional[typing.Union[str, bytes, os.PathLike]] = None,
    objects: typing.List[typing.Union[str, bytes, os.PathLike]] = [],
    compression: typing.Optional[str] = "snappy",
    index: typing.Optional[bool] = True,
    **kwargs,
):
    """
    destination : string or pathlib.Path
        Destination directory for data. Prepend with protocol like s3:// or hdfs:// for remote data.

    compression : {{'snappy', 'gzip', 'brotli', None}}, default 'snappy'
        Name of the compression to use. Use ``None`` for no compression.
    """
    if not os.path.exists(source):
        raise FileNotFoundError(filename=source)

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

    if os.path.exists(pathname):
        image = dask.dataframe.read_csv(pathname)

        image.set_index("ImageNumber")
    else:
        raise FileNotFoundError(filename=pathname)

    # Open object CSVs (e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv, etc.)
    # as Dask DataFrames:
    for object in objects:
        pathname = os.path.join(source, object)

        if os.path.exists(pathname):
            features = dask.dataframe.read_csv(pathname)

            features.set_index("ImageNumber")

            image = image.merge(features, left_index=True, right_index=True)

    # Create a destination directory if one doesn’t exist:
    if not os.path.exists(destination):
        os.mkdir(destination)

    if not os.path.isdir(destination):
        raise NotADirectoryError(filename=destination)

    image.to_parquet(destination, compression=compression)
