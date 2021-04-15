import os
import os.path
import typing
import urllib.parse

import dask.dataframe
import pandas
import tqdm


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

    directories = [file.path for file in os.scandir(source) if file.is_dir()]

    records = []

    concatenated_image_records = pandas.DataFrame()

    for directory in tqdm.tqdm(directories):
        # Open "Image.csv" as a Dask DataFrame:
        image_pathname = os.path.join(directory, image)

        image_records = pandas.read_csv(image_pathname)

        image_records.set_index("ImageNumber", inplace=True)

        concatenated_object_records = pandas.DataFrame()

        # Open object CSVs (e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv, etc.)
        # as Dask DataFrames:
        for object in objects:
            object_pathname = os.path.join(directory, object)

            prefix, _ = os.path.splitext(object)

            object_records = pandas.read_csv(object_pathname)

            object_records = object_records.add_prefix(f"{prefix}_")

            object_records.rename(
                columns={f"{prefix}_ImageNumber": "ImageNumber"}, inplace=True
            )
            object_records.rename(
                columns={f"{prefix}_ObjectNumber": "ObjectNumber"}, inplace=True
            )

            object_records.set_index(
                ["ImageNumber", "ObjectNumber"], drop=False, inplace=True
            )

            object_records[f"{prefix}_ImageNumber"] = object_records["ImageNumber"]
            object_records[f"{prefix}_ObjectNumber"] = object_records["ObjectNumber"]

            object_records.drop(["ImageNumber"], axis=1, inplace=True)
            object_records.drop(["ObjectNumber"], axis=1, inplace=True)

            concatenated_object_records = pandas.concat(
                [concatenated_object_records, object_records], axis=1
            )

        records += [
            image_records.merge(
                concatenated_object_records,
                how="outer",
                left_index=True,
                right_index=True,
            )
        ]

    records = pandas.concat(records)

    records.reset_index(drop=False, inplace=True)

    npartitions = records[partition_on[0]].unique().size

    records = dask.dataframe.from_pandas(records, npartitions=npartitions)

    records.to_parquet(destination, partition_on=partition_on, **kwargs)
