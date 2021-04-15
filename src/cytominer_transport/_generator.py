import os
import os.path
import typing

import dask.dataframe
import pandas


def generator(
    directories: typing.List[typing.Union[str, bytes, os.PathLike]],
    image: typing.Optional[typing.Union[str, bytes, os.PathLike]] = "Image.csv",
    objects: typing.List[typing.Union[str, bytes, os.PathLike]] = [
        "Cells.csv",
        "Cytoplasm.csv",
        "Nuclei.csv",
    ],
    partition_on: typing.Optional[typing.List[str]] = ["Metadata_Well"],
):
    index = 0

    while index < len(directories):
        directory = directories[index]

        image_pathname = os.path.join(directory, image)

        image_records = pandas.read_csv(image_pathname)

        image_records.set_index("ImageNumber", inplace=True)

        concatenated_object_records = pandas.DataFrame()

        # Open object CSVs (e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv, etc.)
        # as Pandas DataFrames:
        for object in objects:
            object_pathname = os.path.join(directory, object)

            prefix, _ = os.path.splitext(object)

            object_records = pandas.read_csv(object_pathname)

            object_records = object_records.add_prefix(f"{prefix}_")

            columns = {
                f"{prefix}_ImageNumber": "ImageNumber",
                f"{prefix}_ObjectNumber": "ObjectNumber",
            }

            object_records.rename(columns=columns, inplace=True)

            object_records.set_index(
                ["ImageNumber", "ObjectNumber"], drop=False, inplace=True
            )

            object_records[f"{prefix}_ImageNumber"] = object_records["ImageNumber"]
            object_records[f"{prefix}_ObjectNumber"] = object_records["ObjectNumber"]

            object_records.drop(["ImageNumber", "ObjectNumber"], axis=1, inplace=True)

            concatenated_object_records = pandas.concat(
                [concatenated_object_records, object_records], axis=1
            )

        records = image_records.merge(
            concatenated_object_records,
            how="outer",
            left_index=True,
            right_index=True,
        )

        records.reset_index(drop=False, inplace=True)

        npartitions = records[partition_on[0]].unique().size

        records = dask.dataframe.from_pandas(records, npartitions=npartitions)

        yield records

        index += 1
