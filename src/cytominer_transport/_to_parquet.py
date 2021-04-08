import os
import os.path
import typing

import dask.dataframe
import pyarrow.csv


def to_parquet(
    source: typing.Union[str, bytes, os.PathLike],
    destination: typing.Union[str, bytes, os.PathLike],
    experiment: typing.Optional[typing.Union[str, bytes, os.PathLike]] = None,
    image: typing.Optional[typing.Union[str, bytes, os.PathLike]] = None,
    object: typing.Optional[typing.List[typing.Union[str, bytes, os.PathLike]]] = None,
) -> os.PathLike:
    if experiment:
        experiment_path = os.path.join(source, experiment)
    else:
        experiment_path = os.path.join(source, "Experiment.csv")

        if not os.path.exists(experiment_path):
            experiment_path = None

    if experiment_path:
        experiment_features = dask.dataframe.read_csv(experiment_path)

    if image:
        image_path = os.path.join(source, image)
    else:
        image_path = os.path.join(source, "Image.csv")

        if not os.path.exists(image_path):
            raise ValueError

    if image_path:
        image_features = dask.dataframe.read_csv(image_path)

        image_features = image_features.set_index("ImageNumber")

    if object:
        for name in object:
            object_path = os.path.join(source, name)

            object_features = None

            try:
                object_features = dask.dataframe.read_csv(object_path)
            except Exception as error:
                pass

            if object_features is not None:
                object_features = object_features.set_index("ImageNumber")

                image_features = dask.dataframe.merge(
                    image_features, object_features, left_index=True, right_index=True
                )
