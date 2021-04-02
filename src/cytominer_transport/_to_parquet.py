import os
import os.path
import typing

import pyarrow.csv


def to_parquet(
    source: typing.Union[str, os.PathLike],
    destination: typing.Union[str, os.PathLike],
    experiment: typing.Optional[typing.Union[str, os.PathLike]] = None,
    image: typing.Optional[typing.Union[str, os.PathLike]] = None,
    object: typing.Optional[typing.List[typing.Union[str, os.PathLike]]] = None,
) -> os.PathLike:
    if experiment:
        experiment_path = os.path.join(source, experiment)
    else:
        experiment_path = os.path.join(source, "Experiment.csv")

        if not os.path.exists(experiment_path):
            experiment_path = None

    if experiment_path:
        experiment = pyarrow.csv.read_csv(experiment_path)

    if image:
        image_path = os.path.join(source, image)
    else:
        image_path = os.path.join(source, "Image.csv")

        if not os.path.exists(image_path):
            image_path = None

    if image_path:
        image = pyarrow.csv.read_csv(image_path)

    if object:
        for name in object:
            pass
