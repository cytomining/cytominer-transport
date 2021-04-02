import os
import typing


def to_parquet(
    source: typing.Union[str, os.PathLike],
    destination: typing.Union[str, os.PathLike],
    experiment: typing.Optional[typing.Union[str, os.PathLike]] = None,
    image: typing.Optional[typing.Union[str, os.PathLike]] = None,
    object: typing.Optional[typing.List[typing.Union[str, os.PathLike]]] = None,
) -> os.PathLike:
    pass
