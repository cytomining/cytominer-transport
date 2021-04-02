import os
import typing

def to_parquet(
    destination: typing.Union[str, os.PathLike],
    source: typing.Union[str, os.PathLike],
) -> os.PathLike:
    pass

@typing.overload
def to_parquet(
    destination: typing.Union[str, os.PathLike],
    image: typing.Union[str, os.PathLike],
    source: typing.Union[str, os.PathLike],
) -> os.PathLike:
    pass

@typing.overload
def to_parquet(
    destination: typing.Union[str, os.PathLike],
    image: typing.Union[str, os.PathLike],
    object: typing.List[typing.Union[str, os.PathLike]],
    source: typing.Union[str, os.PathLike],
) -> os.PathLike:
    pass


@typing.overload
def to_parquet(
    destination: typing.Union[str, os.PathLike],
    experiment: typing.Union[str, os.PathLike],
    image: typing.Union[str, os.PathLike],
    object: typing.List[typing.Union[str, os.PathLike]],
    source: typing.Union[str, os.PathLike],
) -> os.PathLike:
    pass
