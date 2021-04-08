import click


@click.command()
@click.argument(
    "source",
    type=str,
)
@click.argument(
    "destination",
    type=str,
)
@click.option(
    "--experiment",
    default="Experiment.csv",
    help="""
    CSV containing the run details needed to reproduce the `image` and
    `objects` CSVs.
    """,
    nargs=1,
    type=str,
)
@click.option(
    "--image",
    default="Image.csv",
    help="""
    CSV containing data pertaining to images.
    """,
    nargs=1,
    type=str,
)
@click.option(
    "--object",
    default=["Cells.csv", "Cytoplasm.csv", "Nuclei.csv"],
    help="""
    One or more CSVs containing data pertaining to objects or regions of
    interest (e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv, etc.).
    """,
    multiple=True,
    type=str,
)
@click.option(
    "--compression", default="snappy", type=click.Choice(["brotli", "gzip", "snappy"])
)
def __main__(source, destination, experiment, image, object, compression):
    """
    SOURCE directory for data. Prepend with a protocol
    (e.g. s3:// or hdfs://) for remote data.

    DESTINATION directory for data. Prepend with a protocol
    (e.g. s3:// or hdfs://) for remote data.
    """
    pass
