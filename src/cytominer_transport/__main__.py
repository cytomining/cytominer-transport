import click


@click.command()
@click.argument(
    "source",
    help="Source directory for data. Prepend with a protocol (e.g. s3:// or hdfs://) for remote data.",
    type=click.Path(),
)
@click.argument(
    "destination",
    help="Destination directory for data. Prepend with a protocol (e.g. s3:// or hdfs://) for remote data.",
    type=click.Path(),
)
@click.option(
    "--experiment",
    help="CSV containing the run details needed to reproduce the ``image`` and ``objects`` CSVs.",
    nargs=1,
    type=click.File("r"),
)
@click.option(
    "--image",
    help="CSV containing data pertaining to images",
    nargs=1,
    type=click.File("r"),
)
@click.option(
    "--object",
    help="One or more CSVs containing data pertaining to objects or regions of interest (e.g. Cells.csv, Cytoplasm.csv, Nuclei.csv, etc.).",
    multiple=True,
    type=click.File("r"),
)
def __main__():
    pass
