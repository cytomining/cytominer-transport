import click

@click.command()
@click.argument(
    "source",
    type=click.Path()
)
@click.argument(
    "destination",
    type=click.Path()
)
@click.option(
    "--experiment",
    help="CSV file with the run details needed to reproduce the other output files",
    nargs=1,
    type=click.File("r")
)
@click.option(
    "--image",
    help="CSV file containing data pertaining to whole images",
    nargs=1,
    type=click.File("r")
)
@click.option(
    "--object",
    help="one or more CSV files containing data pertaining to objects or sub-areas within the whole image",
    multiple=True,
    type=click.File("r")
)
def __main__():
    pass
