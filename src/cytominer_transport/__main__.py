import click

@click.command()
@click.argument("source", type=click.Path())
@click.argument("output", type=click.Path())
@click.option("--experiment", nargs=1, type=click.File("r"))
@click.option("--image", nargs=1, type=click.File("r"))
@click.option("--object", multiple=True, type=click.File("r"))
def __main__():
    pass
