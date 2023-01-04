import click

__version__ = '0.1.dev0'

from pydrodelta.analysis import run_analysis
# from pydrodelta.simulation import run_simulation

@click.group()
@click.version_option(version=__version__)
def cli():
    pass


cli.add_command(run_analysis)
# cli.add_command(run_simulation)
