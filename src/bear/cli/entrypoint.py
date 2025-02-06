import typer

from typing import List, Annotated
from pathlib import Path
from bear.cli.conflate import conflate_workflow
from bear.cli.conform import conform_workflow
from bear.providers import ProviderKind

cli = typer.Typer(name="bear")


@cli.command(
    help="Perform the conform workflow across the given counties and providers."
)
def conform(
    fips: Annotated[List[str], typer.Argument()],
    providers: Annotated[
        List[ProviderKind], typer.Option()
    ] = ProviderKind.list_providers(),
    output_directory: Annotated[
        Path, typer.Option(file_okay=False, dir_okay=True)
    ] = Path(".bear"),
    input_directory: Annotated[
        Path, typer.Option(file_okay=False, dir_okay=True)
    ] = Path(".bear/raw"),
):
    for param_fips in fips:
        for param_provider in providers:
            conform_workflow(
                param_fips,
                param_provider,
                output_directory,
                input_directory,
            )


@cli.command()
def conflate(
    fips: Annotated[List[str], typer.Argument()],
    output_directory: Annotated[
        Path, typer.Option(file_okay=False, dir_okay=True)
    ] = Path(".bear"),
    input_directory: Annotated[
        Path, typer.Option(file_okay=False, dir_okay=True)
    ] = Path(".bear"),
):
    for param_fips in fips:
        conflate_workflow(
            param_fips,
            output_directory,
            input_directory,
        )
