import asyncio
from pathlib import Path
from typing import List, Tuple

import click

from iotswarm.livecosmos.__main__ import main
from iotswarm.queries import CosmosTable

_ALLOWED_TABLES = [
    CosmosTable.LEVEL_1_NMDB_1HOUR.name,
    CosmosTable.LEVEL_1_PRECIP_1MIN.name,
    CosmosTable.LEVEL_1_PRECIP_RAINE_1MIN.name,
    CosmosTable.LEVEL_1_SOILMET_30MIN.name,
]


@click.group()
def cli() -> None:
    """Entrypoint to cli"""
    pass


async def gather_upload_tasks(config_src: Path, tables: List[str], sites: List[str] = []) -> List:
    return await asyncio.gather(*[main(config_src, table, sites) for table in tables])


@cli.command()
@click.argument("config_src", type=click.Path(file_okay=True, path_type=Path))
@click.option(
    "--table",
    type=click.Choice([*_ALLOWED_TABLES, "all"]),
    required=True,
    multiple=True,
    help="A list of COSMOS-UK tables to upload from. Use `--table all` to select all tables.",
)
@click.option("--site", type=str, multiple=True, default=(), help="A list of sites to target")
def send_live_data(config_src: Path, table: Tuple[str], site: Tuple[str]) -> None:
    """Sends out all live data
    Args:
        config_src: A path to the config.cfg file used.
        table: A list of tables to upload from.
        site: A list of sites to upload from.
    """

    if "all" in table:
        table = _ALLOWED_TABLES

    asyncio.run(gather_upload_tasks(config_src, list(table), list(site)))


if __name__ == "__main__":
    cli()
