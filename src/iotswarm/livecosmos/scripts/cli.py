import asyncio
from pathlib import Path
from typing import List, Optional, Tuple

import click
from config import Config
from driutils.io.aws import S3Writer

from iotswarm.db import Oracle
from iotswarm.livecosmos.liveupload import LiveUploader
from iotswarm.livecosmos.utils import _get_s3_client
from iotswarm.queries import CosmosTable

_ALLOWED_TABLES = [
    CosmosTable.LEVEL_1_NMDB_1HOUR.name,
    CosmosTable.LEVEL_1_PRECIP_1MIN.name,
    CosmosTable.LEVEL_1_PRECIP_RAINE_1MIN.name,
    CosmosTable.LEVEL_1_SOILMET_30MIN.name,
]


async def send_latest(config_file: Path, table: str, sites: Optional[List[str]] = None, fallback_hours: int = 3) -> None:
    """The main invocation method.
        Initialises the Oracle connection and defines which data the query.

    Args:
        config_file: Path to the *.cfg file that contains oracle credentials.
        table: Name of the cosmos table to submit.
        sites: A list of sites to upload from. Grabs sites from the Oracle database if
            not provided.
        fallback_hours: The number of hours to fallback to if no state is found.
    """

    app_config = Config(str(config_file))

    s3_writer = S3Writer(_get_s3_client(app_config))

    oracle = await Oracle.create(**app_config["oracle"])

    if not sites or len(sites) == 0:
        sites = await oracle.list_all_sites()

    uploader = LiveUploader(oracle, CosmosTable[table], sites, app_config["aws"]["bucket"], bucket_prefix=app_config["aws"]["bucket_prefix"], fallback_hours=fallback_hours)

    await uploader.send_latest_data(s3_writer)


@click.group()
def cli() -> None:
    """Entrypoint to cli"""
    pass


async def gather_upload_tasks(config_src: Path, tables: List[str], **kwargs) -> List:
    """Helper method to gather all async upload tasks
    Args:
        config_src: A path to the config.cfg file used.
        tables: A list of tables to upload from.
    Returns:
        A list of async futures
    """

    return await asyncio.gather(*[send_latest(config_src, table, **kwargs) for table in tables])


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
@click.option("--fallback-hours", type=int, default=3, help="The number of hours to fallback to if no state is found.")
def send_live_data(config_src: Path, table: Tuple[str], site: Tuple[str], fallback_hours: str) -> None:
    """Sends out all live data
    Args:
        config_src: A path to the config.cfg file used.
        table: A list of tables to upload from.
        site: A list of sites to upload from. Grabs sites from the Oracle database if
            not provided.
        fallback_hours: The number of hours to fallback to if no state is found
    """

    if "all" in table:
        table = _ALLOWED_TABLES

    asyncio.run(gather_upload_tasks(config_src, list(table), sites=list(site), fallback_hours=fallback_hours))


if __name__ == "__main__":
    cli()
