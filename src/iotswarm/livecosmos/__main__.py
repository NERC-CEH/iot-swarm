"""This is the main module invocation for sending live COSMOS data to AWS"""

import asyncio
import sys
from pathlib import Path

from config import Config

from iotswarm.db import Oracle
from iotswarm.livecosmos.liveupload import LiveUploader
from iotswarm.livecosmos.loggers import get_logger
from iotswarm.queries import CosmosTable

logger = get_logger(__name__)


async def main(config_file: Path, table_name: str) -> None:
    """The main invocation method.
        Initialises the Oracle connection and defines which data the query.

    Args:
        config_file: Path to the *.cfg file that contains oracle credentials.
        table_name: Name of the cosmos table to submit
    """
    oracle_creds = Config(str(config_file))

    oracle = await Oracle.create(oracle_creds["dsn"], oracle_creds["user"], oracle_creds["pass"])

    sites = await oracle.list_all_sites()

    uploader = LiveUploader(oracle, CosmosTable[table_name], sites)

    await uploader.send_latest_data()


if __name__ == "__main__":
    # Temporary hardcoded arguments
    if len(sys.argv) == 1:
        sys.argv.append(str(Path(__file__).parents[3] / "oracle.cfg"))
        sys.argv.append("LEVEL_1_PRECIP_1MIN")

    asyncio.run(main(*sys.argv[1:]))
