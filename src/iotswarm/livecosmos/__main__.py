"""This is the main module invocation for sending live COSMOS data to AWS"""

import asyncio
import sys
from pathlib import Path
from typing import List

from config import Config
from driutils.io.aws import S3Writer

from iotswarm.db import Oracle
from iotswarm.livecosmos.liveupload import LiveUploader
from iotswarm.livecosmos.loggers import get_logger
from iotswarm.livecosmos.utils import _get_s3_client
from iotswarm.queries import CosmosTable

logger = get_logger(__name__)


async def main(config_file: Path, table: str, sites: List[str] = []) -> None:
    """The main invocation method.
        Initialises the Oracle connection and defines which data the query.

    Args:
        config_file: Path to the *.cfg file that contains oracle credentials.
        table: Name of the cosmos table to submit
    """

    app_config = Config(str(config_file))

    s3_writer = S3Writer(_get_s3_client(app_config))

    oracle = await Oracle.create(**app_config["oracle"])

    if len(sites) == 0:
        sites = await oracle.list_all_sites()

    uploader = LiveUploader(oracle, CosmosTable[table], sites, app_config["aws"]["level_m1_bucket"])

    await uploader.send_latest_data(s3_writer)


if __name__ == "__main__":
    # Temporary hardcoded arguments
    if len(sys.argv) == 1:
        sys.argv.append(str(Path(__file__).parents[1] / "__assets__" / "config.cfg"))
        sys.argv.append("LEVEL_1_PRECIP_1MIN")

    asyncio.run(main(*sys.argv[1:]))
