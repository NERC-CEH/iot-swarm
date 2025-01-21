"""This is the main module invocation for sending live COSMOS data to AWS"""

import asyncio
import sys
from pathlib import Path

from config import Config, KeyNotFoundError

from iotswarm.db import Oracle
from iotswarm.livecosmos.liveupload import LiveUploader
from iotswarm.livecosmos.loggers import get_logger
from iotswarm.queries import CosmosTable
from driutils.io.aws import S3Writer
import boto3
logger = get_logger(__name__)

def _get_s3_client(config: Config) -> "boto3.client":
    """Returns the S3 client object.

    Args:
        config: The loaded app config object
    Returns:
        A boto3.s3.client object
    """

    try:
        endpoint = config["aws"]["endpoint_url"]
        return boto3.client("s3", endpoint_url=endpoint)
    except KeyError:
        return boto3.client("s3")

async def main(config_file: Path, table_name: str) -> None:
    """The main invocation method.
        Initialises the Oracle connection and defines which data the query.

    Args:
        config_file: Path to the *.cfg file that contains oracle credentials.
        table_name: Name of the cosmos table to submit
    """

    app_config = Config(str(config_file))

    s3_writer = S3Writer(_get_s3_client(app_config))

    oracle = await Oracle.create(**app_config["oracle"])

    sites = await oracle.list_all_sites()

    uploader = LiveUploader(oracle, CosmosTable[table_name], sites, app_config["aws"]["level_m1_bucket"])

    await uploader.send_latest_data(s3_writer)


if __name__ == "__main__":
    # Temporary hardcoded arguments
    if len(sys.argv) == 1:
        sys.argv.append(str(Path(__file__).parents[1] / "__assets__" / "config.cfg"))
        sys.argv.append("LEVEL_1_PRECIP_1MIN")

    asyncio.run(main(*sys.argv[1:]))
