"""This is the main module invocation for sending live COSMOS data to AWS"""

from config import Config
from pathlib import Path
import sys
from iotswarm.db import Oracle
from iotswarm.queries import CosmosTable
from iotswarm.devices import CR1000XDevice
from iotswarm.messaging.core import MockMessageConnection
import asyncio
from typing import List
from datetime import datetime, timedelta
import logging
import logging.config

logging.config.fileConfig(
    fname=Path(__file__).parents[1] / "__assets__" / "loggers.ini"
)

logger = logging.getLogger(__name__)

MOCK_CONNECTION = MockMessageConnection()


async def get_latest_payloads_for_table(
    oracle: Oracle, table: CosmosTable, datetime_gt: datetime
) -> List[dict]:
    """Gets all payloads after the datetime for a given Oracle table
        Iterates through all sites found in the table and filters by datetimes
        after the specified timestamp.

    Args:
        oracle: The oracle database connection
        table: The database table to search
        datetime_gt: The datetime that values must be greater than.

    Returns:
        A list dictionaries where each dictionary is a payload.
    """

    sites = await oracle.query_site_ids(table)

    logger.debug(f"Found {len(sites)} sites IDs for table: {table}")

    payloads = await asyncio.gather(
        *[
            get_latest_payloads_for_site(oracle, table, datetime_gt, site)
            for site in sites
        ]
    )

    # Flatten lists and return
    return [item for row in payloads for item in row]


async def get_latest_payloads_for_site(
    oracle: Oracle, table: CosmosTable, datetime_gt: datetime, site: str
) -> List[dict]:
    """Gets all payloads after the datetime for a given site from an Oracle table.

    Args:
        oracle: The oracle database connection
        table: The database table to search
        datetime_gt: The datetime that values must be greater than
        site: The name of the site

    Returns:
        A list dictionaries where each dictionary is a payload.
    """
    latest = await oracle.query_datetime_gt_from_site(site, datetime_gt, table)

    if not latest:
        logger.debug(f"Got 0 rows for site {site} in table: {table}")
        return []

    device = CR1000XDevice(
        device_id=site,
        data_source=oracle,
        connection=MOCK_CONNECTION,
        table=table,
    )

    logger.debug(f"Got {len(latest)} rows for site {site} in table: {table}")

    payloads = [device._format_payload(x) for x in latest]

    return payloads


async def main(config_file: Path) -> List[dict]:
    """The main invocation method.
        Initialises the Oracle connection and defines which data the query.

    Args:
        config_file: Path to the *.cfg file that contains oracle credentials.
    """
    oracle_creds = Config(str(config_file))

    oracle = await Oracle.create(
        oracle_creds["dsn"], oracle_creds["user"], oracle_creds["pass"]
    )
    tables = [CosmosTable.LEVEL_1_SOILMET_30MIN, CosmosTable.LEVEL_1_NMDB_1HOUR]

    date_gt = datetime.now() - timedelta(hours=1)
    result = await asyncio.gather(
        *[get_latest_payloads_for_table(oracle, table, date_gt) for table in tables]
    )

    table_data = dict(zip(tables, result))
    print(table_data)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.argv.append(str(Path(__file__).parents[3] / "oracle.cfg"))
    asyncio.run(main(*sys.argv[1:]))
