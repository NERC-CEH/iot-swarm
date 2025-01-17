"""This is the main module invocation for sending live COSMOS data to AWS"""

import asyncio
import logging
import logging.config
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from config import Config

from iotswarm.db import Oracle
from iotswarm.devices import CR1000XDevice
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.queries import CosmosTable
from iotswarm.livecosmos.state import Site, StateTracker

logging.config.fileConfig(fname=Path(__file__).parent / "__assets__" / "logger.ini")

logger = logging.getLogger(__name__)

MOCK_CONNECTION = MockMessageConnection()
FALLBACK_TIME = datetime.now() - timedelta(hours=3)

def _get_search_time(state: StateTracker, site: str) -> datetime:
    """Returns the latest sent data time or uses the fallback time
    Args:
        state: The prior state
        site: The site to search a time for
    Returns:
        A datetime of the most recent sent data or the fallback time
    """

    if site not in state.state["sites"]:
        logger.debug(f"site {site} not in state. Using fallback time.")
        return FALLBACK_TIME
    
    return state.state["sites"][site]["last_data"]

async def get_latest_payloads_for_table(oracle: Oracle, table: CosmosTable, sites: List[str], state: StateTracker) -> List[dict]:
    """Gets all payloads after the datetime for a given Oracle table
        Iterates through all sites found in the table and filters by datetimes
        after the specified timestamp.

    Args:
        oracle: The oracle database connection
        table: The database table to search
        sites: The sites to query
        state: The persistent upload state

    Returns:
        A list dictionaries where each dictionary is a payload.
    """

    payloads = await asyncio.gather(*[get_latest_payloads_for_site(oracle, table, site, state) for site in sites])

    # Flatten lists and return
    return [item for row in payloads for item in row]


async def get_latest_payloads_for_site(
    oracle: Oracle, table: CosmosTable, site: str, state
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

    datetime_gt = _get_search_time(state, site)
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

def send_payload(payload: dict,  state: StateTracker) -> StateTracker:
    """Sends the payload to AWS and writes the state to file"""

    site = Site(
        site_id=payload["head"]["environment"]["station_name"],
        last_data=payload["data"][0]["time"]
    )
    
    state_changed = state.update_state(site)
    
    if state_changed:
        state.write_state()

    return state


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
    tracker = StateTracker(table_name)

    payloads = await get_latest_payloads_for_table(oracle, CosmosTable[table_name], sites, tracker)

    for payload in payloads:
        tracker = send_payload(payload, tracker)


if __name__ == "__main__":

    # Temporary hardcoded arguments
    if len(sys.argv) == 1:
        sys.argv.append(str(Path(__file__).parents[3] / "oracle.cfg"))
        sys.argv.append("LEVEL_1_SOILMET_30MIN")

    asyncio.run(main(*sys.argv[1:]))
