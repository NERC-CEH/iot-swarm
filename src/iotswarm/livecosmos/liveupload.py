"""Module for handling the core logic of the liveuploader"""

import asyncio
from datetime import datetime, timedelta
from typing import List

from iotswarm.db import Oracle
from iotswarm.devices import CR1000XDevice, CR1000XPayload
from iotswarm.livecosmos.loggers import get_logger
from iotswarm.livecosmos.state import Site, StateTracker
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.queries import CosmosTable

logger = get_logger(__name__)

MOCK_CONNECTION = MockMessageConnection()


class LiveUploader:
    """Class for handling core logic for uploading live data from the COSMOS
        database
    """
    _fallback_time: datetime = datetime.now() - timedelta(hours=3)
    """Backup time for retrieving database records"""

    _app_prefix: str
    """Prefix added to the state files"""

    state: StateTracker
    """The current state of the uploaded files"""

    def __init__(self, oracle: Oracle, table: CosmosTable, sites: List[str], app_prefix: str="livecosmos") -> None:
        """Initializes the instance

        Args:
            oracle: A connection to the oracle database
            table: The table to upload records from
            sites: A list of sites to search new records
            app_prefix: Prefix added to the state files
        """

        self.table = table
        self.oracle = oracle
        self.sites = sites
        self._app_prefix = app_prefix
        self.state = StateTracker(str(table), app_name=app_prefix)

    def _get_search_time(self, site: str) -> datetime:
        """Returns the latest sent data time or uses the fallback time
        Args:
            site: The site to search a time for
        Returns:
            A datetime of the most recent sent data or the fallback time
        """

        if site not in self.state.state["sites"]:
            logger.debug(f"site {site} not in state. Using fallback time.")
            return self._fallback_time

        return self.state.state["sites"][site]["last_data"]

    async def get_latest_payloads(self) -> List[CR1000XPayload]:
        """Gets all payloads after the datetime for a given Oracle table
            Iterates through all sites found in the table and filters by datetimes
            after the specified timestamp.

        Returns:
            A list dictionaries where each dictionary is a payload.
        """

        payloads = await asyncio.gather(*[self._get_latest_payloads_for_site(site) for site in self.sites])

        # Flatten lists and return
        return [item for row in payloads for item in row]

    async def _get_latest_payloads_for_site(self, site: str) -> List[CR1000XPayload]:
        """Gets all new payloads from the Oracle table for a given site. If the
        site is present inside the `state` the latest data is taken from it, if not
        the `_fallback_time` is used as a backup to prevent uploading the entire database.

        Args:
            site: The name of the site

        Returns:
            A list dictionaries where each dictionary is a payload.
        """

        datetime_gt = self._get_search_time(site)
        latest = await self.oracle.query_datetime_gt_from_site(site, datetime_gt, self.table)

        if not latest:
            logger.debug(f"Got 0 rows for site {site} in table: {self.table}")
            return []

        device = CR1000XDevice(
            device_id=site,
            data_source=self.oracle,
            connection=MOCK_CONNECTION,
            table=self.table,
        )

        logger.debug(f"Got {len(latest)} rows for site {site} in table: {self.table}")

        payloads = [device._format_payload(x) for x in latest]

        return payloads

    def send_payload(self, payload: CR1000XPayload) -> None:
        """Sends the payload to AWS and writes the state to file

        Args:
            payload: The device formatted payload to upload

        # TODO: Implement upload logic. State should be updated on successful uploads
        """

        site = Site(site_id=payload["head"]["environment"]["station_name"], last_data=payload["data"][0]["time"])

        state_changed = self.state.update_state(site)

        if state_changed:
            self.state.write_state()

    async def send_latest_data(self) -> None:
        """Queries and sends the latest data for all sites"""

        payloads = await self.get_latest_payloads()

        for payload in payloads:
            self.send_payload(payload)
