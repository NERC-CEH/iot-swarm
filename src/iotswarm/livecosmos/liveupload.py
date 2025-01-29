"""Module for handling the core logic of the liveuploader"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import List, Optional

import backoff
import oracledb
from awscrt.exceptions import AwsCrtError
from driutils.io.aws import S3Writer

from iotswarm.db import Oracle
from iotswarm.devices import CR1000XDevice, CR1000XPayload
from iotswarm.livecosmos.loggers import get_logger
from iotswarm.livecosmos.state import Site, StateTracker
from iotswarm.livecosmos.utils import build_aws_object_key
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.queries import CosmosTable
from iotswarm.utils import json_serial

logger = get_logger(__name__)

MOCK_CONNECTION = MockMessageConnection()


class LiveUploader:
    """Class for handling core logic for uploading live data from the COSMOS
    database
    """

    _fallback_time: datetime
    """Backup time for retrieving database records"""

    _app_prefix: str
    """Prefix added to the state files"""

    state: StateTracker
    """The current state of the uploaded files"""

    bucket: str
    """The S3 bucket name that is used for writing"""

    bucket_prefix: Optional[str]
    """Prefix of the path used in the S3 bucket"""

    _s3_manager: S3Writer
    """Object for handling writes to S3"""

    def __init__(
        self,
        oracle: Oracle,
        table: CosmosTable,
        sites: List[str],
        bucket: str,
        bucket_prefix: Optional[str] = None,
        app_prefix: str = "livecosmos",
        fallback_hours: int = 3,
    ) -> None:
        """Initializes the instance

        Args:
            oracle: A connection to the oracle database
            table: The table to upload records from
            sites: A list of sites to search new records
            bucket: Name of the S3 bucket that is written to
            bucket_prefix: Prefix added to the bucket path
            app_prefix: Prefix added to the state files
            fallback_hours: The number of hours to fallback to if no state is found
        """

        self.table = table
        self.oracle = oracle
        self.sites = sites
        self.bucket = bucket
        self.bucket_prefix = bucket_prefix
        self._app_prefix = app_prefix
        self.state = StateTracker(str(table), app_name=app_prefix)
        self._fallback_time = datetime.now() - timedelta(hours=fallback_hours)

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

    @backoff.on_exception(backoff.expo, oracledb.Error, max_time=60, logger=logger)
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

    def _get_s3_key(self, site_id: str, object_name: str) -> str:
        """Builds a fully qualified S3 path for a file upload

        Args:
            object_name: Name of the object to be uploaded
        Returns:
            A fully qualified S3 path
        """

        table_name = f"LIVE_{self.table.replace('LEVEL1_', '')}"
        key = f"{site_id}/{table_name}/{object_name}"

        if self.bucket_prefix:
            key = f"{self.bucket_prefix}/{key}"

        return key

    @backoff.on_exception(backoff.expo, AwsCrtError, max_time=60, logger=logger)
    def send_payload(self, payload: CR1000XPayload, s3_writer: S3Writer) -> None:
        """Sends the payload to AWS and writes the state to file

        Args:
            payload: The device formatted payload to upload
            s3_writer: Object used to write data to S3
        """

        site = Site(site_id=payload["head"]["environment"]["station_name"], last_data=payload["data"][0]["time"])

        state_changed = self.state.update_state(site)

        payload_json = json.dumps(payload, default=json_serial)
        object_name = build_aws_object_key(datetime.now(), payload_json)

        s3_key = self._get_s3_key(site["site_id"], object_name)
        s3_url = f"s3://{self.bucket}/{s3_key}"

        try:
            s3_writer.write(self.bucket, s3_key, payload_json.encode())
            logger.info(f"Wrote payload to {s3_url}")
        except Exception as e:
            logger.error(f"Unexpected error when uploading file: {e}")
            logger.exception(e)
            raise e

        if state_changed:
            logger.info(f"Updated state file with site: {site}")
            self.state.write_state()

    async def send_latest_data(self, s3_writer: S3Writer) -> None:
        """Queries and sends the latest data for all sites

        Args:
            s3_writer: Object used to write data to S3
        """

        payloads = await self.get_latest_payloads()

        for payload in payloads:
            self.send_payload(payload, s3_writer)
