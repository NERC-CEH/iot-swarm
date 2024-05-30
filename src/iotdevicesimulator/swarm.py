"""This is the core module for orchestrating swarms of IoT devices. One swarm defined currently for using COSMOS data."""

from iotdevicesimulator.devices import SensorSite
from iotdevicesimulator.db import Oracle
from iotdevicesimulator.queries import CosmosQuery
from iotdevicesimulator.messaging.core import MessagingBaseClass
import logging.config
from typing import List
from pathlib import Path
import asyncio
import random
import uuid
import config

logger = logging.getLogger(__name__)


class CosmosSwarm:
    """Object for creating a swarm of COSMOS site devices.
    This object instantiates a group of sensor devices that submit data from the
    COSMOS database and then wait for a specified time. When run unrestricted, this
    can simulate the full COSMOS network in real time.

    Instantiation is handled by factory method `CosmosSwarm.Create()` due to
    async nature of database implementation.
    """

    message_connection: MessagingBaseClass
    """Messaging object to submit data to."""

    swarm_name: str
    """Name of swarm applied in logs."""

    _instance_logger: logging.Logger
    """Logger handle used by instance."""

    max_cycles: int = 1
    """Maximum number of data transfer cycles before shutting down."""

    max_sites: int = 5
    """Maximum number of sites allowed to be instantiated."""

    sleep_time: int = 30
    """Time to sleep for each time data is sent."""

    sites: List[SensorSite]
    """List of site objects."""

    delay_first_cycle: bool = False
    """Adds a random delay to first invocation from 0 - `sleep_time`."""

    oracle: Oracle
    """Oracle database object"""

    query: CosmosQuery
    """Query run in database."""

    def __len__(self):
        """Returns number of sites"""
        return len(self.sites)

    @classmethod
    async def create(
        cls,
        query: str,
        message_connection: MessagingBaseClass,
        credentials: str | dict,
        site_ids: List[str] | None = None,
        sleep_time: int | None = None,
        max_cycles: int | None = None,
        max_sites: int | None = None,
        swarm_name: str | None = None,
        delay_first_cycle: bool | None = None,
    ) -> None:
        """Factory method for initialising the class.

        Args:
            query: A query retrieve from the database.
            message_connection: Object used to send data.
            site_ids: A list of site ID strings.
            sleep_time: Length of time to sleep after sending data in seconds.
            max_cycles: Maximum number of data sending cycles.
            max_sites: Maximum number of sites to initialise.
            swarm_name: Name / ID given to swarm.
            delay_first_cycle: Adds a random delay to first invocation from 0 - `sleep_time`.
            credentials: A path to database credentials.
        """
        self = cls()

        if not isinstance(message_connection, MessagingBaseClass):
            raise TypeError(
                f"`message_connection` must be a `MessagingBaseClass`. Received: {type(message_connection)}."
            )
        self.message_connection = message_connection

        if not isinstance(query, CosmosQuery):
            raise TypeError(
                f"`query` must be a `CosmosQuery`. Received: {type(query)}."
            )
        self.query = query

        if not isinstance(credentials, (dict, str)):
            raise TypeError(
                f"`credentials` must be a path to a config file, or a dict of credentials, not {type(credentials)}"
            )

        if isinstance(credentials, str):
            credentials = config.Config(credentials)["oracle"]

        if swarm_name is not None:
            self.swarm_name = str(swarm_name)
        else:
            self.swarm_name = f"swarm-{uuid.uuid4()}"

        self._instance_logger = logger.getChild(self.swarm_name)
        self._instance_logger.debug("Initialising swarm")

        if max_cycles is not None:
            max_cycles = int(max_cycles)
            if max_cycles <= 0 and max_cycles != -1:
                raise ValueError(
                    f"`max_cycles` must be 1 or more, or -1 for no maximum. Received: {max_cycles}"
                )

            self.max_cycles = max_cycles

        if sleep_time is not None:
            sleep_time = int(sleep_time)
            if sleep_time < 0:
                raise ValueError(
                    f"`sleep_time` must be 0 or more. Received: {sleep_time}"
                )
            self.sleep_time = sleep_time

        if max_sites is not None:
            max_sites = int(max_sites)
            if max_sites <= 0 and max_sites != -1:
                raise ValueError(
                    f"`max_sites` must be 1 or more, or -1 for no maximum. Received: {max_sites}"
                )
            self.max_sites = max_sites

        if delay_first_cycle is not None:
            if not isinstance(delay_first_cycle, bool):
                raise TypeError(
                    f"`delay_first_cycle` must be a bool. Received: {delay_first_cycle}."
                )
            self.delay_first_cycle = delay_first_cycle

        self.oracle = await self._get_oracle(
            credentials=credentials, inherit_logger=self._instance_logger
        )

        if site_ids:
            self.sites = self._init_sites(
                site_ids,
                sleep_time=self.sleep_time,
                max_cycles=self.max_cycles,
                max_sites=self.max_sites,
                swarm_logger=self._instance_logger,
                delay_first_cycle=self.delay_first_cycle,
            )
        else:
            self.sites = await self._init_sites_from_db(
                self.oracle,
                sleep_time=self.sleep_time,
                max_cycles=self.max_cycles,
                max_sites=self.max_sites,
                swarm_logger=self._instance_logger,
                delay_first_cycle=self.delay_first_cycle,
            )

        self._instance_logger.debug("Swarm Ready")

        return self

    async def run(self) -> None:
        """Main function for running the swarm. Sends the query
        and message connection object. Runs until all sites reach
        their maximum cycle. If no maximum, it runs forever.
        """

        self._instance_logger.debug("Running main loop")
        await asyncio.gather(
            *[
                site.run(self.oracle, self.query, self.message_connection)
                for site in self.sites
            ]
        )

        self._instance_logger.info("Finished")

    @staticmethod
    async def _get_oracle(
        credentials: dict,
        inherit_logger: logging.Logger | None = None,
    ) -> Oracle:
        """Receives a dict of credentials and returns an asynchronous
        connection to Oracle.

        The expected source of credentials is the config file, but this
        can be provided manually with a dict:

        .. code-block:: python

            credentials = {
                "dsn": "xxxxx",
                "user": "xxxxxx",
                "password": "xxxxxx"
            }

        Args:
            credentials: A dict of credentials.
            inherit_logger: Inherits the module logger if true.

        Returns:
            Oracle: An oracle database.
        """

        oracle = await Oracle.create(
            credentials["dsn"],
            credentials["user"],
            password=credentials["password"],
            inherit_logger=inherit_logger,
        )

        return oracle

    @staticmethod
    def _init_sites(
        site_ids: List[str],
        sleep_time: int = 10,
        max_cycles: int = 3,
        max_sites: int = -1,
        swarm_logger: logging.Logger | None = None,
        delay_first_cycle: bool = False,
    ):
        """Initialises a list of SensorSites.

        Args:
            site_ids: A list of site ID strings.
            sleep_time: Length of time to sleep after sending data in seconds.
            max_cycles: Maximum number of data sending cycles.
            max_sites: Maximum number of sites to initialise. Picks randomly from list if given
            swarm_logger: Passes the instance logger to sites
            delay_first_cycle: Adds a random delay to first invocation from 0 - `sleep_time`.

        Returns:
            List[SensorSite]: A list of sensor sites.
        """
        if max_sites != -1:
            site_ids = CosmosSwarm._random_list_items(site_ids, max_sites)

        return [
            SensorSite(
                site_id,
                sleep_time=sleep_time,
                max_cycles=max_cycles,
                inherit_logger=swarm_logger,
                delay_first_cycle=delay_first_cycle,
            )
            for site_id in site_ids
        ]

    @staticmethod
    async def _init_sites_from_db(
        oracle: Oracle,
        sleep_time: int = 10,
        max_cycles: int = 3,
        max_sites=-1,
        swarm_logger: logging.Logger | None = None,
        delay_first_cycle: bool = False,
    ) -> List[SensorSite]:
        """Initialised sensor sites from the COSMOS DB.

        Args:
            oracle: Oracle DB to query
            sleep_time: Length of time to sleep after sending data in seconds.
            max_cycles: Maximum number of data sending cycles.
            max_sites: Maximum number of sites to initialise. Picks randomly from list if less than number of sites found
            swarm_logger: Passes the instance logger to sites
            delay_first_cycle: Adds a random delay to first invocation from 0 - `sleep_time`.

        Returns:
            List[SensorSite]: A list of sensor sites.

        TODO: Update to grab sites from unique items in DB tables.
        """

        async with oracle.connection.cursor() as cursor:
            await cursor.execute("SELECT UNIQUE(SITE_ID) from COSMOS.SITES")

            site_ids = await cursor.fetchall()

            if max_sites != -1:
                site_ids = CosmosSwarm._random_list_items(site_ids, max_sites)

            return [
                SensorSite(
                    site_id[0],
                    sleep_time=sleep_time,
                    max_cycles=max_cycles,
                    inherit_logger=swarm_logger,
                    delay_first_cycle=delay_first_cycle,
                )
                for site_id in site_ids
            ]

    @staticmethod
    def _random_list_items(list_in: List[object], max_count: int) -> List[object]:
        """Restricts the number of items in a list by picked randomly

        Args:
            list_in: The list to process.
            max_count: Maximum number of items in list.
        Returns:
            List[object]: A list of randomly selected items or the original list.
        """

        max_count = int(max_count)

        if max_count <= 0 and max_count != -1:
            raise ValueError(
                f"`max_count` must be 1 or more, or -1 for no maximum. Received: {max_count}"
            )

        if len(list_in) <= max_count or max_count == -1:
            return list_in

        list_out = []

        while len(list_out) < max_count:
            list_out.append(list_in.pop(random.randint(0, len(list_in) - 1)))

        return list_out
