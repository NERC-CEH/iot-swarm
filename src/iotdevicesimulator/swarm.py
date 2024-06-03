"""This is the core module for orchestrating swarms of IoT devices. One swarm defined currently for using COSMOS data."""

from iotdevicesimulator.devices import SensorSite
from iotdevicesimulator.db import Oracle
from iotdevicesimulator.queries import CosmosQuery, CosmosSiteQuery
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

    topic_prefix: str = None
    """Adds prefix to sensor topic."""

    def __len__(self):
        """Returns number of sites"""
        return len(self.sites)

    def __repr__(self):
        topic_prefix = (
            "" if self.topic_prefix is None else f', topic_prefix="{self.topic_prefix}"'
        )
        return (
            f"CosmosSwarm({type(self.query)}, {self.message_connection}, site_ids={[x.site_id for x in self.sites]}"
            f", sleep_time={self.sleep_time}, max_cycles={self.max_cycles}, max_sites={self.max_sites}"
            f', swarm_name="{self.swarm_name}", delay_first_cycle={self.delay_first_cycle}{topic_prefix})'
        )

    def __str__(self):
        topic_prefix = (
            "" if self.topic_prefix is None else f', topic_prefix="{self.topic_prefix}"'
        )
        return (
            f'CosmosSwarm({self.query.__class__.__name__}.{self.query.name}, swarm_name="{self.swarm_name}"'
            f"{topic_prefix}, sleep_time={self.sleep_time}, max_cycles={self.max_cycles}, delay_start={self.delay_first_cycle})"
        )

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
        topic_prefix: str | None = None,
    ) -> None:
        """Factory method for initialising the class.

        Args:
            query: A query retrieve from the database.
            message_connection: Object used to send data.
            credentials: A path to database credentials.
            site_ids: A list of site ID strings.
            sleep_time: Length of time to sleep after sending data in seconds.
            max_cycles: Maximum number of data sending cycles.
            max_sites: Maximum number of sites to initialise.
            swarm_name: Name / ID given to swarm.
            delay_first_cycle: Adds a random delay to first invocation from 0 - `sleep_time`.
            topic_prefix: Prefixes the sensor topic.
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
            if max_cycles < 0:
                raise ValueError(
                    f"`max_cycles` must be 1 or more, or 0 for no maximum. Received: {max_cycles}"
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
            if max_sites < 0:
                raise ValueError(
                    f"`max_sites` must be 1 or more, or 0 for no maximum. Received: {max_sites}"
                )
            self.max_sites = max_sites

        if delay_first_cycle is not None:
            if not isinstance(delay_first_cycle, bool):
                raise TypeError(
                    f"`delay_first_cycle` must be a bool. Received: {delay_first_cycle}."
                )
            self.delay_first_cycle = delay_first_cycle

        if topic_prefix is not None:
            self.topic_prefix = str(topic_prefix)

        self.oracle = await self._get_oracle(
            credentials=credentials, inherit_logger=self._instance_logger
        )

        if not site_ids:
            site_ids = await self._get_sites_from_db(
                self.oracle, CosmosSiteQuery[self.query.name]
            )

        self.sites = self._init_sites(
            site_ids,
            sleep_time=self.sleep_time,
            max_cycles=self.max_cycles,
            max_sites=self.max_sites,
            swarm_logger=self._instance_logger,
            delay_first_cycle=self.delay_first_cycle,
            topic_prefix=self.topic_prefix,
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
        max_sites: int = 0,
        swarm_logger: logging.Logger | None = None,
        delay_first_cycle: bool = False,
        topic_prefix: str | None = None,
    ):
        """Initialises a list of SensorSites.

        Args:
            site_ids: A list of site ID strings.
            sleep_time: Length of time to sleep after sending data in seconds.
            max_cycles: Maximum number of data sending cycles.
            max_sites: Maximum number of sites to initialise. Picks randomly from list if given
            swarm_logger: Passes the instance logger to sites
            delay_first_cycle: Adds a random delay to first invocation from 0 - `sleep_time`.
            topic_prefix: Prefixes the sensor topic.

        Returns:
            List[SensorSite]: A list of sensor sites.
        """
        if max_sites > 0:
            site_ids = CosmosSwarm._random_list_items(site_ids, max_sites)

        return [
            SensorSite(
                site_id,
                sleep_time=sleep_time,
                max_cycles=max_cycles,
                inherit_logger=swarm_logger,
                delay_first_cycle=delay_first_cycle,
                topic_prefix=topic_prefix,
            )
            for site_id in site_ids
        ]

    @staticmethod
    async def _get_sites_from_db(oracle: Oracle, query: CosmosSiteQuery) -> str:
        """Returns a list of site IDs from a database Query

        Args:
            oracle: An Oracle database connection.
            query: A site ID query.

        Returns:
            List[str]: A list of site ID strings.
        """

        return await oracle.query_site_ids(query)

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
