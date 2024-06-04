"""This module hold logic for device implementation. Currently only a single device time implemented."""

import asyncio
import logging
from iotdevicesimulator.queries import CosmosQuery
from iotdevicesimulator.db import BaseDatabase, Oracle
from iotdevicesimulator.messaging.aws import IotCoreMQTTConnection, MessagingBaseClass
import random
import abc

logger = logging.getLogger(__name__)


class BaseDevice(abc.ABC):
    """Base class for sensing devices."""

    device_type: str | None = None
    """Name of the device type"""

    cycle: int = 0
    """Current cycle."""

    max_cycles: int = 0
    """Maximum number of data transfer cycles before shutting down."""

    sleep_time: int = 60
    """Time to sleep for each time data is sent."""

    device_id: str
    """ID of the site."""

    delay_start: bool = False
    """Adds a random delay to first invocation from 0 - `sleep_time`."""

    _instance_logger: logging.Logger
    """Logger used by the instance."""

    topic_prefix: str = None
    """Added as prefix to topic string."""

    data_source: str = "cosmos"
    """Specifies the source of data to use."""

    @property
    def topic(self):
        """MQTT message topic."""
        return self._topic

    @topic.setter
    def topic(self, query):
        """Gets the topic"""
        _topic = f"fdri/cosmos_site/{self.sensor_type}/{self.device_id}/{query}"

        if self.topic_prefix is not None:
            _topic = f"{self.topic_prefix}/{_topic}"

        self._topic = _topic

    def __init__(
        self,
        device_id: str,
        *,
        sleep_time: int | None = None,
        max_cycles: int | None = None,
        inherit_logger: logging.Logger | None = None,
        delay_start: bool | None = None,
        topic_prefix: str | None = None,
        data_source: str | None = None,
    ) -> None:
        """Initializer

        Args:
            device_id: ID of site.
            sleep_time: Time to sleep between requests (seconds).
            max_cycles: Maximum number of cycles before shutdown.
            inherit_logger: Override for the module logger.
            delay_start: Adds a random delay to first invocation from 0 - `sleep_time`.
            topic_prefix: Prefixes the sensor topic.
            data_source: Source of data to retrieve
        """
        self.device_id = str(device_id)

        if inherit_logger is not None:
            self._instance_logger = inherit_logger.getChild(f"site-{self.device_id}")
        else:
            self._instance_logger = logger.getChild(self.device_id)

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

        if delay_start is not None:
            if not isinstance(delay_start, bool):
                raise TypeError(
                    f"`delay_start` must be a bool. Received: {delay_start}."
                )
            self.delay_start = delay_start

        if data_source is not None:
            if not isinstance(data_source, str):
                raise TypeError(
                    f"`data_source` must be a str. Received: {data_source}."
                )
            self.data_source = data_source

        if topic_prefix is not None:
            self.topic_prefix = str(topic_prefix)

        self._instance_logger.info(f"Initialised Site: {repr(self)}")

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f'"{self.device_id}"'
            f", sleep_time={self.sleep_time}"
            f", max_cycles={self.max_cycles}"
            f", delay_start={self.delay_start}"
            f", topic_prefix={self.topic_prefix}"
            f", data_source={self.data_source}"
            f")"
        )

    def __str__(self):
        return f'Site ID: "{self.device_id}", Sleep Time: {self.sleep_time}, Max Cycles: {self.max_cycles}, Cycle: {self.cycle}'

    async def _add_delay(self):
        delay = random.randint(0, self.sleep_time)
        self._instance_logger.debug(f"Delaying first cycle for: {delay}s")
        await asyncio.sleep(delay)

    def _send_payload(
        self, payload: dict, message_connection: MessagingBaseClass, *args
    ):
        if not payload:
            self._instance_logger.warning(f"No data found.")
            return

        if isinstance(message_connection, IotCoreMQTTConnection):
            message_connection.send_message(payload, self.topic)
            self._instance_logger.info(f"Sent message to: {self.topic}")
        elif isinstance(message_connection, MessagingBaseClass):
            message_connection.send_message()
            self._instance_logger.info("Ate a message")
        else:
            raise TypeError(f"Invalid messaging type: {type(message_connection)}.")

    async def run(self, message_connection: MessagingBaseClass):
        """The main invocation of the method. Expects a Oracle object to do work on
        and a query to retrieve. Runs asynchronously until `max_cycles` is reached.

        Args:
            message_connection: The message object to send data through
        """

        while True:

            if self.delay_start and self.cycle == 0:
                await self._add_delay()

            payload = await self._get_payload()

            self._send_payload(payload, message_connection)

            self.cycle += 1
            if self.max_cycles > 0 and self.cycle >= self.max_cycles:
                break

            await asyncio.sleep(self.sleep_time)

    @abc.abstractmethod
    def _get_payload(self):
        """Method for grabbing the payload to send"""

    @abc.abstractmethod
    def _format_payload(self):
        """Oranises payload into correct structure."""
        pass


class CosmosDevice(BaseDevice):
    """Basic device for devices relying on cosmos database for data"""

    device_type = "cosmos-device"

    query: CosmosQuery | None = None
    """The query used to retrieve data."""

    database: BaseDatabase
    """Database used for data source."""

    def __init__(self, query: CosmosQuery, database: BaseDatabase, *args, **kwargs):

        if not isinstance(query, CosmosQuery):
            raise TypeError(f"`query` must be a CosmosQuery, not {type(query)}")

        if not isinstance(database, BaseDatabase):
            raise TypeError(f"`database` must be a BaseDatabase, not {type(database)}")

        self.query = query
        self.database = database

        super().__init__(*args, **kwargs)

    async def _get_payload(self):
        if isinstance(self.database, Oracle):
            payload = await self.database.query_latest_from_site(
                self.device_id, self.query
            )

            return self._format_payload(payload)

        elif isinstance(self.database, BaseDatabase):
            return await self.database.query_latest_from_site()

    @staticmethod
    def _format_payload(payload: dict):
        """Base class does nothing with payload"""
        return payload

    def __repr__(self):
        parent = super().__repr__().lstrip(f"{self.__class__.__name__}(")

        return (
            f"{self.__class__.__name__}("
            f"{self.query.__class__.__name__}.{self.query.name}"
            f", {self.database.__repr__()}"
            f", {parent}"
        )


class CosmosSensorDevice(CosmosDevice):
    """Digital representation of a site used in FDRI"""

    sensor_type = "cosmos-sensor-site"

    def __init__(self, *args, **kwargs) -> None:

        super().__init__(*args, **kwargs)
