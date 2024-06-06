"""This module hold logic for device implementation. Currently only a single device time implemented."""

import asyncio
import logging
from iotdevicesimulator.queries import CosmosQuery
from iotdevicesimulator.db import BaseDatabase, Oracle
from iotdevicesimulator.messaging.core import MockMessageConnection, MessagingBaseClass
from iotdevicesimulator.messaging.aws import IotCoreMQTTConnection
import random
import abc
import sys

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

    data_source: BaseDatabase
    """Specifies the source of data to use."""

    connection: MessagingBaseClass
    """Connection to the data receiver."""

    def __init__(
        self,
        device_id: str,
        data_source: BaseDatabase,
        connection: MessagingBaseClass,
        *,
        sleep_time: int | None = None,
        max_cycles: int | None = None,
        delay_start: bool | None = None,
        inherit_logger: logging.Logger | None = None,
    ) -> None:
        """Initializer

        Args:
            device_id: ID of site.
            sleep_time: Time to sleep between requests (seconds).
            data_source: Source of data to retrieve
            connection: Connection used to send messages.
            max_cycles: Maximum number of cycles before shutdown.
            delay_start: Adds a random delay to first invocation from 0 - `sleep_time`.
            inherit_logger: Override for the module logger.
        """

        self.device_id = str(device_id)

        if not isinstance(data_source, BaseDatabase):
            raise TypeError(
                f"`data_source` must be a `BaseDatabase`. Received: {data_source}."
            )
        self.data_source = data_source

        if not isinstance(connection, MessagingBaseClass):
            raise TypeError(
                f"`connection` must be a `MessagingBaseClass`. Received: {connection}."
            )
        self.connection = connection

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
                raise ValueError(f"`sleep_time` must 0 or more. Received: {sleep_time}")
            self.sleep_time = sleep_time

        if delay_start is not None:
            if not isinstance(delay_start, bool):
                raise TypeError(
                    f"`delay_start` must be a bool. Received: {delay_start}."
                )
            self.delay_start = delay_start

        if inherit_logger is not None:
            self._instance_logger = inherit_logger.getChild(
                f"{self.__class__.__name__}-{self.device_id}"
            )
        else:
            self._instance_logger = logger.getChild(
                f"{self.__class__.__name__}-{self.device_id}"
            )

        self._instance_logger.info(f"Initialised Site: {repr(self)}")

    def __repr__(self):

        sleep_time_arg = (
            f", sleep_time={self.sleep_time}"
            if self.sleep_time != self.__class__.sleep_time
            else ""
        )
        max_cycles_arg = (
            f", max_cycles={self.max_cycles}"
            if self.max_cycles != self.__class__.max_cycles
            else ""
        )
        delay_start_arg = (
            f", delay_start={self.delay_start}"
            if self.delay_start != self.__class__.delay_start
            else ""
        )
        return (
            f"{self.__class__.__name__}("
            f'"{self.device_id}"'
            f", {self.data_source}"
            f", {self.connection}"
            f"{sleep_time_arg}"
            f"{max_cycles_arg}"
            f"{delay_start_arg}"
            f")"
        )

    async def _add_delay(self):
        delay = random.randint(0, self.sleep_time)
        self._instance_logger.debug(f"Delaying first cycle for: {delay}s.")
        await asyncio.sleep(delay)

    def _send_payload(self, payload: dict, *args, **kwargs):
        self.connection.send_message(payload, *args, **kwargs)

    async def run(self):
        """The main invocation of the method. Expects a Oracle object to do work on
        and a query to retrieve. Runs asynchronously until `max_cycles` is reached.

        Args:
            message_connection: The message object to send data through
        """

        while True:

            if self.delay_start and self.cycle == 0:
                await self._add_delay()

            payload = await self._get_payload()

            if payload:
                self._instance_logger.debug("Requesting payload submission.")
                self._send_payload(payload)
            else:
                self._instance_logger.warning(f"No data found.")

            self.cycle += 1
            if self.max_cycles > 0 and self.cycle >= self.max_cycles:
                break

            await asyncio.sleep(self.sleep_time)

    @abc.abstractmethod
    async def _get_payload(self):
        """Method for grabbing the payload to send"""

    @abc.abstractmethod
    async def _format_payload(self):
        """Oranises payload into correct structure."""


class CosmosDevice(BaseDevice):
    """Basic device for devices relying on cosmos database for data"""

    device_type = "cosmos-device"

    query: CosmosQuery | None = None
    """The query used to retrieve data."""

    def __init__(self, query: CosmosQuery, *args, **kwargs):

        if not isinstance(query, CosmosQuery):
            raise TypeError(f"`query` must be a CosmosQuery, not {type(query)}")

        self.query = query

        super().__init__(*args, **kwargs)

    async def _get_payload(self):
        if isinstance(self.data_source, Oracle):
            payload = await self.data_source.query_latest_from_site(
                self.device_id, self.query
            )

            return self._format_payload(payload)

        elif isinstance(self.data_source, BaseDatabase):
            return await self.data_source.query_latest_from_site()

    @staticmethod
    def _format_payload(payload: dict):
        """Base class does nothing with payload"""
        return payload

    def __repr__(self):
        parent = super().__repr__().lstrip(f"{self.__class__.__name__}(")

        return (
            f"{self.__class__.__name__}("
            f"{self.query.__class__.__name__}.{self.query.name}"
            f", {parent}"
        )


class MQTTCosmosDevice(CosmosDevice):
    """MQTT implementation of the Cosmos DB reliant devices.

    Args:
        topic_prefix: Prefixes the MQTT topic.
        topic_suffix: Suffixes the MQTT topic.
    """

    topic: str
    """MQTT message topic"""

    def __init__(
        self,
        *args,
        topic_prefix: str | None = None,
        topic_suffix: str | None = None,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)

        self.topic = f"{self.device_type}/{self.device_id}"

        if topic_prefix is not None:
            self.topic = f"{topic_prefix}/{self.topic}"

        if topic_suffix is not None:
            self.topic = f"{self.topic}/{topic_suffix}"

    def _send_payload(self, payload: dict, *args, **kwargs):
        self.connection.send_message(payload, self.topic)
        self._instance_logger.info(
            f'Sent {sys.getsizeof(payload)} bytes to "{self.topic}"'
        )


class CR1000XCosmosDevice(CosmosDevice):
    "Represents a CR1000X datalogger."

    device_type = "CR1000X"

    def __init__(self, *args, **kwargs) -> None:

        super().__init__(*args, **kwargs)

    def _format_payload(self, payload: dict):
        """Formats the payload into datalogger method."""

        f_payload = dict()

        f_payload["head"] = {
            "transaction": 0,
            "signature": 111111,
            "environment": {
                "station_name": self.device_id,
                "table_name": self.topic_suffix,
                "model": self.device_type,
                "os_version": "Not a real OS",
                "prog_name": "test",
            },
        }

        time = payload["DATE_TIME"]

        payload.pop("DATE_TIME")

        f_payload["data"] = {"time": time, "vals": list(payload.values())}
        f_payload["fields"] = [{"name": key} for key in payload.keys()]
        return f_payload


class MQTTCR1000XCosmosDevice(MQTTCosmosDevice, CR1000XCosmosDevice):
    pass


class DeviceFactory:

    @staticmethod
    def create_device(
        connection: MessagingBaseClass,
        *args,
        device_type: str = "base",
        **kwargs,
    ):
        if not isinstance(connection, MessagingBaseClass):
            raise TypeError(f"Invalid connection: {type(connection)}")

        handles = {
            IotCoreMQTTConnection: {
                "base": MQTTCosmosDevice,
                "cr1000x": MQTTCR1000XCosmosDevice,
            },
            MockMessageConnection: {
                "base": CosmosDevice,
                "cr1000x": CR1000XCosmosDevice,
            },
        }

        return handles[type(connection)][device_type](
            *args, connection=connection, **kwargs
        )
