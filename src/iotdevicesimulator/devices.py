"""This module hold logic for device implementation. Currently only a single device time implemented."""

import asyncio
import logging
from iotdevicesimulator.queries import CosmosQuery
from iotdevicesimulator.db import BaseDatabase, Oracle
from iotdevicesimulator.messaging.core import MessagingBaseClass
from iotdevicesimulator.messaging.aws import IotCoreMQTTConnection
import random
import abc

logger = logging.getLogger(__name__)


class BaseDevice(abc.ABC):
    """Base class for sensing devices."""

    device_type: str = "base-device"
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

    query: CosmosQuery
    """SQL query sent to database if Oracle type selected as `data_source`."""

    mqtt_base_topic: str
    """Base topic for mqtt topic."""

    mqtt_prefix: str
    """Prefix added to mqtt message."""

    mqtt_suffix: str
    """Suffix added to mqtt message."""

    @property
    def mqtt_topic(self) -> str:
        "Builds the mqtt topic."
        topic = self._mqtt_topic
        if hasattr(self, "mqtt_prefix"):
            topic = f"{self.mqtt_prefix}/{topic}"

        if hasattr(self, "mqtt_suffix"):
            topic = f"{topic}/{self.mqtt_suffix}"

        return topic

    @mqtt_topic.setter
    def mqtt_topic(self, value):
        """Sets the mqtt topic"""
        self._mqtt_topic = value
        self.mqtt_base_topic = value

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
        query: CosmosQuery | None = None,
        mqtt_topic: str | None = None,
        mqtt_prefix: str | None = None,
        mqtt_suffix: str | None = None,
    ) -> None:
        """Initializer

        Args:
            device_id: ID of site.
            data_source: Source of data to retrieve
            connection: Connection used to send messages.
            sleep_time: Time to sleep between requests (seconds).
            max_cycles: Maximum number of cycles before shutdown.
            delay_start: Adds a random delay to first invocation from 0 - `sleep_time`.
            inherit_logger: Override for the module logger.
            query: Sets the query used in COSMOS database. Ignored if `data_source` is not a Cosmos object.
            mqtt_prefix: Prefixes the MQTT topic if MQTT messaging used.
            mqtt_suffix: Suffixes the MQTT topic if MQTT messaging used.
        """

        self.device_id = str(device_id)

        if not isinstance(data_source, BaseDatabase):
            raise TypeError(
                f"`data_source` must be a `BaseDatabase`. Received: {data_source}."
            )
        if isinstance(data_source, Oracle) and query is None:
            raise ValueError(
                f"`query` must be provided if `data_source` is type `Oracle`."
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
                    f"`delay_start` must be a bool. Received: {type(delay_start)}."
                )
            self.delay_start = delay_start

        if query is not None and isinstance(self.data_source, Oracle):
            if not isinstance(query, CosmosQuery):
                raise TypeError(
                    f"`query` must be a `CosmosQuery`. Received: {type(query)}."
                )
            self.query = query

        if isinstance(connection, IotCoreMQTTConnection):
            if mqtt_topic is not None:
                self.mqtt_topic = str(mqtt_topic)
            else:
                self.mqtt_topic = f"{self.device_type}/{self.device_id}"

            if mqtt_prefix is not None:
                self.mqtt_prefix = str(mqtt_prefix)
            if mqtt_suffix is not None:
                self.mqtt_suffix = str(mqtt_suffix)

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
        query_arg = (
            f", query={self.query.__class__.__name__}.{self.query.name}"
            if isinstance(self.data_source, Oracle)
            else ""
        )

        mqtt_topic_arg = (
            f', mqtt_topic="{self.mqtt_base_topic}"'
            if hasattr(self, "mqtt_base_topic")
            and self.mqtt_base_topic != f"{self.device_type}/{self.device_id}"
            else ""
        )

        mqtt_prefix_arg = (
            f', mqtt_prefix="{self.mqtt_prefix}"'
            if hasattr(self, "mqtt_prefix")
            else ""
        )

        mqtt_suffix_arg = (
            f', mqtt_suffix="{self.mqtt_suffix}"'
            if hasattr(self, "mqtt_suffix")
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
            f"{query_arg}"
            f"{mqtt_topic_arg}"
            f"{mqtt_prefix_arg}"
            f"{mqtt_suffix_arg}"
            f")"
        )

    async def _add_delay(self):
        delay = random.randint(0, self.sleep_time)
        self._instance_logger.debug(f"Delaying first cycle for: {delay}s.")
        await asyncio.sleep(delay)

    def _send_payload(self, payload: dict):

        if isinstance(self.connection, IotCoreMQTTConnection):
            self.connection.send_message(payload, topic=self.mqtt_topic)
        else:
            self.connection.send_message(payload)

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
            payload = self._format_payload(payload)

            if payload:
                self._instance_logger.debug("Requesting payload submission.")
                self._send_payload(payload)
            else:
                self._instance_logger.warning(f"No data found.")

            self.cycle += 1
            if self.max_cycles > 0 and self.cycle >= self.max_cycles:
                break

            await asyncio.sleep(self.sleep_time)

    async def _get_payload(self):
        """Method for grabbing the payload to send"""
        if isinstance(self.data_source, Oracle):
            return await self.data_source.query_latest_from_site(
                self.device_id, self.query
            )

        elif isinstance(self.data_source, BaseDatabase):
            return self.data_source.query_latest_from_site()

    def _format_payload(self, payload):
        """Oranises payload into correct structure."""
        return payload


class CR1000XDevice(BaseDevice):
    "Represents a CR1000X datalogger."

    device_type = "cr1000x"

    def _format_payload(self, payload: dict):
        """Formats the payload into datalogger method."""

        f_payload = dict()

        f_payload["head"] = {
            "transaction": 0,
            "signature": 111111,
            "environment": {
                "station_name": self.device_id,
                "table_name": "no table",
                "model": self.device_type,
                "os_version": "Not a real OS",
                "prog_name": "test",
            },
        }

        if isinstance(payload, dict):
            time = payload["DATE_TIME"]

            payload.pop("DATE_TIME")

            f_payload["data"] = {"time": time, "vals": list(payload.values())}
            f_payload["fields"] = [{"name": key} for key in payload.keys()]
        elif hasattr(payload, "__iter__"):
            f_payload["fields"] = ["_" + i for i in range(len(payload))]
            f_payload["data"] = {"vals": payload}

        return f_payload
