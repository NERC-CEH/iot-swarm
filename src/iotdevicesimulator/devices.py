"""This module hold logic for device implementation. Currently only a single device time implemented."""
import asyncio
import logging
from iotdevicesimulator.queries import CosmosQuery
from iotdevicesimulator.db import Oracle
from iotdevicesimulator.messaging.aws import IotCoreMQTTConnection
import random

logger = logging.getLogger(__name__)

class SensorSite:
    """Digital representation of a site used in FDRI
    
    Args:
        site_id: ID of site.
        sleep_time: Time to sleep between requests (seconds).
        max_cycles: Maximum number of cycles before shutdown.
        inherit_logger: Override for the module logger.
        delay_start: Adds a random delay to first invocation from 0 - `sleep_time`.
        topic_prefix: Prefixes the sensor topic.
    """

    cycle: int = 0
    """Current cycle."""

    max_cycles: int = 0
    """Maximum number of data transfer cycles before shutting down."""

    sleep_time: int = 60
    """Time to sleep for each time data is sent."""

    site_id: str
    """ID of the site."""

    delay_start: bool = False
    """Adds a random delay to first invocation from 0 - `sleep_time`."""

    _instance_logger: logging.Logger
    """Logger used by the instance."""

    topic_prefix: str = None
    """Added as prefix to topic string."""

    @property
    def topic(self):
        """MQTT message topic."""
        return self._topic
    
    @topic.setter
    def topic(self, query):
        """Gets the topic"""
        _topic = f"fdri/cosmos_site/{self.site_id}/{query}"

        if self.topic_prefix is not None:
            _topic = f"{self.topic_prefix}/{_topic}"

        self._topic = _topic
    
    def __init__(self, site_id: str,*, sleep_time: int|None=None,
                 max_cycles: int|None=None, inherit_logger:logging.Logger|None=None,
                 delay_start:bool|None=None, topic_prefix: str|None=None) -> None:
        
        self.site_id = str(site_id)
        
        if inherit_logger is not None:
            self._instance_logger = inherit_logger.getChild(f"site-{self.site_id}")
        else:
            self._instance_logger = logger.getChild(self.site_id)

        if max_cycles is not None:
            max_cycles = int(max_cycles)
            if max_cycles < 0:
                raise ValueError(f"`max_cycles` must be 1 or more, or 0 for no maximum. Received: {max_cycles}")
            
            self.max_cycles = max_cycles
        
        if sleep_time is not None:
            sleep_time = int(sleep_time)
            if sleep_time < 0:
                raise ValueError(f"`sleep_time` must be 0 or more. Received: {sleep_time}")
            
            self.sleep_time = sleep_time

        if delay_start is not None:
            if not isinstance(delay_start, bool):
                raise TypeError(
                    f"`delay_start` must be a bool. Received: {delay_start}."
                )

            self.delay_start = delay_start

        if topic_prefix is not None:
            self.topic_prefix = str(topic_prefix)

        self._instance_logger.info(f"Initialised Site: {repr(self)}")

    def __repr__(self):
        return f"SensorSite(\"{self.site_id}\", sleep_time={self.sleep_time}, max_cycles={self.max_cycles})"

    def __str__(self):
        return f"Site ID: \"{self.site_id}\", Sleep Time: {self.sleep_time}, Max Cycles: {self.max_cycles}, Cycle: {self.cycle}"

    async def run(self, oracle: Oracle, query: CosmosQuery,
                  message_connection: IotCoreMQTTConnection):
        """The main invocation of the method. Expects a Oracle object to do work on
        and a query to retrieve. Runs asynchronously until `max_cycles` is reached.
        
        Args:
            oracle: The oracle database.
            query: Query to process by database.
        """

        self.topic = query.name
        while True:

            if self.delay_start and self.cycle == 0:
                delay = random.randint(0, self.sleep_time)
                self._instance_logger.debug(f"Delaying first cycle for: {delay}s")
                await asyncio.sleep(delay)

            row = await oracle.query_latest_from_site(self.site_id, query)

            if not row:
                self._instance_logger.warn(f"No data found.")
            else:
                self._instance_logger.debug(f"Cycle {self.cycle+1}/{self.max_cycles} Read data from: {row["DATE_TIME"]}")
                message_connection.send_message(str(row), self.topic)
                self._instance_logger.info(f"Sent message to: {self.topic}")
            
            self.cycle += 1
            if self.max_cycles > 0 and self.cycle >= self.max_cycles:
                break

            await asyncio.sleep(self.sleep_time)
