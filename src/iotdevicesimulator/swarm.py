"""This is the core module for orchestrating swarms of IoT devices. One swarm defined currently for using COSMOS data."""

from iotdevicesimulator.devices import BaseDevice
import logging.config
from typing import List
import asyncio
import uuid

logger = logging.getLogger(__name__)


class Swarm:
    """Object for creating a swarm of COSMOS site devices.
    This object instantiates a group of sensor devices that submit data from the
    COSMOS database and then wait for a specified time. When run unrestricted, this
    can simulate the full COSMOS network in real time.
    """

    name: str
    """Name of swarm applied in logs."""

    _instance_logger: logging.Logger
    """Logger handle used by instance."""

    devices: List[BaseDevice]
    """List of site objects."""

    def __len__(self):
        """Returns number of sites"""
        return len(self.devices)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.devices}, name={self.name})"

    def __str__(self):
        topic_prefix = (
            "" if self.topic_prefix is None else f', topic_prefix="{self.topic_prefix}"'
        )
        return (
            f'CosmosSwarm({self.query.__class__.__name__}.{self.query.name}, name="{self.name}"'
            f"{topic_prefix}, sleep_time={self.sleep_time}, max_cycles={self.max_cycles}, delay_start={self.delay_start})"
        )

    def __init__(
        self,
        devices: List[BaseDevice],
        name: str | None = None,
    ) -> None:
        """Factory method for initialising the class.

        Args:
            devices: A list of devices to swarmify.
            name: Name / ID given to swarm.
        """

        if not hasattr(devices, "__iter__"):
            devices = set(devices)

        if not all([isinstance(device, BaseDevice) for device in devices]):
            raise TypeError(
                f"`devices` must be an iterable containing `BaseDevice`. not {type(devices)}"
            )

        self.devices = devices

        if name is not None:
            self.name = str(name)
        else:
            self.name = f"swarm-{uuid.uuid4()}"

        self._instance_logger = logger.getChild(self.name)

    async def run(self) -> None:
        """Main function for running the swarm. Sends the query
        and message connection object. Runs until all sites reach
        their maximum cycle. If no maximum, it runs forever.
        """

        self._instance_logger.info(f"Running main loop: swarm-{self.name}")
        await asyncio.gather(*[device.run() for device in self.devices])

        self._instance_logger.info(f"Terminated: swarm-{self.name}")
