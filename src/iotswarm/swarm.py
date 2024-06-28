"""This is the core module for orchestrating swarms of IoT devices. One swarm defined currently for using COSMOS data."""

from iotswarm.devices import BaseDevice
import logging.config
from typing import List
import asyncio
import uuid

logger = logging.getLogger(__name__)


class Swarm:
    """Manages a swarm of IoT devices and runs the main loop
    of all devices. Can receive any number or combination of devices.
    """

    name: str
    """Name of swarm applied in logs."""

    _instance_logger: logging.Logger
    """Logger handle used by instance."""

    devices: List[BaseDevice]
    """List of site objects."""

    def __eq__(self, obj) -> bool:
        return (
            self.name == obj.name
            and self._instance_logger == obj._instance_logger
            and self.devices == obj.devices
        )

    def __len__(self):
        """Returns number of sites"""
        return len(self.devices)

    def __repr__(self):
        name_arg = (
            f', name="{self.name}"'
            if not self.name.startswith("unnamed-swarm-")
            else ""
        )
        devices_arg = (
            self.devices[0].__repr__()
            if len(self.devices) == 1
            else self.devices.__repr__()
        )
        return f"{self.__class__.__name__}({devices_arg}{name_arg})"

    def __init__(
        self,
        devices: List[BaseDevice],
        name: str | None = None,
    ) -> None:
        """Initializes the class.

        Args:
            devices: A list of devices to swarmify.
            name: Name / ID given to swarm.
        """

        if not hasattr(devices, "__iter__"):
            devices = [devices]

        if not all([isinstance(device, BaseDevice) for device in devices]):
            raise TypeError(
                f"`devices` must be an iterable containing `BaseDevice`. not {type(devices)}"
            )

        self.devices = devices

        if name is not None:
            self.name = str(name)
        else:
            self.name = f"unnamed-swarm-{uuid.uuid4()}"

        self._instance_logger = logger.getChild(
            f"{self.__class__.__name__}.{self.name}"
        )

    async def run(self) -> None:
        """Main function for running the swarm. Sends the query
        and message connection object. Runs until all sites reach
        their maximum cycle. If any site has no maximum, it runs forever.
        """

        self._instance_logger.info("Running main loop.")
        await asyncio.gather(*[device.run() for device in self.devices])

        self._instance_logger.info("Terminated.")
