"""This is the core module for orchestrating swarms of IoT devices. One swarm defined currently for using COSMOS data."""

from iotswarm.devices import BaseDevice
import logging.config
from typing import List, Self
import asyncio
import uuid
import dill
from pathlib import Path
from platformdirs import user_data_dir
import os

logger = logging.getLogger(__name__)


class Swarm:
    """Manages a swarm of IoT devices and runs the main loop
    of all devices. Can receive any number or combination of devices.
    """

    base_directory: Path = Path(user_data_dir("iot_swarm"), "swarms")
    """The base directory where swarms are stored."""

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
        base_directory: str | Path | None = None,
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
        for device in self.devices:
            device._attach_swarm(self)

        if name is not None:
            self.name = str(name)
        else:
            self.name = f"unnamed-swarm-{uuid.uuid4()}"

        self._instance_logger = logger.getChild(
            f"{self.__class__.__name__}.{self.name}"
        )

        if base_directory is not None:
            if not isinstance(base_directory, Path):
                base_directory = Path(base_directory)
            self.base_directory = base_directory

    async def run(self) -> None:
        """Main function for running the swarm. Sends the query
        and message connection object. Runs until all sites reach
        their maximum cycle. If any site has no maximum, it runs forever.
        """

        self._instance_logger.info("Running main loop.")
        await asyncio.gather(*[device.run() for device in self.devices])

        self._instance_logger.info("Terminated.")

    @classmethod
    def _get_swarm_file(cls, swarm: object | str) -> Path:
        """Returns a full path to the swarm file.

        Args:
            swarm The swarm to build the path from.
            Assumed to be a swarm ID if str provided.
        Returns:
            Path: A path object to the file.
        """

        if isinstance(swarm, str):
            return Path(cls.base_directory, swarm + ".pkl")
        elif isinstance(swarm, Swarm):
            return Path(swarm.base_directory, swarm.name + ".pkl")
        else:
            raise TypeError(f'`swarm` must be a Swarm, not "{type(swarm)}".')

    @classmethod
    def _initialise_swarm_file(cls, swarm: object | str) -> None:
        """Writes an empty swarm file.

        Args:
            swarm The swarm. May be a swarm or a swarm ID.
        """

        swarm_file = cls._get_swarm_file(swarm)

        if not swarm_file.parent.exists():
            os.makedirs(swarm_file.parent)

        with open(swarm_file, "wb") as file:
            dill.dump("", file)

    @classmethod
    def _swarm_exists(cls, swarm: object | str) -> bool:
        """Returns true if swarm exists.

        Args:
            swarm: The swarm to check.
        Returns:
            bool: True if swarm exists.
        """
        return Swarm._get_swarm_file(swarm).exists()

    @classmethod
    def _write_swarm(cls, swarm: object, replace: bool = False) -> None:
        swarm_file = cls._get_swarm_file(swarm)

        if swarm_file.exists():
            if replace:
                os.remove(swarm_file)
            else:
                raise FileExistsError(
                    f'swarm exists and replace is set to False: "{swarm_file}".'
                )
        elif not swarm_file.parent.exists():
            os.makedirs(swarm_file.parent)

        with open(swarm_file, "wb") as file:
            dill.dump(swarm, file)

    def write_self(self, replace: bool = False) -> None:
        """Writes the swarm state to file.

        Args:
            replace: When True it replaces the swarm. Execption is
            raised if the file exists and replace is False.
        """

        self._write_swarm(self, replace=replace)

    @classmethod
    def destroy_swarm(cls, swarm: object) -> None:
        """Destroys a swarm file."""

        swarm_file = cls._get_swarm_file(swarm)
        if swarm_file.exists():
            os.remove(swarm_file)

    @staticmethod
    def _list_swarms_with_directory(base_directory: str | Path) -> List[str]:
        """Returns a list of stored swarms."""

        if not base_directory.exists():
            return []

        files = os.listdir(base_directory)

        files = [file.removesuffix(".pkl") for file in files if file.endswith(".pkl")]

        return files

    @classmethod
    def _list_swarms(cls) -> List[str]:
        """Returns list of swarms from default directory"""
        return cls._list_swarms_with_directory(cls.base_directory)

    def list_swarms(self) -> List[str]:
        """Returns list of swarms from instance directory"""
        return self._list_swarms_with_directory(self.base_directory)

    @classmethod
    def load_swarm(cls, swarm_id: str) -> Self:
        """Loads a swarm from dill file."""
        swarm_file = cls._get_swarm_file(swarm_id)

        if not swarm_file.exists():
            raise FileNotFoundError(f'swarm not found: "{swarm_id}".')
        with open(swarm_file, "rb") as file:
            swarm = dill.load(file)

        return swarm
