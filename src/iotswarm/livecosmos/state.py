"""This module is for tracking the state of file uploads"""

import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, TypedDict

from platformdirs import user_state_dir

from iotswarm.livecosmos.loggers import get_logger

logger = get_logger(__name__)


class FileStatus(TypedDict):
    """Dictionary for the file status"""

    missing: bool
    corrupted: bool


class Site(TypedDict):
    """Dictionary for the sites model"""

    site_id: str
    last_data: datetime


class State(TypedDict):
    """Dictionary for the state model"""

    last_run: Optional[datetime]
    sites: Dict[str, Site]


class StateTracker:
    """Uses to write the upload state to file"""

    _file: Path
    """The target file for writing"""

    _backup: Path
    """The backup file"""

    state: State
    """The current state"""

    def __init__(self, file: str, app_name: str = "livecosmos"):
        """Initialize the class

        Args:
            file: Name of key to name the file to be appended to the app directory
            app_name: Name of the directory files are placed in
        """
        self._file = Path(user_state_dir(app_name)) / f"{file}.pickle"
        self._backup = Path(f"{self._file}.backup")
        self.state = self.load_state()

    def write_state(self) -> None:
        """Writes the current state to file and backup file"""

        if not self._file.parent.exists():
            os.makedirs(self._file.parent)

        for file in [self._file, self._backup]:
            with open(file, "wb") as f:
                pickle.dump(self.state, f)

            logger.debug(f"Wrote state to file: {file}")

    def load_state(self) -> State:
        """Loads the state from the main file or the backup"""

        file_status = FileStatus(missing=False, corrupted=False)

        try:
            logger.info(f"Loading main state file: {self._file}")
            with open(self._file, "rb") as file:
                return pickle.load(file)
        except FileNotFoundError:
            logger.warning(f"State not found at {self._file}")
            file_status["missing"] = True
        except EOFError:
            logger.warning(f"State file is corrupted: {self._file}")
            file_status["corrupted"] = True

        backup_status = FileStatus(missing=False, corrupted=False)

        try:
            logger.debug(f"Loading backup state file: {self._backup}")
            with open(self._backup, "rb") as file:
                state = pickle.load(file)
            with open(self._file, "wb") as file:
                pickle.dump(state, file)
            logger.warning(f"Rescued state file: {self._file} with backup")
            return state
        except FileNotFoundError:
            logger.warning(f"State not found at {self._backup}")
            backup_status["missing"] = True
        except EOFError:
            logger.warning(f"State file is corrupted: {self._backup}")
            backup_status["corrupted"] = True

        if file_status["missing"] and backup_status["corrupted"]:
            corruption_message = f"Main state file is missing and backup is corrupted. Can't continue for {self._file}"
            logger.critical(corruption_message)
            raise RuntimeError(corruption_message)

        if (file_status["missing"] or file_status["corrupted"]) and backup_status["corrupted"]:
            corruption_message = f"All state files are corrupted. Cannot continue for state {self._file}"
            logger.critical(corruption_message)
            raise RuntimeError(corruption_message)

        logger.warning("No state files found")
        return {"last_run": None, "sites": {}}

    def update_state(self, site: Site) -> bool:
        """Updates the state with a new or existing site.

        Args:
            site: The site to update
        Returns:
            True if the state has changed, False otherwise
        """
        _changed = False

        if not self.state["last_run"] or self.state["last_run"] < site["last_data"]:
            self.state["last_run"] = site["last_data"]
            _changed = True

        if (
            site["site_id"] not in self.state["sites"]
            or site["last_data"] > self.state["sites"][site["site_id"]]["last_data"]
        ):
            self.state["sites"][site["site_id"]] = site
            _changed = True

        return _changed
