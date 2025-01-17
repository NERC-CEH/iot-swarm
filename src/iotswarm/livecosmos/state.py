"""This module is for tracking the state of file uploads"""

import logging
import pickle
from datetime import datetime
from os import PathLike
from pathlib import Path
from typing import List, TypedDict

from platformdirs import user_state_dir

logger = logging.getLogger(__name__)


class FileStatus(TypedDict):
    """Dictionary for the file status"""

    missing: bool
    corrupted: bool


class Sites(TypedDict):
    """Dictionary for the sites model"""

    site_id: str
    last_data: datetime


class State(TypedDict):
    """Dictionary for the state model"""

    last_run: datetime
    table: str
    sites: List[Sites]


class StateTracker:
    """Uses to write the upload state to file"""

    _file: PathLike
    """The target file for writing"""

    _backup: PathLike
    """The backup file"""

    state: dict
    """The current state"""

    def __init__(self, file: str):
        self._file = Path(user_state_dir()) / "iotswarm" / "livecosmos" / f"{file}.pickle"
        self._backup = self._file / ".backup"
        self.state = self.load_state()

    def write_state(self) -> None:
        """Writes the current state to file and backup file"""

        for file in [self._file, self._backup]:
            with open(file, "wb") as f:
                pickle.dump(self.state, f)

            logger.debug(f"Wrote state to file: {file}")

    def load_state(self) -> dict:
        """Loads the state from the main file or the backup"""

        file_status = FileStatus(missing=False, corrupted=False)

        try:
            logger.debug(f"Loading main state file: {self._file}")
            with open(self._file, "rb") as file:
                return pickle.load(file)
        except FileNotFoundError:
            logger.error(f"State not found at {self._file}")
            file_status["missing"] = True
        except EOFError:
            logger.error(f"State file is corrupted: {self._file}")
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
            logger.error(f"State not found at {self._backup}")
            backup_status["missing"] = True
        except EOFError:
            logger.error(f"State file is corrupted: {self._backup}")
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
        return dict()
