"""This package is for holding classes relevant to managing swarm sessions.
It should allow the state of a swarm to be restored from the point of failure"""

from iotswarm.swarm import Swarm
from iotswarm.db import LoopingCsvDB
import uuid
from pathlib import Path
from platformdirs import user_data_dir
import os
import pickle
from typing import List


class Session:
    """Represents the current session. Holds configuration necessary to the
    SessionWriter."""

    swarm: Swarm
    """The swarm object in use."""

    session_id: str
    """Unique ID for the session"""

    def __init__(self, swarm: Swarm, session_id: str | None = None):
        """Initialises the class.

        Args:
            swarm: A Swarm object to track the state of.
            session_id: A unique identifier of the session. Automatically assigned if not provided.
        """

        if not isinstance(swarm, Swarm):
            raise TypeError(f'"swarm" must be a Swarm, not "{type(swarm)}".')

        self.swarm = swarm

        if session_id is not None:
            self.session_id = str(session_id)
        else:
            self.session_id = self._build_session_id(swarm.name)

    def __repr__(self):

        return f'{self.__class__.__name__}({self.swarm}, "{self.session_id}")'

    def __str__(self):

        return f'{self.__class__.__name__}: "{self.session_id}"'

    def __eq__(self, obj) -> bool:
        return self.swarm == obj.swarm and self.session_id == obj.session_id

    @staticmethod
    def _build_session_id(prefix: str | None = None):
        """Builds a session ID with a prefix if requested.

        Args:
            prefix: Adds a prefix to the ID for readability.

        Returns:
            str: A session ID string.
        """

        session_id = str(uuid.uuid4())

        if prefix is not None:
            session_id = f"{prefix}-{session_id}"

        return session_id

    @staticmethod
    def list_sessions() -> List[str]:
        """Returns a list of stored sessions."""

        files = os.listdir(Path(user_data_dir("iot_swarm"), "sessions"))

        files = [file for file in files if file.endswith(".pkl")]


class SessionWriter:
    """Handles writing of the session state to file."""

    session: Session
    """The session to write"""

    session_file: Path
    """File path to the session file"""

    def __init__(self, session: Session):
        """Initializes the class.

        Args:
            session: The session to track.
        """

        self.session = session

        self.session_file = Path(
            user_data_dir("iot_swarm"), "sessions", session.session_id + ".pkl"
        )

    def _write_state(self, replace: bool = False):
        """Creates the session file if not already existing"""

        if self.session_file.exists():
            if replace:
                os.remove(self.session_file)
            else:
                raise FileExistsError(
                    f'Session exists and replace is set to False: "{self.session_file}".'
                )

        with open(self.session_file, "wb") as file:
            pickle.dump(self.session, file)

    def _destroy_session(self):
        """Destroys a session file."""

        if self.session_file.exists():
            os.remove(self.session_file)


class SessionLoader:
    """Loads a session, instantiates the swarm and devices."""

    @staticmethod
    def _load_session(session_id: str) -> Session:
        """Loads a session from pickle file."""
        session_file = Path(user_data_dir("iot_swarm"), "sessions", session_id + ".pkl")

        if not session_file.exists():
            raise FileNotFoundError(f'Session not found: "{session_id}".')
        with open(session_file, "wb") as file:
            session = pickle.load(file)

        return session
