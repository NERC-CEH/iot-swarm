"""This package is for holding classes relevant to managing swarm sessions.
It should allow the state of a swarm to be restored from the point of failure"""

from iotswarm.swarm import Swarm
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


class SessionManager:
    base_directory: Path = Path(user_data_dir("iot_swarm"), "sessions")
    """The base directory where sessions are stored."""

    def __init__(self, base_directory: str | Path | None = None):
        """Initializes the class.

        Args:
            base_directory: Base directory where sessions are stored.
        """

        if base_directory is not None:
            if not isinstance(base_directory, Path):
                base_directory = Path(base_directory)
            self.base_directory = base_directory

    def _get_session_file(self, session: Session | str) -> Path:
        """Returns a full path to the session file.

        Args:
            session: The session to build the path from.
            Assumed to be a session ID if str provided.
        Returns:
            Path: A path object to the file.
        """

        if isinstance(session, str):
            return Path(self.base_directory, session + ".pkl")
        elif isinstance(session, Session):
            return Path(self.base_directory, session.session_id + ".pkl")
        else:
            raise TypeError(f'`session` must be a Session, not "{type(session)}".')

    def write_session(self, session: Session, replace: bool = False) -> None:
        """Writes the session state to file.

        Args:
            session: The Session to write.
            replace: When True it replaces the session. Execption is
            raised if the file exists and replace is False.
        """
        session_file = self._get_session_file(session)

        if session_file.exists():
            if replace:
                os.remove(session_file)
            else:
                raise FileExistsError(
                    f'Session exists and replace is set to False: "{session_file}".'
                )
        elif not session_file.parent.exists():
            os.makedirs(session_file.parent)

        with open(session_file, "wb") as file:
            pickle.dump(session, file)

    def destroy_session(self, session: Session):
        """Destroys a session file."""

        session_file = self._get_session_file(session)
        if session_file.exists():
            os.remove(session_file)

    def list_sessions(self) -> List[str]:
        """Returns a list of stored sessions."""

        files = os.listdir(self.base_directory)

        files = [file.removesuffix(".pkl") for file in files if file.endswith(".pkl")]

        return files

    def load_session(self, session_id: str) -> Session:
        """Loads a session from pickle file."""
        session_file = self._get_session_file(session_id)

        if not session_file.exists():
            raise FileNotFoundError(f'Session not found: "{session_id}".')
        with open(session_file, "rb") as file:
            session = pickle.load(file)

        return session
