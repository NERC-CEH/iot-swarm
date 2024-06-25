"""This package is for holding classes relevant to managing swarm sessions.
It should allow the state of a swarm to be restored from the point of failure"""

from iotswarm.swarm import Swarm
import uuid


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


class SessionWriter:
    """Handles writing of the session state to file."""


class SessionLoader:
    """Loads a session, instantiates the swarm and devices."""
