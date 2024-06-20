from abc import ABC
from abc import abstractmethod
import logging


class MessagingBaseClass(ABC):
    """MessagingBaseClass Base class for messaging implementation

    All messaging classes implement this interface.
    """

    _instance_logger: logging.Logger
    """Logger handle used by instance."""

    def __init__(
        self,
        inherit_logger: logging.Logger | None = None,
    ):
        """Initialises the class.
        Args:
            inherit_logger: Override for the module logger.
        """
        if inherit_logger is not None:
            self._instance_logger = inherit_logger.getChild(self.__class__.__name__)
        else:
            self._instance_logger = logging.getLogger(__name__).getChild(
                self.__class__.__name__
            )

    @property
    @abstractmethod
    def connection(self):
        """A property for the connection object where messages are sent."""

    @abstractmethod
    def send_message(self):
        """Method for sending the message."""

    def __repr__(self):
        return f"{self.__class__.__name__}()"


class MockMessageConnection(MessagingBaseClass):
    """Mock implementation of base class. Consumes `send_message` calls but does no work."""

    connection: None = None
    """Connection object. Not needed in a mock but must be implemented"""

    def send_message(self, *_):
        """Consumes requests to send a message but does nothing with it."""

        self._instance_logger.debug("Message was sent.")
