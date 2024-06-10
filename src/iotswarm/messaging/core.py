from abc import ABC
from abc import abstractmethod
import logging

logger = logging.getLogger(__name__)


class MessagingBaseClass(ABC):
    """MessagingBaseClass Base class for messaging implementation

    All messaging classes implement this interface.
    """

    _instance_logger: logging.Logger
    """Logger handle used by instance."""

    def __init__(self):

        self._instance_logger = logger.getChild(self.__class__.__name__)

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

    def send_message(self, use_logger: logging.Logger | None = None):
        """Consumes requests to send a message but does nothing with it.

        Args:
            use_logger: Sends log message with requested logger."""

        if use_logger is not None and isinstance(use_logger, logging.Logger):
            use_logger = use_logger
        else:
            use_logger = self._instance_logger

        use_logger.info("Message was sent.")
