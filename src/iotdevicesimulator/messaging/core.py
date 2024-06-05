from abc import ABC
from abc import abstractmethod
import logging

logger = logging.getLogger(__name__)


class MessagingBaseClass(ABC):
    """MessagingBaseClass Base class for messaging implementation

    All messaging classes implement this interface.
    """

    @property
    @abstractmethod
    def connection(self):
        """A property for the connection object where messages are sent."""

    @abstractmethod
    def send_message(self):
        """Method for sending the message."""


class MockMessageConnection(MessagingBaseClass):
    """Mock implementation of base class. Consumes `send_message` calls but does no work."""

    connection: None = None
    """Connection object. Not needed in a mock but must be implemented"""

    @staticmethod
    def send_message(*args, use_logger: logging.Logger | None, **kwargs):
        """Consumes requests to send a message but does nothing with it.

        Args:
            use_logger: Sends log message with requested logger."""

        if use_logger is not None and isinstance(use_logger, logging.Logger):
            logger = use_logger

        logger.info("Ate a message.")
