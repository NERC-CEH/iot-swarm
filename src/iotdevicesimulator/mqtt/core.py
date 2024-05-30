from abc import ABC
from abc import abstractmethod


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
    def send_message(*args, **kwargs):
        """Consumes requests to send a message but does nothing with it."""
        pass
