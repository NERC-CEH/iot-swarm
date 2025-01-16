import logging
from abc import ABC, abstractmethod
from typing import Optional


class MessagingBaseClass(ABC):
    """MessagingBaseClass Base class for messaging implementation

    All messaging classes implement this interface.
    """

    _instance_logger: logging.Logger
    """Logger handle used by instance."""

    connection: Optional[object] = None
    """The connection object that is used to send data"""

    def __eq__(self, obj: object) -> bool:
        """Checks for equality"""
        if not isinstance(obj, MessagingBaseClass):
            raise NotImplementedError
        return self._instance_logger == obj._instance_logger and self.connection == obj.connection

    def __init__(
        self,
        inherit_logger: logging.Logger | None = None,
    ) -> None:
        """Initialises the class.
        Args:
            inherit_logger: Override for the module logger.
        """
        if inherit_logger is not None:
            self._instance_logger = inherit_logger.getChild(self.__class__.__name__)
        else:
            self._instance_logger = logging.getLogger(__name__).getChild(self.__class__.__name__)

    @abstractmethod
    def send_message(self, *args, **kwargs) -> bool:
        """Method for sending the message."""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


class MockMessageConnection(MessagingBaseClass):
    """Mock implementation of base class. Consumes `send_message` calls but does no work."""

    def send_message(self, *_) -> bool:
        """Consumes requests to send a message but does nothing with it.

        Returns:
            bool: True if sent sucessfully, else false.
        """

        self._instance_logger.debug("Message was sent.")

        return True
