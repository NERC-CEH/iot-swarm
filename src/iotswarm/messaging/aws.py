"""Contains communication protocols for AWS services."""

import awscrt
from awscrt import mqtt
from awsiot import mqtt_connection_builder
import awscrt.io
import json
from awscrt.exceptions import AwsCrtError
from iotswarm.messaging.core import MessagingBaseClass
from iotswarm.utils import json_serial
import backoff
import logging
import sys

logger = logging.getLogger(__name__)


class IotCoreMQTTConnection(MessagingBaseClass):
    """Handles MQTT communication to AWS IoT Core."""

    connection: awscrt.mqtt.Connection | None = None
    """A connection to the MQTT endpoint."""

    connected_flag: bool = False
    """Tracks whether connected."""

    def __init__(
        self,
        endpoint: str,
        cert_path: str,
        key_path: str,
        ca_cert_path: str,
        client_id: str,
        *args,
        port: int | None = None,
        clean_session: bool = False,
        keep_alive_secs: int = 1200,
        inherit_logger: logging.Logger | None = None,
        **kwargs,
    ) -> None:
        """Initializes the class.

        Args:
            endpoint: Address of endpoint to send data.
            cert_path: Path to certificate file.
            key_path: Path to private key registered to device.
            ca_cert_path: Path to AWS root CA file.
            client_id: Client ID assigned to device "thing". Must match policy permissions assigned to "thing" certificate in IoT Core.
            port: Port used by endpoint. Guesses correct port if not given.
            clean_session: Builds a clean MQTT session if true. Defaults to False.
            keep_alive_secs: Time to keep connection alive. Defaults to 1200.
            inherit_logger: Override for the module logger.
        """

        if not isinstance(endpoint, str):
            raise TypeError(f"`endpoint` must be a `str`, not {type(endpoint)}")

        if not isinstance(cert_path, str):
            raise TypeError(f"`cert_path` must be a `str`, not {type(cert_path)}")

        if not isinstance(key_path, str):
            raise TypeError(f"`key_path` must be a `str`, not {type(key_path)}")

        if not isinstance(ca_cert_path, str):
            raise TypeError(f"`ca_cert_path` must be a `str`, not {type(ca_cert_path)}")

        if not isinstance(client_id, str):
            raise TypeError(f"`client_id` must be a `str`, not {type(client_id)}")

        if not isinstance(clean_session, bool):
            raise TypeError(
                f"`clean_session` must be a bool, not {type(clean_session)}."
            )

        tls_ctx_options = awscrt.io.TlsContextOptions.create_client_with_mtls_from_path(
            cert_path, key_path
        )

        tls_ctx_options.override_default_trust_store_from_path(ca_cert_path)

        if port is None:
            if awscrt.io.is_alpn_available():
                port = 443
                tls_ctx_options.alpn_list = ["x-amzn-mqtt-ca"]
            else:
                port = 8883
        else:
            port = int(port)

            if port < 0:
                raise ValueError(f"`port` cannot be less than 0. Received: {port}.")

        self.connection = mqtt_connection_builder.mtls_from_path(
            endpoint=endpoint,
            port=port,
            cert_filepath=cert_path,
            pri_key_filepath=key_path,
            ca_filepath=ca_cert_path,
            on_connection_interrupted=self._on_connection_interrupted,
            on_connection_resumed=self._on_connection_resumed,
            client_id=client_id,
            proxy_options=None,
            clean_session=clean_session,
            keep_alive_secs=keep_alive_secs,
            on_connection_success=self._on_connection_success,
            on_connection_failure=self._on_connection_failure,
            on_connection_closed=self._on_connection_closed,
        )

        if inherit_logger is not None:
            self._instance_logger = inherit_logger.getChild(
                f"{self.__class__.__name__}.client-{client_id}"
            )
        else:
            self._instance_logger = logger.getChild(
                f"{self.__class__.__name__}.client-{client_id}"
            )

    def _on_connection_interrupted(
        self, connection, error, **kwargs
    ):  # pragma: no cover
        """Callback when connection accidentally lost."""
        self._instance_logger.debug("Connection interrupted. error: {}".format(error))

        self.connected_flag = False

    def _on_connection_resumed(
        self, connection, return_code, session_present, **kwargs
    ):  # pragma: no cover
        """Callback when an interrupted connection is re-established."""

        self._instance_logger.debug(
            "Connection resumed. return_code: {} session_present: {}".format(
                return_code, session_present
            )
        )

        self.connected_flag = True

    def _on_connection_success(self, connection, callback_data):  # pragma: no cover
        """Callback when the connection successfully connects."""

        assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
        self._instance_logger.debug(
            "Connection Successful with return code: {} session present: {}".format(
                callback_data.return_code, callback_data.session_present
            )
        )

        self.connected_flag = True

    def _on_connection_failure(self, connection, callback_data):  # pragma: no cover
        """Callback when a connection attempt fails."""

        assert isinstance(callback_data, mqtt.OnConnectionFailureData)
        self._instance_logger.debug(
            "Connection failed with error code: {}".format(callback_data.error)
        )

    def _on_connection_closed(self, connection, callback_data):  # pragma: no cover
        """Callback when a connection has been disconnected or shutdown successfully"""
        self._instance_logger.debug("Connection closed")
        self.connected_flag = False

    @backoff.on_exception(backoff.expo, exception=AwsCrtError, logger=logger)
    def _connect(self):  # pragma: no cover
        self._instance_logger.debug("Connecting to endpoint")
        connect_future = self.connection.connect()
        connect_future.result()

    @backoff.on_exception(backoff.expo, exception=AwsCrtError, logger=logger)
    def _disconnect(self):  # pragma: no cover
        self._instance_logger.debug("Disconnecting from endpoint")
        disconnect_future = self.connection.disconnect()
        disconnect_future.result()

    def send_message(self, message: dict, topic: str) -> None:
        """Sends a message to the endpoint.

        Args:
            message: The message to send.
            topic: MQTT topic to send message under.
        """
        if not message:
            self._instance_logger.error(f'No message to send for topic: "{topic}".')
            return

        if self.connected_flag == False:
            self._connect()

        if message:  # pragma: no cover
            payload = json.dumps(message, default=json_serial)
            self.connection.publish(
                topic=topic,
                payload=payload,
                qos=mqtt.QoS.AT_LEAST_ONCE,
            )

        self._instance_logger.debug(f'Sent {sys.getsizeof(payload)} bytes to "{topic}"')
