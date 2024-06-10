"""Contains communication protocols for AWS services."""

import awscrt
from awscrt import mqtt
import awscrt.io
import json
from awscrt.exceptions import AwsCrtError
from iotdevicesimulator.messaging.core import MessagingBaseClass
from iotdevicesimulator.utils import json_serial
import backoff
import logging
import sys

logger = logging.getLogger(__name__)


class IotCoreMQTTConnection(MessagingBaseClass):
    """Handles MQTT communication to AWS IoT Core."""

    connection: awscrt.mqtt.Connection | None = None
    """A connection to the MQTT endpoint."""

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
            topic_prefix: A topic prefixed to MQTT topic, useful for attaching a "Basic Ingest" rule. Defaults to None.
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

        socket_options = awscrt.io.SocketOptions()
        socket_options.connect_timeout_ms = 5000
        socket_options.keep_alive = False
        socket_options.keep_alive_timeout_secs = 0
        socket_options.keep_alive_interval_secs = 0
        socket_options.keep_alive_max_probes = 0

        client_bootstrap = awscrt.io.ClientBootstrap.get_or_create_static_default()

        tls_ctx = awscrt.io.ClientTlsContext(tls_ctx_options)
        mqtt_client = awscrt.mqtt.Client(client_bootstrap, tls_ctx)

        self.connection = awscrt.mqtt.Connection(
            client=mqtt_client,
            on_connection_interrupted=self._on_connection_interrupted,
            on_connection_resumed=self._on_connection_resumed,
            client_id=client_id,
            host_name=endpoint,
            port=port,
            clean_session=clean_session,
            reconnect_min_timeout_secs=5,
            reconnect_max_timeout_secs=60,
            keep_alive_secs=keep_alive_secs,
            ping_timeout_ms=3000,
            protocol_operation_timeout_ms=0,
            socket_options=socket_options,
            use_websockets=False,
            on_connection_success=self._on_connection_success,
            on_connection_failure=self._on_connection_failure,
            on_connection_closed=self._on_connection_closed,
        )

        self._instance_logger = logger.getChild(
            f"{self.__class__.__name__}.client-{client_id}"
        )

    def _on_connection_interrupted(
        self, connection, error, **kwargs
    ):  # pragma: no cover
        """Callback when connection accidentally lost."""
        self._instance_logger.debug("Connection interrupted. error: {}".format(error))

    def _on_connection_resumed(
        self, connection, return_code, session_present, **kwargs
    ):  # pragma: no cover
        """Callback when an interrupted connection is re-established."""

        self._instance_logger.debug(
            "Connection resumed. return_code: {} session_present: {}".format(
                return_code, session_present
            )
        )

    def _on_connection_success(self, connection, callback_data):  # pragma: no cover
        """Callback when the connection successfully connects."""

        assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
        self._instance_logger.debug(
            "Connection Successful with return code: {} session present: {}".format(
                callback_data.return_code, callback_data.session_present
            )
        )

    def _on_connection_failure(self, connection, callback_data):  # pragma: no cover
        """Callback when a connection attempt fails."""

        assert isinstance(callback_data, mqtt.OnConnectionFailureData)
        self._instance_logger.debug(
            "Connection failed with error code: {}".format(callback_data.error)
        )

    def _on_connection_closed(self, connection, callback_data):  # pragma: no cover
        """Callback when a connection has been disconnected or shutdown successfully"""
        self._instance_logger.debug("Connection closed")

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

    def send_message(
        self, message: dict, topic: str, use_logger: logging.Logger | None = None
    ) -> None:
        """Sends a message to the endpoint.

        Args:
            message: The message to send.
            topic: MQTT topic to send message under.
            use_logger: Sends log message with requested logger.
        """
        if use_logger is not None and isinstance(use_logger, logging.Logger):
            use_logger = use_logger
        else:
            use_logger = self._instance_logger

        if not message:
            use_logger.error(f'No message to send for topic: "{topic}".')
            return

        self._connect()

        if message:  # pragma: no cover
            payload = json.dumps(message, default=json_serial)
            self.connection.publish(
                topic=topic,
                payload=payload,
                qos=mqtt.QoS.AT_LEAST_ONCE,
            )

        use_logger.info(f'Sent {sys.getsizeof(payload)} bytes to "{topic}"')

        self._disconnect()
