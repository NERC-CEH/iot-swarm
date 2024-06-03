"""Contains communication protocols for AWS services."""

import awscrt
from awscrt import mqtt
import awscrt.io
import time
import json
from awscrt.exceptions import AwsCrtError
from iotdevicesimulator.messaging.core import MessagingBaseClass
import backoff
import logging

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

    @staticmethod
    def _on_connection_interrupted(connection, error, **kwargs):  # pragma: no cover
        """Callback when connection accidentally lost."""
        print("Connection interrupted. error: {}".format(error))

    @staticmethod
    def _on_connection_resumed(
        connection, return_code, session_present, **kwargs
    ):  # pragma: no cover
        """Callback when an interrupted connection is re-established."""

        print(
            "Connection resumed. return_code: {} session_present: {}".format(
                return_code, session_present
            )
        )

    @staticmethod
    def _on_connection_success(connection, callback_data):  # pragma: no cover
        """Callback when the connection successfully connects."""

        assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
        print(
            "Connection Successful with return code: {} session present: {}".format(
                callback_data.return_code, callback_data.session_present
            )
        )

    @staticmethod
    def _on_connection_failure(connection, callback_data):  # pragma: no cover
        """Callback when a connection attempt fails."""

        assert isinstance(callback_data, mqtt.OnConnectionFailureData)
        print("Connection failed with error code: {}".format(callback_data.error))

    @staticmethod
    def _on_connection_closed(connection, callback_data):  # pragma: no cover
        """Callback when a connection has been disconnected or shutdown successfully"""
        print("Connection closed\n")

    @backoff.on_exception(backoff.expo, exception=AwsCrtError, logger=logger)
    def _connect(self):  # pragma: no cover
        connect_future = self.connection.connect()
        connect_future.result()

    @backoff.on_exception(backoff.expo, exception=AwsCrtError, logger=logger)
    def _disconnect(self):  # pragma: no cover
        disconnect_future = self.connection.disconnect()
        disconnect_future.result()

    def send_message(
        self, message: str, topic: str, count: int = 1
    ) -> None:  # pragma: no cover
        """Sends a message to the endpoint.

        Args:
            message: The message to send.
            topic: MQTT topic to send message under.
            count: How many times to repeat the message. If 0, it sends forever.
        """

        self._connect()

        # Publish message to server desired number of times.
        # This step is skipped if message is blank.
        # This step loops forever if count was set to 0.
        if message:
            if count == 0:
                logger.info("Sending messages until program killed")
            else:
                logger.info("Sending {} message(s)".format(count))

            publish_count = 1
            while (publish_count <= count) or (count == 0):
                message_text = "{} [{}]".format(message, publish_count)
                message_json = json.dumps(message_text)
                self.connection.publish(
                    topic=topic,
                    payload=message_json,
                    qos=mqtt.QoS.AT_LEAST_ONCE,
                )

                if count > 1:
                    time.sleep(1)
                publish_count += 1

        self._disconnect()
