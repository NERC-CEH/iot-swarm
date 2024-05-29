"""Contains communication protocols for AWS services."""

import awscrt
from awscrt import mqtt
import awscrt.io
import sys
import time
import json
import config
from pathlib import Path
from awscrt.exceptions import AwsCrtError


class IotCoreMQTTConnection:
    """Handles MQTT communication to AWS IoT Core."""

    connection: awscrt.mqtt.Connection
    """A connection to the MQTT endpoint."""

    topic_prefix: str | None = None
    """Prefix attached to the send topic. Can attach \"Basic Ingest\" rules this way."""

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
        topic_prefix: str | None = None,
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

        if topic_prefix:
            self.topic_prefix = str(topic_prefix)

        tls_ctx_options = awscrt.io.TlsContextOptions.create_client_with_mtls_from_path(
            cert_path, key_path
        )

        tls_ctx_options.override_default_trust_store_from_path(ca_cert_path)

        if not port:
            if awscrt.io.is_alpn_available():
                port = 443
                tls_ctx_options.alpn_list = ["x-amzn-mqtt-ca"]
            else:
                port = 8883

        socket_options = awscrt.io.SocketOptions()
        socket_options.connect_timeout_ms = 5000
        socket_options.keep_alive = False
        socket_options.keep_alive_timeout_secs = 0
        socket_options.keep_alive_interval_secs = 0
        socket_options.keep_alive_max_probes = 0

        username = None

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
            will=None,
            username=username,
            password=None,
            socket_options=socket_options,
            use_websockets=False,
            websocket_handshake_transform=None,
            proxy_options=None,
            on_connection_success=self._on_connection_success,
            on_connection_failure=self._on_connection_failure,
            on_connection_closed=self._on_connection_closed,
        )

    @staticmethod
    def _on_connection_interrupted(connection, error, **kwargs):
        """Callback when connection accidentally lost."""
        print("Connection interrupted. error: {}".format(error))

    @staticmethod
    def _on_connection_resumed(connection, return_code, session_present, **kwargs):
        """Callback when an interrupted connection is re-established."""

        print(
            "Connection resumed. return_code: {} session_present: {}".format(
                return_code, session_present
            )
        )

    @staticmethod
    def _on_connection_success(connection, callback_data):
        """Callback when the connection successfully connects."""

        assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
        print(
            "Connection Successful with return code: {} session present: {}".format(
                callback_data.return_code, callback_data.session_present
            )
        )

    @staticmethod
    def _on_connection_failure(connection, callback_data):
        """Callback when a connection attempt fails."""

        assert isinstance(callback_data, mqtt.OnConnectionFailureData)
        print("Connection failed with error code: {}".format(callback_data.error))

    @staticmethod
    def _on_connection_closed(connection, callback_data):
        """Callback when a connection has been disconnected or shutdown successfully"""
        print("Connection closed")

    def send_message(self, message: str, topic: str, count: int = 1):
        """Sends a message to the endpoint.

        Args:
            message: The message to send.
            topic: MQTT topic to send message under.
            cound: How many times to repeat the message. If 0, it sends forever.
        """

        if self.topic_prefix:
            topic = f"{self.topic_prefix}/{topic}"

        retry_count = 0

        while retry_count < 10:
            connect_future = self.connection.connect()

            # Future.result() waits until a result is available
            try:
                connect_future.result()
                break
            except AwsCrtError:
                print(f"Could not connect. Attempt {retry_count+1}/10")
                retry_count += 1
                time.sleep(2 * retry_count)

        print("Connected!")

        # Publish message to server desired number of times.
        # This step is skipped if message is blank.
        # This step loops forever if count was set to 0.
        if message:
            if count == 0:
                print("Sending messages until program killed")
            else:
                print("Sending {} message(s)".format(count))

            publish_count = 1
            while (publish_count <= count) or (count == 0):
                message_text = "{} [{}]".format(message, publish_count)
                print(
                    "Publishing message to topic '{}': {}".format(topic, message_text)
                )
                message_json = json.dumps(message_text)
                self.connection.publish(
                    topic=topic,
                    payload=message_json,
                    qos=mqtt.QoS.AT_LEAST_ONCE,
                )

                if count > 1:
                    time.sleep(1)
                publish_count += 1

        print("Disconnecting...")
        disconnect_future = self.connection.disconnect()
        disconnect_future.result()
