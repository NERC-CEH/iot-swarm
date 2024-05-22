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
        topic_prefix: str = "",
        **kwargs,
    ) -> None:

        self.topic_prefix = topic_prefix

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

    # Callback when connection is accidentally lost.
    @staticmethod
    def _on_connection_interrupted(connection, error, **kwargs):
        print("Connection interrupted. error: {}".format(error))

    # Callback when an interrupted connection is re-established.
    @staticmethod
    def _on_connection_resumed(connection, return_code, session_present, **kwargs):
        print(
            "Connection resumed. return_code: {} session_present: {}".format(
                return_code, session_present
            )
        )

        if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
            print("Session did not persist. Resubscribing to existing topics...")
            resubscribe_future, _ = connection.resubscribe_existing_topics()

            # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
            # evaluate result with a callback instead.
            resubscribe_future.add_done_callback(
                IotCoreMQTTConnection._on_resubscribe_complete
            )

    @staticmethod
    def _on_resubscribe_complete(resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

        for topic, qos in resubscribe_results["topics"]:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))

    # Callback when the connection successfully connects
    @staticmethod
    def _on_connection_success(connection, callback_data):
        assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
        print(
            "Connection Successful with return code: {} session present: {}".format(
                callback_data.return_code, callback_data.session_present
            )
        )

    # Callback when a connection attempt fails
    @staticmethod
    def _on_connection_failure(connection, callback_data):
        assert isinstance(callback_data, mqtt.OnConnectionFailureData)
        print("Connection failed with error code: {}".format(callback_data.error))

    # Callback when a connection has been disconnected or shutdown successfully
    @staticmethod
    def _on_connection_closed(connection, callback_data):
        print("Connection closed")

    def send_message(self, message: str, topic: str, count: int = 1):

        if self.topic_prefix != "":
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
        print("Disconnected!")


if __name__ == "__main__":
    # Create a MQTT connection from the command line data

    iot_config = config.Config(str(Path(Path(__file__).parents[3], "config.cfg")))[
        "iot_core"
    ]

    conn = IotCoreMQTTConnection(
        endpoint=iot_config["endpoint"],
        cert_path=iot_config["cert_path"],
        key_path=iot_config["pri_key_path"],
        ca_cert_path=iot_config["aws_ca_cert_path"],
        client_id="fdri_swarm",
    )

    conn.send_message("First message", "fdri/cosmos_site/site1", 1)
    # conn.send_message("Second message", "sdk/test/java", 5)
