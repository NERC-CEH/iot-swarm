import unittest.mock
import pytest
import unittest
from unittest.mock import patch
from iotswarm.messaging.core import MockMessageConnection, MessagingBaseClass
from iotswarm.messaging.aws import IotCoreMQTTConnection
from config import Config
from pathlib import Path
import awscrt.mqtt
import awscrt.io
from parameterized import parameterized
import logging


ASSETS_PATH = Path(Path(__file__).parents[1], "iotswarm", "__assets__")
CONFIG_PATH = Path(ASSETS_PATH, "config.cfg")

config_exists = pytest.mark.skipif(
    not CONFIG_PATH.exists(),
    reason="Config file `config.cfg` not found in root directory.",
)
certs_exist = pytest.mark.skipif(
    not Path(ASSETS_PATH, ".certs", "cosmos_soilmet-certificate.pem.crt").exists()
    or not Path(ASSETS_PATH, ".certs", "cosmos_soilmet-private.pem.key").exists()
    or not Path(ASSETS_PATH, ".certs", "AmazonRootCA1.pem").exists(),
    reason="IotCore certificates not present.",
)


class TestBaseClass(unittest.TestCase):

    @patch.multiple(MessagingBaseClass, __abstractmethods__=set())
    def test(self):
        instance = MessagingBaseClass()

        self.assertIsNone(instance.connection)
        self.assertIsNone(instance.send_message())

        self.assertEqual(instance.__repr__(), "MessagingBaseClass()")


class TestMockMessageConnection(unittest.TestCase):

    def test_instantiation(self):
        mock = MockMessageConnection()

        self.assertIsNone(mock.connection)

        self.assertTrue(mock.send_message())

        self.assertIsInstance(mock, MessagingBaseClass)

    def test_no_logger_used(self):

        with self.assertNoLogs():
            mock = MockMessageConnection()
            mock.send_message("")

    def test_logger_used(self):
        logger = logging.getLogger("testlogger")
        with self.assertLogs(logger=logger, level=logging.DEBUG) as cm:
            mock = MockMessageConnection(inherit_logger=logger)
            mock.send_message("")
            self.assertEqual(
                cm.output,
                ["DEBUG:testlogger.MockMessageConnection:Message was sent."],
            )


class TestIoTCoreMQTTConnection(unittest.TestCase):

    @config_exists
    def setUp(self) -> None:
        config = Config(str(CONFIG_PATH))

        self.config = config["iot_core"]

    @config_exists
    @certs_exist
    def test_instantiation(self):

        instance = IotCoreMQTTConnection(**self.config, client_id="test_id")

        self.assertIsInstance(instance, MessagingBaseClass)

        self.assertIsInstance(instance.connection, awscrt.mqtt.Connection)

    @config_exists
    @certs_exist
    def test_non_string_arguments(self):

        with self.assertRaises(TypeError):
            IotCoreMQTTConnection(
                1,
                self.config["cert_path"],
                self.config["key_path"],
                self.config["ca_cert_path"],
                "client_id",
            )

        with self.assertRaises(TypeError):
            IotCoreMQTTConnection(
                self.config["endpoint"],
                1,
                self.config["key_path"],
                self.config["ca_cert_path"],
                "client_id",
            )

        with self.assertRaises(TypeError):
            IotCoreMQTTConnection(
                self.config["endpoint"],
                self.config["cert_path"],
                1,
                self.config["ca_cert_path"],
                "client_id",
            )

        with self.assertRaises(TypeError):
            IotCoreMQTTConnection(
                self.config["endpoint"],
                self.config["cert_path"],
                self.config["key_path"],
                1,
                "client_id",
            )

        with self.assertRaises(TypeError):
            IotCoreMQTTConnection(
                self.config["endpoint"],
                self.config["cert_path"],
                self.config["key_path"],
                self.config["ca_cert_path"],
                1,
            )

    @config_exists
    @certs_exist
    def test_port(self):

        # Expect one of defaults if no port given
        if "port" in self.config:
            del self.config["port"]
        instance = IotCoreMQTTConnection(**self.config, client_id="test_id")

        if awscrt.io.is_alpn_available():
            expected = 443
        else:
            expected = 8883

        self.assertEqual(instance.connection.port, expected)

        # Port set if given
        instance = IotCoreMQTTConnection(**self.config, client_id="test_id", port=420)

        self.assertEqual(instance.connection.port, 420)

    @parameterized.expand([-4, {"f": 4}, "FOUR"])
    @config_exists
    @certs_exist
    def test_bad_port_type(self, port):

        with self.assertRaises((TypeError, ValueError)):
            IotCoreMQTTConnection(
                self.config["endpoint"],
                self.config["cert_path"],
                self.config["key_path"],
                self.config["ca_cert_path"],
                client_id="test_id",
                port=port,
            )

    @config_exists
    @certs_exist
    def test_clean_session_set(self):
        expected = False

        instance = IotCoreMQTTConnection(
            **self.config, client_id="test_id", clean_session=expected
        )
        self.assertEqual(instance.connection.clean_session, expected)

        expected = True
        instance = IotCoreMQTTConnection(
            **self.config, client_id="test_id", clean_session=expected
        )
        self.assertEqual(instance.connection.clean_session, expected)

    @parameterized.expand([0, -1, "true", None])
    @config_exists
    @certs_exist
    def test_bad_clean_session_type(self, clean_session):

        with self.assertRaises(TypeError):
            IotCoreMQTTConnection(
                **self.config, client_id="test_id", clean_session=clean_session
            )

    @config_exists
    @certs_exist
    def test_keep_alive_secs_set(self):
        # Test defualt is not none
        instance = IotCoreMQTTConnection(**self.config, client_id="test_id")
        self.assertIsNotNone(instance.connection.keep_alive_secs)

        # Test value is set
        expected = 20.5
        instance = IotCoreMQTTConnection(
            **self.config, client_id="test_id", keep_alive_secs=expected
        )
        self.assertEqual(instance.connection.keep_alive_secs, expected)

    @parameterized.expand(["FOURTY", "True"])
    @config_exists
    @certs_exist
    def test_bad_keep_alive_secs_type(self, secs):
        with self.assertRaises(TypeError):
            IotCoreMQTTConnection(
                **self.config, client_id="test_id", keep_alive_secs=secs
            )

    @config_exists
    @certs_exist
    def test_no_logger_set(self):
        inst = IotCoreMQTTConnection(**self.config, client_id="test_id")

        expected = 'No message to send for topic: "mytopic".'
        with self.assertLogs() as cm:
            inst.send_message(None, "mytopic")

            self.assertEqual(
                cm.output,
                [
                    f"ERROR:iotswarm.messaging.aws.IotCoreMQTTConnection.client-test_id:{expected}"
                ],
            )

    @config_exists
    @certs_exist
    def test_logger_set(self):
        logger = logging.getLogger("mine")
        inst = IotCoreMQTTConnection(
            **self.config, client_id="test_id", inherit_logger=logger
        )

        expected = 'No message to send for topic: "mytopic".'
        with self.assertLogs(logger=logger, level=logging.INFO) as cm:
            inst.send_message(None, "mytopic")

            self.assertEqual(
                cm.output,
                [f"ERROR:mine.IotCoreMQTTConnection.client-test_id:{expected}"],
            )


if __name__ == "__main__":
    unittest.main()
