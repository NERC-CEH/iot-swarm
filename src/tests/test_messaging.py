import unittest.mock
import pytest
import unittest
from unittest.mock import patch
from iotdevicesimulator.mqtt.core import MockMessageConnection, MessagingBaseClass
from iotdevicesimulator.mqtt.aws import IotCoreMQTTConnection
from config import Config
from pathlib import Path
import awscrt.mqtt
import awscrt.io
from parameterized import parameterized


class TestBaseClass(unittest.TestCase):

    @patch.multiple(MessagingBaseClass, __abstractmethods__=set())
    def test(self):
        instance = MessagingBaseClass()

        self.assertIsNone(instance.connection)
        self.assertIsNone(instance.send_message())


class TestMockMessageConnection(unittest.TestCase):

    def test_instantiation(self):
        mock = MockMessageConnection()

        self.assertIsNone(mock.connection)

        self.assertIsNone(mock.send_message())

        self.assertIsInstance(mock, MessagingBaseClass)


class TestIoTCoreMQTTConnection(unittest.TestCase):

    def setUp(self) -> None:
        config = Config(
            str(
                Path(
                    Path(__file__).parents[1],
                    "iotdevicesimulator",
                    "__assets__",
                    "config.cfg",
                )
            )
        )

        self.config = config["iot_core"]

    def test_instantiation(self):

        instance = IotCoreMQTTConnection(**self.config, client_id="test_id")

        self.assertIsInstance(instance, MessagingBaseClass)

        self.assertIsInstance(instance.connection, awscrt.mqtt.Connection)

    def test_non_string_arguments(self):

        with self.assertRaises(TypeError):
            instance = IotCoreMQTTConnection(
                1,
                self.config["cert_path"],
                self.config["key_path"],
                self.config["ca_cert_path"],
                "client_id",
            )

        with self.assertRaises(TypeError):
            instance = IotCoreMQTTConnection(
                self.config["endpoint"],
                self.config["cert_path"],
                1,
                self.config["ca_cert_path"],
                "client_id",
            )

        with self.assertRaises(TypeError):
            instance = IotCoreMQTTConnection(
                self.config["endpoint"],
                self.config["cert_path"],
                1,
                self.config["ca_cert_path"],
                "client_id",
            )

        with self.assertRaises(TypeError):
            instance = IotCoreMQTTConnection(
                self.config["endpoint"],
                self.config["cert_path"],
                self.config["key_path"],
                1,
                "client_id",
            )

        with self.assertRaises(TypeError):
            instance = IotCoreMQTTConnection(
                self.config["endpoint"],
                self.config["cert_path"],
                self.config["key_path"],
                self.config["ca_cert_path"],
                1,
            )

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
    def test_bad_clean_session_type(self, clean_session):

        with self.assertRaises(TypeError):
            IotCoreMQTTConnection(
                **self.config, client_id="test_id", clean_session=clean_session
            )

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

    @parameterized.expand(["FOURTY", "True", None])
    def test_bad_keep_alive_secs_type(self, secs):
        with self.assertRaises(TypeError):
            IotCoreMQTTConnection(
                **self.config, client_id="test_id", keep_alive_secs=secs
            )


if __name__ == "__main__":
    unittest.main()
