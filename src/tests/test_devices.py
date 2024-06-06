import unittest
import asyncio
import pytest
import logging
from iotdevicesimulator.devices import BaseDevice
from iotdevicesimulator.db import Oracle, BaseDatabase, MockDB
from iotdevicesimulator.queries import CosmosQuery, CosmosSiteQuery
from iotdevicesimulator.messaging.core import MockMessageConnection, MessagingBaseClass
from iotdevicesimulator.messaging.aws import IotCoreMQTTConnection
from parameterized import parameterized
from unittest.mock import patch
import pathlib
import config

CONFIG_PATH = pathlib.Path(
    pathlib.Path(__file__).parents[1], "iotdevicesimulator", "__assets__", "config.cfg"
)
config_exists = pytest.mark.skipif(
    not CONFIG_PATH.exists(),
    reason="Config file `config.cfg` not found in root directory.",
)


class TestBaseClass(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.data_source = MockDB()
        self.connection = MockMessageConnection()


    def test_base_instantiation(self):
        instance = BaseDevice("TEST_ID", self.data_source, self.connection)

        self.assertEqual(instance.device_type, "base-device")

        self.assertEqual(instance.device_id, "TEST_ID")
        self.assertIsInstance(instance.device_id, str)

        self.assertEqual(instance.connection, self.connection)
        self.assertIsInstance(instance.connection, MessagingBaseClass)
        self.assertEqual(instance.data_source, self.data_source)
        self.assertIsInstance(instance.data_source, BaseDatabase)

        self.assertIsInstance(instance.sleep_time, int)
        self.assertIsInstance(instance.max_cycles, int)
        self.assertFalse(instance.delay_start)

    @parameterized.expand([-5, 12.7, "site", MockDB()])
    def test_device_id_validation(self, device_id):
        inst = BaseDevice(device_id, self.data_source, self.connection)

        self.assertIsInstance(inst.device_id, str)
        self.assertEqual(inst.device_id, str(device_id))

    @patch.multiple(BaseDatabase, __abstractmethods__=set())
    def test_data_source_validation(self):
        dbs = [BaseDatabase(), MockDB()]

        for db in dbs:
            inst = BaseDevice("test_id", db, self.connection)
            self.assertIsInstance(inst.data_source, BaseDatabase)
            self.assertEqual(inst.data_source, db)

    @parameterized.expand([1, "Data", {"a": 1}, MockMessageConnection()])
    def test_data_source_type_check(self, data_source):
        with self.assertRaises(TypeError):
            BaseDevice("test_id", data_source, self.connection)


    @patch.multiple(MessagingBaseClass, __abstractmethods__=set())
    def test_connection_validation(self):
        """Tests that messaging classes can be used."""
        connections = [MockMessageConnection(), MessagingBaseClass()]

        for conn in connections:
            inst = BaseDevice("test_id", self.data_source, conn)
            self.assertIsInstance(conn, MessagingBaseClass)
            self.assertEqual(inst.connection, conn)

    @parameterized.expand([1, "Data", {"a": 1}, MockDB()])
    def test_connection_type_check(self, conn):
        """Tests that invalid connection types raises error."""
        with self.assertRaises(TypeError):
            BaseDevice("test_id", self.data_source, conn)

    @parameterized.expand(
        [
            [0, 0, None, (0, 0, False)],
            [100, 30, True, (100, 30, True)],
            [12.4, 30001.9, None, (12, 30001, False)],
        ]
    )
    def test_optional_args_set(self, max_cycles, sleep_time, delay_start, expected):
        inst = BaseDevice(
            "TEST_ID",
            self.data_source,
            self.connection,
            max_cycles=max_cycles,
            sleep_time=sleep_time,
            delay_start=delay_start,
        )

        self.assertEqual(inst.max_cycles, expected[0])
        self.assertEqual(inst.sleep_time, expected[1])
        self.assertEqual(inst.delay_start, expected[2])

    @parameterized.expand([-1, -423.78, "Four", MockDB(), {"a": 1}])
    def test_sleep_time_value_check(self, sleep_time):

        with self.assertRaises((TypeError, ValueError)):
            BaseDevice(
                "test_id", self.data_source, self.connection, sleep_time=sleep_time
            )

    @parameterized.expand([-1, -423.78, "Four", MockDB(), {"a": 1}])
    def test_max_cycles_value_check(self, max_cycles):

        with self.assertRaises((TypeError, ValueError)):
            BaseDevice(
                "test_id", self.data_source, self.connection, max_cycles=max_cycles
            )

    @parameterized.expand([-1, -423.78, "Four", MockDB(), {"a": 1}])
    def test_delay_start_value_check(self, delay_start):

        with self.assertRaises((TypeError, ValueError)):
            BaseDevice(
                "test_id", self.data_source, self.connection, delay_start=delay_start
            )

    @parameterized.expand(
        [
            [
                MockDB(),
                {},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection())',
            ],
            [
                MockDB(),
                {"sleep_time": 5},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection(), sleep_time=5)',
            ],
            [
                MockDB(),
                {"max_cycles": 24},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection(), max_cycles=24)',
            ],
            [
                MockDB(),
                {"delay_start": True},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection(), delay_start=True)',
            ],
            [
                MockDB(),
                {"sleep_time": 5, "delay_start": True},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection(), sleep_time=5, delay_start=True)',
            ],
            [
                MockDB(),
                {"max_cycles": 4, "sleep_time": 5, "delay_start": True},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection(), sleep_time=5, max_cycles=4, delay_start=True)',
            ],
        ]
    )
    def test__repr__(self, data_source, kwargs, expected):
        instance = BaseDevice("TEST_ID", data_source, self.connection, **kwargs)

        self.assertEqual(repr(instance), expected)

    @parameterized.expand(
        [
            [None, logging.getLogger("iotdevicesimulator.devices")],
            [logging.getLogger("name"), logging.getLogger("name")],
        ]
    )
    def test_logger_set(self, logger, expected):

        inst = BaseDevice(
            "site", self.data_source, self.connection, inherit_logger=logger
        )

        self.assertEqual(inst._instance_logger.parent, expected)

    @parameterized.expand([
        [None, None, None],
        ["topic", None, None],
        ["topic", "prefix", None],
        ["topic", "prefix", "suffix"],
    ])
    def test__repr__mqtt_opts_no_mqtt_connection(self, topic, prefix, suffix):
        """Tests that the __repr__ method returns correctly with MQTT options set."""
        expected = 'BaseDevice("site-id", MockDB(), MockMessageConnection())'
        inst = BaseDevice("site-id", MockDB(), MockMessageConnection(), mqtt_topic=topic, mqtt_prefix=prefix, mqtt_suffix=suffix)

        self.assertEqual(inst.__repr__(), expected)

class TestBaseDeviceMQTTOptions(unittest.TestCase):

    def setUp(self) -> None:
        creds = config.Config(str(CONFIG_PATH))["iot_core"]
        self.creds = creds
        self.conn = IotCoreMQTTConnection(
            creds["endpoint"], creds["cert_path"], creds["key_path"], creds["ca_cert_path"], "fdri_swarm",
        )
        self.db = MockDB()

    @parameterized.expand(["this/topic", "1/1/1", "TOPICO!"])
    @config_exists
    def test_mqtt_topic_set(self, topic):
        """Tests that mqtt_topic gets set."""

        inst = BaseDevice("site", self.db, self.conn, mqtt_topic=topic)

        self.assertEqual(inst.mqtt_topic, topic)

    @parameterized.expand(["this/topic", "1/1/1", "TOPICO!"])
    @config_exists
    def test_mqtt_prefix_set(self, topic):
        """Tests that mqtt_prefix gets set"""

        inst = BaseDevice("site", self.db, self.conn, mqtt_prefix=topic)

        self.assertEqual(inst.mqtt_prefix, topic)
        self.assertEqual(inst.mqtt_topic, f"{topic}/base-device/site")

    @parameterized.expand(["this/topic", "1/1/1", "TOPICO!"])
    @config_exists
    def test_mqtt_suffix_set(self, topic):
        """Tests that mqtt_suffix gets set"""

        inst = BaseDevice("site", self.db, self.conn, mqtt_suffix=topic)

        self.assertEqual(inst.mqtt_suffix, topic)
        self.assertEqual(inst.mqtt_topic, f"base-device/site/{topic}")

    @parameterized.expand([["this/prefix", "this/suffix"], ["1/1/1", "2/2/2"], ["TOPICO!", "FOUR"]])
    @config_exists
    def test_mqtt_prefix_and_suffix_set(self, prefix, suffix):
        """Tests that mqtt_suffix gets set"""

        inst = BaseDevice("site", self.db, self.conn, mqtt_prefix=prefix, mqtt_suffix=suffix)

        self.assertEqual(inst.mqtt_suffix, suffix)
        self.assertEqual(inst.mqtt_prefix, prefix)
        self.assertEqual(inst.mqtt_topic, f"{prefix}/base-device/site/{suffix}")

    @config_exists
    def test_default_mqtt_topic_set(self):
        """Tests that default topic gets set"""

        inst = BaseDevice("site-12", self.db, self.conn)

        self.assertEqual(inst.mqtt_topic, "base-device/site-12")


    def test_prefix_suffix_not_set_for_non_mqtt(self):
        "Tests that mqtt prefix and suffix not set for non MQTT messaging."

        inst = BaseDevice("site-1", self.db, MockMessageConnection(), mqtt_prefix="prefix", mqtt_suffix="suffix")

        with self.assertRaises(AttributeError):
            inst.mqtt_topic
        
        with self.assertRaises(AttributeError):
            inst.mqtt_prefix

        with self.assertRaises(AttributeError):
            inst.mqtt_suffix
    
    @parameterized.expand([
        [None, None, None, ""],
        ["topic", None, None, ', mqtt_topic="topic"'],
        ["topic", "prefix", None, ', mqtt_topic="topic", mqtt_prefix="prefix"'],
        ["topic", "prefix", "suffix",', mqtt_topic="topic", mqtt_prefix="prefix", mqtt_suffix="suffix"'],
    ])

    def test__repr__mqtt_opts_no_mqtt_connection(self, topic, prefix, suffix,expected_args):
        """Tests that the __repr__ method returns correctly with MQTT options set."""
        expected = f'BaseDevice("site-id", {str(MockDB())}, {str(self.conn)}{expected_args})'
        inst = BaseDevice("site-id", MockDB(), self.conn, mqtt_topic=topic, mqtt_prefix=prefix, mqtt_suffix=suffix)

        self.assertEqual(inst.__repr__(), expected)

class TestBaseDeviceOracleUsed(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        cred_path = str(CONFIG_PATH)
        creds = config.Config(cred_path)["oracle"]
        self.creds = creds

        self.oracle = await Oracle.create(
            creds["dsn"],
            creds["user"],
            password=creds["password"],
        )
        self.query = CosmosQuery.LEVEL_1_SOILMET_30MIN
    async def asyncTearDown(self) -> None:
        await self.oracle.connection.close()
    
    @parameterized.expand([-1, -423.78, CosmosSiteQuery.LEVEL_1_NMDB_1HOUR, "Four", MockDB(), {"a": 1}])
    def test_query_value_check(self, query):

        with self.assertRaises(TypeError):
            BaseDevice(
                "test_id", self.oracle, MockMessageConnection(), query=query
            )


    def test_query_not_set_for_non_oracle_db(self):
        
        inst = BaseDevice("test", MockDB(), MockMessageConnection(), query=self.query)

        with self.assertRaises(AttributeError):
            inst.query


    @pytest.mark.oracle
    @config_exists
    async def test_error_if_query_not_given(self):

        with self.assertRaises(ValueError):
            BaseDevice("site", self.oracle, MockMessageConnection())


        inst = BaseDevice("site", self.oracle, MockMessageConnection(), query=self.query)

        self.assertEqual(inst.query, self.query)


    @pytest.mark.oracle
    @config_exists
    async def test__repr__oracle_data(self):

        inst_oracle = BaseDevice("site", self.oracle, MockMessageConnection(), query=self.query)
        exp_oracle = f'BaseDevice("site", Oracle("{self.creds['dsn']}"), MockMessageConnection(), query=CosmosQuery.{self.query.name})'
        self.assertEqual(inst_oracle.__repr__(), exp_oracle)

        inst_not_oracle = BaseDevice("site", MockDB(), MockMessageConnection(), query=self.query)
        exp_not_oracle = 'BaseDevice("site", MockDB(), MockMessageConnection())'
        self.assertEqual(inst_not_oracle.__repr__(), exp_not_oracle)

        with self.assertRaises(AttributeError):
            inst_not_oracle.query

    @pytest.mark.oracle
    @config_exists
    async def test__get_payload(self):
        """Tests that Cosmos payload retrieved."""

        inst = BaseDevice("MORLY", self.oracle, MockMessageConnection(), query=self.query)
        payload = await inst._get_payload()

        self.assertIsInstance(payload, dict)

class TestBaseDeviceOperation(unittest.IsolatedAsyncioTestCase):
    """Tests the active behaviour of Device objects."""

    def setUp(self):

        self.database = MockDB()
        self.connection = MockMessageConnection()
    
    @staticmethod
    async def return_list(*args):
        return list(range(5))

    @pytest.mark.asyncio
    async def test_run_stops_after_max_cycles(self):
        """Ensures .run() method breaks after max_cycles"""
        site = BaseDevice(
            "site", self.database, self.connection, max_cycles=5, sleep_time=0
        )
        await site.run()

        self.assertEqual(site.cycle, site.max_cycles)

    @pytest.mark.asyncio
    async def test_multi_instances_stop_at_max_cycles(self):
        """Ensures .run() method breaks after max_cycles for multiple instances"""

        max_cycles = [1, 2, 3]
        device_ids = ["BALRD", "GLENW", "SPENF"]

        sites = [
            BaseDevice(s, self.database, self.connection, max_cycles=i, sleep_time=0)
            for (s, i) in zip(device_ids, max_cycles)
        ]

        await asyncio.gather(*[x.run() for x in sites])

        for i, site in enumerate(sites):
            self.assertEqual(site.cycle, max_cycles[i])

    @pytest.mark.asyncio
    async def test_payload_writes_log(self):
        """Test log is generated when no data found in DB."""

        with self.assertLogs() as cm:
            site = BaseDevice(
                "site",
                self.database,
                self.connection,
                max_cycles=1,
                sleep_time=0,
                inherit_logger=logging.getLogger("mylogger"),
            )
            await site.run()
            expected = "WARNING:mylogger.BaseDevice-site:No data found."
            self.assertIn(expected, cm.output)

        with self.assertLogs(level=logging.DEBUG) as cm:
            site = BaseDevice(
                "site",
                self.database,
                MockMessageConnection(),
                max_cycles=1,
                sleep_time=0,
            )
            site._get_payload = self.return_list
            await site.run()
            expected = "DEBUG:iotdevicesimulator.devices.BaseDevice-site:Requesting payload submission."
            self.assertIn(expected, cm.output)

    @pytest.mark.asyncio
    async def test_delay_writes_log(self):
        """Test log is generated when no data found in DB."""

        with self.assertLogs(level=logging.DEBUG) as cm:
            site = BaseDevice(
                "site",
                self.database,
                self.connection,
                max_cycles=1,
                sleep_time=0,
                delay_start=True,
                inherit_logger=logging.getLogger("mylogger"),
            )
            await site.run()
            expected = "DEBUG:mylogger.BaseDevice-site:Delaying first cycle for: 0s."
            self.assertIn(expected, cm.output)

    @pytest.mark.asyncio
    async def test__get_payload(self):
        """Tests that mock payload retrieved."""

        inst = BaseDevice("MORLY", MockDB(), MockMessageConnection())
        payload = await inst._get_payload()

        self.assertIsInstance(payload, list)
        self.assertEqual(len(payload),0)

if __name__ == "__main__":
    unittest.main()
