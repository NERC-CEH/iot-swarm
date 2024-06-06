import unittest
import asyncio
import pytest
import logging
from iotdevicesimulator.devices import (
    CosmosDevice,
    BaseDevice,
    MQTTCosmosDevice,
    CR1000XCosmosDevice,
    MQTTCR1000XCosmosDevice,
)
from iotdevicesimulator.db import Oracle, BaseDatabase, MockDB
from iotdevicesimulator.queries import CosmosQuery, CosmosSiteQuery
from iotdevicesimulator.messaging.core import MockMessageConnection, MessagingBaseClass
from iotdevicesimulator.messaging.aws import IotCoreMQTTConnection
from parameterized import parameterized
from unittest.mock import patch
from datetime import datetime


class TestBaseClass(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.data_source = MockDB()
        self.connection = MockMessageConnection()

    @patch.multiple(BaseDevice, __abstractmethods__=set())
    def test_base_instantiation(self):
        instance = BaseDevice("TEST_ID", self.data_source, self.connection)

        self.assertIsNone(instance.device_type)

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
    @patch.multiple(BaseDevice, __abstractmethods__=set())
    def test_device_id_validation(self, device_id):
        inst = BaseDevice(device_id, self.data_source, self.connection)

        self.assertIsInstance(inst.device_id, str)
        self.assertEqual(inst.device_id, str(device_id))

    @patch.multiple(BaseDevice, __abstractmethods__=set())
    @patch.multiple(BaseDatabase, __abstractmethods__=set())
    def test_data_source_validation(self):
        dbs = [BaseDatabase(), MockDB()]

        for db in dbs:
            inst = BaseDevice("test_id", db, self.connection)
            self.assertIsInstance(inst.data_source, BaseDatabase)
            self.assertEqual(inst.data_source, db)

    @parameterized.expand([1, "Data", {"a": 1}, MockMessageConnection()])
    @patch.multiple(BaseDevice, __abstractmethods__=set())
    def test_data_source_type_check(self, data_source):
        with self.assertRaises(TypeError):
            BaseDevice("test_id", data_source, self.connection)

    @patch.multiple(BaseDevice, __abstractmethods__=set())
    @patch.multiple(MessagingBaseClass, __abstractmethods__=set())
    def test_connection_validation(self):
        """Tests that messaging classes can be used."""
        connections = [MockMessageConnection(), MessagingBaseClass()]

        for conn in connections:
            inst = BaseDevice("test_id", self.data_source, conn)
            self.assertIsInstance(conn, MessagingBaseClass)
            self.assertEqual(inst.connection, conn)

    @parameterized.expand([1, "Data", {"a": 1}, MockDB()])
    @patch.multiple(BaseDevice, __abstractmethods__=set())
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
    @patch.multiple(BaseDevice, __abstractmethods__=set())
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
    @patch.multiple(BaseDevice, __abstractmethods__=set())
    def test_sleep_time_value_check(self, sleep_time):

        with self.assertRaises((TypeError, ValueError)):
            BaseDevice(
                "test_id", self.data_source, self.connection, sleep_time=sleep_time
            )

    @parameterized.expand([-1, -423.78, "Four", MockDB(), {"a": 1}])
    @patch.multiple(BaseDevice, __abstractmethods__=set())
    def test_max_cycles_value_check(self, max_cycles):

        with self.assertRaises((TypeError, ValueError)):
            BaseDevice(
                "test_id", self.data_source, self.connection, max_cycles=max_cycles
            )

    @parameterized.expand([-1, -423.78, "Four", MockDB(), {"a": 1}])
    @patch.multiple(BaseDevice, __abstractmethods__=set())
    def test_delay_start_value_check(self, delay_start):

        with self.assertRaises((TypeError, ValueError)):
            BaseDevice(
                "test_id", self.data_source, self.connection, delay_start=delay_start
            )

    @parameterized.expand(
        [
            [
                {},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection())',
            ],
            [
                {"sleep_time": 5},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection(), sleep_time=5)',
            ],
            [
                {"max_cycles": 24},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection(), max_cycles=24)',
            ],
            [
                {"delay_start": True},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection(), delay_start=True)',
            ],
            [
                {"sleep_time": 5, "delay_start": True},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection(), sleep_time=5, delay_start=True)',
            ],
            [
                {"max_cycles": 4, "sleep_time": 5, "delay_start": True},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection(), sleep_time=5, max_cycles=4, delay_start=True)',
            ],
        ]
    )
    @patch.multiple(BaseDevice, __abstractmethods__=set())
    def test__repr__(self, kwargs, expected):
        instance = BaseDevice("TEST_ID", self.data_source, self.connection, **kwargs)

        self.assertEqual(repr(instance), expected)

    @parameterized.expand(
        [
            [None, logging.getLogger("iotdevicesimulator.devices")],
            [logging.getLogger("name"), logging.getLogger("name")],
        ]
    )
    @patch.multiple(BaseDevice, __abstractmethods__=set())
    def test_logger_set(self, logger, expected):

        inst = BaseDevice(
            "site", self.data_source, self.connection, inherit_logger=logger
        )

        self.assertEqual(inst._instance_logger.parent, expected)


class TestBaseDeviceOperation(unittest.IsolatedAsyncioTestCase):
    """Tests the active behaviour of Device objects."""

    def setUp(self):

        self.database = MockDB()
        self.connection = MockMessageConnection()

    @pytest.mark.asyncio
    @patch.multiple(
        BaseDevice,
        __abstractmethods__=set(),
    )
    @staticmethod
    async def return_list(*args):
        return list(range(5))

    @patch.multiple(BaseDevice, __abstractmethods__=set())
    async def test_run_stops_after_max_cycles(self):
        """Ensures .run() method breaks after max_cycles"""

        site = BaseDevice(
            "site", self.database, self.connection, max_cycles=5, sleep_time=0
        )
        await site.run()

        self.assertEqual(site.cycle, site.max_cycles)

    @pytest.mark.asyncio
    @patch.multiple(BaseDevice, __abstractmethods__=set())
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
    @patch.multiple(BaseDevice, __abstractmethods__=set())
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
    @patch.multiple(BaseDevice, __abstractmethods__=set())
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


# class TestCosmosDevice(unittest.IsolatedAsyncioTestCase):
#     """Tests the CosmosDevice class."""

#     @patch.multiple(BaseDatabase, __abstractmethods__=set())
#     def setUp(self) -> None:
#         self.database = BaseDatabase()
#         self.query = CosmosQuery.LEVEL_1_SOILMET_30MIN

#     def test_good_initialisation(self):
#         """Tests that initialisation possible with expected values."""

#         device = CosmosDevice(self.query, self.database, "device_id")

#         self.assertIsInstance(device, CosmosDevice)
#         self.assertEqual(device.device_id, "device_id")
#         self.assertEqual(device.query, self.query)
#         self.assertEqual(device.device_type, "cosmos-device")

#     def test_bad_query(self):
#         """Tests that error is raised if the wrong query given."""

#         with self.assertRaises(TypeError):
#             CosmosDevice(CosmosSiteQuery.LEVEL_1_SOILMET_30MIN, self.database, "device")

#     def test_bad_database(self):
#         """Tests that the database object is checked"""
#         with self.assertRaises(TypeError):
#             CosmosDevice(self.query, "database", "device")

#     async def test_payload_retreived(self):
#         "Tests that a payload can be retrieved."

#         device = CosmosDevice(self.query, self.database, "device")
#         self.assertIsInstance(await device._get_payload(), (type(None), dict))

#     @parameterized.expand([1, {"a": "dict"}])
#     def test_payload_formatter(self, payload):
#         """Expected that payload receives no formatting"""

#         self.assertEqual(payload, CosmosDevice._format_payload(payload))

#     @parameterized.expand(
#         [
#             ["MORLY", None, None, "cosmos-device/MORLY"],
#             ["ALIC1", "prefix", None, "prefix/cosmos-device/ALIC1"],
#             ["TEST", None, "suffix", "cosmos-device/TEST/suffix"],
#             ["ANOTHER", "prefix", "suffix", "prefix/cosmos-device/ANOTHER/suffix"],
#         ]
#     )
#     def test_topic_set(self, site_id, prefix, suffix, expected):
#         """Tests the topic setting"""

#         device = CosmosDevice(self.query, self.database, site_id)
#         self.assertEqual(
#             device._get_mqtt_topics(prefix=prefix, suffix=suffix)[0], expected
#         )


# class TestCosmosSensorDeviceInstantiation(unittest.TestCase):
#     """Suite to test objects for simulating FDRI site objects."""

#     @patch.multiple(BaseDatabase, __abstractmethods__=set())
#     def setUp(self) -> None:
#         self.database = BaseDatabase()
#         self.query = CosmosQuery.LEVEL_1_SOILMET_30MIN

#     @parameterized.expand(["ABCDE", "testsite", 12345])
#     def test_instantiation(self, device_id):
#         """Test that the instance can be created and has correct attributes"""

#         site = C(self.query, self.database, device_id)

#         self.assertEqual(site.device_id, str(device_id))
#         self.assertEqual(site.query, self.query)
#         self.assertEqual(site.database, self.database)

#     @parameterized.expand(["ABCDE", "testsite", 12345])
#     def test__repr__(self, device_id):
#         """Tests __repr__ function returns correct string"""

#         instance = CosmosSensorDevice(self.query, self.database, device_id)

#         expected = (
#             f"CosmosSensorDevice("
#             f"{self.query.__class__.__name__}.{self.query.name}"
#             f", {self.database.__repr__()}"
#             f', "{device_id}"'
#             f", sleep_time={instance.sleep_time}"
#             f", max_cycles={instance.max_cycles}"
#             f", delay_start={instance.delay_start}"
#             f", topic_prefix={instance.topic_prefix}"
#             f", data_source={instance.data_source}"
#             f")"
#         )

#         self.assertEqual(repr(instance), expected)

#     @parameterized.expand(["ABCDE", "testsite", 12345])
#     def test__str__(self, device_id):
#         """Tests __str__ function returns correct string"""

#         site = CosmosSensorDevice(self.query, self.database, device_id)

#         self.assertEqual(
#             str(site),
#             f'Site ID: "{site.device_id}", Sleep Time: {site.sleep_time}, Max Cycles: {site.max_cycles}, Cycle: {site.cycle}',
#         )

#     @parameterized.expand([0, 7.9, 1, 5, 10, "10"])
#     def test_max_cycle_argument(self, max_cycles):
#         """Tests that the max argument is set"""

#         site = CosmosSensorDevice(
#             self.query, self.database, "device_id", max_cycles=max_cycles
#         )
#         self.assertEqual(site.max_cycles, int(max_cycles))

#     @parameterized.expand(["four", "TEN", -10, -1])
#     def test_max_cycle_bad_value_gives_error(self, max_cycles):
#         """Tests that negative max_cycles gives ValueError"""

#         with self.assertRaises((ValueError, TypeError)):
#             CosmosSensorDevice(
#                 self.query, self.database, "device_id", max_cycles=max_cycles
#             )

#     @parameterized.expand([0, 7.6, 1, 5, 10, "10"])
#     def test_sleep_time_argument(self, sleep_time):
#         """Tests that the sleep_time is set"""

#         site = CosmosSensorDevice(
#             self.query, self.database, "device_id", sleep_time=sleep_time
#         )
#         self.assertEqual(site.sleep_time, int(sleep_time))

#     @parameterized.expand(["four", "TEN", -10, -1.2])
#     def test_sleep_time_bad_value_gives_error(self, sleep_time):
#         """Tests that bad `sleep_time` values gives ValueError"""

#         with self.assertRaises(ValueError):
#             CosmosSensorDevice(
#                 self.query, self.database, "device_id", sleep_time=sleep_time
#             )

#     @parameterized.expand([["MORLY", None, None, "cosmos-sensor-device/MORLY"]])
#     def test_topic_prefix(self, site_id, prefix, suffix, expected):
#         """Tests that the topic prefix is set"""
#         site = CosmosSensorDevice(self.query, self.database, site_id)

#         self.assertEqual(
#             site._get_mqtt_topics(prefix=prefix, suffix=suffix)[0], expected
#         )


# class TestCR1000X(unittest.TestCase):

#     @patch.multiple(BaseDatabase, __abstractmethods__=set())
#     def setUp(self) -> None:
#         self.database = BaseDatabase()
#         self.query = CosmosQuery.LEVEL_1_SOILMET_30MIN

#     def test_payload_formatting(self):

#         device = CR1000X(self.query, self.database, "site")

#         payload = {"DATE_TIME": datetime.now(), "A": 1, "B": 2.1233}
#         formatted = device._format_payload(payload)

#         print(formatted)


if __name__ == "__main__":
    unittest.main()
