import unittest
import asyncio
import pytest
from iotdevicesimulator.devices import CosmosSensorDevice, BaseDevice, CosmosDevice
from iotdevicesimulator.db import Oracle, BaseDatabase
from iotdevicesimulator.queries import CosmosQuery, CosmosSiteQuery
from iotdevicesimulator.messaging.core import MockMessageConnection
from parameterized import parameterized
from unittest.mock import patch


class TestBaseClass(unittest.TestCase):

    @patch.multiple(BaseDevice, __abstractmethods__=set())
    def test(self):
        instance = BaseDevice("TEST_ID")

        self.assertIsNone(instance.device_type)
        self.assertEqual(instance.device_id, "TEST_ID")

    @patch.multiple(BaseDevice, __abstractmethods__=set())
    def test__repr__(self):
        instance = BaseDevice("TEST_ID")

        expected = (
            f"BaseDevice("
            f'"TEST_ID"'
            f", sleep_time={instance.sleep_time}"
            f", max_cycles={instance.max_cycles}"
            f", delay_start={instance.delay_start}"
            f", topic_prefix={instance.topic_prefix}"
            f", data_source={instance.data_source}"
            f")"
        )

        self.assertEqual(repr(instance), expected)


class TestCosmosDevice(unittest.IsolatedAsyncioTestCase):
    """Tests the CosmosDevice class."""

    @patch.multiple(BaseDatabase, __abstractmethods__=set())
    def setUp(self) -> None:
        self.database = BaseDatabase()
        self.query = CosmosQuery.LEVEL_1_SOILMET_30MIN

    def test_good_initialisation(self):
        """Tests that initialisation possible with expected values."""

        device = CosmosDevice(self.query, self.database, "device_id")

        self.assertIsInstance(device, CosmosDevice)
        self.assertEqual(device.device_id, "device_id")
        self.assertEqual(device.query, self.query)
        self.assertEqual(device.device_type, "cosmos-device")

    def test_bad_query(self):
        """Tests that error is raised if the wrong query given."""

        with self.assertRaises(TypeError):
            CosmosDevice(CosmosSiteQuery.LEVEL_1_SOILMET_30MIN, self.database, "device")

    def test_bad_database(self):
        """Tests that the database object is checked"""
        with self.assertRaises(TypeError):
            CosmosDevice(self.query, "database", "device")

    async def test_payload_retreived(self):
        "Tests that a payload can be retrieved."

        device = CosmosDevice(self.query, self.database, "device")
        self.assertIsInstance(await device._get_payload(), (type(None), dict))

    @parameterized.expand([1, {"a": "dict"}])
    def test_payload_formatter(self, payload):
        """Expected that payload receives no formatting"""

        self.assertEqual(payload, CosmosSensorDevice._format_payload(payload))


class TestCosmosSensorDeviceInstantiation(unittest.TestCase):
    """Suite to test objects for simulating FDRI site objects."""

    @patch.multiple(BaseDatabase, __abstractmethods__=set())
    def setUp(self) -> None:
        self.database = BaseDatabase()
        self.query = CosmosQuery.LEVEL_1_SOILMET_30MIN

    @parameterized.expand(["ABCDE", "testsite", 12345])
    def test_instantiation(self, device_id):
        """Test that the instance can be created and has correct attributes"""

        site = CosmosSensorDevice(self.query, self.database, device_id)

        self.assertEqual(site.device_id, str(device_id))
        self.assertEqual(site.query, self.query)
        self.assertEqual(site.database, self.database)

    @parameterized.expand(["ABCDE", "testsite", 12345])
    def test__repr__(self, device_id):
        """Tests __repr__ function returns correct string"""

        instance = CosmosSensorDevice(self.query, self.database, device_id)

        expected = (
            f"CosmosSensorDevice("
            f"{self.query.__class__.__name__}.{self.query.name}"
            f", {self.database.__repr__()}"
            f', "{device_id}"'
            f", sleep_time={instance.sleep_time}"
            f", max_cycles={instance.max_cycles}"
            f", delay_start={instance.delay_start}"
            f", topic_prefix={instance.topic_prefix}"
            f", data_source={instance.data_source}"
            f")"
        )

        self.assertEqual(repr(instance), expected)

    @parameterized.expand(["ABCDE", "testsite", 12345])
    def test__str__(self, device_id):
        """Tests __str__ function returns correct string"""

        site = CosmosSensorDevice(self.query, self.database, device_id)

        self.assertEqual(
            str(site),
            f'Site ID: "{site.device_id}", Sleep Time: {site.sleep_time}, Max Cycles: {site.max_cycles}, Cycle: {site.cycle}',
        )

    @parameterized.expand([0, 7.9, 1, 5, 10, "10"])
    def test_max_cycle_argument(self, max_cycles):
        """Tests that the max argument is set"""

        site = CosmosSensorDevice(
            self.query, self.database, "device_id", max_cycles=max_cycles
        )
        self.assertEqual(site.max_cycles, int(max_cycles))

    @parameterized.expand(["four", "TEN", -10, -1])
    def test_max_cycle_bad_value_gives_error(self, max_cycles):
        """Tests that negative max_cycles gives ValueError"""

        with self.assertRaises((ValueError, TypeError)):
            CosmosSensorDevice(
                self.query, self.database, "device_id", max_cycles=max_cycles
            )

    @parameterized.expand([0, 7.6, 1, 5, 10, "10"])
    def test_sleep_time_argument(self, sleep_time):
        """Tests that the sleep_time is set"""

        site = CosmosSensorDevice(
            self.query, self.database, "device_id", sleep_time=sleep_time
        )
        self.assertEqual(site.sleep_time, int(sleep_time))

    @parameterized.expand(["four", "TEN", -10, -1.2])
    def test_sleep_time_bad_value_gives_error(self, sleep_time):
        """Tests that bad `sleep_time` values gives ValueError"""

        with self.assertRaises(ValueError):
            CosmosSensorDevice(
                self.query, self.database, "device_id", sleep_time=sleep_time
            )

    def test_topic_prefix(self):
        """Tests that the topic prefix is set"""
        site = CosmosSensorDevice(self.query, self.database, "device_id")

        site.topic = "topic1"
        self.assertEqual(
            site.topic, "fdri/cosmos_site/cosmos-sensor-site/device_id/topic1"
        )

        site = CosmosSensorDevice(
            self.query, self.database, "SITE2", topic_prefix="$some/rule/path"
        )
        site.topic = "topic2"
        self.assertEqual(
            site.topic,
            "$some/rule/path/fdri/cosmos_site/cosmos-sensor-site/SITE2/topic2",
        )


class TestCosmosSensorDeviceOperation(unittest.IsolatedAsyncioTestCase):
    """Tests the active behaviour of CosmosSensorDevice objects."""

    @patch.multiple(BaseDatabase, __abstractmethods__=set())
    def setUp(self):

        self.database = BaseDatabase()

        self.message_connection = MockMessageConnection()
        self.query = CosmosQuery.LEVEL_1_SOILMET_30MIN

    @pytest.mark.asyncio
    async def test_run_stops_after_max_cycles(self):
        """Ensures .run() method breaks after max_cycles"""

        site = CosmosSensorDevice(
            self.query, self.database, "MORLY", max_cycles=5, sleep_time=0
        )
        await site.run(self.message_connection)

        self.assertEqual(site.cycle, site.max_cycles)

    @pytest.mark.asyncio
    async def test_multi_instances_stop_at_max_cycles(self):
        """Ensures .run() method breaks after max_cycles for multiple instances"""

        max_cycles = [1, 2, 3]
        device_ids = ["BALRD", "GLENW", "SPENF"]

        sites = [
            CosmosSensorDevice(self.query, self.database, s, max_cycles=i, sleep_time=0)
            for (s, i) in zip(device_ids, max_cycles)
        ]

        await asyncio.gather(*[x.run(self.message_connection) for x in sites])

        for i, site in enumerate(sites):
            self.assertEqual(site.cycle, max_cycles[i])


if __name__ == "__main__":
    unittest.main()
