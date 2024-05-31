import unittest
import asyncio
import pytest
from iotdevicesimulator.devices import SensorSite
from iotdevicesimulator.db import Oracle
from iotdevicesimulator.queries import CosmosQuery
from iotdevicesimulator.messaging.core import MockMessageConnection
from parameterized import parameterized
import pathlib, config


class TestSensorSiteInstantiation(unittest.TestCase):
    """Suite to test objects for simulating FDRI site objects."""

    @parameterized.expand(["ABCDE", "testsite", 12345])
    def test_instantiation(self, site_id):
        """Test that the instance can be created and has correct attributes"""

        site = SensorSite(site_id)

        self.assertEqual(site.site_id, str(site_id))

    @parameterized.expand(["ABCDE", "testsite", 12345])
    def test__repr__(self, site_id):
        """Tests __repr__ function returns correct string"""

        site = SensorSite(site_id)

        self.assertEqual(
            repr(site),
            f'SensorSite("{site.site_id}", sleep_time={site.sleep_time}, max_cycles={site.max_cycles})',
        )

    @parameterized.expand(["ABCDE", "testsite", 12345])
    def test__str__(self, site_id):
        """Tests __str__ function returns correct string"""

        site = SensorSite(site_id)

        self.assertEqual(
            str(site),
            f'Site ID: "{site.site_id}", Sleep Time: {site.sleep_time}, Max Cycles: {site.max_cycles}, Cycle: {site.cycle}',
        )

    @parameterized.expand([0, 7.9, 1, 5, 10, "10"])
    def test_max_cycle_argument(self, max_cycles):
        """Tests that the max argument is set"""

        site = SensorSite("SITE_ID", max_cycles=max_cycles)
        self.assertEqual(site.max_cycles, int(max_cycles))

    @parameterized.expand(["four", "TEN", -10, -1])
    def test_max_cycle_bad_value_gives_error(self, max_cycles):
        """Tests that negative max_cycles gives ValueError"""

        with self.assertRaises((ValueError, TypeError)):
            SensorSite("SITE_ID", max_cycles=max_cycles)

    @parameterized.expand([0, 7.6, 1, 5, 10, "10"])
    def test_sleep_time_argument(self, sleep_time):
        """Tests that the sleep_time is set"""

        site = SensorSite("SITE_ID", sleep_time=sleep_time)
        self.assertEqual(site.sleep_time, int(sleep_time))

    @parameterized.expand(["four", "TEN", -10, -1.2])
    def test_sleep_time_bad_value_gives_error(self, sleep_time):
        """Tests that bad `sleep_time` values gives ValueError"""

        with self.assertRaises(ValueError):
            SensorSite("SITE_ID", sleep_time=sleep_time)


CONFIG_PATH = pathlib.Path(
    pathlib.Path(__file__).parents[1], "iotdevicesimulator", "__assets__", "config.cfg"
)
config_exists = pytest.mark.skipif(
    not CONFIG_PATH.exists(),
    reason="Config file `config.cfg` not found in root directory.",
)


class TestSensorSiteOperation(unittest.IsolatedAsyncioTestCase):
    """Tests the active behaviour of SensorSite objects."""

    @pytest.mark.oracle
    @pytest.mark.asyncio
    @config_exists
    async def asyncSetUp(self):
        cred_path = str(CONFIG_PATH)
        creds = config.Config(cred_path)["oracle"]

        self.oracle = await Oracle.create(
            creds["dsn"],
            creds["user"],
            password=creds["password"],
        )

        self.message_connection = MockMessageConnection()

    @pytest.mark.oracle
    @pytest.mark.asyncio
    @config_exists
    async def asyncTearDown(self) -> None:
        await self.oracle.connection.close()

    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_run_stops_after_max_cycles(self):
        """Ensures .run() method breaks after max_cycles"""

        query = CosmosQuery.LEVEL_1_SOILMET_30MIN

        site = SensorSite("MORLY", max_cycles=5, sleep_time=0)
        await site.run(self.oracle, query, self.message_connection)

        self.assertEqual(site.cycle, site.max_cycles)

    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_multi_instances_stop_at_max_cycles(self):
        """Ensures .run() method breaks after max_cycles for multiple instances"""

        query = CosmosQuery.LEVEL_1_PRECIP_1MIN

        max_cycles = [1, 2, 3]
        site_ids = ["BALRD", "GLENW", "SPENF"]

        sites = [
            SensorSite(s, max_cycles=i, sleep_time=0)
            for (s, i) in zip(site_ids, max_cycles)
        ]

        await asyncio.gather(
            *[x.run(self.oracle, query, self.message_connection) for x in sites]
        )

        for i, site in enumerate(sites):
            self.assertEqual(site.cycle, max_cycles[i])


if __name__ == "__main__":
    unittest.main()
