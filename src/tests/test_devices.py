import unittest
import asyncio
import pytest
from iotdevicesimulator.devices import SensorSite
from parameterized import parameterized


class TestSensorSiteInstantiation(unittest.TestCase):
    """Suite to test objects for simulating FDRI site objects."""

    @parameterized.expand(["ABCDE", "testsite", 12345])
    def test_instantiation(self, site_id):
        """Test that the instance can be created and has correct attributes"""

        site = SensorSite(site_id)

        self.assertEqual(site.site_id, site_id)

    @parameterized.expand(["ABCDE", "testsite", 12345])
    def test__repr__(self, site_id):
        """Tests __repr__ function returns correct string"""

        site = SensorSite(site_id)

        self.assertEqual(repr(site), f"SensorSite({site.site_id}, {site.max_cycles})")

    @parameterized.expand(["ABCDE", "testsite", 12345])
    def test__str__(self, site_id):
        """Tests __str__ function returns correct string"""

        site = SensorSite(site_id)

        self.assertEqual(
            str(site),
            f"Site ID: {site.site_id}, Max Cycles: {site.max_cycles}, Cycle: {site.cycle}",
        )

    @parameterized.expand([0, 5, 10, "10"])
    def test_max_cycle_argument(self, max_count):
        """Tests that the max argument is set"""

        site = SensorSite("SITE_ID", max_count)
        self.assertEqual(site.max_cycles, int(max_count))

    def test_max_cycle_negative_gives_error(self):
        """Tests that negative max_count gives ValueError"""

        with self.assertRaises(ValueError) as E:
            SensorSite("SITE_ID", -10)

    def test_max_cycle_non_numeric_gives_error(self):
        """Tests that non-numeric max_cycle gives ValueError"""

        with self.assertRaises(ValueError) as E:
            SensorSite("SITE_ID", "ONE")


class TestSensorSiteOperation(unittest.IsolatedAsyncioTestCase):
    """Tests the active behaviour of SensorSite objects."""

    @pytest.mark.asyncio
    async def test_run_stops_after_max_cycles(self):
        """Ensures .run() method breaks after max_cycles"""

        site = SensorSite("TestSite", 5)
        await site.run()

        self.assertEqual(site.cycle, site.max_cycles)

    @pytest.mark.asyncio
    async def test_multi_instances_stop_at_max_cycles(self):
        """Ensures .run() method breaks after max_cycles for multiple instances"""

        max_cycles = [5, 6, 7]

        sites = [SensorSite(f"Site {i}", max_cycles[i]) for i in range(len(max_cycles))]

        await asyncio.gather(*[x.run() for x in sites])

        for i, site in enumerate(sites):
            self.assertEqual(site.cycle, max_cycles[i])


if __name__ == "__main__":
    unittest.main()
