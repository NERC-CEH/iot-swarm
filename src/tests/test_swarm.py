import unittest
import pytest
from parameterized import parameterized
from iotdevicesimulator.swarm import CosmosSwarm
from iotdevicesimulator.db import Oracle
from iotdevicesimulator.devices import SensorSite
from iotdevicesimulator.queries import CosmosQuery, CosmosSiteQuery
from iotdevicesimulator.messaging.core import MockMessageConnection

from pathlib import Path
from config import Config

CONFIG_PATH = Path(
    Path(__file__).parents[1], "iotdevicesimulator", "__assets__", "config.cfg"
)
config_exists = pytest.mark.skipif(
    not CONFIG_PATH.exists(),
    reason="Config file `config.cfg` not found in root directory.",
)


class TestCosmosSwarm(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.config_path = str(CONFIG_PATH)
        self.config = Config(self.config_path)["oracle"]

    @parameterized.expand(
        [
            ["ALIC1", CosmosQuery.LEVEL_1_SOILMET_30MIN, 1, 0, 0],
            [["ALIC1"], CosmosQuery.LEVEL_1_NMDB_1HOUR, 10, 1, 2],
            [["ALIC1", "PORTN"], CosmosQuery.LEVEL_1_PRECIP_1MIN, 12.2, 45, 8],
        ]
    )
    @pytest.mark.asyncio
    @config_exists
    @pytest.mark.oracle
    async def test_instantiation(
        self, site_ids, query, sleep_time, max_cycles, max_sites
    ):
        swarm = await CosmosSwarm.create(
            query,
            MockMessageConnection(),
            self.config,
            site_ids=site_ids,
            sleep_time=sleep_time,
            max_cycles=max_cycles,
            max_sites=max_sites,
        )

        for site, site_id in zip(swarm.sites, site_ids):
            self.assertEqual(site.site_id, site_id)
            self.assertEqual(site.max_cycles, int(max_cycles))
            self.assertEqual(site.sleep_time, int(sleep_time))

        self.assertEqual(swarm.max_cycles, int(max_cycles))
        self.assertEqual(swarm.sleep_time, int(sleep_time))
        self.assertEqual(swarm.max_sites, max_sites)

    @pytest.mark.asyncio
    @config_exists
    @pytest.mark.oracle
    async def test_config_from_dict(self):
        await CosmosSwarm.create(
            CosmosQuery.LEVEL_1_NMDB_1HOUR, MockMessageConnection(), self.config
        )

    @pytest.mark.asyncio
    @config_exists
    @pytest.mark.oracle
    async def test_config_from_path(self):
        await CosmosSwarm.create(
            CosmosQuery.LEVEL_1_NMDB_1HOUR, MockMessageConnection(), self.config_path
        )

    @pytest.mark.asyncio
    @config_exists
    @pytest.mark.oracle
    async def test_delay_set(self):
        query = CosmosQuery.LEVEL_1_SOILMET_30MIN
        swarm = await CosmosSwarm.create(
            query,
            MockMessageConnection(),
            self.config,
            site_ids="MORLY",
            delay_first_cycle=True,
        )

        self.assertTrue(swarm.delay_first_cycle)

        swarm = await CosmosSwarm.create(
            query,
            MockMessageConnection(),
            self.config,
            site_ids="MORLY",
            delay_first_cycle=False,
        )

        self.assertFalse(swarm.delay_first_cycle)

    @pytest.mark.asyncio
    @config_exists
    @pytest.mark.oracle
    async def test_error_if_delay_set_not_bool(self):
        query = CosmosQuery.LEVEL_1_SOILMET_30MIN

        with self.assertRaises(TypeError):
            await CosmosSwarm.create(
                query,
                MockMessageConnection(),
                self.config,
                site_ids="MORLY",
                delay_first_cycle=4,
            )

    @pytest.mark.asyncio
    @config_exists
    @pytest.mark.oracle
    async def test_swarm_name_given(self):
        query = CosmosQuery.LEVEL_1_SOILMET_30MIN
        swarm = await CosmosSwarm.create(
            query, MockMessageConnection(), self.config, swarm_name="myswarm"
        )

        self.assertEqual(swarm.swarm_name, "myswarm")

    @pytest.mark.asyncio
    @config_exists
    @pytest.mark.oracle
    async def test_swarm_name_not_given(self):
        query = CosmosQuery.LEVEL_1_SOILMET_30MIN
        swarm = await CosmosSwarm.create(query, MockMessageConnection(), self.config)

        self.assertIsInstance(swarm.swarm_name, str)

    @parameterized.expand([0, 1, 10, 35.52])
    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_good_max_sites(self, max_sites):
        sites = [f"Site {x}" for x in range(10)]

        swarm = await CosmosSwarm.create(
            CosmosQuery.LEVEL_1_SOILMET_30MIN,
            MockMessageConnection(),
            self.config,
            site_ids=sites,
            max_sites=max_sites,
        )

        expected_length = int(max_sites)

        if max_sites == 0:
            expected_length = len(sites)

        if expected_length > len(sites):
            expected_length = len(sites)

        self.assertEqual(len(swarm), expected_length)

    @parameterized.expand(["Four", -3, -1])
    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_bad_max_sites(self, max_sites):
        sites = [f"Site {x}" for x in range(10)]

        with self.assertRaises(ValueError):
            await CosmosSwarm.create(
                CosmosQuery.LEVEL_1_SOILMET_30MIN,
                MockMessageConnection(),
                self.config,
                site_ids=sites,
                max_sites=max_sites,
            )

    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_get_oracle(self):
        oracle = await CosmosSwarm._get_oracle(self.config)

        self.assertIsInstance(oracle, Oracle)

    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_site_ids_from_db(self):
        query1 = CosmosSiteQuery.LEVEL_1_NMDB_1HOUR
        query2 = CosmosSiteQuery.LEVEL_1_SOILMET_30MIN

        oracle = await CosmosSwarm._get_oracle(self.config)
        sites1 = await CosmosSwarm._init_sites_from_db(oracle, query1)
        sites2 = await CosmosSwarm._init_sites_from_db(oracle, query2)

        for site in sites1:
            self.assertIsInstance(site, SensorSite)
            self.assertGreater(len(site.site_id), 1)

        self.assertNotEqual(len(sites1), len(sites2))


class TestCosmosSwarmStatic(unittest.TestCase):
    @parameterized.expand([-1, 1, 3, 5, 7.2])
    def test_list_restriction_method(self, max_count):

        list_in = list(range(20))

        list_out = CosmosSwarm._random_list_items(list_in, max_count)

        expected_length = int(max_count)

        if max_count == -1:
            expected_length = len(list_in)

        self.assertEqual(len(list_out), expected_length)

    @parameterized.expand(["one", -2])
    def test_list_restriction_method_error(self, max_count):

        list_in = list(range(20))

        with self.assertRaises(ValueError):
            CosmosSwarm._random_list_items(list_in, max_count)


if __name__ == "__main__":
    unittest.main()
