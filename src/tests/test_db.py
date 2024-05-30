import unittest
import pytest
import config
import pathlib
from iotdevicesimulator import db
from iotdevicesimulator.queries import CosmosQuery, CosmosSiteQuery
from parameterized import parameterized

CONFIG_PATH = pathlib.Path(
    pathlib.Path(__file__).parents[1], "iotdevicesimulator", "__assets__", "config.cfg"
)
config_exists = pytest.mark.skipif(
    not CONFIG_PATH.exists(),
    reason="Config file `config.cfg` not found in root directory.",
)


class TestOracleDB(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        cred_path = str(CONFIG_PATH)
        creds = config.Config(cred_path)["oracle"]

        self.oracle = await db.Oracle.create(
            creds["dsn"],
            creds["user"],
            password=creds["password"],
        )

    async def asyncTearDown(self) -> None:
        await self.oracle.connection.close()

    @pytest.mark.oracle
    @pytest.mark.asyncio
    @config_exists
    async def test_instantiation(self):

        self.assertIsInstance(self.oracle, db.Oracle)

    @pytest.mark.oracle
    @pytest.mark.asyncio
    @config_exists
    async def test_latest_data_query(self):

        site_id = "MORLY"
        query = CosmosQuery.LEVEL_1_SOILMET_30MIN

        row = await self.oracle.query_latest_from_site(site_id, query)

        self.assertEqual(row["SITE_ID"], site_id)

    @pytest.mark.oracle
    @pytest.mark.asyncio
    @pytest.mark.slow
    @config_exists
    async def test_site_id_query(self):

        queries = [
            CosmosSiteQuery.LEVEL_1_NMDB_1HOUR,
            CosmosSiteQuery.LEVEL_1_SOILMET_30MIN,
            CosmosSiteQuery.LEVEL_1_PRECIP_1MIN,
            CosmosSiteQuery.LEVEL_1_PRECIP_RAINE_1MIN,
        ]

        for query in queries:
            sites = await self.oracle.query_site_ids(query)

            self.assertIsInstance(sites, list)

            for site in sites:
                self.assertIsInstance(site, str)
                self.assertGreater(len(site), 1)

            self.assertNotEqual(len(sites), 0)

    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_bad_latest_data_query_type(self):

        site_id = "MORLY"
        query = "sql injection goes brr"

        with self.assertRaises(TypeError):
            await self.oracle.query_latest_from_site(site_id, query)
            await self.oracle.query_site_ids(query)


if __name__ == "__main__":
    unittest.main()
