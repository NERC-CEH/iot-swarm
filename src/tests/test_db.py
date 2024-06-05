import unittest
import pytest
import config
import pathlib
from iotdevicesimulator import db
from iotdevicesimulator.queries import CosmosQuery, CosmosSiteQuery
from parameterized import parameterized
import logging
from unittest.mock import patch

CONFIG_PATH = pathlib.Path(
    pathlib.Path(__file__).parents[1], "iotdevicesimulator", "__assets__", "config.cfg"
)
config_exists = pytest.mark.skipif(
    not CONFIG_PATH.exists(),
    reason="Config file `config.cfg` not found in root directory.",
)

class TestBaseDatabase(unittest.TestCase):

    @patch.multiple(db.BaseDatabase, __abstractmethods__=set())
    def test_instantiation(self):

        inst = db.BaseDatabase()

        self.assertIsNotNone(inst._instance_logger)

        self.assertIsNone(inst.query_latest_from_site())

class TestMockDB(unittest.TestCase):

    def test_instantiation(self):
        """Tests that the object can be instantiated"""

        inst = db.MockDB()

        self.assertIsInstance(inst, db.MockDB)

    def test_latest_query(self):
        """Tests that the query function returns an empty list."""

        inst = db.MockDB()

        result = inst.query_latest_from_site()

        self.assertEqual(len(result), 0)
        self.assertIsInstance(result, list)

    @parameterized.expand(
        [
            [None, logging.getLogger("iotdevicesimulator.db")],
            [logging.getLogger("name"), logging.getLogger("name")],
        ]
    )
    def test_logger_set(self, logger, expected):

        inst = db.MockDB(inherit_logger=logger)

        self.assertEqual(inst._instance_logger.parent, expected)

    @parameterized.expand(
        [
            [None, "MockDB()"],
            [
                logging.getLogger("notroot"),
                "MockDB(inherit_logger=<Logger notroot (WARNING)>)",
            ],
        ]
    )
    def test__repr__(self, logger, expected):

        inst = db.MockDB(inherit_logger=logger)

        self.assertEqual(inst.__repr__(), expected)


class TestOracleDB(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        cred_path = str(CONFIG_PATH)
        creds = config.Config(cred_path)["oracle"]
        self.creds = creds

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

    @parameterized.expand([
            CosmosSiteQuery.LEVEL_1_NMDB_1HOUR,
            CosmosSiteQuery.LEVEL_1_SOILMET_30MIN,
            CosmosSiteQuery.LEVEL_1_PRECIP_1MIN,
            CosmosSiteQuery.LEVEL_1_PRECIP_RAINE_1MIN,
        ])
    @pytest.mark.oracle
    @pytest.mark.asyncio
    @pytest.mark.slow
    @config_exists
    async def test_site_id_query(self,query):

        sites = await self.oracle.query_site_ids(query)

        self.assertIsInstance(sites, list)

        for site in sites:
            self.assertIsInstance(site, str)
            self.assertGreater(len(site), 1)

        self.assertNotEqual(len(sites), 0)

    @parameterized.expand([1, 5 , 7])
    @pytest.mark.oracle
    @pytest.mark.asyncio
    @config_exists
    async def test_site_id_query_max_sites(self, max_sites):

        query = CosmosSiteQuery.LEVEL_1_SOILMET_30MIN

        sites = await self.oracle.query_site_ids(query, max_sites=max_sites)

        self.assertEqual(len(sites), max_sites)


    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_bad_latest_data_query_type(self):

        site_id = "MORLY"
        query = "sql injection goes brr"

        with self.assertRaises(TypeError):
            await self.oracle.query_latest_from_site(site_id, query)

    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_bad_site_query_type(self):

        query = "sql injection goes brr"

        with self.assertRaises(TypeError):
            await self.oracle.query_site_ids(query)

    @parameterized.expand([-1, -100, "STRING"])
    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_bad_site_query_max_sites_type(self, max_sites):
        """Tests bad values for max_sites."""

        with self.assertRaises((TypeError,ValueError)):
            await self.oracle.query_site_ids(CosmosSiteQuery.LEVEL_1_SOILMET_30MIN, max_sites=max_sites)

    async def test__repr__(self):
        """Tests string representation."""

        oracle1 = self.oracle = await db.Oracle.create(
            self.creds["dsn"],
            self.creds["user"],
            password=self.creds["password"],
        )
        expected1 = f"Oracle(\"{self.creds["dsn"]}\")"

        self.assertEqual(
            oracle1.__repr__(), expected1
        )

        oracle2 = self.oracle = await db.Oracle.create(
            self.creds["dsn"],
            self.creds["user"],
            password=self.creds["password"],
            inherit_logger=logging.getLogger("test")
        )

        expected2 = (
                f"Oracle(\"{self.creds["dsn"]}\""
                f", inherit_logger={logging.getLogger("test")})"
            )
        self.assertEqual(
            oracle2.__repr__(), expected2
        )


if __name__ == "__main__":
    unittest.main()
