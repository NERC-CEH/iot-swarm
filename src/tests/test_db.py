import unittest
import pytest
import config
import pathlib
import threading
from typing import Awaitable
import asyncio
from iotdevicesimulator import db
from iotdevicesimulator.queries import CosmosQuery
import oracledb

CONFIG_PATH = pathlib.Path(pathlib.Path(__file__).parents[2], "config.cfg")


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
    async def test_query(self):

        site_id = "MORLY"
        query = CosmosQuery.LEVEL_1_SOILMET_30MIN

        row = await self.oracle.query_latest_from_site(site_id, query)

        self.assertEqual(row["SITE_ID"], site_id)

    @pytest.mark.asyncio
    @config_exists
    async def test_bad_query_type(self):

        site_id = "MORLY"
        query = "sql injection goes brr"

        with self.assertRaises(TypeError):
            await self.oracle.query_latest_from_site(site_id, query)


if __name__ == "__main__":
    unittest.main()
