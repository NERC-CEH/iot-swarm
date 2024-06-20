import unittest
import pytest
import config
from pathlib import Path
from iotswarm import db
from iotswarm.devices import BaseDevice
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.queries import CosmosTable
from iotswarm.swarm import Swarm
from parameterized import parameterized
import logging
from unittest.mock import patch
import pandas as pd
from glob import glob
from math import isnan
import sqlite3

CONFIG_PATH = Path(
    Path(__file__).parents[1], "iotswarm", "__assets__", "config.cfg"
)
config_exists = pytest.mark.skipif(
    not CONFIG_PATH.exists(),
    reason="Config file `config.cfg` not found in root directory.",
)

COSMOS_TABLES = list(CosmosTable)
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
            [None, logging.getLogger("iotswarm.db")],
            [logging.getLogger("name"), logging.getLogger("name")],
        ]
    )
    def test_logger_set(self, logger, expected):

        inst = db.MockDB(inherit_logger=logger)

        self.assertEqual(inst._instance_logger.parent, expected)

    
    def test__repr__no_logger(self):

        inst = db.MockDB()

        self.assertEqual(inst.__repr__(), "MockDB()")

    def test__repr__logger_given(self):
        logger = logging.getLogger("testdblogger")
        logger.setLevel(logging.CRITICAL)

        expected = "MockDB(inherit_logger=<Logger testdblogger (CRITICAL)>)"

        mock = db.MockDB(inherit_logger=logger)
        self.assertEqual(mock.__repr__(), expected)

        
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
        self.table = CosmosTable.LEVEL_1_SOILMET_30MIN

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

        row = await self.oracle.query_latest_from_site(site_id, self.table)

        self.assertEqual(row["SITE_ID"], site_id)

    @parameterized.expand(COSMOS_TABLES)
    @pytest.mark.oracle
    @pytest.mark.asyncio
    @pytest.mark.slow
    @config_exists
    async def test_site_id_query(self, table):

        sites = await self.oracle.query_site_ids(table)

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

        sites = await self.oracle.query_site_ids(self.table, max_sites=max_sites)

        self.assertEqual(len(sites), max_sites)


    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_bad_latest_data_table_type(self):

        site_id = "MORLY"
        table = "sql injection goes brr"

        with self.assertRaises(TypeError):
            await self.oracle.query_latest_from_site(site_id, table)

    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_bad_site_table_type(self):

        table = "sql injection goes brr"

        with self.assertRaises(TypeError):
            await self.oracle.query_site_ids(table)

    @parameterized.expand([-1, -100, "STRING"])
    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test_bad_site_table_max_sites_type(self, max_sites):
        """Tests bad values for max_sites."""

        with self.assertRaises((TypeError,ValueError)):
            await self.oracle.query_site_ids(self.table, max_sites=max_sites)

    @pytest.mark.asyncio
    @pytest.mark.oracle
    @config_exists
    async def test__repr__(self):
        """Tests string representation."""

        oracle1 = await db.Oracle.create(
            self.creds["dsn"],
            self.creds["user"],
            password=self.creds["password"],
        )
        expected1 = f"Oracle(\"{self.creds["dsn"]}\")"

        self.assertEqual(
            oracle1.__repr__(), expected1
        )

        oracle2 = await db.Oracle.create(
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

CSV_PATH = Path(Path(__file__).parents[1], "iotswarm", "__assets__", "data")
CSV_DATA_FILES = [Path(x) for x in glob(str(Path(CSV_PATH, "*.csv")))]
sqlite_db_exist = pytest.mark.skipif(not Path(CSV_PATH, "cosmos.db").exists(), reason="Local cosmos.db does not exist.")

data_files_exist = pytest.mark.skipif(
    not CSV_PATH.exists() or len(CSV_DATA_FILES) == 0,
    reason="No data files are present"
)

class TestLoopingCsvDB(unittest.TestCase):
    """Tests the LoopingCsvDB class."""

    def setUp(self):
        self.data_path = {v.name.removesuffix("_DATA_TABLE.csv"):v for v in CSV_DATA_FILES}

        self.soilmet_table = db.LoopingCsvDB(self.data_path["LEVEL_1_SOILMET_30MIN"])
        self.maxDiff = None
    
    @data_files_exist
    @pytest.mark.slow
    def test_instantiation(self):
        """Tests that the database can be instantiated."""

        database = self.soilmet_table

        self.assertIsInstance(database, db.LoopingCsvDB)
        self.assertIsInstance(database, db.BaseDatabase)


        self.assertIsInstance(database.cache, dict)
        self.assertIsInstance(database.connection, pd.DataFrame)

    @data_files_exist
    @pytest.mark.slow
    def test_site_data_return_value(self):
        database = self.soilmet_table

        site = "MORLY"

        data = database.query_latest_from_site(site)

        expected_cache = {site: 1}
        self.assertDictEqual(database.cache, expected_cache)

        self.assertIsInstance(data, dict)
    
    @data_files_exist
    @pytest.mark.slow
    def test_multiple_sites_added_to_cache(self):
        sites = ["ALIC1", "MORLY", "HOLLN","EUSTN"]

        database = self.soilmet_table

        data = [database.query_latest_from_site(x) for x in sites]
        
        for i, site in enumerate(sites):
            self.assertEqual(site, data[i]["SITE_ID"])
            self.assertIn(site, database.cache)
            self.assertEqual(database.cache[site], 1)

    @data_files_exist
    @pytest.mark.slow
    def test_cache_incremented_on_each_request(self):
        database = self.soilmet_table

        site = "MORLY"

        expected = 1

        last_data = None
        for _ in range(10):
            data = database.query_latest_from_site(site)
            self.assertNotEqual(last_data, data)
            self.assertEqual(expected, database.cache[site])
            
            last_data = data
            expected += 1
        
        self.assertEqual(expected, 11)

    @data_files_exist
    @pytest.mark.slow
    def test_cache_counter_restarts_at_end(self):

        short_table_path = Path(Path(__file__).parent, "data", "ALIC1_4_ROWS.csv")
        database = db.LoopingCsvDB(short_table_path)

        site = "ALIC1"

        expected = [1,2,3,4,1]
        data = []
        for e in expected:
            data.append(database.query_latest_from_site(site))

            self.assertEqual(database.cache[site], e)

        for key in data[0].keys():
            try:
                self.assertEqual(data[0][key], data[-1][key])
            except AssertionError as err:
                if not isnan(data[0][key]) and isnan(data[-1][key]):
                    raise(err)

        self.assertEqual(len(expected), len(data))

    @data_files_exist
    @pytest.mark.slow
    def test_site_ids_can_be_retrieved(self):
        database = self.soilmet_table

        site_ids_full = database.query_site_ids()
        site_ids_exp_full = database.query_site_ids(max_sites=0)


        self.assertIsInstance(site_ids_full, list)

        self.assertGreater(len(site_ids_full), 0)
        for site in site_ids_full:
            self.assertIsInstance(site, str)

        self.assertEqual(len(site_ids_full), len(site_ids_exp_full))

        site_ids_limit = database.query_site_ids(max_sites=5)

        self.assertEqual(len(site_ids_limit), 5)
        self.assertGreater(len(site_ids_full), len(site_ids_limit))

        with self.assertRaises(ValueError):

            database.query_site_ids(max_sites=-1)

class TestLoopingCsvDBEndToEnd(unittest.IsolatedAsyncioTestCase):
    """Tests the LoopingCsvDB class."""

    def setUp(self):
        self.data_path = {v.name.removesuffix("_DATA_TABLE.csv"):v for v in CSV_DATA_FILES}
        self.maxDiff = None

    @data_files_exist
    @pytest.mark.slow
    async def test_flow_with_device_attached(self):
        """Tests that data is looped through with a device making requests."""

        database = db.LoopingCsvDB(self.data_path["LEVEL_1_SOILMET_30MIN"])
        device = BaseDevice("ALIC1", database, MockMessageConnection(), sleep_time=0, max_cycles=5)

        await device.run()

        self.assertDictEqual(database.cache, {"ALIC1": 5})

    @data_files_exist
    @pytest.mark.slow
    async def test_flow_with_swarm_attached(self):
        """Tests that the database is looped through correctly with multiple sites in a swarm."""
        
        database = db.LoopingCsvDB(self.data_path["LEVEL_1_SOILMET_30MIN"])
        sites = ["MORLY", "ALIC1", "EUSTN"]
        cycles = [1, 4, 6]
        devices = [
            BaseDevice(s, database, MockMessageConnection(), sleep_time=0, max_cycles=c)
            for (s,c) in zip(sites, cycles)
            ]
        
        swarm = Swarm(devices)

        await swarm.run()

        self.assertDictEqual(database.cache, {"MORLY": 1, "ALIC1": 4, "EUSTN": 6})

class TestSqliteDB(unittest.TestCase):

    @sqlite_db_exist
    def setUp(self):
        self.db_path = Path(Path(__file__).parents[1], "iotswarm", "__assets__", "data", "cosmos.db")
        self.table = CosmosTable.LEVEL_1_SOILMET_30MIN
        
        if self.db_path.exists():
            self.database = db.LoopingSQLite3(self.db_path)
        self.maxDiff = None
    
    @sqlite_db_exist
    def test_instantiation(self):
        self.assertIsInstance(self.database, db.LoopingSQLite3)
        self.assertIsInstance(self.database.connection, sqlite3.Connection)

    @sqlite_db_exist
    def test_latest_data(self):

        site_id = "MORLY"

        data = self.database.query_latest_from_site(site_id, self.table)

        self.assertIsInstance(data, dict)

    @sqlite_db_exist
    def test_site_id_query(self):

        sites = self.database.query_site_ids(self.table)

        self.assertGreater(len(sites), 0)

        self.assertIsInstance(sites, list)

        for site in sites:
             self.assertIsInstance(site, str)

    @sqlite_db_exist
    def test_multiple_sites_added_to_cache(self):
        sites = ["ALIC1", "MORLY", "HOLLN","EUSTN"]

        data = [self.database.query_latest_from_site(x, self.table) for x in sites]
        
        for i, site in enumerate(sites):
            self.assertEqual(site, data[i]["SITE_ID"])
            self.assertIn(site, self.database.cache)
            self.assertEqual(self.database.cache[site], 0)
    
    @sqlite_db_exist
    def test_cache_incremented_on_each_request(self):
        site = "MORLY"

        last_data = {}
        for i in range(3):
            if i == 0:
                self.assertEqual(self.database.cache, {})
            else:
                self.assertEqual(i-1, self.database.cache[site])
            data = self.database.query_latest_from_site(site, self.table)
            self.assertNotEqual(last_data, data)
            
            last_data = data
    
    @sqlite_db_exist
    def test_cache_counter_restarts_at_end(self):
        database = db.LoopingSQLite3(Path(Path(__file__).parent, "data", "database.db"))

        site = "ALIC1"

        expected = [0,1,2,3,0]
        data = []
        for e in expected:
            data.append(database.query_latest_from_site(site, self.table))

            self.assertEqual(database.cache[site], e)

        self.assertEqual(data[0], data[-1])

        self.assertEqual(len(expected), len(data))

class TestLoopingSQLite3DBEndToEnd(unittest.IsolatedAsyncioTestCase):
    """Tests the LoopingCsvDB class."""

    @sqlite_db_exist
    def setUp(self):
        self.db_path = Path(Path(__file__).parents[1], "iotswarm", "__assets__", "data", "cosmos.db")
        if self.db_path.exists():
            self.database = db.LoopingSQLite3(self.db_path)
        self.maxDiff = None
        self.table = CosmosTable.LEVEL_1_PRECIP_1MIN

    @sqlite_db_exist
    async def test_flow_with_device_attached(self):
        """Tests that data is looped through with a device making requests."""

        device = BaseDevice("ALIC1", self.database, MockMessageConnection(), table=self.table, sleep_time=0, max_cycles=5)

        await device.run()

        self.assertDictEqual(self.database.cache, {"ALIC1": 4})

    @sqlite_db_exist
    async def test_flow_with_swarm_attached(self):
        """Tests that the database is looped through correctly with multiple sites in a swarm."""
        
        sites = ["MORLY", "ALIC1", "EUSTN"]
        cycles = [1, 2, 3]
        devices = [
            BaseDevice(s, self.database, MockMessageConnection(), sleep_time=0, max_cycles=c,table=self.table)
            for (s,c) in zip(sites, cycles)
            ]
        
        swarm = Swarm(devices)

        await swarm.run()

        self.assertDictEqual(self.database.cache, {"MORLY": 0, "ALIC1": 1, "EUSTN": 2})


if __name__ == "__main__":
    unittest.main()
