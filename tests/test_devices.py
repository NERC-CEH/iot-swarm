import unittest
import asyncio
import pytest
import logging
import json
from iotswarm.utils import json_serial
from iotswarm.devices import BaseDevice, CR1000XDevice, CR1000XField
from iotswarm.db import Oracle, BaseDatabase, MockDB, LoopingSQLite3
from iotswarm.queries import CosmosQuery, CosmosTable
from iotswarm.messaging.core import MockMessageConnection, MessagingBaseClass
from iotswarm.messaging.aws import IotCoreMQTTConnection
from parameterized import parameterized
from unittest.mock import patch
from pathlib import Path
import config
from datetime import datetime, timedelta

CONFIG_PATH = Path(
    Path(__file__).parents[1], "src", "iotswarm", "__assets__", "config.cfg"
)
config_exists = pytest.mark.skipif(
    not CONFIG_PATH.exists(),
    reason="Config file `config.cfg` not found in root directory.",
)

DATA_DIR = Path(Path(__file__).parents[1], "src", "iotswarm", "__assets__", "data")
sqlite_db_exist = pytest.mark.skipif(not Path(DATA_DIR, "cosmos.db").exists(), reason="Local cosmos.db does not exist.")



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
            [0, 0, None, 5, (0, 0, False)],
            [100, 30, True, 9, (100, 30, True)],
            [12.4, 30001.9, None, 100, (12, 30001, False)],
        ]
    )
    def test_optional_args_set(self, max_cycles, sleep_time, delay_start,no_send_probability, expected):
        inst = BaseDevice(
            "TEST_ID",
            self.data_source,
            self.connection,
            max_cycles=max_cycles,
            sleep_time=sleep_time,
            delay_start=delay_start,
            no_send_probability=no_send_probability
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
            [
                MockDB(),
                {"max_cycles": 4, "sleep_time": 5, "delay_start": True, "no_send_probability":10},
                'BaseDevice("TEST_ID", MockDB(), MockMessageConnection(), sleep_time=5, max_cycles=4, delay_start=True, no_send_probability=10)',
            ],
        ]
    )
    def test__repr__(self, data_source, kwargs, expected):
        instance = BaseDevice("TEST_ID", data_source, self.connection, **kwargs)

        self.assertEqual(repr(instance), expected)

    def test_table_not_set_for_non_oracle_db(self):
        
        inst = BaseDevice("test", MockDB(), MockMessageConnection(), table=CosmosTable.LEVEL_1_NMDB_1HOUR)

        with self.assertRaises(AttributeError):
            inst.table

    @parameterized.expand(
        [
            [None, logging.getLogger("iotswarm.devices")],
            [logging.getLogger("name"), logging.getLogger("name")],
        ]
    )
    def test_logger_set(self, logger, expected):

        inst = BaseDevice(
            "site", self.data_source, self.connection, inherit_logger=logger
        )

        self.assertEqual(inst._instance_logger.parent, expected)

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
        self.assertEqual(inst.mqtt_topic, f"{topic}/site")

    @parameterized.expand(["this/topic", "1/1/1", "TOPICO!"])
    @config_exists
    def test_mqtt_suffix_set(self, topic):
        """Tests that mqtt_suffix gets set"""

        inst = BaseDevice("site", self.db, self.conn, mqtt_suffix=topic)

        self.assertEqual(inst.mqtt_suffix, topic)
        self.assertEqual(inst.mqtt_topic, f"site/{topic}")

    @parameterized.expand([["this/prefix", "this/suffix"], ["1/1/1", "2/2/2"], ["TOPICO!", "FOUR"]])
    @config_exists
    def test_mqtt_prefix_and_suffix_set(self, prefix, suffix):
        """Tests that mqtt_suffix gets set"""

        inst = BaseDevice("site", self.db, self.conn, mqtt_prefix=prefix, mqtt_suffix=suffix)

        self.assertEqual(inst.mqtt_suffix, suffix)
        self.assertEqual(inst.mqtt_prefix, prefix)
        self.assertEqual(inst.mqtt_topic, f"{prefix}/site/{suffix}")

    @config_exists
    def test_default_mqtt_topic_set(self):
        """Tests that default topic gets set"""

        inst = BaseDevice("site-12", self.db, self.conn)

        self.assertEqual(inst.mqtt_topic, "site-12")
    
    @parameterized.expand([
        [None, None, None, ""],
        ["topic", None, None, ', mqtt_topic="topic"'],
        ["topic", "prefix", None, ', mqtt_topic="topic", mqtt_prefix="prefix"'],
        ["topic", "prefix", "suffix",', mqtt_topic="topic", mqtt_prefix="prefix", mqtt_suffix="suffix"'],
    ])
    @config_exists
    def test__repr__mqtt_opts_mqtt_connection(self, topic, prefix, suffix,expected_args):
        """Tests that the __repr__ method returns correctly with MQTT options set."""
        expected = f'BaseDevice("site-id", {str(MockDB())}, {str(self.conn)}{expected_args})'
        inst = BaseDevice("site-id", MockDB(), self.conn, mqtt_topic=topic, mqtt_prefix=prefix, mqtt_suffix=suffix)

        self.assertEqual(inst.__repr__(), expected)

class TestProbabilitySend(unittest.TestCase):
    def setUp(self):
        self.data_source = MockDB()
        self.connection = MockMessageConnection()

    @parameterized.expand([0, 10, 20, 50, 81, 100])
    def test_probability_send(self, probability):
        device = BaseDevice("ID", self.data_source, self.connection, no_send_probability=probability)

        skipped = 0

        for i in range(10000):
            if device._skip_send():
                skipped += 1
        
        self.assertAlmostEqual(skipped/100, probability, delta=1)
    
    def test_probability_zero_if_not_given(self):
        device = BaseDevice("ID", self.data_source, self.connection)

        self.assertEqual(device.no_send_probability, 0)

    @parameterized.expand(["Four", None])
    def test_probability_bad_values(self, value):

        with self.assertRaises((TypeError, ValueError)):
            device = BaseDevice("ID", self.data_source, self.connection, no_send_probability=value)

    @parameterized.expand([[2,2], [0,0], [27.34, 27], [99.5, 100]])
    def test_probability_set(self, value, expected):
        device = BaseDevice("ID", self.data_source, self.connection, no_send_probability=value)

        self.assertEqual(device.no_send_probability, expected)


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
        self.table = CosmosTable.LEVEL_1_SOILMET_30MIN
    async def asyncTearDown(self) -> None:
        await self.oracle.connection.close()
    
    @parameterized.expand([-1, -423.78, CosmosQuery.ORACLE_LATEST_DATA, "Four", MockDB(), {"a": 1}])
    @config_exists
    @pytest.mark.oracle
    def test_table_value_check(self, table):

        with self.assertRaises(TypeError):
            BaseDevice(
                "test_id", self.oracle, MockMessageConnection(), table=table
            )

    @pytest.mark.oracle
    @config_exists
    async def test_error_if_table_not_given(self):

        with self.assertRaises(ValueError):
            BaseDevice("site", self.oracle, MockMessageConnection())


        inst = BaseDevice("site", self.oracle, MockMessageConnection(), table=self.table)

        self.assertEqual(inst.table, self.table)


    @pytest.mark.oracle
    @config_exists
    async def test__repr__oracle_data(self):

        inst_oracle = BaseDevice("site", self.oracle, MockMessageConnection(), table=self.table)
        exp_oracle = f'BaseDevice("site", Oracle("{self.creds['dsn']}"), MockMessageConnection(), table=CosmosTable.{self.table.name})'
        self.assertEqual(inst_oracle.__repr__(), exp_oracle)

        inst_not_oracle = BaseDevice("site", MockDB(), MockMessageConnection(), table=self.table)
        exp_not_oracle = 'BaseDevice("site", MockDB(), MockMessageConnection())'
        self.assertEqual(inst_not_oracle.__repr__(), exp_not_oracle)

        with self.assertRaises(AttributeError):
            inst_not_oracle.table

    @pytest.mark.oracle
    @config_exists
    async def test__get_payload(self):
        """Tests that Cosmos payload retrieved."""

        inst = BaseDevice("MORLY", self.oracle, MockMessageConnection(), table=self.table)
        print(inst)
        payload = await inst._get_payload()

        self.assertIsInstance(payload, dict)

class TestBaseDevicesSQLite3Used(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self):
        db_path = Path(Path(__file__).parents[1], "src", "iotswarm","__assets__", "data", "cosmos.db")
        if db_path.exists():
            self.db = LoopingSQLite3(db_path)
        self.table = CosmosTable.LEVEL_1_SOILMET_30MIN
    
    @parameterized.expand([-1, -423.78, CosmosQuery.ORACLE_LATEST_DATA, "Four", MockDB(), {"a": 1}])
    @sqlite_db_exist
    def test_table_value_check(self, table):
        with self.assertRaises(TypeError):
            BaseDevice(
                "test_id", self.db, MockMessageConnection(), table=table
            )

    @sqlite_db_exist
    def test_error_if_table_not_given(self):

        with self.assertRaises(ValueError):
            BaseDevice("site", self.db, MockMessageConnection())


        inst = BaseDevice("site", self.db, MockMessageConnection(), table=self.table)

        self.assertEqual(inst.table, self.table)

    @sqlite_db_exist
    async def test__get_payload(self):
        """Tests that Cosmos payload retrieved."""

        inst = BaseDevice("MORLY", self.db, MockMessageConnection(), table=self.table)

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
            expected = "INFO:mylogger.BaseDevice-site:Message sent to topic: site"
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
            expected = "DEBUG:iotswarm.devices.BaseDevice-site:Requesting payload submission."
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


class TestCr1000xDevice(unittest.TestCase):
    """Test suite for the CR1000X Device."""

    def setUp(self):
        self.db = MockDB()
        self.conn = MockMessageConnection()
        self.maxDiff = None

    
    def test_instantiation(self):
        """Tests that object can be instantiated."""

        inst = CR1000XDevice("device", self.db, self.conn)

        self.assertIsInstance(inst, CR1000XDevice)
        self.assertIsInstance(inst, BaseDevice)
        self.assertEqual(inst.device_type, "CR1000X")

    @parameterized.expand([123456, "newserial", 123.2])
    def test_serial_number_set(self, arg):
        """Tests that serial number gets set when argument given."""
        inst = CR1000XDevice("device", self.db, self.conn, serial_number=arg)

        self.assertEqual(inst.serial_number, str(arg))

    @parameterized.expand(["test os version", "newserial", 123.2])
    def test_os_version_set(self, arg):
        """Tests that os_version gets set when argument given."""
        inst = CR1000XDevice("device", self.db, self.conn, os_version=arg)

        self.assertEqual(inst.os_version, str(arg))

    @parameterized.expand([123456, "newserial", 123.2])
    def test_program_name_set(self, arg):
        """Tests that program name gets set when argument given."""
        inst = CR1000XDevice("device", self.db, self.conn, program_name=arg)

        self.assertEqual(inst.program_name, str(arg))

    @parameterized.expand([123456, "newserial", 123.2])
    def test_table_name_set(self, arg):
        """Tests that table_name gets set when argument given."""
        inst = CR1000XDevice("device", self.db, self.conn, table_name=arg)

        self.assertEqual(inst.table_name, str(arg))

    @parameterized.expand([
        [
            [1,"data", 0.0, True],
            [[1,"data", 0.0, True]]
        ],
        [
            [[50,"short", -7, False],[1,"data", 0.0, True]],
            [[50,"short", -7, False], [1, "data", 0.0, True]]
        ]
    ])
    def test_list_payload_formatting(self, payload, expected_vals):

        device = CR1000XDevice("my_device", self.db, self.conn)

        keys = [f"_{i}" for i in range(len(expected_vals[0]))]
        collected_vals = []
        for i in range(len(expected_vals[0])):
            collected_vals.append([x[i] for x in expected_vals])

        formatted = device._format_payload(payload)

        expected = {
            "head": {
                "transaction": 0,
                "signature": 111111,
                "environment": {
                    "station_name": device.device_id,
                    "table_name": device.table_name,
                    "model": device.device_type,
                    "serial_no": device.serial_number,
                    "os_version": device.os_version,
                    "prog_name": device.program_name
                },
                "fields": [CR1000XField(k, data_values=v) for k,v in zip(keys, collected_vals)]
            },
            "data": [
                {}
            ]
        }

        self.assertEqual(formatted.keys(), expected.keys(), "payload must have same base keys.")
        self.assertDictEqual(formatted["head"], expected["head"], "head of payload must be equal")
        
        for f_row, e_row in zip(formatted["data"], expected_vals):
            self.assertEqual(list(f_row.keys()), ["time", "vals"], "Data segment must have same keys.")
            self.assertListEqual(f_row["vals"], e_row)
            # Error if not isoformat
            datetime.fromisoformat(f_row["time"])

    @parameterized.expand([
        [
            {"temp": 17.16, "door_open": False, "BattV": int(1e20), "BattLevel": 1e-50},
            [[17.16, False, int(1e20), 1e-50]],
         ],
         [
             [{"temp": 20.0, "door_open": True, "BattV": int(5e20), "BattLevel": 4e-50},
              {"temp": True, "door_open": None, "BattV": int(5), "BattLevel": 1.2},
              {"temp": 17.16, "door_open": False, "BattV": int(1e20), "BattLevel": 1e-50}],
              [[20.0, True, 5e20, 4e-50],[True, None, int(5), 1.2],[17.16, False, int(1e20), 1e-50]],
        ],

         ])
    def test_dict_payload_formatting(self, payload, expected_data_vals):

        device = CR1000XDevice("my_dict_device", self.db, self.conn)

        keys = payload.keys() if isinstance(payload, dict) else payload[0].keys()

        collected_vals = []
        for i in range(len(expected_data_vals[0])):
            collected_vals.append([x[i] for x in expected_data_vals])

        formatted = device._format_payload(payload)

        expected = {
            "head": {
                "transaction": 0,
                "signature": 111111,
                "environment": {
                    "station_name": device.device_id,
                    "table_name": device.table_name,
                    "model": device.device_type,
                    "serial_no": device.serial_number,
                    "os_version": device.os_version,
                    "prog_name": device.program_name
                },
                "fields": [CR1000XField(k, data_values=v) for k,v in zip(keys, collected_vals)]
            },
            "data": {}
        }

        self.assertEqual(formatted.keys(), expected.keys(), "payload must have same base keys.")
        self.assertDictEqual(formatted["head"], expected["head"])

        for f_row, e_row in zip(formatted["data"], expected_data_vals):
            self.assertEqual(list(f_row.keys()), ["time", "vals"], "Data segment must have same keys.")
            self.assertListEqual(f_row["vals"], e_row)
            # Error if not isoformat
            datetime.fromisoformat(f_row["time"])

    @parameterized.expand([
        [{"a": 1, "date_time": datetime.now()}],
        [
            [
                {"a": 1, "date_time": datetime.now()},
                {"a": 2, "date_time": datetime.now()+timedelta(days=1)}
            ]
        ]
    ])
    def test_payload_with_datetime_included(self, payload):
        """Tests that datetime is popped if included in payload"""

        device = CR1000XDevice("my_dict_device", self.db, self.conn)

        formatted = device._format_payload(payload)

        for item in formatted["data"]:

            self.assertIsInstance(item["time"], datetime)

    
    @parameterized.expand([
        [{"a": 1, "date_time": '2024-06-10T10:20:41.540116'}],
        [
            [
                {"a": 1, "date_time": '2024-06-10T10:20:41.540116'},
                {"a": 2, "date_time": '2024-06-10T09:20:41.540116'}
            ]
        ]
    ])
    def test_payload_with_datetime_included(self, payload):
        """Tests that datetime is popped if included in payload"""

        device = CR1000XDevice("my_dict_device", self.db, self.conn)

        formatted = device._format_payload(payload)

        for item in formatted["data"]:

            self.assertIsInstance(item["time"], str)
            datetime.fromisoformat(item["time"])



    @parameterized.expand([
        [1, [[1]]],
        [[1,2,3], [[1,2,3]]],
        [
            [[1,2,3], [4,5,6]], [[1,2,3], [4,5,6]]
        ],
        [{"a": 1, "b": 2}, [[1,2]]],
        [[{"a": 1, "b": 2, "c": 3}], [[1,2,3]]],
        [
            [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}],
            [[1,2,3], [4,5,6]]
        ]
    ])
    def test_payload_data(self, payload, expected):
        device = CR1000XDevice("my_dict_device", self.db, self.conn)
        formatted = device._format_payload(payload)

        for f_row, e_row in zip(formatted["data"], expected):

            self.assertListEqual(f_row["vals"], e_row)
    
    @parameterized.expand([
        [1, [{"_0": 1}]],
        [[1,2,3], [{"_0": 1, "_1": 2, "_2": 3}]],
        [
            [[1,2,3], [4,5,6]],
            [{"_0": 1, "_1": 2, "_2": 3}, {"_0": 4, "_1": 5, "_2": 6}]
        ],
        [{"a": 1}, [{"a": 1}]],
        [
            [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        ]
    ])
    def test_conversion_to_payload(self,values, expected):
        """Test that values can be converted to payload"""
        result = CR1000XDevice._steralize_payload(values)

        self.assertEqual(result, expected)


    def test_format_payload_errors(self):
        """Tests that errors during formatting are raised."""

        device = CR1000XDevice("my_dict_device", self.db, self.conn)

        bad_key_payload = [
            {"a":1, "b":2, "c":3},
            {"a":4}
        ]
        with self.assertRaises(ValueError):
            device._format_payload(bad_key_payload)


    @parameterized.expand([
        ["ABCDE", "65-66-67-68-69"],
        ["ALIC1", "65-76-73-67-49"],
        ["MORLY", "77-79-82-76-89"]
    ])
    def test_serial_number_from_site(self, site, expected):

        result = CR1000XDevice._get_serial_number_from_site(site)

        self.assertEqual(result, expected)

        result = CR1000XDevice(site, self.db, self.conn)

        self.assertEqual(result.serial_number, expected)


class TestCR1000XField(unittest.TestCase):
    """Tests the datalogger field objects."""

    @parameterized.expand(["myfield","my field","myfield!","myfield123","myfield~"])
    def test_name_set(self, name):
        """Tests that the object fields are set."""

        obj = CR1000XField(name, data_type="xsd:float")

        self.assertEqual(obj.name, name)
        self.assertEqual(obj.data_type, "xsd:float")
        self.assertEqual(obj.units, "")
        self.assertEqual(obj.process, "Smp")
        self.assertFalse(obj.settable)

    @parameterized.expand(["xsd:float", "xsd:double"])
    def test_type_set(self, data_type):
        """Tests that the object fields are set."""

        obj = CR1000XField("name", data_type=data_type)

        self.assertEqual(obj.data_type, data_type)

    @parameterized.expand(["Volts", "Deg C"])
    def test_units_set(self, units):
        """Tests that the object fields are set."""

        obj = CR1000XField("name", data_type="xsd:float", units=units)

        self.assertEqual(obj.units, units)
    
    @parameterized.expand(["Avg", "Std"])
    def test_process_set(self, process):
        """Tests that the object fields are set."""

        obj = CR1000XField("name", data_type="xsd:float", process=process)

        self.assertEqual(obj.process, process)
    
    def test_data_type_errors(self):
        """Tests that erros are raised"""

        with self.assertRaises(ValueError):
            CR1000XField("name")

    @parameterized.expand([
        [[1,2,3,4], "xsd:short"],
        ["strr", "xsd:string"],
        [[1.2,4.9,2], "xsd:float"]
    ])
    def test_data_values_generates_type(self, values, expected):
        """Tests that providing data values autogenerates type."""
        obj = CR1000XField("name", data_values=values)

        self.assertEqual(obj.data_type, expected)

    def test_data_type_priority(self):
        """Tests that the the data_type argument takes preference over data_values."""

        obj = CR1000XField("name", data_type="xsd:string", data_values=[1,2,3])

        self.assertEqual(obj.data_type, "xsd:string")

    @parameterized.expand([True, False])
    def test_settable_set(self, settable):
        """Tests that the object fields are set."""

        obj = CR1000XField("name", data_type="xsd:float", settable=settable)

        self.assertEqual(obj.settable, settable)

    @parameterized.expand([1, "yes", 1.75])
    def test_non_bool_settable_error(self, value):
        """Ensures that non bool arguments for `settable` raises error."""
        
        with self.assertRaises(TypeError):
            CR1000XField("name", data_type="xsd:float", settable=value)

    @parameterized.expand([
        [0, "xsd:int"],
        [32767, "xsd:short"],
        [-32768, "xsd:short"],
        [-2147483648, "xsd:int"],
        [2147483647, "xsd:int"],
        [-9223372036854775808, "xsd:long"],
        [9223372036854775807, "xsd:long"],
        [-9923372036854775808, "xsd:integer"],
        [9923372036854775807, "xsd:integer"],
        [-3.4028234663852886e+38, "xsd:float"],
        [3.4028234663852886e+38, "xsd:float"],
        [-1.1754943508222875e-38, "xsd:float"],
        [1.1754943508222875e-38, "xsd:float"],
        [0.0, "xsd:float"],
        [1e39, "xsd:double"],
        [-1e39, "xsd:double"],
        [1e-39, "xsd:double"],
        [-1e-39, "xsd:double"],
        [True, "xsd:boolean"],
        [False, "xsd:boolean"],
        [datetime.now().isoformat(), "xsd:dateTime"],
        [datetime.now(), "xsd:dateTime"],
        ["value", "xsd:string"]

    ])
    def test_data_type_matching(self, value, expected):
        """Tests that the `xsd` data type can be extracted."""

        result = CR1000XField._get_xsd_type(value)

        self.assertEqual(result.value["schema"], expected)

    def test_error_if_xsd_type_not_valid(self):

        with self.assertRaises(TypeError):
            CR1000XField._get_xsd_type(BaseDevice)

    def test_json_serialisation(self):
        obj = CR1000XField("fieldname", data_type="xsd:string", units="Volts", process="Std")
        expected = '{"name": "fieldname", "type": "xsd:string", "units": "Volts", "process": "Std", "settable": false}'

        self.assertEqual(json.dumps(obj, default=json_serial), expected)

    @parameterized.expand([
        ["Temp", "Smp"],
        ["Temp_avg", "Avg"],
        ["Temp_AVG", "Avg"],
        ["Temp_avg_C", "Smp"],
        ["Temp_STD", "Std"],
        ["Temp_Max", "Max"],
        ["Temp_Min", "Min"],
        ["Temp_cov", "Cov"],
        ["Temp_tot", "Tot"],
        ["Temp_Mom", "Mom"],
    ])
    def test_process_calculation(self, variable: str, expected: str): 
        """Tests that the expected process can be calculated."""

        result = CR1000XField._get_process(variable)

        self.assertEqual(result, expected)

    @parameterized.expand([
        ["Temp", "Smp"],
        ["Temp_avg", "Avg"],
        ["Temp_AVG", "Avg"],
    ])
    def test_initialization_with_process_calculated(self, name, expected):
        """Checking that process gets set from the variable name."""
        result = CR1000XField(name, data_type="xsd:float")

        self.assertEqual(result.process, expected)


if __name__ == "__main__":
    unittest.main()
