import unittest
import pytest
import os
from parameterized import parameterized
from iotswarm.swarm import Swarm
from iotswarm.db import LoopingSQLite3
from iotswarm.devices import BaseDevice, CR1000XDevice
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.queries import CosmosTable
from iotswarm.db import MockDB
import tempfile
from pathlib import Path

SQL_PATH = Path(
    Path(__file__).parents[1], "iotswarm", "__assets__", "data", "cosmos.db"
)
sqlite_db_exist = pytest.mark.skipif(
    not SQL_PATH.exists(), reason="Local cosmos.db does not exist."
)


class TestCosmosSwarm(unittest.IsolatedAsyncioTestCase):

    def setUp(self):

        site_ids = list(range(10))

        self.base_devices = [
            BaseDevice(site, MockDB(), MockMessageConnection()) for site in site_ids
        ]

        self.cr1000x_devices = [
            CR1000XDevice(site, MockDB(), MockMessageConnection()) for site in site_ids
        ]

    def test_instantiation(self):

        # Test that BaseDevice list is compatible
        swarm = Swarm(self.base_devices)
        self.assertListEqual(swarm.devices, self.base_devices)

    def test_device_subclass_compatible(self):
        swarm = Swarm(self.cr1000x_devices)
        self.assertListEqual(swarm.devices, self.cr1000x_devices)

    def test_mixed_device_types_instantiated(self):
        swarm = Swarm(self.base_devices + self.cr1000x_devices)
        self.assertListEqual(swarm.devices, self.base_devices + self.cr1000x_devices)

    def test_single_device_converts_to_list(self):
        swarm = Swarm(self.base_devices[0])
        self.assertListEqual(swarm.devices, [self.base_devices[0]])

    @parameterized.expand(["myswarm", "another swarm", 1234])
    def test_swarm_name_given(self, name):
        swarm = Swarm(self.base_devices, name=name)

        self.assertEqual(swarm.name, str(name))

    @parameterized.expand(
        ["123", 45, [[BaseDevice(1, MockDB(), MockMessageConnection()), 7]]]
    )
    def test_devices_type_check(self, devices):
        """Tests that a TypeError is raised if non-device passed to Swarm."""
        with self.assertRaises(TypeError):
            Swarm(devices)

    @parameterized.expand([0, 1, 4, 6, 10])
    def test__len__(self, count):
        """Test that __len__ method functions."""

        devices = [
            BaseDevice(c, MockDB(), MockMessageConnection()) for c in range(count)
        ]

        swarm = Swarm(devices)

        self.assertEqual(len(swarm), count)

    @parameterized.expand(["swarm1", "MYSWARM", "Creative Name"])
    def test_logger_set(self, name):
        """Tests that the logger name gets set correctly."""

        swarm = Swarm([], name=name)

        self.assertEqual(swarm._instance_logger.name, f"iotswarm.swarm.Swarm.{name}")

    @parameterized.expand(
        [
            [[], None, "Swarm([])"],
            [
                BaseDevice("test_swarm", MockDB(), MockMessageConnection()),
                None,
                'Swarm(BaseDevice("test_swarm", MockDB(), MockMessageConnection()))',
            ],
            [
                CR1000XDevice("site", MockDB(), MockMessageConnection()),
                "swarm-1",
                'Swarm(CR1000XDevice("site", MockDB(), MockMessageConnection()), name="swarm-1")',
            ],
            [
                [BaseDevice(x, MockDB(), MockMessageConnection()) for x in range(2)],
                "swarm-2",
                'Swarm([BaseDevice("0", MockDB(), MockMessageConnection()), BaseDevice("1", MockDB(), MockMessageConnection())], name="swarm-2")',
            ],
            [
                [CR1000XDevice(x, MockDB(), MockMessageConnection()) for x in range(2)],
                None,
                'Swarm([CR1000XDevice("0", MockDB(), MockMessageConnection()), CR1000XDevice("1", MockDB(), MockMessageConnection())])',
            ],
        ]
    )
    def test__repr__(self, devices, name, expected):
        """Tests that __repr__ returns right value."""

        swarm = Swarm(devices, name=name)

        self.assertEqual(swarm.__repr__(), expected)


class TestSwarmRunning(unittest.IsolatedAsyncioTestCase):

    def setUp(self):

        site_ids = list(range(10))

        self.base_devices = [
            BaseDevice(
                site, MockDB(), MockMessageConnection(), max_cycles=5, sleep_time=0
            )
            for site in site_ids
        ]

        self.cr1000x_devices = [
            CR1000XDevice(
                site, MockDB(), MockMessageConnection(), max_cycles=3, sleep_time=0
            )
            for site in site_ids
        ]

    async def test_run_single_device_type(self):

        swarm = Swarm(self.base_devices + self.cr1000x_devices, "base-swarm")
        log_base = f"{swarm.__class__.__module__}.{swarm.__class__.__name__}.base-swarm"
        with self.assertLogs(level="INFO") as cm:
            await swarm.run()

            self.assertEqual(cm.output[0], f"INFO:{log_base}:Running main loop.")
            self.assertEqual(cm.output[-1], f"INFO:{log_base}:Terminated.")

        # Sanity check that all devices reach max cycles.
        for device in swarm.devices:
            self.assertEqual(device.cycle, device.max_cycles)


class TestSwarmSessions(unittest.TestCase):
    """Suite for testing that a swarm session can be resumed from file."""

    def setUp(self) -> None:
        self.site_ids = ["MORLY", "ALIC1", "SPENC"]

        self.devices = [
            BaseDevice(x, MockDB(), MockMessageConnection()) for x in self.site_ids
        ]
        self.swarm = Swarm(self.devices, "test-swarm")
        self.maxDiff = None

    def test_session_file_written(self):
        tempdir = tempfile.mkdtemp(prefix="iot-swarm")
        swarm_names = sorted(["test-swarm", "another", "bites"])

        swarms = [
            Swarm(self.devices, name=x, base_directory=tempdir) for x in swarm_names
        ]

        [swarm.write_self() for swarm in swarms]

        files = os.listdir(tempdir)

        for file, swarm in zip(files, swarms):
            self.assertTrue(file.startswith(swarm.name))

    def test_swarm_file_builder(self):

        tempdir = tempfile.mkdtemp(prefix="iot-swarm")

        # String argument uses class default directory
        swarm_id = "a-string-id"
        swarm = Swarm(self.devices, "another-id", base_directory=tempdir)

        expected = Path(Swarm.base_directory, swarm_id + ".pkl")
        actual = swarm._get_swarm_file(swarm_id)

        self.assertEqual(expected, actual)

        # Swarm instance uses instance directory

        expected = Path(swarm.base_directory, swarm.name + ".pkl")
        actual = swarm._get_swarm_file(swarm)

        self.assertEqual(expected, actual)

    def test_swarm_file_listing(self):

        tempdir = tempfile.mkdtemp(prefix="iot-swarm")

        swarm_names = sorted(["swarm1", "swarma", "test"])

        swarms = [
            Swarm(self.devices, name=x, base_directory=tempdir) for x in swarm_names
        ]

        [swarm.write_self() for swarm in swarms]

        listed = swarms[0].list_swarms()

        self.assertListEqual(listed, swarm_names)

    def test_swarm_exists(self):

        tempdir = tempfile.mkdtemp(prefix="iot-swarm")
        swarm = Swarm(self.devices, "real-swarm", base_directory=tempdir)
        swarm2 = Swarm(self.devices, "no-writes", base_directory=tempdir)
        swarm.write_self()

        self.assertTrue(Swarm._swarm_exists(swarm))

        self.assertFalse(Swarm._swarm_exists(swarm2))

    def test_swarm_file_init(self):

        tempdir = tempfile.mkdtemp(prefix="iot-swarm")
        swarm = Swarm(self.devices, "testing-init-swarm", base_directory=tempdir)

        Swarm._initialise_swarm_file("testing-init-string")
        Swarm._initialise_swarm_file(swarm)

        swarm_file = Swarm._get_swarm_file(swarm)
        swarm_str_file = Swarm._get_swarm_file("testing-init-string")
        self.assertTrue(swarm_str_file.exists())

        self.assertTrue(swarm_file.exists())

        self.assertTrue(swarm_file.name in os.listdir(tempdir))
        self.assertTrue(swarm_str_file.name in os.listdir(Swarm.base_directory))

    def test_swarm_writing_format(self):
        """Tests that the correct aspects of the swarm are written."""

        tempdir = tempfile.mkdtemp(prefix="iot-swarm")
        swarm_id = "my-swarm"

        data_source = MockDB()
        conn = MockMessageConnection()
        devices = [
            BaseDevice("MORLY", data_source, conn),
            CR1000XDevice("ALIC1", data_source, conn),
            BaseDevice("SPENC", data_source, conn),
            CR1000XDevice("HARLO", data_source, conn),
        ]

        devices[0].cycle = 5
        devices[1].cycle = 10
        devices[2].cycle = 1
        devices[3].cycle = 3

        Swarm.base_directory = tempdir
        swarm = Swarm(devices, name=swarm_id)

        swarm.write_self()

        actual = Swarm.load_swarm(swarm_id)

        self.assertEqual(swarm, actual)

        self.assertEqual(actual.devices[0].cycle, 5)
        self.assertEqual(actual.devices[1].cycle, 10)
        self.assertEqual(actual.devices[2].cycle, 1)
        self.assertEqual(actual.devices[3].cycle, 3)


class TestSwarmSessionEndtoEnd(unittest.IsolatedAsyncioTestCase):

    async def test_swarm_can_be_loaded_and_resumed(self):
        tempdir = tempfile.mkdtemp(prefix="iot-swarm")
        swarm_id = "my-swarm"
        max_cycles = [1, 2, 3, 4]
        data_source = MockDB()
        conn = MockMessageConnection()
        devices = [
            BaseDevice(
                "MORLY", data_source, conn, max_cycles=max_cycles[0], sleep_time=0
            ),
            CR1000XDevice(
                "ALIC1", data_source, conn, max_cycles=max_cycles[1], sleep_time=0
            ),
            BaseDevice(
                "SPENC", data_source, conn, max_cycles=max_cycles[2], sleep_time=0
            ),
            CR1000XDevice(
                "HARLO", data_source, conn, max_cycles=max_cycles[3], sleep_time=0
            ),
        ]

        Swarm.base_directory = tempdir
        swarm = Swarm(devices, name=swarm_id)

        await swarm.run()

        loaded = Swarm.load_swarm(swarm_id)

        for device, expected in zip(loaded.devices, max_cycles):
            self.assertEqual(device.cycle, expected)
            self.assertEqual(device.max_cycles, expected)

        # Test resumption
        new_max_cycles = [x * 2 for x in max_cycles]

        for device, new_max in zip(loaded.devices, new_max_cycles):
            device.max_cycles = new_max

        await loaded.run()

        loaded2 = Swarm.load_swarm(swarm_id)

        for device, expected in zip(loaded2.devices, new_max_cycles):
            self.assertEqual(device.cycle, expected)
            self.assertEqual(device.max_cycles, expected)


if __name__ == "__main__":
    unittest.main()
