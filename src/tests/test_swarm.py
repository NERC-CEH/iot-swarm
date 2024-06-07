from typing import Coroutine
import unittest
import pytest
from parameterized import parameterized
from iotdevicesimulator.swarm import Swarm
from iotdevicesimulator.devices import BaseDevice, CR1000XDevice
from iotdevicesimulator.messaging.core import MockMessageConnection
from iotdevicesimulator.db import MockDB

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

        site_ids = ["MORLY", "ALIC1", "ABCD"]

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
        print(devices)
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

        self.assertEqual(
            swarm._instance_logger.name, f"iotdevicesimulator.swarm.Swarm.{name}"
        )

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
        self.config_path = str(CONFIG_PATH)
        self.config = Config(self.config_path)["oracle"]

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


if __name__ == "__main__":
    unittest.main()
