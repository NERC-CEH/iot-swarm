import unittest
from unittest.mock import patch, mock_open
import pytest
from pathlib import Path
from platformdirs import user_data_dir
from parameterized import parameterized
from iotswarm.session import Session, SessionLoader, SessionWriter
from iotswarm.swarm import Swarm
from iotswarm.devices import BaseDevice
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.db import LoopingSQLite3, MockDB
from iotswarm.queries import CosmosTable
import tempfile
import json

LOOPED_DB_FILE = Path(
    Path(__file__).parents[1], "iotswarm", "__assets__", "data", "cosmos.db"
)

db_exists = pytest.mark.skipif(
    not LOOPED_DB_FILE.exists(), reason="SQLITE database file must exist."
)


class TestSession(unittest.TestCase):
    """Tests the Session class"""

    @classmethod
    def setUpClass(cls) -> None:
        cls.devices = [
            BaseDevice(x, MockDB(), MockMessageConnection())
            for x in ["MORLY", "ALIC1", "SPENF", "RISEH"]
        ]

        cls.swarm = Swarm(cls.devices, "test-swarm")
        cls.maxDiff = None

    def test_instantiation(self):
        """Tests that the class can be instantiated under normal conditions."""

        session = Session(self.swarm)

        print(session)

        self.assertIsInstance(session.swarm, Swarm)
        self.assertIsInstance(session.session_id, str)

    def test_error_if_bad_swarm_type(self):

        with self.assertRaises(TypeError):
            Session("swarm?")

    def test_session_id_set_if_given(self):
        """Tests that the session_id attribute is set when the argument is supplied."""
        session_id = "this-is-my-session"
        session = Session(self.swarm, session_id=session_id)

        self.assertEqual(session.session_id, session_id)

    def test_session_id_assigned_if_not_given(self):
        """Tests that an id is generated if not given."""

        session = Session(self.swarm)

        self.assertTrue(session.session_id.startswith(self.swarm.name))

    def test_session_id_method(self):
        """Tests the _build_session_id method returns an ID"""

        value = Session._build_session_id()

        self.assertIsInstance(value, str)
        self.assertEqual(len(value), 36)

        # string prefix
        prefix = "my-swarm"
        value = Session._build_session_id(prefix)

        self.assertTrue(value.startswith(f"my-swarm-"))
        self.assertEqual(len(value), 36 + len(prefix) + 1)

        # number prefix
        prefix = 12345
        value = Session._build_session_id(prefix)

        self.assertTrue(value.startswith(f"12345-"))
        self.assertEqual(len(value), 36 + 6)

    @parameterized.expand([None, "my-swarm"])
    def test_repr(self, session_id):

        session = Session(self.swarm, session_id=session_id)

        if session_id is not None:
            expected_start = (
                f'{session.__class__.__name__}({self.swarm}, "{session_id}"'
            )
        else:
            expected_start = expected_start = (
                f'{session.__class__.__name__}({self.swarm}, "{self.swarm.name}-'
            )
        self.assertTrue(session.__repr__().startswith(expected_start))

    @parameterized.expand([None, "my-swarm"])
    def test_str(self, session_id):

        session = Session(self.swarm, session_id=session_id)

        if session_id is not None:
            expected_start = f'Session: "{session_id}"'
        else:
            expected_start = expected_start = f'Session: "{self.swarm.name}-'

        self.assertTrue(str(session).startswith(expected_start))


class TestSessionWriter(unittest.TestCase):
    """Tests the SessionWriter class."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.device_ids = ["MORLY", "ALIC1", "SPENF", "RISEH"]
        cls.devices = [
            BaseDevice(x, MockDB(), MockMessageConnection()) for x in cls.device_ids
        ]

        cls.swarm = Swarm(cls.devices, "test-swarm")
        cls.named_session = Session(cls.swarm, session_id="test-session")
        cls.unnamed_session = Session(cls.swarm)

    def test_initialisation(self):
        """Tests normal case,"""

        writer = SessionWriter(self.named_session)

        self.assertEqual(writer.session, self.named_session)
        self.assertIsInstance(writer.session_file, Path)

        self.assertEqual(
            writer.session_file,
            Path(user_data_dir("iot_swarm"), "sessions", self.named_session.session_id),
        )

    @db_exists
    def test_device_index_dict(self):
        """Tests that a session can return a dict of its devices indexes"""

        data_source = LoopingSQLite3(LOOPED_DB_FILE)
        table = CosmosTable.LEVEL_1_SOILMET_30MIN
        devices = [
            BaseDevice(
                x,
                data_source,
                MockMessageConnection(),
                table=table,
            )
            for x in self.device_ids
        ]

        for device in devices:
            data_source.query_latest_from_site(device.device_id, table)

        session = Session(Swarm(devices), "test-session")

        index_dict = SessionWriter._get_device_index_dict(session)

        expected = {k: 0 for k in self.device_ids}

        self.assertDictEqual(index_dict, expected)

    @db_exists
    def test_write_from_empty(self):
        """Tests that the session file in initialised."""
        data_source = LoopingSQLite3(LOOPED_DB_FILE)
        table = CosmosTable.LEVEL_1_SOILMET_30MIN
        devices = [
            BaseDevice(
                x,
                data_source,
                MockMessageConnection(),
                table=table,
            )
            for x in self.device_ids
        ]

        session = Session(Swarm(devices), "test-session")
        writer = SessionWriter(session)

        writer.session_file = Path(tempfile.mkdtemp(), "test-sesion")
        writer._write_state(replace=False)

        with open(writer.session_file, "r") as f:
            file_content = json.load(f)

        expected = dict()

        self.assertDictEqual(file_content, expected)

    @db_exists
    def test_write_non_empty(self):
        """Tests that the session file in initialised."""
        data_source = LoopingSQLite3(LOOPED_DB_FILE)
        table = CosmosTable.LEVEL_1_SOILMET_30MIN
        devices = [
            BaseDevice(
                x,
                data_source,
                MockMessageConnection(),
                table=table,
            )
            for x in self.device_ids
        ]

        for device in devices:
            data_source.query_latest_from_site(device.device_id, table)

        session = Session(Swarm(devices), "test-session")
        writer = SessionWriter(session)

        writer.session_file = Path(tempfile.mkdtemp(), "test-sesion")
        writer._write_state(replace=False)

        with open(writer.session_file, "r") as f:
            file_content = json.load(f)

        expected = {k: 0 for k in self.device_ids}

        self.assertDictEqual(file_content, expected)

        for device in devices:
            data_source.query_latest_from_site(device.device_id, table)

        writer._write_state(replace=True)
        with open(writer.session_file, "r") as f:
            file_content = json.load(f)

        expected = {k: 1 for k in self.device_ids}

        self.assertDictEqual(file_content, expected)

    @db_exists
    def test_destroy_session(self):
        """Tests that a session file is destroyed."""

        data_source = LoopingSQLite3(LOOPED_DB_FILE)
        table = CosmosTable.LEVEL_1_SOILMET_30MIN
        devices = [
            BaseDevice(
                x,
                data_source,
                MockMessageConnection(),
                table=table,
            )
            for x in self.device_ids
        ]

        session = Session(Swarm(devices), "test-session")

        writer = SessionWriter(session)
        writer.session_file = Path(tempfile.mkdtemp(), "test-sesion")

        writer._write_state()

        self.assertTrue(
            writer.session_file.exists(), msg="Session file was not created."
        )

        writer._destroy_session()

        self.assertFalse(
            writer.session_file.exists(), msg="Session file was not destroyed"
        )


if __name__ == "__main__":
    unittest.main()
