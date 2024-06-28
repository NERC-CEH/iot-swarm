import unittest
import pytest
from pathlib import Path
from platformdirs import user_data_dir
from parameterized import parameterized
from iotswarm.session import (
    Session,
    SessionLoader,
    SessionWriter,
    SessionManager,
    SessionManagerBase,
)
from iotswarm.swarm import Swarm
from iotswarm.devices import BaseDevice
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.db import LoopingSQLite3, MockDB
from iotswarm.queries import CosmosTable
import tempfile
import pickle

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


class TestSessionManagerBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.device_ids = ["MORLY", "ALIC1", "SPENF", "RISEH"]
        cls.devices = [
            BaseDevice(x, MockDB(), MockMessageConnection()) for x in cls.device_ids
        ]

        cls.swarm = Swarm(cls.devices, "test-swarm")
        cls.session = Session(cls.swarm)

    def test_initialisation(self):
        """Tests normal case"""

        writer = SessionWriter()

        self.assertIsInstance(writer.base_directory, Path)

        temp = tempfile.gettempdir()
        writer = SessionWriter(temp)

        self.assertIsInstance(writer.base_directory, Path)
        self.assertEqual(writer.base_directory, Path(temp))

    def test_get_session_file_from_session(self):

        tempdir = tempfile.gettempdir()

        sm = SessionManagerBase(base_directory=tempdir)

        file = sm._get_session_file(self.session)
        expected = Path(tempdir, self.session.session_id + ".pkl")

        self.assertIsInstance(file, Path)
        self.assertEqual(file, expected)

    def test_get_session_file_from_session_id(self):

        tempdir = tempfile.gettempdir()

        sm = SessionManagerBase(base_directory=tempdir)
        session_id = "this-is-a-session-id"

        file = sm._get_session_file(session_id)
        expected = Path(tempdir, session_id + ".pkl")

        self.assertIsInstance(file, Path)
        self.assertEqual(file, expected)

    def test_get_session_file_bad_type(self):
        sm = SessionManagerBase()

        with self.assertRaises(TypeError):
            sm._get_session_file(123)


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

        tempdir = tempfile.mkdtemp()
        session = Session(Swarm(devices), "test-session")

        writer = SessionWriter(base_directory=tempdir)

        writer.write_session(session, replace=False)

        with open(writer._get_session_file(session), "rb") as f:
            file_content = pickle.load(f)

        self.assertEqual(file_content, session)

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

        tempdir = tempfile.mkdtemp()

        session = Session(Swarm(devices), "test-session")
        writer = SessionWriter(base_directory=tempdir)

        writer.write_session(session, replace=False)

        with open(writer._get_session_file(session), "rb") as f:
            file_content = pickle.load(f)

        self.assertEqual(file_content, session)

        for device in devices:
            data_source.query_latest_from_site(device.device_id, table)

        writer.write_session(session, replace=True)
        with open(writer._get_session_file(session), "rb") as f:
            file_content = pickle.load(f)

        self.assertEqual(file_content, session)


class TestSessionManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.device_ids = ["MORLY", "ALIC1", "SPENF", "RISEH"]
        cls.devices = [
            BaseDevice(x, MockDB(), MockMessageConnection()) for x in cls.device_ids
        ]

        cls.swarm = Swarm(cls.devices, "test-swarm")
        cls.session = Session(cls.swarm)

    @db_exists
    def test_destroy_session(self):
        """Tests that a session file is destroyed."""

        tempdir = tempfile.mkdtemp()

        writer = SessionWriter(base_directory=tempdir)

        writer.write_session(self.session)

        self.assertTrue(
            writer._get_session_file(self.session).exists(),
            msg="Session file was not created.",
        )

        SessionManager(tempdir).destroy_session(self.session)

        self.assertFalse(
            writer._get_session_file(self.session).exists(),
            msg="Session file was not destroyed",
        )

    def test_list_sessions(self):

        session_ids = sorted(["test-1", "tmp-2", "mysession"])

        sessions = [Session(self.swarm, session_id) for session_id in session_ids]

        tempdir = tempfile.mkdtemp()

        writer = SessionWriter(base_directory=tempdir)

        [writer.write_session(session) for session in sessions]

        sm = SessionManager(base_directory=tempdir)

        found_sessions = sm.list_sessions()

        self.assertListEqual(found_sessions, session_ids)


class TestSessionLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.device_ids = ["MORLY", "ALIC1", "SPENF", "RISEH"]
        cls.devices = [
            BaseDevice(x, MockDB(), MockMessageConnection()) for x in cls.device_ids
        ]

        cls.swarm = Swarm(cls.devices, "test-swarm")
        cls.session = Session(cls.swarm)

    def test_session_loaded(self):
        temp = tempfile.mkdtemp()

        writer = SessionWriter(base_directory=temp)
        loader = SessionLoader(base_directory=temp)

        writer.write_session(self.session)

        session = loader.load_session(self.session.session_id)

        self.assertIsInstance(session, Session)

        self.assertEqual(session, self.session)


if __name__ == "__main__":
    unittest.main()
