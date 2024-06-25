import unittest
import pytest
from parameterized import parameterized
from iotswarm.session import Session
from iotswarm.swarm import Swarm
from iotswarm.devices import BaseDevice
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.db import LoopingSQLite3, MockDB


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
