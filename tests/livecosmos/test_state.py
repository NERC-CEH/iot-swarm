import builtins
from unittest import TestCase
from unittest.mock import Mock, call, mock_open, patch, DEFAULT
from iotswarm.livecosmos.state import State, StateTracker
from platformdirs import user_state_dir
from pathlib import Path


class TestStateManager(TestCase):
    """Test suite for the state manager"""

    def test_path_set(self):
        """Tests that the file path is set correctly"""

        filename = "mystate"
        obj = StateTracker(filename)

        result = obj._file
        expected = Path(user_state_dir()) / "iotswarm" / "livecosmos" / f"{filename}.pickle"

        assert result == expected

    def test_new_state(self):
        """Test behaviour when state files don't exist yet"""

        with self.assertLogs() as logs:
            obj = StateTracker("mystate")
            self.assertTrue(logs.output[-1].endswith("No state files found"))

        assert obj.state == {}

    @patch("builtins.open")
    @patch("pickle.load", Mock(return_value="123"))
    def test_state_file_exists(self, mock_open):
        """Tests that early return if state file is loaded"""

        state = StateTracker("mystate")

        mock_open.assert_called_once()

        assert state.state == "123"

    @patch("builtins.open", wraps=builtins.open)
    def test_backup_state_loaded_if_no_main_state(self, mock_open):
        """If the main state is not found, the backup should be loaded
        and used to rescue the main state"""

        obj = StateTracker("mystate")
        expected_calls = [call(obj._file, "rb"), call(obj._backup, "rb")]
        mock_open.assert_has_calls(expected_calls)

    @patch("pickle.dump")
    @patch("pickle.load", side_effect=[EOFError(), "123"])
    @patch("builtins.open")
    def test_state_corrupted(self, mock_open, _, mock_dump):
        """Tests that backup state is loaded if main gets corrupted"""

        obj = StateTracker("mystate")
        assert mock_open.call_count == 3
        expected_calls = [call(obj._file, "rb"), call(obj._backup, "rb"), call(obj._file, "wb")]

        for exp in expected_calls:
            assert exp in mock_open.mock_calls

        assert mock_dump.call_args.args[0] == obj.state

    @patch("pickle.load", side_effect=EOFError())
    @patch("builtins.open", side_effect=[FileNotFoundError(), DEFAULT])
    def test_main_missing_and_backup_state_corrupted(self, *args):
        """Tests that exception is caught if the main state file is missing
        and the backup is corrupted"""

        with self.assertRaises(RuntimeError) as e:
            StateTracker("mystate")

        self.assertIn("Main state file is missing and backup is corrupted", str(e.exception))

    @patch("pickle.load", side_effect=EOFError())
    @patch("builtins.open")
    def test_both_states_corrupted(self, *args):
        """Tests critical error when both states are corrupted"""

        with self.assertRaises(RuntimeError) as e:
            StateTracker("mystate")

        self.assertIn("All state files are corrupted", str(e.exception))

    @patch("builtins.open", mock_open())
    @patch("iotswarm.livecosmos.state.StateTracker.load_state", return_value="123")
    @patch("pickle.dump")
    def test_state_written_to_file(self, mock_dump, *args):
        """Tests that state and backup are written to file"""

        obj = StateTracker("mystate")

        obj.write_state()

        self.assertEqual(mock_dump.call_count, 2)
