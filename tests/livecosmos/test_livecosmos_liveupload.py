from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import patch
from iotswarm.livecosmos.state import Site
from iotswarm.livecosmos.liveupload import LiveUploader
from iotswarm.db import Oracle
from iotswarm.queries import CosmosTable
from iotswarm.devices import CR1000XPayload
from datetime import datetime
import typeguard

class TestCosmosUploader(TestCase):

    def test_get_search_time_from_state(self):
        """Test that the time is retrieved from the state if it exists"""

        uploader = LiveUploader(Oracle(), CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"],
        app_prefix="livecosmos/tests")
        dt = datetime.now()
        site = Site(site_id="ALIC1", last_data=dt)
        uploader.state.update_state(site)

        self.assertEqual(dt, uploader._get_search_time(site["site_id"]))

    def test_fallback_time_used_if_not_in_state(self):
        """Tests that the fallback time is used"""

        uploader = LiveUploader(Oracle(), CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"],
        app_prefix="livecosmos/tests")

        self.assertEqual(uploader._fallback_time, uploader._get_search_time("ALIC1"))

class TestCosmosUploaderAsync(IsolatedAsyncioTestCase):

    @patch("iotswarm.db.Oracle.query_datetime_gt_from_site")
    @patch("oracledb.Connection")
    async def test_payload_retreived_from_site(self, mock_oracle_conn, mock_oracle):
        """Test that payloads are returned in the right format from the database"""

        oracle = Oracle()
        oracle.connection = mock_oracle_conn
        mock_oracle.return_value = [{"DATE_TIME": datetime.now(), "data1": 2.4}]
        uploader = LiveUploader(oracle, CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"],
        app_prefix="livecosmos/tests")

        payloads = await uploader.get_latest_payloads()

        for p in payloads:
            typeguard.check_type(p, CR1000XPayload)

    @patch("iotswarm.db.Oracle.query_datetime_gt_from_site")
    @patch("oracledb.Connection")
    async def test_emtpty_payload_list_from_site_if_no_data(self, mock_oracle_conn, mock_oracle):
        """Tests that the payload list is empty if no data is returned"""

        oracle = Oracle()
        oracle.connection = mock_oracle_conn
        mock_oracle.return_value = []
        uploader = LiveUploader(oracle, CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"],
        app_prefix="livecosmos/tests")

        with self.assertLogs(level="DEBUG") as logs:
            payloads = await uploader.get_latest_payloads()

            self.assertIn("Got 0 rows", logs.output[-1])
        self.assertListEqual(payloads, [])

    @patch("iotswarm.livecosmos.state.StateTracker.write_state")
    @patch("iotswarm.db.Oracle.query_datetime_gt_from_site")
    @patch("oracledb.Connection")
    async def test_payload_upload(self, mock_oracle_conn, mock_oracle, mock_state):
        """Test that the payload is uploaded and that the state is written to file only
            when the upload status is changed
        """

        oracle = Oracle()
        oracle.connection = mock_oracle_conn
        mock_oracle.return_value = [{"DATE_TIME": datetime.now(), "data1": 2.4}]
        uploader = LiveUploader(oracle, CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"],
        app_prefix="livecosmos/tests")

        payloads = await uploader.get_latest_payloads()

        # State write to file should be called when state changes
        uploader.send_payload(payloads[0])

        mock_state.assert_called()
        
        # State should not be written if no state change
        mock_state.reset_mock()
        uploader.send_payload(payloads[0])

        mock_state.assert_not_called()

    @patch("iotswarm.livecosmos.liveupload.LiveUploader.send_payload")
    @patch("iotswarm.livecosmos.liveupload.LiveUploader.get_latest_payloads")
    @patch("iotswarm.db.Oracle.query_datetime_gt_from_site")
    @patch("oracledb.Connection")
    async def test_latest_payload_upload(self, mock_oracle_conn, mock_oracle, mock_get_latest, mock_send_payload):
        """Test that the latest payloads method will upload all found payloads"""

        oracle = Oracle()
        oracle.connection = mock_oracle_conn
        uploader = LiveUploader(oracle, CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"],
        app_prefix="livecosmos/tests")

        # Test with 3 payloads
        mock_get_latest.return_value = [1,2,3]
        await uploader.send_latest_data()
        mock_get_latest.assert_called_once()
        self.assertEqual(mock_send_payload.call_count, len(mock_get_latest.return_value))

        # Test with no payloads
        mock_get_latest.reset_mock()
        mock_send_payload.reset_mock()
        mock_get_latest.return_value = []
        await uploader.send_latest_data()
        mock_get_latest.assert_called_once()
        self.assertEqual(mock_send_payload.call_count, len(mock_get_latest.return_value))