from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import patch
from iotswarm.livecosmos.state import Site
from iotswarm.livecosmos.liveupload import LiveUploader
from iotswarm.db import Oracle
from iotswarm.queries import CosmosTable
from iotswarm.devices import CR1000XPayload
from datetime import datetime
import typeguard
from parameterized import parameterized


class TestCosmosUploader(TestCase):
    def test_get_search_time_from_state(self):
        """Test that the time is retrieved from the state if it exists"""

        uploader = LiveUploader(
            Oracle(), CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"], "fakebucket", app_prefix="livecosmos/tests"
        )
        dt = datetime.now()
        site = Site(site_id="ALIC1", last_data=dt)
        uploader.state.update_state(site)

        self.assertEqual(dt, uploader._get_search_time(site["site_id"]))

    def test_fallback_time_used_if_not_in_state(self):
        """Tests that the fallback time is used"""

        uploader = LiveUploader(
            Oracle(), CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"], "fakebucket", app_prefix="livecosmos/tests"
        )

        self.assertEqual(uploader._fallback_time, uploader._get_search_time("ALIC1"))

    @parameterized.expand(
        [
            [
                CosmosTable.COSMOS_STATUS_1HOUR,
                "aprefix",
                "aprefix/ALIC1/LIVE_COSMOS_STATUS_1HOUR/payload_hash.json",
            ],
            [
                CosmosTable.LEVEL_1_NMDB_1HOUR,
                "helpful",
                "helpful/ALIC1/LIVE_NMDB_1HOUR/payload_hash.json",
            ],
            [
                CosmosTable.LEVEL_1_PRECIP_1MIN,
                "not/a/aprefix",
                "not/a/aprefix/ALIC1/LIVE_PRECIP_1MIN/payload_hash.json",
            ],
            [
                CosmosTable.LEVEL_1_PRECIP_RAINE_1MIN,
                "aprefix",
                "aprefix/ALIC1/LIVE_PRECIP_RAINE_1MIN/payload_hash.json",
            ],
            [
                CosmosTable.LEVEL_1_SOILMET_30MIN,
                "aprefix",
                "aprefix/ALIC1/LIVE_SOILMET_30MIN/payload_hash.json",
            ],
        ]
    )
    def test_get_s3_path(self, table: CosmosTable, prefix: str, expected: str):
        """Test that the time is retrieved from the state if it exists"""
        object_name = "payload_hash.json"
        uploader = LiveUploader(
            Oracle(), table, ["ALIC1"], "bucket", bucket_prefix=prefix, app_prefix="livecosmos/tests"
        )

        result = uploader._get_s3_key("ALIC1", object_name)

        self.assertEqual(expected, result)


class TestCosmosUploaderAsync(IsolatedAsyncioTestCase):
    @patch("iotswarm.db.Oracle.query_datetime_gt_from_site")
    @patch("oracledb.Connection")
    async def test_payload_retrieved_from_site(self, mock_oracle_conn, mock_oracle):
        """Test that payloads are returned in the right format from the database"""

        oracle = Oracle()
        oracle.connection = mock_oracle_conn
        mock_oracle.return_value = [{"DATE_TIME": datetime.now(), "data1": 2.4}]
        uploader = LiveUploader(
            oracle, CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"], "fakebucket", app_prefix="livecosmos/tests"
        )

        payloads = await uploader.get_latest_payloads()

        for p in payloads:
            typeguard.check_type(p, CR1000XPayload)

    @patch("iotswarm.db.Oracle.query_datetime_gt_from_site")
    @patch("oracledb.Connection")
    async def test_empty_payload_list_from_site_if_no_data(self, mock_oracle_conn, mock_oracle):
        """Tests that the payload list is empty if no data is returned"""

        oracle = Oracle()
        oracle.connection = mock_oracle_conn
        mock_oracle.return_value = []
        uploader = LiveUploader(
            oracle, CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"], "fakebucket", app_prefix="livecosmos/tests"
        )

        with self.assertLogs(level="DEBUG") as logs:
            payloads = await uploader.get_latest_payloads()

            self.assertIn("Got 0 rows", logs.output[-1])
        self.assertListEqual(payloads, [])

    @patch("driutils.io.aws.S3Writer")
    @patch("iotswarm.livecosmos.state.StateTracker.write_state")
    @patch("iotswarm.db.Oracle.query_datetime_gt_from_site")
    @patch("oracledb.Connection")
    async def test_payload_upload(self, mock_oracle_conn, mock_oracle, mock_state, mock_s3_writer):
        """Test that the payload is uploaded and that the state is written to file only
        when the upload status is changed
        """

        oracle = Oracle()
        oracle.connection = mock_oracle_conn
        mock_oracle.return_value = [{"DATE_TIME": datetime.now(), "data1": 2.4}]
        uploader = LiveUploader(
            oracle, CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"], "fakebucket", app_prefix="livecosmos/tests"
        )

        payloads = await uploader.get_latest_payloads()

        # State write to file should be called when state changes
        uploader.send_payload(payloads[0], mock_s3_writer)

        mock_state.assert_called()
        mock_s3_writer.write.assert_called_once()

        # State should not be written if no state change
        mock_state.reset_mock()
        mock_s3_writer.reset_mock()
        uploader.send_payload(payloads[0], mock_s3_writer)

        mock_state.assert_not_called()
        mock_s3_writer.write.assert_called_once()

    @patch("driutils.io.aws.S3Writer")
    @patch("iotswarm.livecosmos.liveupload.LiveUploader.send_payload")
    @patch("iotswarm.livecosmos.liveupload.LiveUploader.get_latest_payloads")
    @patch("iotswarm.db.Oracle.query_datetime_gt_from_site")
    @patch("oracledb.Connection")
    async def test_latest_payload_upload(
        self, mock_oracle_conn, mock_oracle, mock_get_latest, mock_send_payload, mock_writer
    ):
        """Test that the latest payloads method will upload all found payloads"""

        oracle = Oracle()
        oracle.connection = mock_oracle_conn
        uploader = LiveUploader(
            oracle, CosmosTable.COSMOS_STATUS_1HOUR, ["ALIC1"], "fakebucket", app_prefix="livecosmos/tests"
        )

        # Test with 3 payloads
        mock_get_latest.return_value = [1, 2, 3]
        await uploader.send_latest_data(mock_writer)
        mock_get_latest.assert_called_once()
        self.assertEqual(mock_send_payload.call_count, len(mock_get_latest.return_value))

        # Test with no payloads
        mock_get_latest.reset_mock()
        mock_send_payload.reset_mock()
        mock_get_latest.return_value = []
        await uploader.send_latest_data(mock_writer)
        mock_get_latest.assert_called_once()
        self.assertEqual(mock_send_payload.call_count, len(mock_get_latest.return_value))
