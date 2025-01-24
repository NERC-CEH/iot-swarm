from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import Mock, patch, AsyncMock

from pathlib import Path
from iotswarm.livecosmos.liveupload import LiveUploader
from iotswarm.utils import json_serial
from iotswarm.db import Oracle
from driutils.io.aws import S3ReaderWriter
from boto3 import client
import subprocess
from datetime import datetime
import json
from iotswarm.queries import CosmosTable
import pytest
from click.testing import CliRunner
from iotswarm.livecosmos.scripts.cli import cli

import os

os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "fake"
os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

_APP_PREFIX = "tests"
_ENDPOINT_URL = "http://localhost:4566"
_BUCKET = "testbucket"
_BUCKET_PREFIX = "test"

_CONFIG_SRC = Path(__file__).parents[3] / "src" / "iotswarm" / "__assets__" / "live_cosmos_tests.cfg"


@pytest.mark.end_to_end
@patch("iotswarm.livecosmos.state.StateTracker.write_state", Mock())
class TestEndToEndLiveCosmos(IsolatedAsyncioTestCase):
    """Test that files are uploaded to the S3 endpoint"""

    @classmethod
    def setUpClass(cls):
        subprocess.run(["awslocal", "s3", "mb", f"s3://{_BUCKET}"])

    @classmethod
    def tearDownClass(cls) -> None:
        subprocess.run(["awslocal", "s3", "rb", f"s3://{_BUCKET}"])
        return super().tearDownClass()

    def setUp(self):
        subprocess.run(["awslocal", "s3", "rm", f"s3://{_BUCKET}", "--recursive"])

    def tearDown(self):
        subprocess.run(["awslocal", "s3", "rm", f"s3://{_BUCKET}", "--recursive"])

    @patch("iotswarm.db.Oracle.query_datetime_gt_from_site")
    @patch("oracledb.Connection")
    async def test_payload_uploaded(self, mock_oracle_conn, mock_oracle):
        """Test that a payload is uploaded to the S3 endpoint"""

        payload = [{"DATE_TIME": datetime.now(), "data1": 2.4}]
        oracle = Oracle()
        oracle.connection = mock_oracle_conn
        mock_oracle.return_value = payload

        sites = ["test_site"]
        writer = S3ReaderWriter(client("s3", endpoint_url=_ENDPOINT_URL))

        uploader = LiveUploader(
            oracle,
            CosmosTable.LEVEL_1_SOILMET_30MIN,
            sites,
            _BUCKET,
            bucket_prefix=_BUCKET_PREFIX,
            app_prefix=_APP_PREFIX,
        )
        payloads = await uploader.get_latest_payloads()

        uploader.send_payload(payloads[0], writer)

        bucket_items = writer._connection.list_objects(Bucket=_BUCKET)["Contents"]

        # Test that one object was uploaded
        self.assertEqual(len(bucket_items), 1)

        # Test that the object key is correct
        object_key = bucket_items[0]["Key"]
        expected_key_start = f"{_BUCKET_PREFIX}/{sites[0]}/LIVE_SOILMET_30MIN/"
        self.assertTrue(object_key.startswith(expected_key_start))
        # Test that the object is equal to the payload in JSON form
        obj = json.loads(writer.read(_BUCKET, object_key))
        expected_dict = json.loads(json.dumps(payloads[0], default=json_serial))
        self.assertDictEqual(obj, expected_dict)

    @patch("iotswarm.db.Oracle.query_datetime_gt_from_site")
    @patch("oracledb.Connection")
    async def test_multiple_payloads_uploaded_for_site(self, mock_oracle_conn, mock_oracle):
        """Test that multiple payloads are uploaded to the S3 endpoint"""

        payload = [{"DATE_TIME": datetime(2025, 1, 2), "data1": 2.4}, {"DATE_TIME": datetime(2025, 1, 3), "data1": 11}]
        oracle = Oracle()
        oracle.connection = mock_oracle_conn
        mock_oracle.return_value = payload

        sites = ["test_site"]
        writer = S3ReaderWriter(client("s3", endpoint_url=_ENDPOINT_URL))

        uploader = LiveUploader(
            oracle,
            CosmosTable.LEVEL_1_SOILMET_30MIN,
            sites,
            _BUCKET,
            bucket_prefix=_BUCKET_PREFIX,
            app_prefix=_APP_PREFIX,
        )
        expected_payloads = await uploader.get_latest_payloads()

        await uploader.send_latest_data(writer)

        bucket_items = writer._connection.list_objects(Bucket=_BUCKET)["Contents"]

        # Expect that 2 items exist in the bucket
        self.assertEqual(len(bucket_items), 2)

        for exp_payload, obj in zip(expected_payloads, bucket_items):
            # Test that the object key is correct
            object_key = obj["Key"]
            expected_key_start = f"{_BUCKET_PREFIX}/{sites[0]}/LIVE_SOILMET_30MIN/"
            self.assertTrue(object_key.startswith(expected_key_start))
            # Test that the object is equal to the payload in JSON form
            act_dict = json.loads(writer.read(_BUCKET, object_key))
            expected_dict = json.loads(json.dumps(exp_payload, default=json_serial))
            self.assertDictEqual(act_dict, expected_dict)

    @patch("iotswarm.db.Oracle.query_datetime_gt_from_site")
    @patch("oracledb.Connection")
    async def test_multiple_payloads_uploaded_for_multiple_sites(self, mock_oracle_conn, mock_oracle):
        """Test that multiple payloads are uploaded to the S3 endpoint for
        multiple sites. Expects that there will be 2 payloads in each S3 prefix"""

        payload = [{"DATE_TIME": datetime(2025, 1, 2), "data1": 2.4}, {"DATE_TIME": datetime(2025, 1, 3), "data1": 11}]
        oracle = Oracle()
        oracle.connection = mock_oracle_conn
        mock_oracle.return_value = payload

        sites = ["test_site", "test_site2"]
        writer = S3ReaderWriter(client("s3", endpoint_url=_ENDPOINT_URL))

        uploader = LiveUploader(
            oracle,
            CosmosTable.LEVEL_1_SOILMET_30MIN,
            sites,
            _BUCKET,
            bucket_prefix=_BUCKET_PREFIX,
            app_prefix=_APP_PREFIX,
        )
        expected_payloads = await uploader.get_latest_payloads()

        await uploader.send_latest_data(writer)

        bucket_items = writer._connection.list_objects(Bucket=_BUCKET)["Contents"]

        for site in sites:
            site_items = [item for item in bucket_items if item["Key"].split("/")[1] == site]
            exp_site_payloads = [p for p in expected_payloads if p["head"]["environment"]["station_name"] == site]
            self.assertEqual(len(site_items), len(payload))

            for exp_payload, obj in zip(exp_site_payloads, site_items):
                # Test that the object key is correct
                object_key = obj["Key"]
                expected_key_start = f"{_BUCKET_PREFIX}/{site}/LIVE_SOILMET_30MIN/"
                self.assertTrue(object_key.startswith(expected_key_start))
                # Test that the object is equal to the payload in JSON form
                act_dict = json.loads(writer.read(_BUCKET, object_key))
                expected_dict = json.loads(json.dumps(exp_payload, default=json_serial))
                self.assertDictEqual(act_dict, expected_dict)


MOCK_ORACLE = Oracle()
MOCK_ORACLE.connection = Mock()


@pytest.mark.end_to_end
@patch("iotswarm.livecosmos.state.StateTracker.write_state", Mock())
class TestCLI(TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["awslocal", "s3", "mb", f"s3://{_BUCKET}"])

    @classmethod
    def tearDownClass(cls) -> None:
        subprocess.run(["awslocal", "s3", "rb", f"s3://{_BUCKET}"])
        return super().tearDownClass()

    def setUp(self):
        subprocess.run(["awslocal", "s3", "rm", f"s3://{_BUCKET}", "--recursive"])

    def tearDown(self):
        subprocess.run(["awslocal", "s3", "rm", f"s3://{_BUCKET}", "--recursive"])

    @patch("iotswarm.db.Oracle.create", AsyncMock(return_value=MOCK_ORACLE))
    @patch(
        "iotswarm.db.Oracle.query_datetime_gt_from_site",
        AsyncMock(
            return_value=[
                {"DATE_TIME": datetime(2025, 1, 2), "data1": 2.4},
                {"DATE_TIME": datetime(2025, 1, 3), "data1": 11},
            ]
        ),
    )
    def test_all(self):
        _BUCKET_PREFIX = "fdri/cosmos_swarm"

        sites = ["test_site1", "test_site2"]
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli, ["send-live-data", str(_CONFIG_SRC), "--table", "all", "--site", sites[0], "--site", sites[1]]
            )

        assert result.exit_code == 0

        # Verifying results
        writer = S3ReaderWriter(client("s3", endpoint_url=_ENDPOINT_URL))
        bucket_items = writer._connection.list_objects(Bucket=_BUCKET)["Contents"]

        for site in sites:
            for table in ["LIVE_SOILMET_30MIN", "LIVE_PRECIP_1MIN", "LIVE_PRECIP_RAINE_1MIN", "LIVE_NMDB_1HOUR"]:
                items = [item for item in bucket_items if site in item["Key"] and table in item["Key"]]
                self.assertEqual(len(items), 2)
                for item in items:
                    # Test that the object key is correct
                    object_key = item["Key"]
                    expected_key_start = f"{_BUCKET_PREFIX}/{site}/{table}/"
                    self.assertTrue(object_key.startswith(expected_key_start))

                    # Test site name is in the payload
                    object_dict = json.loads(writer.read(_BUCKET, object_key))
                    self.assertEqual(object_dict["head"]["environment"]["station_name"], site)
