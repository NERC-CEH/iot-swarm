from unittest import TestCase
from parameterized import parameterized

from iotswarm.livecosmos.utils import build_aws_object_key, get_md5_hash, get_unix_timestamp, _get_s3_client
from datetime import datetime


class TestUtils(TestCase):
    """Suite for testing livecosmos utility methods"""

    @parameterized.expand(
        [
            ["Hello World!", "ed076287532e86365e841e92bfc50d8c"],
            [
                '{ "time": "09:00:00", "values": { "precip": "yes", "result": [ 1, 2, 2.3 ] } }',
                "0a21d64c8db379b6bd5e1a0850c2354f",
            ],
        ]
    )
    def test_md5_hash(self, target, expected) -> None:
        """Test that the md5 hash is generated correctly"""

        result = get_md5_hash(target)

        self.assertEqual(expected, result)

    @parameterized.expand(
        [
            [datetime(year=2020, month=1, day=1), 1577836800000],
            [datetime(year=2024, month=12, day=31, hour=3, minute=31, second=2, microsecond=100), 1735615862000],
            [datetime(year=2024, month=2, day=29), 1709164800000],
        ]
    )
    def test_get_unix_timestamp(self, target, expected) -> None:
        """Test that any datetime object can return a 13 digit timestamp"""

        result = get_unix_timestamp(target)

        self.assertEqual(13, len(str(result)), "Timestamp should be 13 digits")
        self.assertEqual(expected, result)

    def test_aws_object_key_building(self):
        """Tests that an object key filename can be created"""

        expected = "1709164800000-ed076287532e86365e841e92bfc50d8c"
        result = build_aws_object_key(datetime(year=2024, month=2, day=29), "Hello World!")

        self.assertEqual(expected, result)

    def test_local_s3_client_loaded_from_config(self):
        """If the aws::endpoint_url key exists in a config object, a local s3_client is returned"""

        config_obj = {"aws": {"endpoint_url": "http://local"}}

        result = _get_s3_client(config_obj)

        self.assertEqual(config_obj["aws"]["endpoint_url"], result._endpoint.host)

    def test_normat_s3_client_loaded_from_config(self):
        """If the aws::endpoint_url key not exists in a config object, a default s3_client is returned"""

        config_obj = {"aws": {"not_endpoint_url": "http://local"}}

        result = _get_s3_client(config_obj)

        self.assertNotEqual(config_obj["aws"]["not_endpoint_url"], result._endpoint.host)
