import hashlib
from datetime import datetime

from config import Config
import boto3

def get_md5_hash(target: str) -> str:
    """Converts the object into an md5 hash

    Args:
        target: A string to hash
    Returns
        The hash string.
    """

    return hashlib.md5(target.encode()).hexdigest()


def get_unix_timestamp(target: datetime) -> int:
    """Calculates a 13 digit timestamp for a datetime object

    Args:
        target: the datetime to convert

    Returns:
        A integer with 13 digits
    """

    return int(target.timestamp() * 1000)


def build_aws_object_key(time: datetime, value: str) -> str:
    """Builds the object key filename to upload to AWS with

    Args:
        time: The datetime of the upload time
        value: The value of the payload
    Returns:
        A string of the filename in format <timestamp>_<md5_hash>
    """

    return f"{get_unix_timestamp(time)}_{get_md5_hash(value)}"

def _get_s3_client(config: Config) -> "boto3.client":
    """Returns the S3 client object.

    Args:
        config: The loaded app config object
    Returns:
        A boto3.s3.client object
    """

    try:
        endpoint = config["aws"]["endpoint_url"]
        return boto3.client("s3", endpoint_url=endpoint)
    except KeyError:
        return boto3.client("s3")