"""Utility methods for the module"""

from typing import Optional

import boto3


def get_alphabetically_last_s3_object(
    s3_client: "boto3.client.s3", bucket_name: str, prefix: str = ""
) -> Optional[str]:
    """Returns the alohabetically last object in an s3 bucket"""
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    last_key = None

    for page in pages:
        if "Contents" in page:
            # The last key in the current page (sorted lexicographically within the page)
            page_last_key = page["Contents"][-1]["Key"]

            # Update the global last key if this page's last key is greater
            if last_key is None or page_last_key > last_key:
                last_key = page_last_key

    return last_key
