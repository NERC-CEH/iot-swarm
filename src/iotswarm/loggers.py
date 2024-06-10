"""Logging module for defining custom log handlers."""

import logging.handlers
import os
from pathlib import Path
import platformdirs


class TimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """TimedRotatingFileHandler Handler for rotating logs on a timed basis.

    Extended this handler to ensure log file and directory are created
    according to platform.
    """

    def __init__(self, *args, **kwargs):

        logpath = Path(
            platformdirs.user_data_dir("iot_device_simulator"),
            "log.log",
        )

        if not os.path.exists(logpath.parent):
            os.makedirs(logpath.parent)

        super().__init__(logpath, *args, **kwargs)
