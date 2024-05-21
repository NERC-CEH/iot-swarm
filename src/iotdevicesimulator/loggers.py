import logging
import logging.handlers
import logging.config
import os
from pathlib import Path
from platformdirs import site_data_dir


class TimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):

    def __init__(self, *args, **kwargs):

        logpath = Path(
            site_data_dir("iot_device_simulator"),
            "log.log",
        )

        if not os.path.exists(logpath.parent):
            os.makedirs(logpath.parent)

        super().__init__("path", *args, **kwargs)
