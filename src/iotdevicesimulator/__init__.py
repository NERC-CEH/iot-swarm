import autosemver
from pathlib import Path
import logging
import logging.config

__version__ = autosemver.packaging.get_current_version(
    project_name="iot-device-simulator"
)

log_config = Path(Path(__file__).parents[2], "loggers.ini")

logging.config.fileConfig(fname=log_config)

logger = logging.getLogger(__name__)

logger.info(f"Started.")
