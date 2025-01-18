import logging

from iotswarm.loggers import TimedRotatingFileHandlerPrefixed


def get_logger(name: str, prefix: str = "livecosmos") -> logging.Logger:
    """
    Creates and returns a logger with console and file handlers.

    Args:
        name: Name of the logger.
        prefix: Prefix added to the log file path

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)

    # Avoid duplicate handlers if the logger is already configured
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        # File handler
        file_handler = TimedRotatingFileHandlerPrefixed(prefix, when="W0", interval=1, backupCount=7)
        file_handler.setLevel(logging.DEBUG)

        # Formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger
