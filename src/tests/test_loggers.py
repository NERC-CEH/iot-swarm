import logging.handlers
import pytest
import unittest
import logging
from iotdevicesimulator import loggers
from parameterized import parameterized


class TestLoggers(unittest.TestCase):

    @parameterized.expand([["S", 1, 4], ["W0", 10, 30], ["H", 12, 4]])
    def test_instantiation(self, when, interval, backups):
        handler = loggers.TimedRotatingFileHandler(when, interval, backups)

        self.assertIsInstance(handler, loggers.TimedRotatingFileHandler)
        self.assertEqual(handler.when, when)
        self.assertEqual(handler.backupCount, backups)


if __name__ == "__main__":
    unittest.main()
