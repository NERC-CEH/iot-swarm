import pytest
import unittest
from iotdevicesimulator import queries


class TestCosmosQueries(unittest.TestCase):

    def test_enums(self):

        enums = list(queries.CosmosQuery)

        for item in enums:
            self.assertIsInstance(item, queries.CosmosQuery)
            self.assertIsInstance(item.value, str)


if __name__ == "__main__":
    unittest.main()
