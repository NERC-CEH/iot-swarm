"""This module holds all implementation for databases. Currently only supporting Oracle."""

import oracledb
import getpass
import logging
import abc
from iotswarm.queries import CosmosQuery, CosmosSiteQuery
import pandas as pd
from pathlib import Path
from math import nan

logger = logging.getLogger(__name__)


class BaseDatabase(abc.ABC):
    """Base class for implementing database objects

    Args:
        inherit_logger: Assigns the passed logger to instance.
    """

    _instance_logger: logging.Logger
    """Logger handle for the instance."""

    def __init__(self, inherit_logger: logging.Logger | None = None):
        if inherit_logger is not None:
            self._instance_logger = inherit_logger.getChild(self.__class__.__name__)
        else:
            self._instance_logger = logger.getChild(self.__class__.__name__)

    def __repr__(self):
        if self._instance_logger.parent.name == "iotswarm.db":
            logger_arg = ""
        else:
            logger_arg = f"inherit_logger={self._instance_logger.parent}"
        return f"{self.__class__.__name__}({logger_arg})"

    @abc.abstractmethod
    def query_latest_from_site(self):
        pass


class MockDB(BaseDatabase):

    @staticmethod
    def query_latest_from_site():
        return []


class Oracle(BaseDatabase):
    """Class for handling oracledb logic and retrieving values from DB."""

    connection: oracledb.Connection
    """Connection to oracle database."""

    def __repr__(self):
        parent_repr = (
            super().__repr__().lstrip(f"{self.__class__.__name__}(").rstrip(")")
        )
        if len(parent_repr) > 0:
            parent_repr = ", " + parent_repr
        return (
            f"{self.__class__.__name__}("
            f'"{self.connection.dsn}"'
            f"{parent_repr}"
            f")"
        )

    @classmethod
    async def create(cls, dsn: str, user: str, password: str = None, **kwargs):
        """Factory method for initialising the class.
            Initialization is done through the `create() method`: `Oracle.create(...)`.

        Args:
            dsn: Oracle data source name.
            user: Username used for query.
            pw: User password for auth.
        """

        if not password:
            password = getpass.getpass("Enter Oracle password: ")

        self = cls(**kwargs)

        self.connection = await oracledb.connect_async(
            dsn=dsn, user=user, password=password
        )

        self._instance_logger.info("Initialized Oracle connection.")

        return self

    async def query_latest_from_site(self, site_id: str, query: CosmosQuery) -> dict:
        """Requests the latest data from a table for a specific site.

        Args:
            site_id: ID of the site to retrieve records from.
            query: Query to parse and submit.

        Returns:
            dict | None: A dict containing the database columns as keys, and the values as values.
                Returns `None` if no data retrieved.
        """

        if not isinstance(query, CosmosQuery):
            raise TypeError(
                f"`query` must be a `CosmosQuery` Enum, not a `{type(query)}`"
            )

        async with self.connection.cursor() as cursor:
            await cursor.execute(
                query.value,
                mysite=site_id,
            )

            columns = [i[0] for i in cursor.description]
            data = await cursor.fetchone()

            if not data:
                return None

            return dict(zip(columns, data))

    async def query_site_ids(
        self, query: CosmosSiteQuery, max_sites: int | None = None
    ) -> list:
        """query_site_ids returns a list of site IDs from COSMOS database

        Args:
            query: The query to run.
            max_sites: Maximum number of sites to retreive

        Returns:
            List[str]: A list of site ID strings.
        """

        if not isinstance(query, CosmosSiteQuery):
            raise TypeError(
                f"`query` must be a `CosmosSiteQuery` Enum, not a `{type(query)}`"
            )

        if max_sites is not None:
            max_sites = int(max_sites)
            if max_sites < 0:
                raise ValueError(
                    f"`max_sites` must be 1 or more, or 0 for no maximum. Received: {max_sites}"
                )

        async with self.connection.cursor() as cursor:
            await cursor.execute(query.value)

            data = await cursor.fetchall()
            if max_sites == 0:
                data = [x[0] for x in data]
            else:
                data = [x[0] for x in data[:max_sites]]

            if not data:
                data = []

            return data


class LoopingCsvDB(BaseDatabase):
    """A database that reads from csv files and loops through items
    for a given table or site. The site and index is remembered via a
    dictionary key and incremented each time data is requested."""

    connection: pd.DataFrame
    """Connection to the pd object holding data."""

    cache: dict
    """Cache object containing current index of each site queried."""

    def __init__(self, csv_file: str | Path, *args, **kwargs):
        """Initialises the database object.

        Args:
            csv_file: A pathlike object pointing to the datafile.
        """

        super().__init__(*args, **kwargs)

        self.connection = pd.read_csv(csv_file)
        self.cache = dict()

    def query_latest_from_site(self, site_id: str) -> dict:
        """Queries the datbase for a `SITE_ID` incrementing by 1 each time called
        for a specific site. If the end is reached, it loops back to the start.

        Args:
            site_id: ID of the site to query for.
        Returns:
            A dict of the data row.
        """

        data = self.connection.query("SITE_ID == @site_id").replace({nan: None})

        if site_id not in self.cache or self.cache[site_id] >= len(data):
            self.cache[site_id] = 1
        else:
            self.cache[site_id] += 1

        return data.iloc[self.cache[site_id] - 1].to_dict()

    def query_site_ids(self, max_sites: int | None = None) -> list:
        """query_site_ids returns a list of site IDs from the database

        Args:
            max_sites: Maximum number of sites to retreive

        Returns:
            List[str]: A list of site ID strings.
        """
        if max_sites is not None:
            max_sites = int(max_sites)
            if max_sites < 0:
                raise ValueError(
                    f"`max_sites` must be 1 or more, or 0 for no maximum. Received: {max_sites}"
                )

        sites = self.connection["SITE_ID"].drop_duplicates().to_list()

        if max_sites is not None and max_sites > 0:
            sites = sites[:max_sites]

        return sites
