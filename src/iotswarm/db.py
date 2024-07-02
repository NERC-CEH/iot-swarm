"""This module holds all implementation for databases. Currently only supporting Oracle."""

import oracledb
import getpass
import logging
import abc
from iotswarm.queries import (
    CosmosQuery,
    CosmosTable,
)
import pandas as pd
from pathlib import Path
from math import nan
import sqlite3
from typing import List

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

    def __eq__(self, obj):
        return self._instance_logger == obj._instance_logger

    @abc.abstractmethod
    def query_latest_from_site(self) -> List:
        pass


class MockDB(BaseDatabase):

    @staticmethod
    def query_latest_from_site() -> List:
        return []


class CosmosDB(BaseDatabase):
    """Base class for databases using COSMOS_UK data."""

    connection: object
    """Connection to database."""

    site_data_query: CosmosQuery
    """SQL query for retrieving a single record."""

    site_id_query: CosmosQuery
    """SQL query for retrieving list of site IDs"""

    def __eq__(self, obj):
        return (
            type(self.connection) == type(obj.connection)
            and self.site_data_query == obj.site_data_query
            and self.site_id_query == obj.site_id_query
            and BaseDatabase.__eq__(self, obj)
        )

    @staticmethod
    def _validate_table(table: CosmosTable) -> None:
        """Validates that the query is legal"""

        if not isinstance(table, CosmosTable):
            raise TypeError(
                f"`table` must be a `{CosmosTable.__class__}` Enum, not a `{type(table)}`"
            )

    @staticmethod
    def _fill_query(query: str, table: CosmosTable) -> str:
        """Fills a query string with a CosmosTable enum."""

        CosmosDB._validate_table(table)

        return query.format(table=table.value)

    @staticmethod
    def _validate_max_sites(max_sites: int) -> int:
        """Validates that a valid maximum sites is given:
        Args:
            max_sites: The maximum number of sites required.

        Returns:
            An integer 0 or more.
        """

        if max_sites is not None:
            max_sites = int(max_sites)
            if max_sites < 0:
                raise ValueError(
                    f"`max_sites` must be 1 or more, or 0 for no maximum. Received: {max_sites}"
                )

        return max_sites

    def query_latest_from_site(self):
        pass


class Oracle(CosmosDB):
    """Class for handling oracledb logic and retrieving values from DB."""

    connection: oracledb.Connection
    """Connection to oracle database."""

    site_data_query = CosmosQuery.ORACLE_LATEST_DATA

    site_id_query = CosmosQuery.ORACLE_SITE_IDS

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
    async def create(
        cls,
        dsn: str,
        user: str,
        password: str = None,
        inherit_logger: logging.Logger | None = None,
        **kwargs,
    ):
        """Factory method for initialising the class.
            Initialization is done through the `create() method`: `Oracle.create(...)`.

        Args:
            dsn: Oracle data source name.
            user: Username used for query.
            pw: User password for auth.
            inherit_logger: Uses the given logger if provided
        """

        if not password:
            password = getpass.getpass("Enter Oracle password: ")

        self = cls(**kwargs)

        self.connection = await oracledb.connect_async(
            dsn=dsn, user=user, password=password
        )

        if inherit_logger is not None:
            self._instance_logger = inherit_logger.getChild(self.__class__.__name__)
        else:
            self._instance_logger = logger.getChild(self.__class__.__name__)

        self._instance_logger.info("Initialized Oracle connection.")

        return self

    async def query_latest_from_site(self, site_id: str, table: CosmosTable) -> dict:
        """Requests the latest data from a table for a specific site.

        Args:
            site_id: ID of the site to retrieve records from.
            table: A valid table from the database

        Returns:
            dict | None: A dict containing the database columns as keys, and the values as values.
                Returns `None` if no data retrieved.
        """

        query = self._fill_query(self.site_data_query, table)

        with self.connection.cursor() as cursor:
            await cursor.execute(query, site_id=site_id)

            columns = [i[0] for i in cursor.description]
            data = await cursor.fetchone()

            if not data:
                return None

            return dict(zip(columns, data))

    async def query_site_ids(
        self, table: CosmosTable, max_sites: int | None = None
    ) -> list:
        """query_site_ids returns a list of site IDs from COSMOS database

        Args:
            table: A valid table from the database
            max_sites: Maximum number of sites to retreive

        Returns:
            List[str]: A list of site ID strings.
        """

        max_sites = self._validate_max_sites(max_sites)

        query = self._fill_query(self.site_id_query, table)

        async with self.connection.cursor() as cursor:
            await cursor.execute(query)

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

    db_file: str | Path
    """Path to the database file."""

    def __eq__(self, obj):

        return (
            type(self.connection) == type(obj.connection)
            and self.db_file == obj.db_file
            and BaseDatabase.__eq__(self, obj)
        )

    @staticmethod
    def _get_connection(*args) -> pd.DataFrame:
        """Gets the database connection."""
        return pd.read_csv(*args)

    def __init__(self, csv_file: str | Path):
        """Initialises the database object.

        Args:
            csv_file: A pathlike object pointing to the datafile.
        """

        BaseDatabase.__init__(self)

        if not isinstance(csv_file, Path):
            csv_file = Path(csv_file)

        self.db_file = csv_file
        self.connection = self._get_connection(csv_file)

    def query_latest_from_site(self, site_id: str, index: int) -> dict:
        """Queries the datbase for a `SITE_ID` incrementing by 1 each time called
        for a specific site. If the end is reached, it loops back to the start.

        Args:
            site_id: ID of the site to query for.
            index: An offset index to query.
        Returns:
            A dict of the data row.
        """

        data = self.connection.query("SITE_ID == @site_id").replace({nan: None})

        # Automatically loops back to start
        db_index = index % len(data)

        return data.iloc[db_index].to_dict()

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


class LoopingSQLite3(CosmosDB, LoopingCsvDB):
    """A database that reads from .db files using sqlite3 and loops through
    entries in sequential order. There is a script that generates the .db file
    in the `__assets__/data` directory relative to this file. .csv datasets should
    be downloaded from the accompanying S3 bucket before running."""

    connection: sqlite3.Connection
    """Connection to the database."""

    site_data_query = CosmosQuery.SQLITE_LOOPED_DATA

    site_id_query = CosmosQuery.SQLITE_SITE_IDS

    @staticmethod
    def _get_connection(*args) -> sqlite3.Connection:
        """Gets a database connection."""

        return sqlite3.connect(*args)

    def __init__(self, db_file: str | Path):
        """Initialises the database object.

        Args:
            csv_file: A pathlike object pointing to the datafile.
        """
        LoopingCsvDB.__init__(self, db_file)

        self.cursor = self.connection.cursor()

    def __eq__(self, obj) -> bool:
        return CosmosDB.__eq__(self, obj) and super(LoopingCsvDB, self).__eq__(obj)

    def __getstate__(self) -> object:

        state = self.__dict__.copy()

        del state["connection"]
        del state["cursor"]

        return state

    def __setstate__(self, state) -> object:

        self.__dict__.update(state)

        self.connection = self._get_connection(self.db_file)
        self.cursor = self.connection.cursor()

    def query_latest_from_site(
        self, site_id: str, table: CosmosTable, index: int
    ) -> dict:
        """Queries the datbase for a `SITE_ID` incrementing by 1 each time called
        for a specific site. If the end is reached, it loops back to the start.

        Args:
            site_id: ID of the site to query for.
            table: A valid table from the database
            index: Offset of index.
        Returns:
            A dict of the data row.
        """
        query = self._fill_query(self.site_data_query, table)

        data = self._query_latest_from_site(
            query, {"site_id": site_id, "offset": index}
        )

        if data is None:
            index = 0

        data = self._query_latest_from_site(
            query, {"site_id": site_id, "offset": index}
        )

        return data

    def _query_latest_from_site(self, query, arg_dict: dict) -> dict:
        """Requests the latest data from a table for a specific site.

        Args:
            table: A valid table from the database
            arg_dict: Dictionary of query arguments.

        Returns:
            dict | None: A dict containing the database columns as keys, and the values as values.
                Returns `None` if no data retrieved.
        """

        self.cursor.execute(query, arg_dict)

        columns = [i[0] for i in self.cursor.description]
        data = self.cursor.fetchone()

        if not data:
            return None

        return dict(zip(columns, data))

    def query_site_ids(self, table: CosmosTable, max_sites: int | None = None) -> list:
        """query_site_ids returns a list of site IDs from COSMOS database

        Args:
            table: A valid table from the database
            max_sites: Maximum number of sites to retreive

        Returns:
            List[str]: A list of site ID strings.
        """

        query = self._fill_query(self.site_id_query, table)

        max_sites = self._validate_max_sites(max_sites)

        try:
            cursor = self.connection.cursor()
            cursor.execute(query)

            data = cursor.fetchall()
            if max_sites == 0:
                data = [x[0] for x in data]
            else:
                data = [x[0] for x in data[:max_sites]]

            if not data:
                data = []
        finally:
            cursor.close()

        return data
