"""This module holds all implementation for databases. Currently only supporting Oracle."""

import oracledb
import getpass
import logging

from iotdevicesimulator.queries import CosmosQuery, CosmosSiteQuery

logger = logging.getLogger(__name__)


class Oracle:
    """Class for handling oracledb logic and retrieving values from DB."""

    _instance_logger: logging.Logger
    """Logger handle for the instance."""

    connection: oracledb.Connection
    """Connection to oracle database."""

    @classmethod
    async def create(
        cls,
        dsn: str,
        user: str,
        password: str = None,
        inherit_logger: logging.Logger | None = None,
    ):
        """Factory method for initialising the class.
            Initialization is done through the `create() method`: `Oracle.create(...)`.

        Args:
            dsn: Oracle data source name.
            user: Username used for query.
            pw: User password for auth.
        """

        if not password:
            password = getpass.getpass("Enter Oracle password: ")

        self = cls()

        if inherit_logger is not None:
            self._instance_logger = inherit_logger.getChild("db")
        else:
            self._instance_logger = logger

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

    async def query_site_ids(self, query: CosmosSiteQuery) -> list:
        """query_site_ids returns a list of site IDs from COSMOS database

        Args:
            query (CosmosSiteQuery): The query to run.

        Returns:
            List[str]: A list of site ID strings.
        """

        if not isinstance(query, CosmosSiteQuery):
            raise TypeError(
                f"`query` must be a `CosmosSiteQuery` Enum, not a `{type(query)}`"
            )

        async with self.connection.cursor() as cursor:
            await cursor.execute(query.value)

            data = await cursor.fetchall()
            data = [x[0] for x in data]

            if not data:
                data = []

            return data
