import oracledb
import getpass
import logging

from iotdevicesimulator.queries import CosmosQuery

logger = logging.getLogger(__name__)


class Oracle:
    """Class for handling oracledb logic and retrieving values from DB."""

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
            dsn (str): Oracle data source name.
            user (str): Username used for query.
            pw (str): User password for auth.

        Attributes:
            connection (oracledb.Connection): Connection to oracle database."""

        if not password:
            password = getpass.getpass("Enter Oracle password: ")

        self = cls()

        if inherit_logger:
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
            site_id (str): ID of the site to retrieve records from.
            query (CosmosQuery): Query to parse and submit.

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
