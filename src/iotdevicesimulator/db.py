import oracledb
import getpass
import logging

logger = logging.getLogger(__name__)


class Oracle:
    """Class for handling oracledb logic and retrieving values from DB."""

    @classmethod
    async def create(cls, dsn: str, user: str, password: str = None):
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
        self.connection = await oracledb.connect_async(
            dsn=dsn, user=user, password=password
        )

        logger.info("Initialized Oracle connection.")

        return self

    async def query_latest_COSMOS_level1_soilmet_30min(self, site_id: str) -> dict:
        """Requests the latest data from a table for a specific site.

        Args:
            site_id (str): ID of the site to retrieve records from.

        Returns:
            dict: A dict containing the database columns as keys, and the values as values.
        """

        async with self.connection.cursor() as cursor:
            await cursor.execute(
                "SELECT * FROM COSMOS.level1_soilmet_30min "
                "WHERE site_id = :mysite "
                "ORDER BY date_time DESC "
                "FETCH NEXT 1 ROWS ONLY",
                mysite=site_id,
            )

            columns = [i[0] for i in cursor.description]
            data = await cursor.fetchone()

            if not data:
                return None

            return dict(zip(columns, data))
