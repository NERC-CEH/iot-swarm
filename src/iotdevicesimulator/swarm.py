from iotdevicesimulator.devices import SensorSite
from iotdevicesimulator.db import Oracle
import logging

from typing import List
import pathlib
import asyncio
import config
import random

# TODO: Implement messaging logic
# TODO: Implement delay in first message

config_path = pathlib.Path(pathlib.Path(__file__).parents[2], "config.cfg")

logger = logging.getLogger(__name__)


class CosmosSwarm:

    @classmethod
    async def create(
        cls,
        queries: List[str],
        site_ids: List[str] = None,
        sleep_time: int = 5,
        max_cycles: int = 3,
        max_sites: int = -1,
    ) -> None:
        """Factory method for initialising the class.
            Initialization is done through the `create() method`: `CosmosSwarm.create(...)`.

        Args:
            queries (List[str]): A list of data queries to submit.
            site_ids (List[str]): A list of site ID strings.
            sleep_time (int): Length of time to sleep after sending data in seconds.
            max_cycles (int): Maximum number of data sending cycles.
            max_sites (int): Maximum number of sites to initialise.
        """
        self = cls()

        max_cycles = int(max_cycles)
        sleep_time = int(sleep_time)
        max_sites = int(max_sites)

        if max_cycles <= 0 and max_cycles != -1:
            raise ValueError(
                f"`max_cycles` must be 1 or more, or -1 for no maximum. Received: {max_cycles}"
            )

        if max_sites <= 0 and max_sites != -1:
            raise ValueError(
                f"`max_sites` must be 1 or more, or -1 for no maximum. Received: {max_sites}"
            )

        if sleep_time < 0:
            raise ValueError(f"`sleep_time` must be 0 or more. Received: {sleep_time}")

        self.sleep_time = sleep_time
        self.max_cycles = max_cycles
        self.max_sites = max_sites
        self.queries = queries

        self.oracle = await self._get_oracle()

        if site_ids:
            self.sites = self._init_sites(
                site_ids,
                sleep_time=sleep_time,
                max_cycles=max_cycles,
                max_sites=max_sites,
            )
        else:
            self.sites = await self._init_sites_from_db(
                self.oracle,
                sleep_time=sleep_time,
                max_cycles=max_cycles,
                max_sites=max_sites,
            )

        return self

    async def run(self):
        """Main function for running the swarm."""

        query = self.oracle.query_latest_COSMOS_level1_soilmet_30min

        await asyncio.gather(*[site.run(query) for site in self.sites])

        logger.info("Finished")

    @staticmethod
    async def _get_oracle(cred_path: pathlib.Path | str | None = None) -> Oracle:
        """Reads Oracle credentials from `config_path` and returns an asynchronous
        connection to Oracle.

        Args:
            cred_path (pathlib.Path|str|None): Path to a config file containing an \"oracle\" section.

        Returns:
            Oracle: An oracle object.
        """

        if not cred_path:
            cred_path = config_path

        creds = config.Config(str(cred_path))["oracle"]

        oracle = await Oracle.create(
            creds["dsn"],
            creds["user"],
            password=creds["password"],
        )

        return oracle

    @staticmethod
    def _init_sites(
        site_ids: List[str],
        sleep_time: int = 10,
        max_cycles: int = 3,
        max_sites: int = -1,
    ):
        """Initialises a list of SensorSites.

        Args:
            site_ids (List[str]): A list of site ID strings.
            sleep_time (int): Length of time to sleep after sending data in seconds.
            max_cycles (int): Maximum number of data sending cycles.
            max_sites (int): Maximum number of sites to initialise.

        Returns:
            List[SensorSite]: A list of sensor sites.
        """
        if max_sites != -1:
            site_ids = CosmosSwarm._random_list_items(site_ids, max_sites)

        return [
            SensorSite(site_id, sleep_time=sleep_time, max_cycles=max_cycles)
            for site_id in site_ids
        ]

    @staticmethod
    async def _init_sites_from_db(
        oracle: Oracle, sleep_time: int = 10, max_cycles: int = 3, max_sites=-1
    ) -> List[SensorSite]:
        """Initialised sensor sites from the COSMOS DB.

        Args:
            oracle (Oracle): Oracle DB to query
            sleep_time (int): Length of time to sleep after sending data in seconds.
            max_cycles (int): Maximum number of data sending cycles.
            max_sites (int): Maximum number of sites to initialise.
        Returns:
            List[SensorSite]: A list of sensor sites.
        """

        async with oracle.connection.cursor() as cursor:
            await cursor.execute("SELECT UNIQUE(SITE_ID) from COSMOS.SITES")

            site_ids = await cursor.fetchall()

            if max_sites != -1:
                site_ids = CosmosSwarm._random_list_items(site_ids, max_sites)

            return [
                SensorSite(site_id[0], sleep_time=sleep_time, max_cycles=max_cycles)
                for site_id in site_ids
            ]

    @staticmethod
    def _random_list_items(list_in: List[object], max_count: int) -> List[object]:
        """Restricts the number of items in a list by picked randomly

        Args:
            list_in (List[str]): The list to process.
            max_count (int): Maximum number of items in list.
        Returns:
            List[object]: A list of randomly selected items or the original list.
        """

        max_count = int(max_count)

        if max_count <= 0 and max_count != -1:
            raise ValueError(
                f"`max_count` must be 1 or more, or -1 for no maximum. Received: {max_count}"
            )

        if len(list_in) <= max_count or max_count == -1:
            return list_in

        list_out = []

        while len(list_out) < max_count:
            list_out.append(list_in.pop(random.randint(0, len(list_in) - 1)))

        return list_out


async def main():
    swarm = await CosmosSwarm.create("")
    await swarm.run()


if __name__ == "__main__":

    asyncio.run(main())
