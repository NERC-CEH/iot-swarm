import asyncio
import logging
from iotdevicesimulator.queries import CosmosQuery
from iotdevicesimulator.db import Oracle
import random

logger = logging.getLogger(__name__)

class SensorSite:
    """Digital representation of a site used in FDRI
    
    Args:
        site_id (str): ID of site.
        sleep_time (int | float): Time to sleep between requests (seconds).
        max_cycles (int): Maximum number of cycles before shutdown.
        inherit_logger (logging.Logger): Override for the module logger.
        delay_first_cycle (bool|None): Adds a random delay to first invocation from 0 - `sleep_time`.
    
    Attributes:
        site_id (str): ID of site.
        sleep_time (int | float): Time to sleep between requests (seconds).
        max_cycles (int): Maximum number of cycles before shutdown.
        cycle (int): The current cycle."""

    def __init__(self, site_id: str,*, sleep_time: int | float = 0, max_cycles: int = 1, inherit_logger:logging.Logger|None=None, delay_first_cycle:bool=False) -> None:
        self.site_id = str(site_id)

        if inherit_logger:
            self._instance_logger = inherit_logger.getChild(f"site-{self.site_id}")
        else:
            self._instance_logger = logger.getChild(self.site_id)

        max_cycles = int(max_cycles)
        sleep_time = int(sleep_time)

        if not isinstance(delay_first_cycle, bool):
            raise TypeError(
                f"`delay_first_cycle` must be a bool. Received: {delay_first_cycle}."
            )

        self.delay_first_cycle = delay_first_cycle

        if max_cycles <= 0 and max_cycles != -1:
            raise ValueError(f"`max_cycles` must be 1 or more, or -1 for no maximum. Received: {max_cycles}")

        if sleep_time < 0:
            raise ValueError(f"`sleep_time` must be 0 or more. Received: {sleep_time}")
        
        self.sleep_time = sleep_time
        self.max_cycles = max_cycles
        self.cycle = 0

        self._instance_logger.info(f"Initialised Site: {repr(self)}")

    def __repr__(self):
        return f"SensorSite(\"{self.site_id}\", sleep_time={self.sleep_time}, max_cycles={self.max_cycles})"

    def __str__(self):
        return f"Site ID: \"{self.site_id}\", Sleep Time: {self.sleep_time}, Max Cycles: {self.max_cycles}, Cycle: {self.cycle}"

    async def run(self, oracle: Oracle, query: CosmosQuery):
        """The main invocation of the method. Expects a query function from
            iotthingsimulator.db.Oracle to be injected for the work.
        
        Args:
            oracle (Oracle): The oracle database.
            query (CosmosQuery): Query to process by database."""
        while True:

            if self.delay_first_cycle and self.cycle == 0:
                delay = random.randint(0, self.sleep_time)
                self._instance_logger.debug(f"Delaying first cycle for: {delay}s")
                await asyncio.sleep(delay)

            row = await oracle.query_latest_from_site(self.site_id, query)

            if not row:
                self._instance_logger.warn(f"No data found.")
            else:
                self._instance_logger.debug(f"Cycle {self.cycle+1}/{self.max_cycles} Read data from: {row["DATE_TIME"]}")
            
            self.cycle += 1
            if self.cycle >= self.max_cycles:
                break

            await asyncio.sleep(self.sleep_time)
