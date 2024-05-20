import asyncio


class SensorSite:
    """Digital representation of a site used in FDRI
    
    Args:
        site_id (str): ID of site.
        sleep_time (int | float): Time to sleep between requests (seconds).
        max_cycles (int): Maximum number of cycles before shutdown.
    
    Attributes:
        site_id (str): ID of site.
        sleep_time (int | float): Time to sleep between requests (seconds).
        max_cycles (int): Maximum number of cycles before shutdown.
        cycle (int): The current cycle."""

    def __init__(self, site_id: str,*, sleep_time: int | float = 0, max_cycles: int = 1) -> None:
        self.site_id = site_id

        max_cycles = int(max_cycles)
        sleep_time = int(sleep_time)

        if max_cycles <= 0 and max_cycles != -1:
            raise ValueError(f"`max_cycles` must be 1 or more, or -1 for no maximum. Received: {max_cycles}")

        if sleep_time < 0:
            raise ValueError(f"`sleep_time` must be 0 or more. Received: {sleep_time}")
        
        self.sleep_time = sleep_time
        self.max_cycles = max_cycles
        self.cycle = 0

    def __repr__(self):
        return f"SensorSite({self.site_id}, sleep_time={self.sleep_time}, max_cycles={self.max_cycles})"

    def __str__(self):
        return f"Site ID: {self.site_id}, Sleep Time: {self.sleep_time}, Max Cycles: {self.max_cycles}, Cycle: {self.cycle}"

    async def run(self, query_function: callable):
        """The main invocation of the method. Expects a query function from
            iotthingsimulator.db.Oracle to be injected for the work.
        
        Args:
            query_function (callable): Query callable from database class."""
        while True:
            row = await query_function(self.site_id)

            await asyncio.sleep(self.sleep_time)

            print(f"{str(self)} Site: {row["SITE_ID"]}, WD: {row["WD"]}")
            self.cycle += 1

            if self.cycle >= self.max_cycles:
                break
