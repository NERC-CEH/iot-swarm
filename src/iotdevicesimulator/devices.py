import asyncio
import random


class SensorSite:
    """Digital representation of a site used in FDRI"""

    def __init__(self, site_id: str, max_cycles: int = 1) -> None:
        self.site_id = site_id

        max_cycles = int(max_cycles)

        if max_cycles < 0:
            raise ValueError(f"'max_cycles cannot be negative. Received: {max_cycles}")

        self.max_cycles = max_cycles
        self.cycle = 0

    def __repr__(self):
        return f"SensorSite({self.site_id}, {self.max_cycles})"

    def __str__(self):
        return f"Site ID: {self.site_id}, Max Cycles: {self.max_cycles}, Cycle: {self.cycle}"

    async def run(self):

        while True:
            sleep = random.randrange(0, 1)
            print(f"{str(self)} sleeping for: {sleep}")

            await asyncio.sleep(sleep)
            self.cycle += 1

            if self.cycle >= self.max_cycles:
                break


async def main():

    max_cycles = [5, 10, 15]

    sites = [SensorSite(f"Site {i}", max_cycles[i]) for i in range(len(max_cycles))]

    await asyncio.gather(*[x.run() for x in sites])


if __name__ == "__main__":

    asyncio.run(main())
