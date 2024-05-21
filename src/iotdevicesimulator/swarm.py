from iotdevicesimulator.devices import SensorSite
from iotdevicesimulator.db import Oracle
import logging

from typing import List
import pathlib
import asyncio
import config
import random

# TODO: Rewrite as a class
# TODO: Implement messaging logic
# TODO: Implement delay in first message

CONFIG_PATH = pathlib.Path(pathlib.Path(__file__).parents[2], "config.cfg")

logger = logging.getLogger(__name__)


async def main() -> None:

    oracle = await get_oracle()
    query = oracle.query_latest_COSMOS_level1_soilmet_30min

    site_ids = get_sites()[:5]
    sleep_time = [random.randint(1, 5) for _ in site_ids]
    max_cycles = 3

    sites = [
        SensorSite(site, sleep_time=t, max_cycles=max_cycles)
        for (t, site) in zip(sleep_time, site_ids)
    ]

    await asyncio.gather(*[site.run(query) for site in sites])

    logger.info("Finished")


async def get_oracle() -> Oracle:
    cred_path = str(CONFIG_PATH)
    creds = config.Config(cred_path)["oracle"]

    oracle = await Oracle.create(
        creds["dsn"],
        creds["user"],
        password=creds["password"],
    )

    return oracle


def get_sites() -> List[str]:

    sites = [
        "ALIC1",
        "BALRD",
        "BUNNY",
        "COCHN",
        "EUSTN",
        "FINCH",
        "GLENW",
        "HARTW",
        "PLYNL",
        "WADDN",
        "CHIMN",
        "COCLP",
        "EASTB",
        "HADLW",
        "LIZRD",
        "MORLY",
        "NWYKE",
        "REDHL",
        "ROTHD",
        "SOURH",
        "SPENF",
        "STIPS",
        "WIMPL",
        "WRTTL",
        "BICKL",
        "CHOBH",
        "CRICH",
        "ELMST",
        "GISBN",
        "GLENS",
        "HARWD",
        "HILLB",
        "LODTN",
        "MOORH",
        "RISEH",
        "SHEEP",
        "STGHT",
        "SYDLG",
        "TADHM",
        "WYTH1",
        "CARDT",
        "CGARW",
        "FIVET",
        "HENFS",
        "HLACY",
        "HOLLN",
        "HYBRY",
        "LULLN",
        "MOREM",
        "PORTN",
        "RDMER",
    ]

    return sites


if __name__ == "__main__":
    asyncio.run(main())
