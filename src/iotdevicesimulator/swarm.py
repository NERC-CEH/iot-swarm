import logging
import logging.handlers

from iotdevicesimulator.devices import SensorSite
from iotdevicesimulator.db import Oracle
from typing import List
import pathlib
import asyncio
import config
import random
import platformdirs
import os

CONFIG_PATH = pathlib.Path(pathlib.Path(__file__).parents[2], "config.cfg")
LOGFILE = pathlib.Path(
    platformdirs.site_data_dir("iot_device_simulator"),
    "swarm.log",
)


def get_logger():

    if not LOGFILE.parent.exists():
        os.makedirs(LOGFILE.parent)

    # rotating_handler = logging.handlers.RotatingFileHandler(
    #     LOGFILE, maxBytes=(1048576 * 5), backupCount=7
    # )

    rotating_handler = logging.handlers.TimedRotatingFileHandler(
        LOGFILE, when="W0", backupCount=7
    )

    stream_handler = logging.StreamHandler()

    logging.basicConfig(
        handlers=[stream_handler, rotating_handler],
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%d/%m/%Y %H:%M:%S",
    )


async def main() -> None:

    get_logger()

    logging.info(f"Started. Writing to logfile: {LOGFILE}")

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

    logging.info("Finished")


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
