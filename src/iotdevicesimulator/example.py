"""Module for demonstrating invocation of a swarm."""

from iotdevicesimulator.queries import CosmosQuery
from iotdevicesimulator.swarm import CosmosSwarm
from iotdevicesimulator.messaging.aws import IotCoreMQTTConnection
import asyncio
import config
from pathlib import Path
import logging


async def main(config_path: str):
    """Runs the main loop of the program.

    Args:
        config_path: A path to the config file for credentials and connection points.
    """
    log_config = Path(Path(__file__).parent, "__assets__", "loggers.ini")

    logging.config.fileConfig(fname=log_config)

    app_config = config.Config(config_path)

    iot_config = app_config["iot_core"]
    oracle_config = app_config["oracle"]

    mqtt_connection = IotCoreMQTTConnection(
        endpoint=iot_config["endpoint"],
        cert_path=iot_config["cert_path"],
        key_path=iot_config["key_path"],
        ca_cert_path=iot_config["ca_cert_path"],
        client_id="fdri_swarm",
    )
    swarm = await CosmosSwarm.create(
        CosmosQuery.LEVEL_1_SOILMET_30MIN,
        mqtt_connection,
        oracle_config,
        swarm_name="soilmet",
        delay_start=True,
        max_cycles=5,
        max_sites=5,
        sleep_time=30,
    )
    await swarm.run()


if __name__ == "__main__":

    config_path = str(Path(Path(__file__).parent, "__assets__", "config.cfg"))
    asyncio.run(main(config_path))
