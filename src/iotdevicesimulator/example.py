"""Module for demonstrating invocation of a swarm."""

from iotdevicesimulator.queries import CosmosQuery, CosmosSiteQuery
from iotdevicesimulator.swarm import Swarm
from iotdevicesimulator import devices
from iotdevicesimulator.messaging.core import MockMessageConnection
from iotdevicesimulator.messaging.aws import IotCoreMQTTConnection
from iotdevicesimulator import db
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

    data_source = await db.Oracle.create(
        oracle_config["dsn"], oracle_config["user"], oracle_config["password"]
    )

    site_query = CosmosSiteQuery.LEVEL_1_SOILMET_30MIN
    query = CosmosQuery[site_query.name]

    device_ids = await data_source.query_site_ids(site_query, max_sites=5)

    mqtt_connection = IotCoreMQTTConnection(
        endpoint=iot_config["endpoint"],
        cert_path=iot_config["cert_path"],
        key_path=iot_config["key_path"],
        ca_cert_path=iot_config["ca_cert_path"],
        client_id="fdri_swarm",
    )

    device = devices.DeviceFactory.create_device(
        MockMessageConnection(),
        data_source,
        query,
        "site_id",
        device_type="cr1000x",
    )

    # devices = [
    #     MQTTCosmosDevice(
    #         query, site, data_source, mqtt_connection, topic_prefix="fdri/cosmos_sites"
    #     )
    #     for site in device_ids
    # ]

    # swarm = Swarm(devices, name="soilmet")
    # await swarm.run()


if __name__ == "__main__":

    config_path = str(Path(Path(__file__).parent, "__assets__", "config.cfg"))
    asyncio.run(main(config_path))
