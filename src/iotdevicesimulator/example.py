from iotdevicesimulator.queries import CosmosQuery
from iotdevicesimulator.swarm import CosmosSwarm
from iotdevicesimulator.mqtt.aws import IotCoreMQTTConnection
import asyncio
import config
from pathlib import Path


async def main(config_path: str):
    """Runs the main loop of the program.

    Args:
        config_path: A path to the config file for credentials and connection points.
    """
    app_config = config.Config(config_path)

    iot_config = app_config["iot_core"]
    oracle_config = app_config["oracle"]

    mqtt_connection = IotCoreMQTTConnection(
        endpoint=iot_config["endpoint"],
        cert_path=iot_config["cert_path"],
        key_path=iot_config["pri_key_path"],
        ca_cert_path=iot_config["aws_ca_cert_path"],
        client_id="fdri_swarm",
    )
    swarm = await CosmosSwarm.create(
        CosmosQuery.LEVEL_1_SOILMET_30MIN,
        mqtt_connection,
        swarm_name="soilmet",
        delay_first_cycle=True,
        max_cycles=5,
        max_sites=5,
        sleep_time=30,
        credentials=oracle_config,
    )
    await swarm.run()


if __name__ == "__main__":

    config_path = str(Path(Path(__file__).parent, "__assets__", "config.cfg"))
    asyncio.run(main(config_path))
