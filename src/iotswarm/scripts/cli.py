"""CLI exposed when the package is installed."""

import click
from iotswarm import queries
from iotswarm.devices import BaseDevice, CR1000XDevice
from iotswarm.swarm import Swarm
from iotswarm.db import Oracle, LoopingCsvDB
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.messaging.aws import IotCoreMQTTConnection
import asyncio
from pathlib import Path
import logging

TABLES = [table.name for table in queries.CosmosQuery]


@click.command
@click.pass_context
def test(ctx: click.Context):
    """Enables testing of cosmos group arguments."""
    print(ctx.obj)


@click.group()
@click.pass_context
@click.option(
    "--log-config",
    type=click.Path(exists=True),
    help="Path to a logging config file. Uses default if not given.",
)
def main(ctx: click.Context, log_config: Path):
    """Core group of the cli."""
    ctx.ensure_object(dict)

    if not log_config:
        log_config = Path(Path(__file__).parents[1], "__assets__", "loggers.ini")

    logging.config.fileConfig(fname=log_config)


main.add_command(test)


@main.command()
@click.pass_context
@click.argument(
    "client-id",
    type=click.STRING,
)
@click.argument("topic", type=click.STRING)
@click.argument("message", type=click.STRING)
@click.option(
    "--endpoint",
    type=click.STRING,
    required=True,
    envvar="IOT_SWARM_MQTT_ENDPOINT",
    help="Endpoint of the MQTT receiving host.",
)
@click.option(
    "--cert-path",
    type=click.Path(exists=True),
    required=True,
    envvar="IOT_SWARM_MQTT_CERT_PATH",
    help="Path to public key certificate for the device. Must match key assigned to the `--client-id` in the cloud provider.",
)
@click.option(
    "--key-path",
    type=click.Path(exists=True),
    required=True,
    envvar="IOT_SWARM_MQTT_KEY_PATH",
    help="Path to the private key that pairs with the `--cert-path`.",
)
@click.option(
    "--ca-cert-path",
    type=click.Path(exists=True),
    required=True,
    envvar="IOT_SWARM_MQTT_CA_CERT_PATH",
    help="Path to the root Certificate Authority (CA) for the MQTT host.",
)
def test_mqtt(
    ctx: click.Context,
    message,
    topic,
    client_id: str,
    endpoint: str,
    cert_path: str,
    key_path: str,
    ca_cert_path: str,
):
    """Tests that a basic message can be sent via mqtt."""

    connection = IotCoreMQTTConnection(
        endpoint=endpoint,
        cert_path=cert_path,
        key_path=key_path,
        ca_cert_path=ca_cert_path,
        client_id=client_id,
    )

    connection.send_message(message, topic)


@main.group()
@click.pass_context
@click.option(
    "--site",
    type=click.STRING,
    multiple=True,
    help="Adds a site to be initialized. Can be invoked multiple times for other sites."
    " Grabs all sites from database query if none provided",
)
@click.option(
    "--dsn",
    type=click.STRING,
    required=True,
    envvar="IOT_SWARM_COSMOS_DSN",
    help="Data source name (DSN) for the COSMOS database.",
)
@click.option(
    "--user",
    type=click.STRING,
    required=True,
    envvar="IOT_SWARM_COSMOS_USER",
    help="Username for accessing the COSMOS database. Must have at least read access.",
)
@click.option(
    "--password",
    type=click.STRING,
    required=True,
    prompt=True,
    envvar="IOT_SWARM_COSMOS_PASSWORD",
    help="Password corresponding to `--user` for COMSOS database login.",
)
def cosmos(ctx: click.Context, site: str, dsn: str, user: str, password: str):
    """Uses the COSMOS database as the source for data to send."""
    ctx.obj["credentials"] = {"dsn": dsn, "user": user, "password": password}
    ctx.obj["sites"] = site


cosmos.add_command(test)


@cosmos.command()
@click.pass_context
@click.argument("query", type=click.Choice(TABLES))
@click.option("--max-sites", type=click.IntRange(min=0), default=0)
def list_sites(ctx, query, max_sites):
    """Lists site IDs from the database from table QUERY."""

    async def _list_sites(ctx, query):
        oracle = await Oracle.create(
            dsn=ctx.obj["credentials"]["dsn"],
            user=ctx.obj["credentials"]["user"],
            password=ctx.obj["credentials"]["password"],
        )
        sites = await oracle.query_site_ids(
            queries.CosmosSiteQuery[query], max_sites=max_sites
        )
        return sites

    click.echo(asyncio.run(_list_sites(ctx, query)))


def common_device_options(function):
    click.option(
        "--sleep-time",
        type=click.INT,
        help="The number of seconds each site goes idle after sending a message.",
    )(function)

    click.option(
        "--max-cycles",
        type=click.IntRange(0),
        help="Maximum number message sending cycles. Runs forever if set to 0.",
    )(function)

    click.option(
        "--max-sites",
        type=click.IntRange(0),
        help="Maximum number of sites allowed to initialize. No limit if set to 0.",
    )(function)

    click.option(
        "--swarm-name",
        type=click.STRING,
        help="Name given to swarm. Appears in the logs.",
    )(function)

    click.option(
        "--delay-start",
        is_flag=True,
        default=False,
        help="Adds a random delay before the first message from each site up to `--sleep-time`.",
    )(function)

    click.option(
        "--device-type", type=click.Choice(["basic", "cr1000x"]), default="basic"
    )(function)

    return function


def common_iotcore_options(function):
    click.argument(
        "client-id",
        type=click.STRING,
        required=True,
    )(function)

    click.option(
        "--endpoint",
        type=click.STRING,
        required=True,
        envvar="IOT_SWARM_MQTT_ENDPOINT",
        help="Endpoint of the MQTT receiving host.",
    )(function)

    click.option(
        "--cert-path",
        type=click.Path(exists=True),
        required=True,
        envvar="IOT_SWARM_MQTT_CERT_PATH",
        help="Path to public key certificate for the device. Must match key assigned to the `--client-id` in the cloud provider.",
    )(function)

    click.option(
        "--key-path",
        type=click.Path(exists=True),
        required=True,
        envvar="IOT_SWARM_MQTT_KEY_PATH",
        help="Path to the private key that pairs with the `--cert-path`.",
    )(function)

    click.option(
        "--ca-cert-path",
        type=click.Path(exists=True),
        required=True,
        envvar="IOT_SWARM_MQTT_CA_CERT_PATH",
        help="Path to the root Certificate Authority (CA) for the MQTT host.",
    )(function)

    click.option(
        "--mqtt-prefix",
        type=click.STRING,
        help="Prefixes the MQTT topic with a string. Can augment the calculated MQTT topic returned by each site.",
    )(function)

    click.option(
        "--mqtt-suffix",
        type=click.STRING,
        help="Suffixes the MQTT topic with a string. Can augment the calculated MQTT topic returned by each site.",
    )(function)

    return function


@cosmos.command()
@click.pass_context
@click.argument(
    "query",
    type=click.Choice(TABLES),
)
@common_device_options
@common_iotcore_options
@click.option("--dry", is_flag=True, default=False, help="Doesn't send out any data.")
def mqtt(
    ctx,
    query,
    endpoint,
    cert_path,
    key_path,
    ca_cert_path,
    client_id,
    sleep_time,
    max_cycles,
    max_sites,
    swarm_name,
    delay_start,
    mqtt_prefix,
    mqtt_suffix,
    dry,
    device_type,
):
    """Sends The cosmos data via MQTT protocol using IoT Core.
    Data is collected from the db using QUERY and sent using CLIENT_ID.

    Currently only supports sending through AWS IoT Core."""

    async def _mqtt():
        oracle = await Oracle.create(
            dsn=ctx.obj["credentials"]["dsn"],
            user=ctx.obj["credentials"]["user"],
            password=ctx.obj["credentials"]["password"],
        )

        data_query = queries.CosmosQuery[query]
        site_query = queries.CosmosSiteQuery[query]

        sites = ctx.obj["sites"]
        if len(sites) == 0:
            sites = await oracle.query_site_ids(site_query, max_sites=max_sites)

        if dry == True:
            connection = MockMessageConnection()
        else:
            connection = IotCoreMQTTConnection(
                endpoint=endpoint,
                cert_path=cert_path,
                key_path=key_path,
                ca_cert_path=ca_cert_path,
                client_id=client_id,
            )

        if device_type == "basic":
            DeviceClass = BaseDevice
        elif device_type == "cr1000x":
            DeviceClass = CR1000XDevice

        site_devices = [
            DeviceClass(
                site,
                oracle,
                connection,
                sleep_time=sleep_time,
                query=data_query,
                max_cycles=max_cycles,
                delay_start=delay_start,
                mqtt_prefix=mqtt_prefix,
                mqtt_suffix=mqtt_suffix,
            )
            for site in sites
        ]

        swarm = Swarm(site_devices, swarm_name)

        await swarm.run()

    asyncio.run(_mqtt())


@main.group()
@click.pass_context
@click.option(
    "--site",
    type=click.STRING,
    multiple=True,
    help="Adds a site to be initialized. Can be invoked multiple times for other sites."
    " Grabs all sites from database query if none provided",
)
@click.option(
    "--file",
    type=click.Path(exists=True),
    required=True,
    envvar="IOT_SWARM_CSV_DB",
    help="*.csv file used to instantiate a pandas database.",
)
def looping_csv(ctx, site, file):
    """Instantiates a pandas dataframe from a csv file  which is used as the database.
    Responsibility falls on the user to ensure the correct file is selected."""

    ctx.obj["db"] = LoopingCsvDB(file)
    ctx.obj["sites"] = site


looping_csv.add_command(test)


@looping_csv.command
@click.pass_context
@click.option("--max-sites", type=click.IntRange(min=0), default=0)
def list_sites(ctx, max_sites):
    """Prints the sites present in database."""

    sites = ctx.obj["db"].query_site_ids(max_sites=max_sites)
    click.echo(sites)


@looping_csv.command()
@click.pass_context
@common_device_options
@common_iotcore_options
@click.option("--dry", is_flag=True, default=False, help="Doesn't send out any data.")
def mqtt(
    ctx,
    endpoint,
    cert_path,
    key_path,
    ca_cert_path,
    client_id,
    sleep_time,
    max_cycles,
    max_sites,
    swarm_name,
    delay_start,
    mqtt_prefix,
    mqtt_suffix,
    dry,
    device_type,
):
    """Sends The cosmos data via MQTT protocol using IoT Core.
    Data is collected from the db using QUERY and sent using CLIENT_ID.

    Currently only supports sending through AWS IoT Core."""

    async def _mqtt():

        sites = ctx.obj["sites"]
        db = ctx.obj["db"]
        if len(sites) == 0:
            sites = db.query_site_ids(max_sites=max_sites)

        if dry == True:
            connection = MockMessageConnection()
        else:
            connection = IotCoreMQTTConnection(
                endpoint=endpoint,
                cert_path=cert_path,
                key_path=key_path,
                ca_cert_path=ca_cert_path,
                client_id=client_id,
            )

        if device_type == "basic":
            DeviceClass = BaseDevice
        elif device_type == "cr1000x":
            DeviceClass = CR1000XDevice

        site_devices = [
            DeviceClass(
                site,
                db,
                connection,
                sleep_time=sleep_time,
                max_cycles=max_cycles,
                delay_start=delay_start,
                mqtt_prefix=mqtt_prefix,
                mqtt_suffix=mqtt_suffix,
            )
            for site in sites
        ]

        swarm = Swarm(site_devices, swarm_name)

        await swarm.run()

    asyncio.run(_mqtt())


if __name__ == "__main__":
    main(auto_envvar_prefix="IOT_SWARM", obj={})
