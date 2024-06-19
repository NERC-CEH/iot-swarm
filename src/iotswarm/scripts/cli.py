"""CLI exposed when the package is installed."""

import click
from iotswarm import __version__ as package_version
from iotswarm.queries import CosmosTable
from iotswarm.devices import BaseDevice, CR1000XDevice
from iotswarm.swarm import Swarm
from iotswarm.db import Oracle, LoopingCsvDB, LoopingSQLite3
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.messaging.aws import IotCoreMQTTConnection
import iotswarm.scripts.common as cli_common
import asyncio
from pathlib import Path
import logging

TABLE_NAMES = [table.name for table in CosmosTable]


@click.group()
@click.pass_context
@click.option(
    "--log-config",
    type=click.Path(exists=True),
    help="Path to a logging config file. Uses default if not given.",
    default=Path(Path(__file__).parents[1], "__assets__", "loggers.ini"),
)
@click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    help="Overrides the logging level.",
    envvar="IOT_SWARM_LOG_LEVEL",
)
def main(ctx: click.Context, log_config: Path, log_level: str):
    """Core group of the cli."""
    ctx.ensure_object(dict)

    logging.config.fileConfig(fname=log_config)
    logger = logging.getLogger(__name__)

    if log_level:
        logger.setLevel(log_level)
        click.echo(f"Set log level to {log_level}.")

    ctx.obj["logger"] = logger


main.add_command(cli_common.test)


@main.command
def get_version():
    """Gets the package version"""
    click.echo(package_version)


@main.command()
@click.pass_context
@cli_common.iotcore_options
def test_mqtt(
    ctx,
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
        inherit_logger=ctx.obj["logger"],
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


cosmos.add_command(cli_common.test)


@cosmos.command()
@click.pass_context
@click.argument("table", type=click.Choice(TABLE_NAMES))
@click.option("--max-sites", type=click.IntRange(min=0), default=0)
def list_sites(ctx, table, max_sites):
    """Lists unique `site_id` from an oracle database table."""

    async def _list_sites():
        oracle = await Oracle.create(
            dsn=ctx.obj["credentials"]["dsn"],
            user=ctx.obj["credentials"]["user"],
            password=ctx.obj["credentials"]["password"],
            inherit_logger=ctx.obj["logger"],
        )
        sites = await oracle.query_site_ids(CosmosTable[table], max_sites=max_sites)
        return sites

    click.echo(asyncio.run(_list_sites()))


@cosmos.command()
@click.pass_context
@click.argument(
    "table",
    type=click.Choice(TABLE_NAMES),
)
@cli_common.device_options
@cli_common.iotcore_options
@click.option("--dry", is_flag=True, default=False, help="Doesn't send out any data.")
def mqtt(
    ctx,
    table,
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
    Data is from the cosmos database TABLE and sent using CLIENT_ID.

    Currently only supports sending through AWS IoT Core."""
    table = CosmosTable[table]

    async def _mqtt():
        oracle = await Oracle.create(
            dsn=ctx.obj["credentials"]["dsn"],
            user=ctx.obj["credentials"]["user"],
            password=ctx.obj["credentials"]["password"],
            inherit_logger=ctx.obj["logger"],
        )

        sites = ctx.obj["sites"]
        if len(sites) == 0:
            sites = await oracle.query_site_ids(table, max_sites=max_sites)

        if dry == True:
            connection = MockMessageConnection(inherit_logger=ctx.obj["logger"])
        else:
            connection = IotCoreMQTTConnection(
                endpoint=endpoint,
                cert_path=cert_path,
                key_path=key_path,
                ca_cert_path=ca_cert_path,
                client_id=client_id,
                inherit_logger=ctx.obj["logger"],
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
                table=table,
                max_cycles=max_cycles,
                delay_start=delay_start,
                mqtt_prefix=mqtt_prefix,
                mqtt_suffix=mqtt_suffix,
                inherit_logger=ctx.obj["logger"],
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


looping_csv.add_command(cli_common.test)
looping_csv.add_command(cli_common.list_sites)


@looping_csv.command()
@click.pass_context
@cli_common.device_options
@cli_common.iotcore_options
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
            connection = MockMessageConnection(inherit_logger=ctx.obj["logger"])
        else:
            connection = IotCoreMQTTConnection(
                endpoint=endpoint,
                cert_path=cert_path,
                key_path=key_path,
                ca_cert_path=ca_cert_path,
                client_id=client_id,
                inherit_logger=ctx.obj["logger"],
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
                inherit_logger=ctx.obj["logger"],
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
    envvar="IOT_SWARM_LOCAL_DB",
    help="*.db file used to instantiate a sqlite3 database.",
)
def looping_sqlite3(ctx, site, file):
    """Instantiates a sqlite3 database as sensor source.."""
    ctx.obj["db"] = LoopingSQLite3(file)
    ctx.obj["sites"] = site


looping_sqlite3.add_command(cli_common.test)


@looping_sqlite3.command
@click.pass_context
@click.option("--max-sites", type=click.IntRange(min=0), default=0)
@click.argument(
    "table",
    type=click.Choice(TABLE_NAMES),
)
def list_sites(ctx, max_sites, table):
    """Prints the sites present in database."""
    sites = ctx.obj["db"].query_site_ids(table, max_sites=max_sites)
    click.echo(sites)


@looping_sqlite3.command()
@click.pass_context
@cli_common.device_options
@cli_common.iotcore_options
@click.argument("table", type=click.Choice(TABLE_NAMES))
@click.option("--dry", is_flag=True, default=False, help="Doesn't send out any data.")
def mqtt(
    ctx,
    table,
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

    table = CosmosTable[table]

    async def _mqtt():

        sites = ctx.obj["sites"]
        db = ctx.obj["db"]
        if len(sites) == 0:
            sites = db.query_site_ids(table, max_sites=max_sites)

        if dry == True:
            connection = MockMessageConnection(inherit_logger=ctx.obj["logger"])
        else:
            connection = IotCoreMQTTConnection(
                endpoint=endpoint,
                cert_path=cert_path,
                key_path=key_path,
                ca_cert_path=ca_cert_path,
                client_id=client_id,
                inherit_logger=ctx.obj["logger"],
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
                table=table,
                inherit_logger=ctx.obj["logger"],
            )
            for site in sites
        ]

        swarm = Swarm(site_devices, swarm_name)

        await swarm.run()

    asyncio.run(_mqtt())


if __name__ == "__main__":
    main(auto_envvar_prefix="IOT_SWARM", obj={})
