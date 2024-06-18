"""CLI exposed when the package is installed."""

import click
from iotswarm import __version__ as package_version
from iotswarm import queries
from iotswarm.devices import BaseDevice, CR1000XDevice
from iotswarm.swarm import Swarm
from iotswarm.db import Oracle, LoopingCsvDB, LoopingSQLite3
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.messaging.aws import IotCoreMQTTConnection
import iotswarm.scripts.common as cli_common
import asyncio
from pathlib import Path
import logging

TABLES = [table.name for table in queries.CosmosQuery]


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


main.add_command(cli_common.test)


@main.command
def get_version():
    """Gets the package version"""
    click.echo(package_version)


@main.command()
@click.pass_context
@cli_common.iotcore_options
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


cosmos.add_command(cli_common.test)


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


@cosmos.command()
@click.pass_context
@click.argument(
    "query",
    type=click.Choice(TABLES),
)
@cli_common.device_options
@cli_common.iotcore_options
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
    "query",
    type=click.Choice(TABLES),
)
def list_sites(ctx, max_sites, query):
    """Prints the sites present in database."""
    query = queries.CosmosSiteSqliteQuery[query]
    sites = ctx.obj["db"].query_site_ids(max_sites=max_sites, query=query)
    click.echo(sites)


@looping_sqlite3.command()
@click.pass_context
@cli_common.device_options
@cli_common.iotcore_options
@click.argument("query", type=click.Choice(TABLES))
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

        sites = ctx.obj["sites"]
        db = ctx.obj["db"]
        if len(sites) == 0:
            sites = db.query_site_ids(
                max_sites=max_sites, query=queries.CosmosSiteSqliteQuery[query]
            )

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
                query=queries.CosmosSqliteQuery[query],
            )
            for site in sites
        ]

        swarm = Swarm(site_devices, swarm_name)

        await swarm.run()

    asyncio.run(_mqtt())


if __name__ == "__main__":
    main(auto_envvar_prefix="IOT_SWARM", obj={})
