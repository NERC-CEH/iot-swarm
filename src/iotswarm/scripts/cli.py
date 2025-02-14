"""CLI exposed when the package is installed."""

# ruff: noqa: F811 # Ignore redefinition of unused name (It's a CLI, they're all unused)

import asyncio
import logging
import logging.config
from pathlib import Path
from typing import List

import click

import iotswarm.scripts.common as cli_common
from iotswarm import __version__ as package_version
from iotswarm.db import LoopingCsvDB, LoopingSQLite3, Oracle
from iotswarm.devices import BaseDevice, CR1000XDevice
from iotswarm.messaging.aws import IotCoreMQTTConnection
from iotswarm.messaging.core import MockMessageConnection
from iotswarm.queries import CosmosTable
from iotswarm.swarm import Swarm

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
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    help="Overrides the logging level.",
    envvar="IOT_SWARM_LOG_LEVEL",
)
def main(ctx: click.Context, log_config: Path, log_level: str) -> None:
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
def get_version() -> None:
    """Gets the package version"""
    click.echo(package_version)


@main.command()
@click.pass_context
@cli_common.iotcore_options
def test_mqtt(
    ctx: click.Context,
    message: str,
    topic: str,
    client_id: str,
    endpoint: str,
    cert_path: str,
    key_path: str,
    ca_cert_path: str,
) -> None:
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
def cosmos(ctx: click.Context, site: str, dsn: str, user: str, password: str) -> None:
    """Uses the COSMOS database as the source for data to send."""
    ctx.obj["credentials"] = {"dsn": dsn, "user": user, "password": password}
    ctx.obj["sites"] = site


cosmos.add_command(cli_common.test)


@cosmos.command()
@click.pass_context
@click.argument("table", type=click.Choice(TABLE_NAMES))
@click.option("--max-sites", type=click.IntRange(min=0), default=0)
def list_sites(ctx: click.Context, table: str, max_sites: int) -> None:
    """Lists unique `site_id` from an oracle database table."""

    async def _list_sites() -> List[str]:
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
    ctx: click.Context,
    table: str,
    endpoint: str,
    cert_path: str,
    key_path: str,
    ca_cert_path: str,
    client_id: str,
    sleep_time: int,
    max_cycles: int,
    max_sites: int,
    swarm_name: str,
    delay_start: bool,
    mqtt_prefix: str,
    mqtt_suffix: str,
    dry: bool,
    device_type: str,
    no_send_probability: int,
) -> None:
    """Sends The cosmos data via MQTT protocol using IoT Core.
    Data is from the cosmos database TABLE and sent using CLIENT_ID.

    Currently only supports sending through AWS IoT Core."""
    table = CosmosTable[table]

    async def _mqtt() -> None:
        oracle = await Oracle.create(
            dsn=ctx.obj["credentials"]["dsn"],
            user=ctx.obj["credentials"]["user"],
            password=ctx.obj["credentials"]["password"],
            inherit_logger=ctx.obj["logger"],
        )

        sites = ctx.obj["sites"]
        if len(sites) == 0:
            sites = await oracle.query_site_ids(table, max_sites=max_sites)

        if dry:
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
                no_send_probability=no_send_probability,
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
def looping_csv(ctx: click.Context, site: str, file: str) -> None:
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
@click.option(
    "--resume-session",
    is_flag=True,
    default=False,
    help='Resumes the session if it exists. Must be used with "session-name".',
)
def mqtt(
    ctx: click.Context,
    endpoint: str,
    cert_path: str,
    key_path: str,
    ca_cert_path: str,
    client_id: str,
    sleep_time: int,
    max_cycles: int,
    max_sites: int,
    swarm_name: str,
    delay_start: bool,
    mqtt_prefix: str,
    mqtt_suffix: str,
    dry: bool,
    device_type: str,
    resume_session: bool,
    no_send_probability: int,
) -> None:
    """Sends The cosmos data via MQTT protocol using IoT Core.
    Data is collected from the db using QUERY and sent using CLIENT_ID.

    Currently only supports sending through AWS IoT Core."""

    async def _mqtt_resume_session() -> None:
        swarm = Swarm.load_swarm(swarm_name)
        connection = IotCoreMQTTConnection(
            endpoint=endpoint,
            cert_path=cert_path,
            key_path=key_path,
            ca_cert_path=ca_cert_path,
            client_id=client_id,
            inherit_logger=ctx.obj["logger"],
        )

        for i in range(len(swarm.devices)):
            swarm.devices[i].connection = connection
            swarm.devices[i].delay_start = delay_start
            swarm.devices[i].device_type = device_type

            if max_cycles is not None:
                swarm.devices[i].max_cycles = max_cycles
            if sleep_time is not None:
                swarm.devices[i].sleep_time = sleep_time
            if mqtt_prefix is not None:
                swarm.devices[i].mqtt_prefix = mqtt_prefix
            if mqtt_suffix is not None:
                swarm.devices[i].mqtt_suffix = mqtt_suffix
            if no_send_probability is not None:
                swarm.devices[i].no_send_probability = no_send_probability

        click.echo("Loaded swarm from pickle")

        await swarm.run()

    async def _mqtt_clean_session() -> None:
        sites = ctx.obj["sites"]
        db = ctx.obj["db"]
        if len(sites) == 0:
            sites = db.query_site_ids(max_sites=max_sites)

        if dry:
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
                no_send_probability=no_send_probability,
            )
            for site in sites
        ]

        swarm = Swarm(site_devices, swarm_name)
        [device._attach_swarm(swarm) for device in swarm.devices]
        await swarm.run()

    if resume_session and swarm_name is not None and Swarm._swarm_exists(swarm_name):
        asyncio.run(_mqtt_resume_session())
    else:
        asyncio.run(_mqtt_clean_session())


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
def looping_sqlite3(ctx: click.Context, site: str, file: str) -> None:
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
def list_sites(ctx: click.Context, max_sites: int, table: str) -> None:
    """Prints the sites present in database."""
    sites = ctx.obj["db"].query_site_ids(table, max_sites=max_sites)
    click.echo(sites)


@looping_sqlite3.command()
@click.pass_context
@cli_common.device_options
@cli_common.iotcore_options
@click.argument("table", type=click.Choice(TABLE_NAMES))
@click.option("--dry", is_flag=True, default=False, help="Doesn't send out any data.")
@click.option(
    "--resume-session",
    is_flag=True,
    default=False,
    help='Resumes the session if it exists. Must be used with "session-name".',
)
def mqtt(
    ctx: click.Context,
    table: str,
    endpoint: str,
    cert_path: str,
    key_path: str,
    ca_cert_path: str,
    client_id: str,
    sleep_time: int,
    max_cycles: int,
    max_sites: int,
    swarm_name: str,
    delay_start: bool,
    mqtt_prefix: str,
    mqtt_suffix: str,
    dry: bool,
    device_type: str,
    resume_session: bool,
    no_send_probability: int,
) -> None:
    """Sends The cosmos data via MQTT protocol using IoT Core.
    Data is collected from the db using QUERY and sent using CLIENT_ID.

    Currently only supports sending through AWS IoT Core."""

    table = CosmosTable[table]

    async def _mqtt_resume_session() -> None:
        swarm = Swarm.load_swarm(swarm_name)
        connection = IotCoreMQTTConnection(
            endpoint=endpoint,
            cert_path=cert_path,
            key_path=key_path,
            ca_cert_path=ca_cert_path,
            client_id=client_id,
            inherit_logger=ctx.obj["logger"],
        )

        for i in range(len(swarm.devices)):
            swarm.devices[i].connection = connection
            swarm.devices[i].delay_start = delay_start
            swarm.devices[i].device_type = device_type

            if max_cycles is not None:
                swarm.devices[i].max_cycles = max_cycles
            if sleep_time is not None:
                swarm.devices[i].sleep_time = sleep_time
            if mqtt_prefix is not None:
                swarm.devices[i].mqtt_prefix = mqtt_prefix
            if mqtt_suffix is not None:
                swarm.devices[i].mqtt_suffix = mqtt_suffix
            if no_send_probability is not None:
                swarm.devices[i].no_send_probability = no_send_probability

        click.echo(swarm.devices[0].cycle)
        click.echo("Loaded swarm from pickle")
        await swarm.run()

    async def _mqtt_clean_session() -> None:
        click.echo("Starting clean session")

        sites = ctx.obj["sites"]
        db = ctx.obj["db"]
        if len(sites) == 0:
            sites = db.query_site_ids(table, max_sites=max_sites)

        if dry:
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
                no_send_probability=no_send_probability,
            )
            for site in sites
        ]

        swarm = Swarm(site_devices, swarm_name)

        [device._attach_swarm(swarm) for device in swarm.devices]
        await swarm.run()

    if resume_session and swarm_name is not None and Swarm._swarm_exists(swarm_name):
        asyncio.run(_mqtt_resume_session())
    else:
        asyncio.run(_mqtt_clean_session())


@main.group()
def sessions() -> None:
    """Group for managing sessions."""


@sessions.command()
def ls() -> None:
    """Lists the swarms."""
    click.echo(Swarm._list_swarms())


@sessions.command()
@click.argument("session-id", type=click.STRING)
def init(session_id: str) -> None:
    """Creates an empty swarm file."""
    Swarm._initialise_swarm_file(session_id)


@sessions.command()
@click.argument("session-id", type=click.STRING)
def rm(session_id: str) -> None:
    """Deletes a swarm."""
    if Swarm._swarm_exists(session_id):
        Swarm.destroy_swarm(session_id)
        click.echo(f'Session "{session_id}" deleted.')
    else:
        click.echo(f'Session "{session_id}" does not exist.')


if __name__ == "__main__":
    main(auto_envvar_prefix="IOT_SWARM", obj={})
