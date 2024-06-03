"""CLI exposed when the package is installed."""

import click
from iotdevicesimulator import queries
from iotdevicesimulator.swarm import CosmosSwarm
from iotdevicesimulator.messaging.core import MockMessageConnection
from iotdevicesimulator.messaging.aws import IotCoreMQTTConnection
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
@click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    ),
    help="Sets the level of logs outputted.",
)
def main(ctx: click.Context, log_config: Path, log_level: str):
    """Core group of the cli."""
    ctx.ensure_object(dict)

    if not log_config:
        log_config = Path(Path(__file__).parents[1], "__assets__", "loggers.ini")

    logging.config.fileConfig(fname=log_config)

    if log_level:
        logging.getLogger().setLevel(log_level)


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


@cosmos.command()
def test():
    """Enables testing of cosmos group arguments."""
    pass


@cosmos.command()
@click.pass_context
@click.argument("query", type=click.Choice(TABLES))
def list_sites(ctx, query):
    """Lists site IDs from the database from table QUERY."""

    async def _list_sites(ctx, query):
        oracle = await CosmosSwarm._get_oracle(ctx.obj["credentials"])
        sites = await CosmosSwarm._get_sites_from_db(
            oracle, queries.CosmosSiteQuery[query]
        )
        return sites

    click.echo(asyncio.run(_list_sites(ctx, query)))


@cosmos.command()
@click.pass_context
@click.argument(
    "provider",
    type=click.Choice(["aws"]),
)
@click.argument(
    "query",
    type=click.Choice(TABLES),
)
@click.argument(
    "client-id",
    type=click.STRING,
    required=True,
)
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
@click.option(
    "--sleep-time",
    type=click.INT,
    help="The number of seconds each site goes idle after sending a message.",
)
@click.option(
    "--max-cycles",
    type=click.IntRange(0),
    help="Maximum number message sending cycles. Runs forever if set to 0.",
)
@click.option(
    "--max-sites",
    type=click.IntRange(0),
    help="Maximum number of sites allowed to initialize. No limit if set to 0.",
)
@click.option(
    "--swarm-name", type=click.STRING, help="Name given to swarm. Appears in the logs."
)
@click.option(
    "--delay-start",
    is_flag=True,
    default=False,
    help="Adds a random delay before the first message from each site up to `--sleep-time`.",
)
@click.option(
    "--topic-prefix",
    type=click.STRING,
    help="Prefixes the MQTT topic with a string. Can augment the calculated MQTT topic returned by each site.",
)
@click.option("--dry", is_flag=True, default=False, help="Doesn't send out any data.")
def mqtt(
    ctx,
    provider,
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
    topic_prefix,
    dry,
):
    """Sends The cosmos data via MQTT protocol using PROVIDER.
    Data is collected from the db using QUERY and sent using CLIENT_ID.

    Currently only supports sending through AWS IoT Core."""
    query = queries.CosmosQuery[query]

    async def _swarm(query, mqtt_connection, credentials, *args, **kwargs):
        swarm = await CosmosSwarm.create(
            query, mqtt_connection, credentials, *args, **kwargs
        )

        await swarm.run()

    if dry == True:
        connection = MockMessageConnection()
    elif provider == "aws":
        connection = IotCoreMQTTConnection(
            endpoint=endpoint,
            cert_path=cert_path,
            key_path=key_path,
            ca_cert_path=ca_cert_path,
            client_id=client_id,
        )

    asyncio.run(
        _swarm(
            query,
            connection,
            ctx.obj["credentials"],
            site_ids=ctx.obj["sites"],
            sleep_time=sleep_time,
            max_cycles=max_cycles,
            max_sites=max_sites,
            swarm_name=swarm_name,
            delay_start=delay_start,
            topic_prefix=topic_prefix,
        )
    )


if __name__ == "__main__":
    main(auto_envvar_prefix="IOT_SWARM", obj={})
