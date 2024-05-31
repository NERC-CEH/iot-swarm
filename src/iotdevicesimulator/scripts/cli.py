import click
from iotdevicesimulator import queries
from iotdevicesimulator.swarm import CosmosSwarm
from iotdevicesimulator.messaging.aws import IotCoreMQTTConnection
import asyncio
from pathlib import Path
import logging

TABLES = [table.name for table in queries.CosmosQuery]


@click.group()
@click.pass_context
def main(ctx):
    """Core group of the cli."""
    ctx.ensure_object(dict)

    log_config = Path(Path(__file__).parents[1], "__assets__", "loggers.ini")

    click.echo(log_config)
    logging.config.fileConfig(fname=log_config)


@main.group()
@click.pass_context
@click.option("--site", type=click.STRING, multiple=True)
@click.option("--dsn", type=click.STRING, required=True, envvar="IOT_SWARM_COSMOS_DSN")
@click.option(
    "--user", type=click.STRING, required=True, envvar="IOT_SWARM_COSMOS_USER"
)
@click.option(
    "--password",
    type=click.STRING,
    required=True,
    prompt=True,
    envvar="IOT_SWARM_COSMOS_PASSWORD",
)
def cosmos(ctx, site, dsn, user, password):

    ctx.obj["credentials"] = {"dsn": dsn, "user": user, "password": password}
    ctx.obj["sites"] = site


@cosmos.command()
def test():
    pass


@cosmos.command()
@click.pass_context
@click.argument("provider", type=click.Choice(["aws"]))
@click.argument("query", type=click.Choice(TABLES))
@click.argument("client-id", type=click.STRING, required=True)
@click.option(
    "--endpoint", type=click.STRING, required=True, envvar="IOT_SWARM_MQTT_ENDPOINT"
)
@click.option(
    "--cert-path",
    type=click.Path(exists=True),
    required=True,
    envvar="IOT_SWARM_MQTT_CERT_PATH",
)
@click.option(
    "--key-path",
    type=click.Path(exists=True),
    required=True,
    envvar="IOT_SWARM_MQTT_KEY_PATH",
)
@click.option(
    "--ca-cert-path",
    type=click.Path(exists=True),
    required=True,
    envvar="IOT_SWARM_MQTT_CA_CERT_PATH",
)
@click.option("--sleep-time", type=click.INT)
@click.option("--max-cycles", type=click.IntRange(0))
@click.option("--max-sites", type=click.IntRange(0))
@click.option("--swarm-name", type=click.STRING)
@click.option("--delay-start", type=click.BOOL, default=False)
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
):
    """Gets an MQTT connection"""
    query = queries.CosmosQuery[query]

    async def _swarm(query, mqtt_connection, credentials, *args, **kwargs):
        swarm = await CosmosSwarm.create(
            query, mqtt_connection, credentials, *args, **kwargs
        )

        await swarm.run()

    if provider == "aws":
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
            delay_first_cycle=delay_start,
        )
    )


if __name__ == "__main__":
    main(auto_envvar_prefix="IOT_SWARM", obj={})
