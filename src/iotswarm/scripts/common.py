"""Location for common CLI commands"""

import click


def device_options(function):
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

    click.option(
        "--no-send-probability",
        type=click.IntRange(0, 100),
        default = 0,
        help="Probability of not sending a message, can be 0 - 100 where 0 is no skip and 100 is always skip",
    )(function)

    return function


def iotcore_options(function):
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


@click.command
@click.pass_context
@click.option("--max-sites", type=click.IntRange(min=0), default=0)
def list_sites(ctx, max_sites):
    """Prints the sites present in database."""

    sites = ctx.obj["db"].query_site_ids(max_sites=max_sites)
    click.echo(sites)


@click.command
@click.pass_context
def test(ctx: click.Context):
    """Enables testing of cosmos group arguments."""
    print(ctx.obj)
