from click.testing import CliRunner
from iotswarm.scripts import cli
from parameterized import parameterized
import re

RUNNER = CliRunner()


def test_main_ctx():
    result = RUNNER.invoke(cli.main, ["test"])
    assert not result.exception
    assert result.output == "{'logger': <Logger iotswarm.scripts.cli (DEBUG)>}\n"


def test_main_log_config():
    with RUNNER.isolated_filesystem():
        with open("logger.ini", "w") as f:
            f.write(
                """[loggers]
keys=root

[handlers]
keys=consoleHandler

[formatters]
keys=sampleFormatter

[logger_root]
level=INFO
handlers=consoleHandler

[handler_consoleHandler]
class=StreamHandler
level=INFO
formatter=sampleFormatter
args=(sys.stdout,)

[formatter_sampleFormatter]
format=%(asctime)s - %(name)s - %(levelname)s - %(message)s"""
            )
        result = RUNNER.invoke(cli.main, ["--log-config", "logger.ini", "test"])
        assert not result.exception
        assert result.output == "{'logger': <Logger iotswarm.scripts.cli (INFO)>}\n"


@parameterized.expand(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_log_level_set(log_level):
    result = RUNNER.invoke(cli.main, ["--log-level", log_level, "test"])
    assert not result.exception
    assert (
        result.output == f"Set log level to {log_level}.\n{{'logger': <Logger iotswarm.scripts.cli ({log_level})>}}\n"
    )


def test_get_version():
    """Tests that the verison can be retrieved."""

    result = RUNNER.invoke(cli.main, ["get-version"])
    assert not result.exception
    assert re.match(r"^\d+\.\d+\.\d+$", result.output)


def test_list_sessions():
    result = RUNNER.invoke(cli.main, ["sessions", "ls"])

    assert not result.exception
    assert re.match(r"\[(('[\d\w-]+')+(, )*)*\]", result.output)


def test_session_rm():
    session = "theres-no-way-this-is-a-real-session"

    RUNNER.invoke(cli.main, ["sessions", "init", session])

    assert session in RUNNER.invoke(cli.main, ["sessions", "ls"]).output

    RUNNER.invoke(cli.main, ["sessions", "rm", session])
    assert session not in RUNNER.invoke(cli.main, ["sessions", "ls"]).output


def test_session_init():
    session = "theres-no-way-this-is-a-real-session"

    RUNNER.invoke(cli.main, ["sessions", "rm", session])

    result = RUNNER.invoke(cli.main, ["sessions", "init", session])

    assert not result.exception

    assert session in RUNNER.invoke(cli.main, ["sessions", "ls"]).output

    RUNNER.invoke(cli.main, ["sessions", "rm", session])
