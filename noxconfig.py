from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from exasol.toolbox.config import BaseConfig
from exasol.toolbox.nox.plugin import hookimpl
from nox import Session

DEFAULT_PORT = 8563
DEFAULT_DB_VERSION = "8.29.6"
CONTAINER_SUFFIX = "test"
CONTAINER_NAME = f"db_container_{CONTAINER_SUFFIX}"


def start_test_db(
    session: Session,
    port: int = DEFAULT_PORT,
    db_version: str = DEFAULT_DB_VERSION,
    with_certificate: bool = True,
) -> None:
    # For Docker in a VM setup, refer to the ``doc/user_guide/developer_guide.rst``
    command = [
        "itde",
        "spawn-test-environment",
        "--environment-name",
        CONTAINER_SUFFIX,
        "--database-port-forward",
        f"{port}",
        "--bucketfs-port-forward",
        "2580",
        "--docker-db-image-version",
        db_version,
        "--db-mem-size",
        "4GB",
        "--log-level",
        "DEBUG",
    ]
    # if with_certificate:
    #     command.append(
    #         "--create-certificates",
    #     )

    session.run(*command, external=True)


def stop_test_db(session: Session) -> None:
    session.run("docker", "kill", CONTAINER_NAME, external=True)


class StartDB:

    @hookimpl
    def pre_integration_tests_hook(self, session, config, context):
        port = context.get("port", DEFAULT_PORT)
        db_version = context.get("db_version", DEFAULT_DB_VERSION)
        start_test_db(session=session, port=port, db_version=db_version)


class StopDB:

    @hookimpl
    def post_integration_tests_hook(self, session, config, context):
        stop_test_db(session=session)


class Config(BaseConfig):
    root: Path = Path(__file__).parent
    doc: Path = Path(__file__).parent / "doc"
    version_file: Path = Path(__file__).parent / "pyexasol" / "version.py"
    source: Path = Path("pyexasol")
    path_filters: Iterable[str] = (
        "dist",
        ".eggs",
        "venv",
    )
    plugins: list = [StartDB, StopDB]


PROJECT_CONFIG = Config(
    # Changes for 7.x and 2025.1.x have not yet been made. 7.x works for all tests,
    # except for the examples/UDFs. These will be resolved in:
    # https://github.com/exasol/pyexasol/issues/273
    exasol_versions=(BaseConfig().exasol_versions[1],),
)
