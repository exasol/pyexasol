from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from exasol.toolbox.nox.plugin import hookimpl
from nox import Session

DEFAULT_PORT = 8563
DEFAULT_DB_VERSION = "8.32.0"
CONTAINER_SUFFIX = "test"
CONTAINER_NAME = f"db_container_{CONTAINER_SUFFIX}"


def start_test_db(
    session: Session, port: int = DEFAULT_PORT, db_version: str = DEFAULT_DB_VERSION
) -> None:
    # For Docker in a VM setup, refer to the ``doc/user_guide/developer_guide.rst``
    session.run(
        "itde",
        "spawn-test-environment",
        "--create-certificates",
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
        external=True,
    )


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


@dataclass(frozen=True)
class Config:
    root: Path = Path(__file__).parent
    doc: Path = Path(__file__).parent / "doc"
    version_file: Path = Path(__file__).parent / "pyexasol" / "version.py"
    source: Path = Path("pyexasol")
    path_filters: Iterable[str] = (
        "dist",
        ".eggs",
        "venv",
    )
    python_versions = ["3.9", "3.10", "3.11", "3.12", "3.13"]
    plugins = [StartDB, StopDB]
    exasol_versions = ["8.31.0", DEFAULT_DB_VERSION]
    # need --keep-runtime-typing, as pydantic with python3.9 does not accept str | None
    # format, and it is not resolved with from __future__ import annotations. pyupgrade
    # will keep switching Optional[str] to str | None leading to issues.
    pyupgrade_args = ("--py39-plus", "--keep-runtime-typing")


PROJECT_CONFIG = Config()
