from __future__ import annotations
from pathlib import Path
from typing import Iterable
from contextlib import contextmanager
import nox
from nox import Session

# imports all nox task provided by the toolbox
from exasol.toolbox.nox.tasks import *  # pylint: disable=wildcard-import disable=unused-wildcard-import

# default actions to be run if nothing is explicitly specified with the -s option
nox.options.sessions = ["project:fix"]


__all__ = [
    "unit_tests",
    "integration_tests",
]

_ROOT: Path = Path(__file__).parent


def _test_command(path: Path) -> Iterable[str]:
    base_command = ["poetry", "run"]
    pytest_command = [
        "pytest",
        "-v",
        "--log-level=INFO",
        "--log-cli-level=INFO",
        f"{path}"
    ]
    return base_command + pytest_command


@nox.session(name="db:start", python=False)
def start_db(session: Session) -> None:
    """Start a test database"""

    session.run(
        "itde",
        "spawn-test-environment",
        "--environment-name",
        "test",
        "--database-port-forward",
        "8563",
        "--bucketfs-port-forward",
        "2580",
        "--docker-db-image-version",
        "7.1.17",
        "--db-mem-size",
        "4GB",
        external=True,
    )


@nox.session(name="db:stop", python=False)
def stop_db(session: Session) -> None:
    """Stop the test database"""
    session.run("docker", "kill", "db_container_test", external=True)


@nox.session(name="db:import", python=False)
def import_data(session: Session) -> None:
    """Import test data into the database"""
    import sys

    path = _ROOT / "test" / "integration"
    data_dir = _ROOT / "test" / "data"
    sys.path.append(f"{path}")
    from conftest import DockerDataLoader

    loader = DockerDataLoader(
        dsn="127.0.0.1:8563",
        username="sys",
        password="exasol",
        container_name='db_container_test',
        data_directory=data_dir
    )
    loader.load()


# Import documentation related nox tasks from toolbox
