from __future__ import annotations

import json
from pathlib import Path

import nox

# imports all nox task provided by the toolbox
from exasol.toolbox.nox.tasks import *  # pylint: disable=wildcard-import disable=unused-wildcard-import
from nox import Session

from noxconfig import (
    start_test_db,
    stop_test_db,
)

# default actions to be run if nothing is explicitly specified with the -s option
nox.options.sessions = ["project:fix"]

__all__ = [
    "unit_tests",
    "integration_tests",
]

_ROOT: Path = Path(__file__).parent


@nox.session(name="db:start", python=False)
def start_db(session: Session) -> None:
    """Start a test database"""
    start_test_db(session=session)


@nox.session(name="db:stop", python=False)
def stop_db(session: Session) -> None:
    """Stop the test database"""
    stop_test_db(session=session)


@nox.session(name="db:import", python=False)
def import_data(session: Session) -> None:
    """Import test data into the database"""
    import sys

    path = _ROOT / "test" / "integration"
    data_dir = _ROOT / "test" / "data"
    sys.path.append(f"{path}")

    from pyexasol_utils.docker_util import DockerDataLoader

    loader = DockerDataLoader(
        dsn="127.0.0.1:8563",
        username="sys",
        password="exasol",
        container_name="db_container_test",
        data_directory=data_dir,
    )
    loader.load()


@nox.session(name="run:examples", python=False)
def run_examples(session: Session) -> None:
    """Execute examples, assuming a DB already is ready"""
    path = _ROOT / "examples"

    errors = []
    for file in sorted(path.glob("[abcj]*.py")):
        try:
            session.run("python", str(file))
        except Exception:
            errors.append(file.name)

    if len(errors) > 0:
        escape_red = "\033[31m"
        print(escape_red + "Errors running examples:")
        for error in errors:
            print(f"- {error}")
        session.error(1)
