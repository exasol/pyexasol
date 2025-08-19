from __future__ import annotations

import argparse
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


def _create_start_db_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nox -s start:db",
        usage="nox -s start:db -- [-h] [-t | --port {int} --db-version {str} --with-certificate]",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--port", default=8563, type=int, help="forward port for the Exasol DB"
    )
    parser.add_argument(
        "--db-version", default="8.32.0", type=str, help="Exasol DB version to be used"
    )
    parser.add_argument(
        "--with-certificate",
        default=False,
        action="store_true",
        help="Add a certificate to the Exasol DB",
    )
    return parser


@nox.session(name="db:start", python=False)
def start_db(session: Session) -> None:
    """Start a test database"""
    parser = _create_start_db_parser()
    args = parser.parse_args(session.posargs)
    start_test_db(
        session=session,
        port=args.port,
        db_version=args.db_version,
        with_certificate=args.with_certificate,
    )


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


@nox.session(name="test:performance", python=False)
def performance_tests(session: Session) -> None:
    """Execute performance tests, assuming a DB already is ready

    In the event of reasonable changes, the benchmark file should be updated
    by running:
        poetry run -- pytest ./test/performance/ --benchmark-sort=name
        --benchmark-save=benchmark_performance
        --benchmark-storage=file://test/performance/.benchmarks
    Please save the newly created benchmark file over the existing one
    at test/performance/.benchmarks/Linux-CPython-3.10-64bit/0001_benchmark_performance.json
    and replace your local `"machine_info": { "key": "blah" }` with
    `"machine_info": { }`.

    Additionally, the value used in this nox session for `--benchmark-compare-fail`
    should be re-evaluated and potentially updated.
    """
    test_path = _ROOT / "test/performance"
    benchmark_path = test_path / ".benchmarks"

    command = [
        "pytest",
        str(test_path),
        "--benchmark-sort=name",
        "--benchmark-compare=0001",
        # the greatest stddev ~1s, so we set this at 1.25 of that for a robust estimate
        # alternately if we wanted by percent, we could compare median to stddev
        # or split into export vs import tests to have tighter bounds
        "--benchmark-compare-fail=median:1.25",
        f"--benchmark-storage=file://{benchmark_path}",
    ]

    session.run(*command)
