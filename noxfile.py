from __future__ import annotations

import argparse
import json
import subprocess
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

PERFORMANCE_TEST_DIRECTORY = _ROOT / "test" / "performance"
BENCHMARK_FILEPATH = PERFORMANCE_TEST_DIRECTORY / ".benchmarks"
PREVIOUS_BENCHMARK = BENCHMARK_FILEPATH / "0001_performance.json"
CURRENT_BENCHMARK = BENCHMARK_FILEPATH / "0002_performance.json"


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


def _get_name_from_path(line: str) -> str:
    name = str(Path(line).name)

    for old, new in {".py::": "__", "[": "_", "]": ""}.items():
        name = name.replace(old, new)
    return f"{name}"


@nox.session(name="performance:json", python=False)
def performance_json(session: Session) -> None:
    """Output JSON of performance tests."""
    output = subprocess.run(
        [
            "pytest",
            "--collect-only",
            PERFORMANCE_TEST_DIRECTORY,
            "-q",
        ],
        capture_output=True,
        text=True,
    )

    if output.returncode != 0:
        print(output)
        session.error()

    processed_output = [
        {"path": line, "key": _get_name_from_path(line)}
        for line in output.stdout.splitlines()
        if PERFORMANCE_TEST_DIRECTORY.name in line
    ]

    config = {
        "performance-tests": processed_output,
        "python-version": ["3.12"],
        "exasol-version": ["8.32.0"],
    }

    print(json.dumps(config))


def _create_performance_test_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nox -s performance:test",
        usage="nox -s performance:test -- [-h] [--test_path {test_path} --file-name {file_name}]",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--test-path", dest="test_path", help="Path to test file (can include its name)"
    )
    parser.add_argument(
        "--file-name",
        dest="file_name",
        help="Name under with to save the created benchmark file",
        default=CURRENT_BENCHMARK.name,
    )
    return parser


@nox.session(name="performance:test", python=False)
def performance_test(session: Session) -> None:
    """
    Execute one or more performance tests, assuming a DB already is ready,
    and save the benchmarked results.
    """
    parser = _create_performance_test_arg_parser()
    args = parser.parse_args(session.posargs)
    file_path = BENCHMARK_FILEPATH / args.file_name

    command = [
        "pytest",
        args.test_path,
        "--benchmark-sort=name",
        f"--benchmark-json={file_path}",
    ]
    session.run(*command)
