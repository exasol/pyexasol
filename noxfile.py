from __future__ import annotations

import argparse
from dataclasses import (
    dataclass,
    field,
)
from json import loads
from pathlib import Path
from typing import (
    Any,
    Optional,
)

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


@nox.session(name="performance:test", python=False)
def performance_tests(session: Session) -> None:
    """Execute performance tests, assuming a DB already is ready"""
    performance_tests_folder = "test/performance"
    test_path = _ROOT / performance_tests_folder
    benchmark_path = f"{performance_tests_folder}/.benchmarks"

    command = [
        "pytest",
        str(test_path),
        "--benchmark-sort=name",
        f"--benchmark-storage=file://{benchmark_path}",
        "--benchmark-save=benchmark_performance",
    ]

    session.run(*command)


@dataclass
class Benchmark:
    filepath: Path
    benchmark_data: list[dict[str, Any]] = field(init=False)

    def get_benchmark_test(self, fullname: str) -> Optional[dict[str, float]]:
        match_test = list(
            filter(lambda x: x["fullname"] == fullname, self.benchmark_data)
        )
        if len(match_test) == 1:
            return match_test[0]["stats"]
        return None

    def set_benchmark_data(self) -> None:
        file_json = self.filepath.read_text()
        file_dict = loads(file_json)
        self.benchmark_data = file_dict["benchmarks"]

    @property
    def fullname_tests(self) -> set[str]:
        return {entry["fullname"] for entry in self.benchmark_data}


@nox.session(name="performance:check", python=False)
def performance_check(session: Session) -> None:
    """Compare previous & current results of performance tests

    In the event of reasonable changes, the benchmark file should be updated
    by copying the one saved by the CI locally & updating the existing one
    at test/performance/.benchmarks/Linux-CPython-3.11-64bit/0001_benchmark_performance.json.

    Additionally, the value used in this nox session for comparison should be
    re-evaluated and potentially updated.
    """
    benchmark_path = _ROOT / "test/performance/.benchmarks/Linux-CPython-3.11-64bit"

    expected_benchmark = Benchmark(benchmark_path / "0001_benchmark_performance.json")
    expected_benchmark.set_benchmark_data()

    current_benchmark = Benchmark(benchmark_path / "0002_benchmark_performance.json")
    current_benchmark.set_benchmark_data()

    errors = []
    all_tests = expected_benchmark.fullname_tests | current_benchmark.fullname_tests
    for test in all_tests:
        expected_results = expected_benchmark.get_benchmark_test(test)
        current_results = current_benchmark.get_benchmark_test(test)

        if current_results is None:
            errors.append(
                f"- {test} is not present in {current_benchmark.filepath.name}"
            )
            continue
        elif expected_results is None:
            errors.append(
                f"- {test} is not present in {expected_benchmark.filepath.name}"
            )
            continue

        if (
            current_results["mean"]
            > expected_results["mean"] + expected_results["stddev"]
            or current_results["mean"]
            < expected_results["mean"] - expected_results["stddev"]
        ):
            errors.append(
                f"- mean of {test} is not in {expected_results['mean']} +/- {expected_results['stddev']}"
            )

    if errors:
        errors = ["the comparison failed due to:"] + errors
        session.error("\n".join(errors))
