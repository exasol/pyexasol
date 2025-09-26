from __future__ import annotations

import argparse
import copy
import glob
import json
import subprocess
from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    Any,
    Optional,
)

import nox

# imports all nox task provided by the toolbox
from exasol.toolbox.nox.tasks import *  # pylint: disable=wildcard-import disable=unused-wildcard-import
from nox import Session
from rich.console import Console
from rich.table import Table

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
    """Output JSON of performance tests for running in the CI."""
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


def _create_performance_combine_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nox -s performance:combine",
        usage="nox -s performance:test -- [-h] [--path {path}]",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--path",
        dest="path",
        help="Path specifier for files to combine; should include glob(s) ending with *.json",
    )
    return parser


@nox.session(name="performance:combine", python=False)
def performance_combine(session: Session) -> None:
    """
    Combine performance benchmarks from multiple JSON files to one
    file.
    """
    parser = _create_performance_combine_arg_parser()
    args = parser.parse_args(session.posargs)

    matches = glob.glob(args.path)
    files = sorted(
        [filepath for i in matches if (filepath := Path(i)) and filepath.is_file()]
    )

    new_json_list = []
    for file in files:
        file_dict = json.loads(file.read_text())
        for entry in file_dict["benchmarks"]:
            entry_dict = copy.deepcopy(entry)
            entry_dict["machine_info"] = copy.deepcopy(file_dict["machine_info"])
            entry_dict["commit_info"] = copy.deepcopy(file_dict["commit_info"])
            entry_dict["datetime"] = copy.deepcopy(file_dict["datetime"])
            entry_dict["version"] = copy.deepcopy(file_dict["version"])
            new_json_list.append(entry_dict)

    new_json = json.dumps({"benchmarks": new_json_list}, indent=4)
    CURRENT_BENCHMARK.write_text(new_json)


@dataclass
class Benchmark:
    filepath: Path
    benchmark_data: list[dict[str, Any]] = field(init=False)

    def get_test(self, fullname: str) -> Optional[dict[str, float]]:
        match_test = list(
            filter(lambda x: x["fullname"] == fullname, self.benchmark_data)
        )
        if len(match_test) == 1:
            return match_test[0]
        return None

    def set_benchmark_data(self) -> None:
        file_json = self.filepath.read_text()
        file_dict = json.loads(file_json)
        self.benchmark_data = file_dict["benchmarks"]

    @property
    def fullname_tests(self) -> set[str]:
        return {entry["fullname"] for entry in self.benchmark_data}


@dataclass
class CompareBenchmarks:
    previous_benchmark: Benchmark
    current_benchmark: Benchmark
    relative_median_threshold: float
    errors: list[str] = field(init=False)

    def _check_for_error(
        self,
        previous_test: Optional[dict[str, float]],
        current_test: Optional[dict[str, float]],
    ) -> Optional[str]:
        if previous_test is None:
            return "is not present in previous_benchmark"
        elif current_test is None:
            return "is not present in current_benchmark"
        else:
            median_ratio = (
                current_test["stats"]["median"] / previous_test["stats"]["median"]
            )
            percent_difference = median_ratio - 1
            if percent_difference > self.relative_median_threshold:
                return "is slower than in previous_benchmark"
            elif percent_difference < -self.relative_median_threshold:
                return "is faster than in previous_benchmark"
        return None

    def _initialize_errors(self) -> None:
        self.errors = []

    def _print_compared_results(
        self,
        previous_test: Optional[dict[str, float]],
        current_test: Optional[dict[str, float]],
    ) -> None:
        def get_stats_value(stats: Optional[dict[str, float]]) -> str:
            if stats is None:
                return ""
            return f"{previous_test['stats'][row_title]:.2f}"

        console = Console()
        table = Table()
        table.add_column("Metric")
        table.add_column("previous_benchmark")
        table.add_column("current_benchmark")

        # add machine-info.cpu info
        for row_title in ["arch", "brand_raw", "hz_actual_friendly"]:
            table.add_row(
                row_title,
                previous_test["machine_info"]["cpu"][row_title],
                current_test["machine_info"]["cpu"][row_title],
            )

        table.add_section()

        # add statistics
        for row_title in [
            "min",
            "max",
            "median",
            "mean",
            "stddev",
            "iqr",
            "q1",
            "q3",
            "ld15iqr",
            "hd15iqr",
        ]:
            table.add_row(
                row_title,
                get_stats_value(previous_test),
                get_stats_value(current_test),
            )

        console.print(table)

    def do_comparison(self) -> list[str]:
        self._initialize_errors()

        tests = (
            self.current_benchmark.fullname_tests
            | self.previous_benchmark.fullname_tests
        )
        for test in sorted(tests):
            print(f"\033[92m[TEST] {test}\033[0m")

            previous_test = self.previous_benchmark.get_test(test)
            current_test = self.current_benchmark.get_test(test)

            error_message = self._check_for_error(
                previous_test=previous_test, current_test=current_test
            )
            if error_message is not None:
                print(f"\033[91m{error_message}\033[0m")
                self.errors.append(f"- {test} {error_message}")

            self._print_compared_results(
                previous_test=previous_test, current_test=current_test
            )
        return self.errors


@nox.session(name="performance:check", python=False)
def performance_check(session: Session) -> None:
    """Compare previous & current results of benchmarked performance tests.

    An exception is raised if any of the following conditions are met:
     - A test is present in only the previous or current results.
     - A test is present in both results, but their medians differ by more than 5% in
       either direction.

    Note:
      As tests are matched based on their full name, if a test is renamed or moved,
      this would result in the names not being matched up properly and at least one
      exception being raised.

    In the event of reasonable changes, the benchmark file should be updated as
    specified in the section `Updating the Benchmark JSON File` of the
    ``doc/developer_guide``. If substantial changes are needed, like the condition
    for comparing the tests is flakey, then many runs (> 15) should be done in the CI
    to collect metrics to develop a new comparison threshold.

    Note:
      While it was not selected due to time-constraints and out-of-the-box performance,
      it would be nice to use a non-parameterized test, like the
      `Mann-Whitney U test <https://en.wikipedia.org/wiki/Mann%E2%80%93Whitney_U_test>`
      to determine if the benchmarked results significantly deviated, as opposed to the
      metric and threshold limit employed here.

    The JSON files used for comparison are created using the ``pytest-benchmark``
    package. While that package provides a CLI to compare benchmark files,
    unfortunately, it does not indicate when a test is not present in the previous
    benchmark data nor does it indicate if a test is running faster than before
    (only slower).
    """
    previous_benchmark = Benchmark(PREVIOUS_BENCHMARK)
    previous_benchmark.set_benchmark_data()

    current_benchmark = Benchmark(CURRENT_BENCHMARK)
    current_benchmark.set_benchmark_data()

    compare_benchmarks = CompareBenchmarks(
        previous_benchmark=previous_benchmark,
        current_benchmark=current_benchmark,
        # medians must be with 5% of one another
        relative_median_threshold=0.05,
    )
    errors = compare_benchmarks.do_comparison()

    if errors:
        errors = ["\nthe comparison failed due to:"] + errors
        session.error("\n".join(errors))
