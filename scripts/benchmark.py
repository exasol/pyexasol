import json
from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    Optional,
    Union,
)

from rich.console import Console
from rich.table import Table


@dataclass
class Benchmark:
    filepath: Path
    benchmark_data: list[dict[str, Union[dict, str]]] = field(init=False)

    def _check_benchmark_data(self):
        if not hasattr(self, "benchmark_data"):
            raise ValueError(
                "benchmark_data must be initialized with `set_benchmark_data`"
            )

    def get_test(self, fullname: str) -> Optional[dict[str, Union[dict, str]]]:
        self._check_benchmark_data()

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
        self._check_benchmark_data()
        return {entry["fullname"] for entry in self.benchmark_data}  # type: ignore


@dataclass
class CompareBenchmarks:
    previous_benchmark: Benchmark
    current_benchmark: Benchmark
    relative_median_threshold: float
    errors: list[str] = field(init=False)

    def _check_for_error(
        self,
        previous_test: Optional[dict[str, Union[dict, str]]],
        current_test: Optional[dict[str, Union[dict, str]]],
    ) -> Optional[str]:
        if previous_test is None:
            return "is not present in previous_benchmark"
        elif current_test is None:
            return "is not present in current_benchmark"
        else:
            median_ratio = (
                current_test["stats"]["median"]  # type: ignore
                / previous_test["stats"]["median"]  # type: ignore
            )
            percent_difference = median_ratio - 1
            if percent_difference > self.relative_median_threshold:
                return "is slower than in previous_benchmark"
            elif percent_difference < -self.relative_median_threshold:
                return "is faster than in previous_benchmark"
        return None

    def _initialize_errors(self) -> None:
        self.errors = []

    @staticmethod
    def _print_compared_results(
        previous_test: Optional[dict[str, Union[dict, str]]],
        current_test: Optional[dict[str, Union[dict, str]]],
    ) -> None:
        def get_machine_info(
            test: Optional[dict[str, Union[dict, str]]], key: str
        ) -> str:
            if test is None:
                return ""
            return test["machine_info"]["cpu"][key]  # type: ignore

        def get_stats_value(
            test: Optional[dict[str, Union[dict, str]]], key: str
        ) -> str:
            if test is None:
                return ""
            return f"{test['stats'][key]:.2f}"  # type: ignore

        console = Console()
        table = Table()
        table.add_column("Metric")
        table.add_column("previous_benchmark")
        table.add_column("current_benchmark")

        # add machine-info.cpu info
        for row_name in ["arch", "brand_raw", "hz_actual_friendly"]:
            table.add_row(
                row_name,
                get_machine_info(previous_test, row_name),
                get_machine_info(current_test, row_name),
            )

        table.add_section()

        # add statistics
        for row_name in [
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
                row_name,
                get_stats_value(previous_test, key=row_name),
                get_stats_value(current_test, key=row_name),
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
