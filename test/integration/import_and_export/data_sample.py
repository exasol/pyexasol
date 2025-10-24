import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from io import StringIO

DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


@dataclass
class DataSample:
    columns: list[str]
    list_dict: list[dict]

    @staticmethod
    def _convert_to_output_from_select(value):
        if value is None:
            return value
        # a float needs to be converted to a string, like "SCORE"
        if isinstance(value, float):
            return f"{value}"
        # a datetime needs to be converted to a formatted string, like "LAST_VISIT_TS"
        if isinstance(value, datetime):
            return value.strftime(DATETIME_STR_FORMAT)
        return value

    @staticmethod
    def _convert_to_csv_format(value):
        if value is None:
            return value
        # a bool needs to be converted to an integer
        if isinstance(value, bool):
            return int(value)
        # a datetime needs to be converted to a formatted string & in additional quotes
        if isinstance(value, datetime):
            return f'"{value.strftime(DATETIME_STR_FORMAT)}"'
        return value

    def list_tuple(self, selected_columns: Optional[list[str]] = None) -> list[tuple]:
        if selected_columns is None:
            selected_columns = self.columns

        return [
            tuple(
                [
                    self._convert_to_output_from_select(row[key])
                    for key in selected_columns
                ]
            )
            for row in self.list_dict
        ]

    def csv_str(self, selected_columns: Optional[list[str]] = None) -> str:
        if selected_columns is None:
            selected_columns = self.columns

        with StringIO() as output:
            writer = csv.writer(
                output, dialect="unix", quoting=csv.QUOTE_NONE, quotechar=None
            )
            for row in self.list_dict:
                writer.writerow(
                    [self._convert_to_csv_format(row[key]) for key in selected_columns]
                )
            return output.getvalue()

    def write_csv(
        self, directory: Path, selected_columns: Optional[list[str]] = None
    ) -> Path:
        filepath = directory / "data.csv"

        if selected_columns is None:
            selected_columns = self.columns

        csv_text = self.csv_str(selected_columns=selected_columns)
        filepath.write_text(csv_text)
        return filepath
