import copy
import csv
from dataclasses import dataclass
from datetime import datetime
from inspect import cleandoc
from io import StringIO
from pathlib import Path
from typing import Optional

import pytest

ALL_COLUMNS = [
    "FIRST_NAME",
    "LAST_NAME",
    "REGISTER_DT",
    "LAST_VISIT_TS",
    "IS_GRADUATING",
    "AGE",
    "SCORE",
]
DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
TABLE_NAME = "USER_SCORES"


@pytest.fixture
def table_name():
    return TABLE_NAME


@pytest.fixture(scope="session", autouse=True)
def faker_seed():
    return 12345


@pytest.fixture
def empty_table(connection, table_name):
    ddl = cleandoc(
        f"""
        CREATE OR REPLACE TABLE {table_name}
        (
            FIRST_NAME    VARCHAR(200),
            LAST_NAME     VARCHAR(200),
            REGISTER_DT   DATE,
            LAST_VISIT_TS TIMESTAMP,
            IS_GRADUATING BOOLEAN,
            AGE           INTEGER,
            SCORE         DECIMAL(10,2)
        );
        """
    )
    connection.execute(ddl)
    connection.commit()

    yield table_name

    ddl = f"DROP TABLE IF EXISTS {table_name};"
    connection.execute(ddl)
    connection.commit()


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


@pytest.fixture
def number_entries():
    return 10


@pytest.fixture
def data_dict(faker, number_entries) -> list[dict]:
    dates = [faker.date() for _ in range(number_entries)]
    data = [
        {
            "FIRST_NAME": faker.first_name(),
            "LAST_NAME": faker.last_name(),
            "REGISTER_DT": dates[i],
            # setting of faker's random seed did not seem to be
            # fixing seconds, so this was hard-coded instead of
            # using faker's datetime
            "LAST_VISIT_TS": datetime.strptime(
                f"{dates[i]} 12:00:00.900000", DATETIME_STR_FORMAT
            ),
            "IS_GRADUATING": faker.boolean(),
            "AGE": faker.random_int(min=18, max=65),
            "SCORE": faker.pyfloat(min_value=69, max_value=100, right_digits=2),
        }
        for i in range(number_entries)
    ]

    return sorted(data, key=lambda n: n["FIRST_NAME"])


@pytest.fixture
def all_data(data_dict):
    return DataSample(
        columns=ALL_COLUMNS,
        list_dict=copy.deepcopy(data_dict),
    )


@pytest.fixture
def reduced_import_data(data_dict):
    modified_list_dict = copy.deepcopy(data_dict)
    for row in modified_list_dict:
        for key in row.keys():
            if key in ("REGISTER_DT", "LAST_VISIT_TS", "IS_GRADUATING"):
                row[key] = None

    return DataSample(
        columns=ALL_COLUMNS,
        list_dict=modified_list_dict,
    )


@pytest.fixture
def fill_table(connection, table_name, empty_table, data_dict):
    insert = f"INSERT INTO {table_name} VALUES({{FIRST_NAME}}, {{LAST_NAME}}, {{REGISTER_DT}}, {{LAST_VISIT_TS}}, {{IS_GRADUATING}}, {{AGE}}, {{SCORE}});"
    for row in data_dict:
        connection.execute(insert, row)
