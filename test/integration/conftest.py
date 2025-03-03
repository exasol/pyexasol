import decimal
import logging
import os
import ssl
import subprocess
import uuid
from inspect import cleandoc
from pathlib import Path

import pytest

import pyexasol


@pytest.fixture(scope="session")
def dsn():
    return os.environ.get("EXAHOST", "localhost:8563")


@pytest.fixture(scope="session")
def user():
    return os.environ.get("EXAUID", "SYS")


@pytest.fixture(scope="session")
def password():
    return os.environ.get("EXAPWD", "exasol")


@pytest.fixture(scope="session")
def schema():
    return os.environ.get("EXASCHEMA", "PYEXASOL_TEST")


@pytest.fixture(scope="session")
def websocket_sslopt():
    # For CI usage of Docker containers, we disable strict certification
    # verification.
    return {"cert_reqs": ssl.CERT_NONE}


@pytest.fixture
def connection(dsn, user, password, schema, websocket_sslopt):
    con = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        websocket_sslopt=websocket_sslopt,
    )
    yield con
    con.close()


@pytest.fixture
def view(connection, faker):
    name = f"TEST_VIEW_{uuid.uuid4()}"
    name = name.replace("-", "_").upper()
    ddl = f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM users;"
    connection.execute(ddl)
    connection.commit()

    yield name

    delete_stmt = f"DROP VIEW IF EXISTS {name};"
    connection.execute(delete_stmt)
    connection.commit()


@pytest.fixture
def flush_statistics(connection):
    connection.execute("FLUSH STATISTICS TASKS;")
    connection.commit()


@pytest.fixture(scope="session", autouse=True)
def prepare_database(dsn, user, password):
    data_directory = Path(__file__).parent / ".." / "data"
    loader = DockerDataLoader(
        dsn=dsn,
        username=user,
        password=password,
        container_name="db_container_test",
        data_directory=data_directory,
    )
    loader.load()


@pytest.fixture
def edge_case_ddl():
    table_name = "edge_case"
    ddl = cleandoc(
        f"""CREATE OR REPLACE TABLE {table_name}
        (
            dec36_0         DECIMAL(36,0),
            dec36_36        DECIMAL(36,36),
            dbl             DOUBLE,
            bl              BOOLEAN,
            dt              DATE,
            ts              TIMESTAMP,
            var100          VARCHAR(100),
            var2000000      VARCHAR(2000000)
        )
        """
    )
    yield table_name, ddl


@pytest.fixture
def edge_cases():
    return {
        "Biggest-Values": {
            "DEC36_0": decimal.Decimal("+" + ("9" * 36)),
            "DEC36_36": decimal.Decimal("+0." + ("9" * 36)),
            "DBL": 1.7e308,
            "BL": True,
            "DT": "9999-12-31",
            "TS": "9999-12-31 23:59:59.999",
            "VAR100": "ひ" * 100,
            "VAR2000000": "ひ" * 2000000,
        },
        "Smallest-Values": {
            "DEC36_0": decimal.Decimal("-" + ("9" * 36)),
            "DEC36_36": decimal.Decimal("-0." + ("9" * 36)),
            "DBL": -1.7e308,
            "BL": False,
            "DT": "0001-01-01",
            "TS": "0001-01-01 00:00:00",
            "VAR100": "",
            "VAR2000000": "ひ",
        },
        "All-Nulls": {
            "DEC36_0": None,
            "DEC36_36": None,
            "DBL": None,
            "BL": None,
            "DT": None,
            "TS": None,
            "VAR100": None,
            "VAR2000000": None,
        },
    }


class DockerDataLoader:
    """Data loader for docker based Exasol DB"""

    def __init__(self, dsn, username, password, container_name, data_directory):
        self._logger = logging.getLogger("DockerDataLoader")
        self._dsn = dsn
        self._user = username
        self._password = password
        self._container = container_name
        self._data_directory = data_directory
        self._tmp_dir = f"data-{uuid.uuid4()}"

    @property
    def data_directory(self):
        return self._data_directory

    @property
    def ddl_file(self):
        return self.data_directory / "import.sql"

    @property
    def csv_files(self):
        return self.data_directory.rglob("*.csv")

    def load(self):
        self._create_dir()
        self._upload_files()
        self._import_data()

    def _execute_command(self, command):
        self._logger.info("Executing docker command: %s", command)
        result = subprocess.run(
            command,
            check=True,
            text=True,
            capture_output=True,
        )
        self._logger.debug("Stderr: %s", result.stderr)
        return result.stdout

    def _exaplus(self) -> str:
        find_exaplus = [
            "docker",
            "exec",
            self._container,
            "find",
            "/usr",
            "-name",
            "exaplus",
            "-type",
            "f",  # only files
            "-executable",  # only executable files
            "-print",  # -print -quit will stop after the result is found
            "-quit",
        ]
        exaplus = self._execute_command(find_exaplus).strip()
        self._logger.info("Found exaplus at %s", exaplus)
        return exaplus

    def _create_dir(self):
        """Create data directory within the docker container."""
        mkdir = ["docker", "exec", self._container, "mkdir", self._tmp_dir]
        stdout = self._execute_command(mkdir)
        self._logger.info("Stdout: %s", stdout)

    def _upload_files(self):
        files = [self.ddl_file]
        files.extend(self.csv_files)
        for file in files:
            copy_file = [
                "docker",
                "cp",
                f"{file.resolve()}",
                f"{self._container}:{self._tmp_dir}/{file.name}",
            ]
            stdout = self._execute_command(copy_file)
            self._logger.debug("Stdout: %s", stdout)

    def _import_data(self):
        """Load test data into a backend."""
        execute_ddl_file = [
            "docker",
            "exec",
            "-w",
            f"/{self._tmp_dir}",
            self._container,
            self._exaplus(),
            "-c",
            f"{self._dsn}",
            "-u",
            self._user,
            "-p",
            self._password,
            "-f",
            self.ddl_file.name,
            "--jdbcparam",
            "validateservercertificate=0",
        ]
        stdout = self._execute_command(execute_ddl_file)
        self._logger.info("Stdout: %s", stdout)
