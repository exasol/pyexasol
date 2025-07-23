import logging
import subprocess
import uuid


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
        # Note that the directory for the binary might change between DB versions
        find_exaplus = [
            "docker",
            "exec",
            self._container,
            "find",
            "/opt",
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
