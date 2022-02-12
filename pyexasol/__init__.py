
__all__ = [
    '__version__',
    'connect',
    'connect_local_config',
    'http_transport',
    'exasol_mapper',
    'ExaError',
    'ExaCommunicationError',
    'ExaConcurrencyError',
    'ExaRuntimeError',
    'ExaRequestError',
    'ExaAuthError',
    'ExaQueryError',
    'ExaQueryAbortError',
    'ExaQueryTimeoutError',
    'ExaConnection',
    'ExaConnectionError',
    'ExaConnectionDsnError',
    'ExaConnectionFailedError',
    'ExaStatement',
    'ExaFormatter',
    'ExaLogger',
    'ExaExtension',
    'ExaMetaData',
    'ExaHTTPTransportWrapper',
    'ExaLocalConfig',
    'ExaTimeDelta',
    'PROTOCOL_V1',
    'PROTOCOL_V2',
    'PROTOCOL_V3',
]

from .connection import ExaConnection
from .constant import PROTOCOL_V1, PROTOCOL_V2, PROTOCOL_V3

from .exceptions import (
    ExaError,
    ExaCommunicationError,
    ExaConcurrencyError,
    ExaRuntimeError,
    ExaRequestError,
    ExaAuthError,
    ExaQueryError,
    ExaQueryAbortError,
    ExaQueryTimeoutError,
    ExaConnectionError,
    ExaConnectionDsnError,
    ExaConnectionFailedError,
)

from .ext import ExaExtension
from .formatter import ExaFormatter
from .http_transport import ExaHTTPTransportWrapper
from .local_config import ExaLocalConfig
from .logger import ExaLogger
from .mapper import ExaTimeDelta, exasol_mapper
from .meta import ExaMetaData
from .statement import ExaStatement
from .version import __version__


def connect(**kwargs) -> ExaConnection:
    """
    Constructor of connection objects
    Please check ExaConnection object for list of arguments
    """
    return ExaConnection(**kwargs)


def connect_local_config(config_section, config_path=None, **kwargs) -> ExaConnection:
    """
    Constructor of connection objects based on local config file
    Default config path is ~/.pyexasol.ini

    Extra arguments override values from config

    :param config_section: Name of config section (required!)
    :param config_path: Custom path to local config file
    :param kwargs: Arguments for "connect()" function
    """

    conf = ExaLocalConfig(config_path)
    conf_args = conf.get_args(config_section)

    return connect(**{**conf_args, **kwargs})


def http_transport(ipaddr, port, compression=False, encryption=True) -> ExaHTTPTransportWrapper:
    """
    Constructor of HTTP Transport wrapper for parallel HTTP Transport (EXPORT or IMPORT)
    Compression and encryption arguments should match pyexasol.connect()

    How to use:
    1) Parent process opens main connection to Exasol with pyexasol.connect()
    2)
    2) Parent process creates any number of child processes (possibly on remote host or another container)
    3) Every child process starts HTTP transport sub-connection with pyexasol.http_transport()
        and gets "ipaddr:port" string using ExaHTTPTransportWrapper.address
    4) Every child process sends address string to parent process using any communication method (Pipe, Queue, Redis, etc.)
    5) Parent process runs .export_parallel() or .import_parallel(), which initiates EXPORT or IMPORT query in Exasol
    6) Every child process receives or sends a chunk of data using ExaHTTPTransportWrapper.export_*() or .import_*()
    7) Parent process waits for Exasol query and for child processes to finish

    All child processes should run in parallel.
    It is NOT possible to process some data first, and process some more data later.

    If an exception is raised in child process, it will close the pipe used for HTTP transport.
    Closing the pipe prematurely will cause SQL query to fail and will raise an exception in parent process.
    Parent process is responsible for closing other child processes and cleaning up.

    PyEXASOL does not provide a complete solution to manage child processes, only examples.
    The final solution depends on your hardware, network configuration, cloud provider and container orchestration software.
    """
    return ExaHTTPTransportWrapper(ipaddr, port, compression, encryption)
