
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
    'HTTP_EXPORT',
    'HTTP_IMPORT',
    'PROTOCOL_V1',
    'PROTOCOL_V2',
]

from .version import __version__
from .exceptions import *
from .connection import ExaConnection
from .statement import ExaStatement
from .formatter import ExaFormatter
from .local_config import ExaLocalConfig
from .logger import ExaLogger
from .ext import ExaExtension
from .meta import ExaMetaData
from .mapper import exasol_mapper
from .http_transport import ExaHTTPTransportWrapper, HTTP_EXPORT, HTTP_IMPORT
from .constant import PROTOCOL_V1, PROTOCOL_V2


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


def http_transport(host, port, mode, compression=False, encryption=False) -> ExaHTTPTransportWrapper:
    """
    Constructor of HTTP Transport wrapper objects for IMPORT / EXPORT parallelism

    How to use this:
    1) Parent process opens main connection to Exasol with .connect()
    2) Parent process creates any number of child processes (possibly on remote host)
    3) Every child process inits HTTP transport sub-connection with .http_transport()
           and gets proxy "host:port" string using .get_proxy()
    4) Every child process sends proxy string to parent process using any communication method
    5) Parent process runs .export_parallel(), which executes EXPORT query in Exasol
    6) Every child process receives chunk of data, process it and finish
    7) Parent process waits for EXPORT query and child processes to finish

    All child processes should run in parallel. It is not possible to run some processes first, than run some more.
    """
    return ExaHTTPTransportWrapper(host, port, mode, compression, encryption)
