__all__ = [
    "__version__",
    "connect",
    "connect_local_config",
    "http_transport",
    "exasol_mapper",
    "ExaError",
    "ExaCommunicationError",
    "ExaConcurrencyError",
    "ExaRuntimeError",
    "ExaRequestError",
    "ExaAuthError",
    "ExaQueryError",
    "ExaQueryAbortError",
    "ExaQueryTimeoutError",
    "ExaConnection",
    "ExaConnectionError",
    "ExaConnectionDsnError",
    "ExaConnectionFailedError",
    "ExaStatement",
    "ExaFormatter",
    "ExaLogger",
    "ExaExtension",
    "ExaMetaData",
    "ExaHTTPTransportWrapper",
    "ExaLocalConfig",
    "ExaTimeDelta",
    "PROTOCOL_V1",
    "PROTOCOL_V2",
    "PROTOCOL_V3",
]

from .connection import ExaConnection
from .constant import (
    PROTOCOL_V1,
    PROTOCOL_V2,
    PROTOCOL_V3,
)
from .exceptions import (
    ExaAuthError,
    ExaCommunicationError,
    ExaConcurrencyError,
    ExaConnectionDsnError,
    ExaConnectionError,
    ExaConnectionFailedError,
    ExaError,
    ExaQueryAbortError,
    ExaQueryError,
    ExaQueryTimeoutError,
    ExaRequestError,
    ExaRuntimeError,
)
from .ext import ExaExtension
from .formatter import ExaFormatter
from .http_transport import ExaHTTPTransportWrapper
from .local_config import ExaLocalConfig
from .logger import ExaLogger
from .mapper import (
    ExaTimeDelta,
    exasol_mapper,
)
from .meta import ExaMetaData
from .statement import ExaStatement
from .version import __version__


def connect(**kwargs) -> ExaConnection:
    """
    Create a new connection object.

    Args:
        **kwargs:
            For details, refer to the :class:`pyexasol.ExaConnection` class.
    """
    return ExaConnection(**kwargs)


def connect_local_config(config_section, config_path=None, **kwargs) -> ExaConnection:
    """
    Constructor for connection objects based on a local config file.

    Info:
        - The default config path is ~/.pyexasol.ini
        - Extra arguments override values from config

    Args:

        config_section:
            Name of config section (required!)
        config_path:
            Custom path to local config file
        kwargs:
            Arguments for "connect()" function
    """

    conf = ExaLocalConfig(config_path)
    conf_args = conf.get_args(config_section)

    return connect(**{**conf_args, **kwargs})


def http_transport(
    ipaddr, port, compression=False, encryption=True
) -> ExaHTTPTransportWrapper:
    """
    Constructor for HTTP Transport wrapper for parallel HTTP Transport (EXPORT or IMPORT)

    Args:
        ipaddr:
            IP address of one of Exasol nodes received from :meth:`pyexasol.ExaConnection.get_nodes`
        port:
            Port of one of Exasol nodes received from :meth:`pyexasol.ExaConnection.get_nodes`
        compression:
            Use zlib compression for HTTP transport, must be the same as `compression` of main connection
        encryption:
            Use SSL/TLS encryption for HTTP transport, must be the same as `encryption` of main connection

    Info:
        Compression and encryption arguments should match :func:`pyexasol.connect`

        How to use:

        #. Parent process opens main connection to Exasol with pyexasol.connect()
        #. Parent process creates any number of child processes (possibly on remote host or another container)
        #. Every child process starts HTTP transport sub-connection with pyexasol.http_transport()
        #.  and gets "ipaddr:port" string using ExaHTTPTransportWrapper.address
        #. Every child process sends address string to parent process using any communication method (Pipe, Queue, Redis, etc.)
        #. Parent process runs .export_parallel() or .import_parallel(), which initiates EXPORT or IMPORT query in Exasol
        #. Every child process receives or sends a chunk of data using ExaHTTPTransportWrapper.export_*() or .import_*()
        #. Parent process waits for Exasol query and for child processes to finish

        All child processes should run in parallel.
        It is NOT possible to process some data first, and process some more data later.

        If an exception is raised in child process, it will close the pipe used for HTTP transport.
        Closing the pipe prematurely will cause SQL query to fail and will raise an exception in parent process.
        Parent process is responsible for closing other child processes and cleaning up.

        PyExasol does not provide a complete solution to manage child processes, only examples.
        The final solution depends on your hardware, network configuration, cloud provider and container orchestration software.
    """
    return ExaHTTPTransportWrapper(ipaddr, port, compression, encryption)
