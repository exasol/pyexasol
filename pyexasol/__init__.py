
__all__ = [
    'connect',
    'exasol_mapper',
    'ExaError',
    'ExaCommunicationError',
    'ExaRuntimeError',
    'ExaRequestError',
    'ExaQueryError',
    'ExaQueryTimeoutError',
    'ExaConnection',
    'ExaStatement',
    'ExaFormatter',
    'ExaLogger',
    'ExaExtension',
    'ExaHTTPTransportWrapper',
    'HTTP_EXPORT',
    'HTTP_IMPORT',
]

from .version import __version__
from .exceptions import ExaError, ExaCommunicationError, ExaRuntimeError, ExaRequestError, \
                        ExaQueryError, ExaQueryTimeoutError
from .connection import ExaConnection
from .statement import ExaStatement
from .formatter import ExaFormatter
from .logger import ExaLogger
from .ext import ExaExtension
from .mapper import exasol_mapper
from .http_transport import ExaHTTPTransportWrapper, HTTP_EXPORT, HTTP_IMPORT


def connect(**kwargs):
    """
    Constructor of connection objects
    Please see ExaConnection object for list of arguments
    """
    if 'cls_connection' in kwargs:
        connection_cls = kwargs['cls_connection']

        if not issubclass(connection_cls, ExaConnection):
            raise ValueError(f"Class [{kwargs['cls_connection']}] is not subclass of ExaConnection")
    else:
        connection_cls = ExaConnection

    if 'cls_statement' in kwargs and not issubclass(kwargs['cls_statement'], ExaStatement):
        raise ValueError(f"Class [{kwargs['cls_statement']} is not subclass of ExaStatement")

    if 'cls_formatter' in kwargs and not issubclass(kwargs['cls_formatter'], ExaFormatter):
        raise ValueError(f"Class [{kwargs['cls_formatter']} is not subclass of ExaFormatter")

    if 'cls_logger' in kwargs and not issubclass(kwargs['cls_logger'], ExaLogger):
        raise ValueError(f"Class [{kwargs['cls_logger']} is not subclass of ExaLogger")

    if 'cls_extension' in kwargs and not issubclass(kwargs['cls_extension'], ExaExtension):
        raise ValueError(f"Class [{kwargs['cls_extension']} is not subclass of ExaExtension")

    return connection_cls(**kwargs)


def http_transport(dsn, mode, compression=False):
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
    return ExaHTTPTransportWrapper(dsn, mode, compression)
