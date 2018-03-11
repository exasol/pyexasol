
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
]

from .version import __version__
from .exceptions import ExaError, ExaCommunicationError, ExaRuntimeError, ExaRequestError, \
                        ExaQueryError, ExaQueryTimeoutError
from .connection import ExaConnection
from .statement import ExaStatement
from .formatter import ExaFormatter
from .logger import ExaLogger
from .mapper import exasol_mapper


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

    return connection_cls(**kwargs)
