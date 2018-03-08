"""
TODO: Add comments
"""

__all__ = [
    'connect',
    'exasol_mapper',
    'ExaError',
    'ExaCommunicationError',
    'ExaRuntimeError',
    'ExaRequestError',
    'ExaSQLError',
    'ExaConnection',
    'ExaFormatter',
    'ExaLogger',
]

from .version import __version__
from .exceptions import ExaError, ExaCommunicationError, ExaRuntimeError, ExaRequestError, ExaSQLError
from .connection import ExaConnection
from .formatter import ExaFormatter
from .logger import ExaLogger
from .mapper import exasol_mapper


def connect(**kwargs):
    """
    Constructor of connection objects
    """
    if 'cls_connection' in kwargs:
        cls = kwargs['cls_connection']

        if not issubclass(cls, ExaConnection):
            raise ValueError('Class passed in "cls_connection" is not instance of ExaConnection')
    else:
        cls = ExaConnection

    return cls(**kwargs)
