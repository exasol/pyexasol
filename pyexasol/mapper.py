"""Deprecated compatibility exports.

Import ``ExaTimeDelta`` and ``convert_websocket_to_python`` from ``pyexasol.data_types``
instead.
"""

from warnings import warn

# Compatibility exports
from pyexasol.data_types.converters import ExaTimeDelta  # noqa: F401
from pyexasol.data_types.websocket_to_python import (  # noqa: F401
    exasol_mapper,
)
from pyexasol.warnings import PyexasolDeprecationWarning

warn(
    "The pyexasol.mapper module is deprecated and will be removed in the future. "
    "Import ExaTimeDelta and convert_websocket_to_python from pyexasol.data_types instead.",
    PyexasolDeprecationWarning,
    stacklevel=2,
)

__all__ = ["ExaTimeDelta", "exasol_mapper"]
