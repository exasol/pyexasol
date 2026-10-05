"""Deprecated compatibility exports.

Import ``ExaTimeDelta`` from ``pyexasol.data_types.converters`` and
``exasol_mapper`` from ``pyexasol.data_types.websocket_to_python`` instead.
"""

from warnings import warn

# Compatibility exports
from pyexasol.data_types.converters import ExaTimeDelta  # noqa: F401
from pyexasol.data_types.websocket_to_python import exasol_mapper  # noqa: F401
from pyexasol.warnings import PyexasolDeprecationWarning

warn(
    "The pyexasol.mapper module is deprecated and will be removed in the future. "
    "Import ExaTimeDelta and exasol_mapper from pyexasol.data_types instead.",
    PyexasolDeprecationWarning,
    stacklevel=2,
)

__all__ = ["ExaTimeDelta", "exasol_mapper"]
