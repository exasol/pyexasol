"""Deprecated compatibility exports.

Import ``ExaTimeDelta`` from ``pyexasol.data_types.converters`` and
``exasol_mapper`` from ``pyexasol.data_types.websocket_to_python`` instead.
"""

# Compatibility exports
from pyexasol.data_types.converters import ExaTimeDelta  # noqa: F401
from pyexasol.data_types.websocket_to_python import exasol_mapper  # noqa: F401

__all__ = ["ExaTimeDelta", "exasol_mapper"]
