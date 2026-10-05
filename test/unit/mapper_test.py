import datetime
import importlib

import pytest

import pyexasol
import pyexasol.mapper as mapper_module
from pyexasol.data_types.converters import ExaTimeDelta as CanonicalExaTimeDelta
from pyexasol.data_types.websocket_to_python import (
    exasol_mapper as CanonicalExasolMapper,
)
from pyexasol.mapper import (
    ExaTimeDelta,
    exasol_mapper,
)
from pyexasol.warnings import PyexasolDeprecationWarning


def test_import_mapper_module_emits_deprecation_warning():
    with pytest.warns(PyexasolDeprecationWarning):
        importlib.reload(mapper_module)


def test_exposes_exatimedelta_compatibility_import():
    assert ExaTimeDelta is CanonicalExaTimeDelta
    assert ExaTimeDelta.from_interval("+000000003 10:59:59.123000000") == ExaTimeDelta(
        days=3,
        hours=10,
        minutes=59,
        seconds=59,
        microseconds=123000,
    )


def test_exposes_exasol_mapper_compatibility_import():
    assert exasol_mapper is CanonicalExasolMapper
    assert exasol_mapper("123", {"type": "DECIMAL", "scale": 0}) == 123


def test_exposes_root_level_public_imports():
    assert pyexasol.ExaTimeDelta is CanonicalExaTimeDelta
    assert pyexasol.exasol_mapper is CanonicalExasolMapper
    assert pyexasol.ExaTimeDelta.from_interval("+000000003 10:59:59.123000000") == (
        CanonicalExaTimeDelta(
            days=3,
            hours=10,
            minutes=59,
            seconds=59,
            microseconds=123000,
        )
    )
    assert pyexasol.exasol_mapper(
        "2026-09-11", {"type": "DATE", "scale": 0}
    ) == datetime.date(2026, 9, 11)
