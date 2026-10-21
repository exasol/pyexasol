import datetime
import subprocess
import sys

import pyexasol
from pyexasol.data_types.converters import ExaTimeDelta as CanonicalExaTimeDelta


def _run_in_subprocess(import_script):
    subprocess.run(
        [sys.executable, "-c", import_script],
        check=True,
        capture_output=True,
        text=True,
    )


def test_root_level_exasol_mapper_emits_one_deprecation_warning():
    import_script = """
import warnings

import pyexasol
from pyexasol.warnings import PyexasolDeprecationWarning

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    assert pyexasol.exasol_mapper(
        val="123", data_type={"type": "DECIMAL", "scale": 0}
    ) == 123
    assert pyexasol.exasol_mapper(
        val="456", data_type={"type": "DECIMAL", "scale": 0}
    ) == 456

assert any(
    issubclass(warning.category, PyexasolDeprecationWarning)
    for warning in caught
)
assert len(caught) == 1
"""
    _run_in_subprocess(import_script)


def test_exposes_exatimedelta_compatibility_import():
    import_script = """
import warnings

from pyexasol.data_types.converters import ExaTimeDelta as CanonicalExaTimeDelta
from pyexasol.warnings import PyexasolDeprecationWarning

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    from pyexasol.mapper import ExaTimeDelta

assert len(caught) == 1
assert issubclass(caught[0].category, PyexasolDeprecationWarning)
assert ExaTimeDelta.from_interval(
    "+000000003 10:59:59.123000000"
) == CanonicalExaTimeDelta(
    days=3,
    hours=10,
    minutes=59,
    seconds=59,
    microseconds=123000,
)
"""
    _run_in_subprocess(import_script)


def test_exposes_exasol_mapper_compatibility_import():
    import_script = """
import warnings

from pyexasol.warnings import PyexasolDeprecationWarning

with warnings.catch_warnings(record=True) as import_warnings:
    warnings.simplefilter("always")
    from pyexasol.mapper import exasol_mapper

assert len(import_warnings) == 1
assert issubclass(import_warnings[0].category, PyexasolDeprecationWarning)

with warnings.catch_warnings(record=True) as mapper_warnings:
    warnings.simplefilter("always")
    result = exasol_mapper(val="123", data_type={"type": "DECIMAL", "scale": 0})

assert result == 123
assert len(mapper_warnings) == 1
assert issubclass(mapper_warnings[0].category, PyexasolDeprecationWarning)
"""
    _run_in_subprocess(import_script)


def test_exposes_root_level_public_imports():
    assert pyexasol.ExaTimeDelta is CanonicalExaTimeDelta
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
