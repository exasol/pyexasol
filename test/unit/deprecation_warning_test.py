import pytest

from pyexasol.warnings import PyexasolDeprecationWarning


def test_import_db2_module_emits_deprecation_warning():
    with pytest.warns(PyexasolDeprecationWarning):
        from pyexasol import db2  # noqa: F401
