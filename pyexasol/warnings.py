class PyexasolWarning(UserWarning):
    """Base class for all warnings emitted by pyexasol."""


class PyexasolDeprecationWarning(PyexasolWarning, DeprecationWarning):
    """Warn about features that will be removed in future versions."""
