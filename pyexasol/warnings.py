class PyexasolWarning(UserWarning):
    """Base class for all warnings emitted by pyexasol."""


class PyexasolDeprecationWarning(PyexasolWarning, DeprecationWarning):
    """Warning class for features that will be removed in future versions."""
