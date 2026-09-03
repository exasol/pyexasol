from unittest.mock import Mock

import pytest

from pyexasol.formatter import ExaFormatter


@pytest.fixture
def formatter():
    connection = Mock()
    connection.options = {"quote_ident": True}
    return ExaFormatter(connection)
