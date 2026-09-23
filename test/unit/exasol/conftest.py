from unittest.mock import Mock

import pytest


@pytest.fixture
def source_connection():
    connection = Mock()
    connection.options = {"verbose_error": False}
    return connection
