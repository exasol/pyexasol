import datetime
from unittest.mock import patch

import pytest

from pyexasol.statement import ExaStatement


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(datetime.datetime(2026, 9, 11, 12, 34), id="python-datetime"),
    ],
)
def test_unserializable_parameter_raises_type_error(mock_exaconnection_factory, value):
    connection = mock_exaconnection_factory()

    def prepare(statement):
        statement.statement_handle = 1

    with patch.object(ExaStatement, "_prepare", prepare):
        statement = connection.create_prepared_statement("INSERT INTO T VALUES (?)")

    with pytest.raises(TypeError, match="is not JSON serializable"):
        statement.execute_prepared([(value,)])
