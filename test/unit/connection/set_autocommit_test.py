from unittest.mock import MagicMock

import pytest


@pytest.mark.parametrize("value", [True, False])
def test_set_autocommit_accepts_boolean_values(mock_exaconnection_factory, value):
    connection = mock_exaconnection_factory()
    connection.set_attr = MagicMock()

    connection.set_autocommit(value)

    connection.set_attr.assert_called_once_with({"autocommit": value})


@pytest.mark.parametrize("value", [None, 0, 1, [], "true", object()])
def test_set_autocommit_rejects_non_boolean_values(mock_exaconnection_factory, value):
    connection = mock_exaconnection_factory()
    connection.set_attr = MagicMock()

    with pytest.raises(ValueError, match="Autocommit value must be boolean"):
        connection.set_autocommit(value)

    connection.set_attr.assert_not_called()
