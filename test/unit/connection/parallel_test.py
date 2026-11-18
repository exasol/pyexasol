"""
Unit tests for :meth:`pyexasol.ExaConnection.import_parallel` and
:meth:`pyexasol.ExaConnection.export_parallel`.

The real methods are called on a mocked connection, so the query that would be sent
to the database can be inspected without a database.
"""

from unittest.mock import Mock

import pytest
from packaging.version import Version
from pydantic import ValidationError

from pyexasol import (
    ExaConnection,
    ExaFormatter,
)
from pyexasol.query_builders.csv.builders import BooleanRepresentation

ADDRESS_LIST = ["10.0.0.1:8564", "10.0.0.2:8565"]


@pytest.fixture
def mock_connection():
    mock = Mock(ExaConnection)
    mock.options = {"encryption": False, "compression": False, "quote_ident": False}
    mock.exasol_db_version = Version("8.0.0")
    mock.format = ExaFormatter(connection=mock)
    return mock


def executed_query(connection) -> str:
    connection.execute.assert_called_once()
    return connection.execute.call_args.args[0]


class TestExportParallel:
    @staticmethod
    def test_exports_table_to_all_addresses(mock_connection):
        ExaConnection.export_parallel(mock_connection, ADDRESS_LIST, "MY_TABLE")

        query = executed_query(mock_connection)
        assert query.startswith("EXPORT MY_TABLE INTO CSV")
        for index, address in enumerate(ADDRESS_LIST):
            assert f"AT 'http://{address}' FILE '{index:03d}.csv'" in query

    @staticmethod
    def test_exports_query_with_placeholders(mock_connection):
        ExaConnection.export_parallel(
            mock_connection,
            ADDRESS_LIST,
            "SELECT * FROM t WHERE id > {min_id}",
            query_params={"min_id": 5},
        )

        assert "SELECT * FROM t WHERE id > '5'" in executed_query(mock_connection)

    @staticmethod
    def test_exports_table_tuple_ignores_query_params(mock_connection):
        ExaConnection.export_parallel(
            mock_connection,
            ADDRESS_LIST,
            ("MY_SCHEMA", "MY_TABLE"),
            query_params={"min_id": 5},
        )

        assert executed_query(mock_connection).startswith(
            "EXPORT MY_SCHEMA.MY_TABLE INTO CSV"
        )

    @staticmethod
    def test_export_params_are_used(mock_connection):
        ExaConnection.export_parallel(
            mock_connection,
            ADDRESS_LIST,
            "MY_TABLE",
            export_params={
                "with_column_names": True,
                "column_separator": ";",
                "boolean": BooleanRepresentation.NUMERIC,
            },
        )

        query = executed_query(mock_connection)
        assert "WITH COLUMN NAMES" in query
        assert "COLUMN SEPARATOR = ';'" in query
        assert "BOOLEAN = '1/0'" in query

    @staticmethod
    def test_compression_follows_connection_option(mock_connection):
        mock_connection.options["compression"] = True

        ExaConnection.export_parallel(mock_connection, ADDRESS_LIST, "MY_TABLE")

        assert "FILE '000.gz'" in executed_query(mock_connection)

    @staticmethod
    def test_format_param_disables_compression(mock_connection):
        mock_connection.options["compression"] = True

        ExaConnection.export_parallel(
            mock_connection,
            ADDRESS_LIST,
            "MY_TABLE",
            export_params={"format": "csv"},
        )

        assert "FILE '000.csv'" in executed_query(mock_connection)

    @staticmethod
    def test_unknown_param_raises_without_executing(mock_connection):
        with pytest.raises(ValidationError):
            ExaConnection.export_parallel(
                mock_connection,
                ADDRESS_LIST,
                "MY_TABLE",
                export_params={"not_a_param": 1},
            )

        mock_connection.execute.assert_not_called()


class TestImportParallel:
    @staticmethod
    def test_imports_into_table_from_all_addresses(mock_connection):
        ExaConnection.import_parallel(mock_connection, ADDRESS_LIST, "MY_TABLE")

        query = executed_query(mock_connection)
        assert query.startswith("IMPORT INTO MY_TABLE FROM CSV")
        for index, address in enumerate(ADDRESS_LIST):
            assert f"AT 'http://{address}' FILE '{index:03d}.csv'" in query

    @staticmethod
    def test_import_params_are_used(mock_connection):
        ExaConnection.import_parallel(
            mock_connection,
            ADDRESS_LIST,
            "MY_TABLE",
            import_params={"skip": 1, "column_separator": ";"},
        )

        query = executed_query(mock_connection)
        assert "SKIP = 1" in query
        assert "COLUMN SEPARATOR = ';'" in query

    @staticmethod
    def test_compression_follows_connection_option(mock_connection):
        mock_connection.options["compression"] = True

        ExaConnection.import_parallel(mock_connection, ADDRESS_LIST, "MY_TABLE")

        assert "FILE '000.gz'" in executed_query(mock_connection)

    @staticmethod
    def test_format_param_disables_compression(mock_connection):
        mock_connection.options["compression"] = True

        ExaConnection.import_parallel(
            mock_connection,
            ADDRESS_LIST,
            "MY_TABLE",
            import_params={"format": "csv"},
        )

        assert "FILE '000.csv'" in executed_query(mock_connection)

    @staticmethod
    def test_unknown_param_raises_without_executing(mock_connection):
        with pytest.raises(ValidationError):
            ExaConnection.import_parallel(
                mock_connection,
                ADDRESS_LIST,
                "MY_TABLE",
                import_params={"not_a_param": 1},
            )

        mock_connection.execute.assert_not_called()
