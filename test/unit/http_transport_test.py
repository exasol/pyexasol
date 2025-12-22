from unittest.mock import (
    Mock,
    patch,
)

import pytest
from packaging.version import Version

from pyexasol import (
    ExaConnection,
    ExaFormatter,
)
from pyexasol.http_transport import (
    ExaHttpThread,
    ExaHTTPTransportWrapper,
    ExportQuery,
    ImportQuery,
    SqlQuery,
)


@pytest.fixture
def mock_connection():
    mock = Mock(ExaConnection)
    mock.options = {"encryption": True, "quote_ident": "'"}
    mock.exasol_db_version = Version("8.32.0")
    mock.format = ExaFormatter(connection=mock)
    return mock


@pytest.fixture
def sql_query(mock_connection):
    return SqlQuery(connection=mock_connection, compression=True)


@pytest.fixture
def import_sql_query(mock_connection):
    return ImportQuery(connection=mock_connection, compression=True)


@pytest.fixture
def export_sql_query(mock_connection):
    return ExportQuery(connection=mock_connection, compression=True)


class TestSqlQuery:
    @staticmethod
    @pytest.mark.parametrize(
        "columns,expected",
        [(None, ""), ([], ""), (["LASTNAME", "FIRSTNAME"], '("LASTNAME","FIRSTNAME")')],
    )
    def test_column_spec(sql_query, columns, expected):
        sql_query.columns = columns
        assert sql_query._column_spec == expected

    @staticmethod
    @pytest.mark.parametrize(
        "csv_cols,expected",
        [
            pytest.param(None, "", id="none_specified"),
            pytest.param([], "", id="empty_iterable_specified"),
            pytest.param(["1..3"], "(1..3)", id="col_gap_specified"),
            pytest.param(["123"], "(123)", id="col_without_spaces"),
            pytest.param(
                ["1..3", "4 FORMAT='DD-MM-YYYY'"],
                "(1..3,4 FORMAT='DD-MM-YYYY')",
                id="multi_specifier_with_format",
            ),
        ],
    )
    def test_build_csv_cols(sql_query, csv_cols: list[str] | None, expected: str):
        sql_query.csv_cols = csv_cols
        assert sql_query._build_csv_cols() == expected

    @staticmethod
    def test_build_csv_cols_raises_exception(sql_query):
        sql_query.csv_cols = ["1.2"]
        with pytest.raises(ValueError, match="is not a safe csv_cols part"):
            sql_query._build_csv_cols()

    @staticmethod
    @pytest.mark.parametrize(
        "ip_address, public_key",
        [
            pytest.param(
                "127.18.0.2:8156",
                "tfdCUbrFQxEBTtrD9yet67fwCQMlxNVGqIdagPXvnlM=",
                id="ip",
            ),
            pytest.param(
                "127.18.0.2:8364",
                None,
                id="url_without_public_key",
            ),
        ],
    )
    def test_split_exa_address_into_known_components(ip_address: str, public_key: str):
        exa_address = f"{ip_address}"
        if public_key:
            exa_address = f"{ip_address}/{public_key}"
        result = SqlQuery._split_exa_address_into_components(exa_address)
        assert result[0] == ip_address
        assert result[1] == public_key

    @staticmethod
    @pytest.mark.parametrize(
        "exa_address",
        [
            pytest.param(
                "127.18.0.2:8364/YHistZoLhU9+FKoSEH", id="incomplete_public_key"
            ),
            pytest.param("127.18.0.2/64:8364", id="cidr_notation"),
            pytest.param("localhost:1729", id="localhost"),
        ],
    )
    def test_split_exa_address_into_known_components_raises_exception(exa_address: str):
        with pytest.raises(ValueError, match="Could not split exa_address"):
            SqlQuery._split_exa_address_into_components(exa_address)

    @staticmethod
    @pytest.mark.parametrize(
        "db_version,expected_end",
        [
            pytest.param(Version("7.1.19"), "FILE '000.gz'", id="lower_version"),
            pytest.param(
                Version("8.32.0"),
                "PUBLIC KEY 'sha256//YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' FILE '000.gz'",
                id="greater_than_or_equal_version",
            ),
        ],
    )
    def test_get_file_list(mock_connection, sql_query, db_version, expected_end):
        mock_connection.exasol_db_version = db_version
        exa_address_list = [
            "127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o="
        ]

        result = sql_query._get_file_list(exa_address_list)

        assert result == [f"AT 'https://127.18.0.2:8364' {expected_end}"]

    @staticmethod
    def test_get_query_str():
        query_lines = [None, "test", None, "this"]
        assert SqlQuery._get_query_str(query_lines) == "test\nthis"

    @staticmethod
    @pytest.mark.parametrize(
        "db_version,encryption,expected",
        [
            pytest.param(
                Version("7.1.19"), False, False, id="lower_version_without_encryption"
            ),
            pytest.param(
                Version("7.1.19"), True, False, id="lower_version_with_encryption"
            ),
            pytest.param(
                Version("8.32.0"), True, True, id="equal_version_with_encryption"
            ),
            pytest.param(
                Version("8.32.0"), False, False, id="equal_version_without_encryption"
            ),
        ],
    )
    def test_requires_tls_public_key(
        sql_query, mock_connection, db_version, encryption, expected
    ):
        mock_connection.options["encryption"] = encryption
        mock_connection.exasol_db_version = db_version

        result = sql_query._requires_tls_public_key()
        assert result == expected

    @staticmethod
    @pytest.mark.parametrize(
        "column_delimiter,expected", [(";", "COLUMN DELIMITER = ';'"), (None, None)]
    )
    def test_column_delimiter(sql_query, column_delimiter, expected):
        sql_query.column_delimiter = column_delimiter
        assert sql_query._column_delimiter == expected

    @staticmethod
    @pytest.mark.parametrize(
        "column_separator,expected", [("TAB", "COLUMN SEPARATOR = 'TAB'"), (None, None)]
    )
    def test_column_separator(sql_query, column_separator, expected):
        sql_query.column_separator = column_separator
        assert sql_query._column_separator == expected

    @staticmethod
    @pytest.mark.parametrize(
        "comment,expected",
        [("This is a comment", "/*This is a comment*/"), (None, None)],
    )
    def test_comment(sql_query, comment, expected):
        sql_query.comment = comment
        assert sql_query._comment == expected

    @staticmethod
    def test_comment_raises_exception(sql_query):
        sql_query.comment = "*/This is a comment"
        with pytest.raises(ValueError, match="Comment must not contain"):
            sql_query._comment

    @staticmethod
    @pytest.mark.parametrize(
        "encoding,expected", [("UTF-8", "ENCODING = 'UTF-8'"), (None, None)]
    )
    def test_encoding(sql_query, encoding, expected):
        sql_query.encoding = encoding
        assert sql_query._encoding == expected

    @staticmethod
    @pytest.mark.parametrize(
        "compression,file_ext,expected",
        [
            pytest.param(True, None, "gz", id="compressed_defaults_to_format_gz"),
            pytest.param(False, None, "csv", id="uncompressed_defaults_to_format_csv"),
            pytest.param(True, "gz", "gz", id="format_gz_accepted"),
        ],
    )
    def test_file_ext(
        sql_query, compression: bool, file_ext: str | None, expected: str
    ):
        sql_query.compression = compression
        sql_query.format = file_ext
        assert sql_query._file_ext == expected

    @staticmethod
    def test_file_ext_raises_exception(sql_query):
        sql_query.format = "not_a_valid_format"
        with pytest.raises(
            ValueError, match="Unsupported compression format: not_a_valid_format"
        ):
            sql_query._file_ext

    @staticmethod
    @pytest.mark.parametrize("null,expected", [("NONE", "NULL = 'NONE'"), (None, None)])
    def test_null(sql_query, null, expected):
        sql_query.null = null
        assert sql_query._null == expected

    @staticmethod
    @pytest.mark.parametrize(
        "encryption,expected",
        [
            (False, "http://"),
            (True, "https://"),
        ],
    )
    def test_url_prefix(sql_query, mock_connection, encryption, expected):
        mock_connection.options["encryption"] = encryption
        assert sql_query._url_prefix == expected

    @staticmethod
    @pytest.mark.parametrize(
        "row_separator,expected", [("LF", "ROW SEPARATOR = 'LF'"), (None, None)]
    )
    def test_row_separator(sql_query, row_separator, expected):
        sql_query.row_separator = row_separator
        assert sql_query._row_separator == expected


class TestImportQuery:
    @staticmethod
    def test_build_query(import_sql_query, mock_connection):
        result = import_sql_query.build_query(
            table="TABLE",
            exa_address_list=[
                "127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o="
            ],
        )
        assert (
            result
            == "IMPORT INTO TABLE FROM CSV\nAT 'https://127.18.0.2:8364' PUBLIC KEY 'sha256//YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' FILE '000.gz'"
        )

    @staticmethod
    def test_load_from_dict(mock_connection):
        ImportQuery.load_from_dict(
            connection=mock_connection, compression=False, params={"skip": 2}
        )

    @staticmethod
    @pytest.mark.parametrize(
        "columns,expected",
        [
            (
                ["LASTNAME", "FIRSTNAME"],
                'IMPORT INTO TABLE("LASTNAME","FIRSTNAME") FROM CSV',
            ),
            (None, "IMPORT INTO TABLE FROM CSV"),
        ],
    )
    def test_get_import(import_sql_query, columns, expected):
        import_sql_query.columns = columns
        result = import_sql_query._get_import(table="TABLE")
        assert result == expected

    @staticmethod
    @pytest.mark.parametrize(
        "skip,expected",
        [("1", "SKIP = 1"), (1, "SKIP = 1"), ("2", "SKIP = 2"), (None, None)],
    )
    def test_skip(import_sql_query, skip, expected):
        import_sql_query.skip = skip
        assert import_sql_query._skip == expected

    @staticmethod
    @pytest.mark.parametrize(
        "trim,expected", [("trim", "TRIM"), ("TriM", "TRIM"), (None, None)]
    )
    def test_trim(import_sql_query, trim, expected):
        import_sql_query.trim = trim
        assert import_sql_query._trim == expected

    @staticmethod
    def test_trim_raises_exception(import_sql_query):
        import_sql_query.trim = "not_a_valid_trim"
        with pytest.raises(ValueError, match="Invalid value for import parameter TRIM"):
            assert import_sql_query._trim


class TestExportQuery:
    @staticmethod
    def test_build_query(export_sql_query, mock_connection):
        result = export_sql_query.build_query(
            table="TABLE",
            exa_address_list=[
                "127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o="
            ],
        )
        assert (
            result
            == "EXPORT TABLE INTO CSV\nAT 'https://127.18.0.2:8364' PUBLIC KEY 'sha256//YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' FILE '000.gz'"
        )

    #
    @staticmethod
    def test_load_from_dict(mock_connection):
        ExportQuery.load_from_dict(
            connection=mock_connection, compression=False, params={"delimit": "auto"}
        )

    @staticmethod
    @pytest.mark.parametrize(
        "columns,expected",
        [
            (
                ["LASTNAME", "FIRSTNAME"],
                'EXPORT TABLE("LASTNAME","FIRSTNAME") INTO CSV',
            ),
            (None, "EXPORT TABLE INTO CSV"),
        ],
    )
    def test_get_export(export_sql_query, columns, expected):
        export_sql_query.columns = columns
        result = export_sql_query._get_export(table="TABLE")
        assert result == expected

    @staticmethod
    @pytest.mark.parametrize(
        "delimit,expected",
        [("auto", "DELIMIT=AUTO"), ("AutO", "DELIMIT=AUTO"), (None, None)],
    )
    def test_delimit(export_sql_query, delimit, expected):
        export_sql_query.delimit = delimit
        assert export_sql_query._delimit == expected

    @staticmethod
    def test_delimit_raises_exception(export_sql_query):
        export_sql_query.delimit = "not_a_valid_delimit"
        with pytest.raises(
            ValueError, match="Invalid value for export parameter DELIMIT"
        ):
            assert export_sql_query._delimit

    @staticmethod
    @pytest.mark.parametrize(
        "value,expected",
        [(True, "WITH COLUMN NAMES"), (False, None)],
    )
    def test_with_column_names(export_sql_query, value, expected):
        export_sql_query.with_column_names = value
        assert export_sql_query._with_column_names == expected

    @staticmethod
    @pytest.mark.parametrize("value", ["False", "true", "abc", 1, 0])
    def test_with_column_names_wrong_value_raises_exception(export_sql_query, value):
        export_sql_query.with_column_names = value
        with pytest.raises(
            ValueError, match="Invalid value for export parameter WITH_COLUMNS"
        ):
            _ = export_sql_query._with_column_names


ERROR_MESSAGE = "Error from callback"


def export_callback(pipe, dst, **kwargs):
    raise Exception(ERROR_MESSAGE)


def import_callback(pipe, src, **kwargs):
    raise Exception(ERROR_MESSAGE)


@pytest.fixture
def mock_http_thread():
    return Mock(ExaHttpThread)


@pytest.fixture
def http_transport_wrapper_with_mocks(mock_http_thread):
    with patch.object(ExaHTTPTransportWrapper, "__init__", return_value=None):
        http_wrapper = ExaHTTPTransportWrapper(ipaddr="dummy", port=8000)
        http_wrapper.http_thread = mock_http_thread
        return http_wrapper


class TestExaHTTPTransportWrapper:
    @staticmethod
    def test_export_to_callback_fails_as_not_a_callback(
        http_transport_wrapper_with_mocks,
    ):
        with pytest.raises(ValueError, match="is not callable"):
            http_transport_wrapper_with_mocks.export_to_callback(
                callback="string", dst=None
            )

    @staticmethod
    def test_import_to_callback_fails_as_not_a_callback(
        http_transport_wrapper_with_mocks,
    ):
        with pytest.raises(ValueError, match="is not callable"):
            http_transport_wrapper_with_mocks.import_from_callback(
                callback="string", src=None
            )
