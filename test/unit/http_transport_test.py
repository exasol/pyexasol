from typing import Optional
from unittest.mock import Mock

import pytest
from packaging.version import Version

from pyexasol import (
    ExaConnection,
    ExaFormatter,
)
from pyexasol.http_transport import (
    ExportQuery,
    ImportQuery,
    SqlQuery,
)


@pytest.fixture
def connection():
    mock = Mock(ExaConnection)
    mock.options = {"encryption": True, "quote_ident": "'"}
    mock.exasol_db_version = Version("8.32.0")
    mock.format = ExaFormatter(connection=mock)
    return mock


@pytest.fixture
def sql_query(connection):
    return SqlQuery(connection=connection, compression=True)


@pytest.fixture
def import_sql_query(connection):
    return ImportQuery(connection=connection, compression=True)


@pytest.fixture
def export_sql_query(connection):
    return ExportQuery(connection=connection, compression=True)


class TestSqlQuery:
    @staticmethod
    @pytest.mark.parametrize(
        "columns,expected",
        [(None, ""), (["LASTNAME", "FIRSTNAME"], '("LASTNAME","FIRSTNAME")')],
    )
    def test_build_columns_str(sql_query, columns, expected):
        sql_query.columns = columns
        assert sql_query._build_columns_str() == expected

    @staticmethod
    @pytest.mark.parametrize(
        "csv_cols,expected",
        [
            pytest.param(None, "", id="none_specified_returns_empty_string"),
            pytest.param(["1..2"], "(1..2)", id="values_specified_returns_value"),
        ],
    )
    def test_build_csv_cols(sql_query, csv_cols: Optional[list[str]], expected: str):
        sql_query.csv_cols = csv_cols
        assert sql_query._build_csv_cols() == expected

    @staticmethod
    def test_build_csv_cols_raises_exception(sql_query):
        sql_query.csv_cols = ["1.2"]
        with pytest.raises(ValueError, match="is not a safe csv_cols part"):
            sql_query._build_csv_cols()

    @staticmethod
    @pytest.mark.parametrize(
        "exa_address, expected",
        [
            pytest.param(
                "localhost:12583",
                "+vGxqpxBMYlTgzLlQqNS2X9kcpXIFJSZxU54GqJ0ZCo=",
                id="localhost",
            ),
            pytest.param(
                "127.18.0.2:8156",
                "tfdCUbrFQxEBTtrD9yet67fwCQMlxNVGqIdagPXvnlM=",
                id="ip",
            ),
            pytest.param(
                "127.18.0.2/64:8364",
                "YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=",
                id="url_with_cidr",
            ),
        ],
    )
    def test_extract_public_key(exa_address: str, expected: str):
        exa_address = f"{exa_address}/{expected}"
        assert SqlQuery._extract_public_key(exa_address) == expected

    @staticmethod
    def test_extract_public_key_raises_exception():
        exa_address = "127.18.0.2/64:8364/YHistZoLhU9+FKoSEH"
        with pytest.raises(
            ValueError, match="Could not extract public key from exa_address"
        ):
            SqlQuery._extract_public_key(exa_address)

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
    def test_get_file_list(connection, sql_query, db_version, expected_end):
        connection.exasol_db_version = db_version
        exa_address_list = [
            "127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o="
        ]

        result = sql_query._get_file_list(exa_address_list)

        assert result == [
            f"AT 'https://127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' {expected_end}"
        ]

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
        sql_query, connection, db_version, encryption, expected
    ):
        connection.options["encryption"] = encryption
        connection.exasol_db_version = db_version

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
        with pytest.raises(ValueError, match="Invalid comment, cannot contain"):
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
            pytest.param(True, None, "gz", id="no_format_maps_to_gz"),
            pytest.param(False, None, "csv", id="no_format_maps_to_csv"),
            pytest.param(True, "gz", "gz", id="gz_accepted"),
        ],
    )
    def test_file_ext(
        sql_query, compression: bool, file_ext: Optional[str], expected: str
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
    def test_url_prefix(sql_query, connection, encryption, expected):
        connection.options["encryption"] = encryption
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
    def test_build_query(import_sql_query, connection):
        result = import_sql_query.build_query(
            source="TABLE",
            exa_address_list=[
                "127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o="
            ],
        )
        assert (
            result
            == "IMPORT INTO TABLE FROM CSV\nAT 'https://127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' PUBLIC KEY 'sha256//YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' FILE '000.gz'"
        )

    @staticmethod
    def test_load_from_dict(connection):
        ImportQuery.load_from_dict(
            connection=connection, compression=False, params={"skip": 2}
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
        result = import_sql_query._get_import(source="TABLE")
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
    def test_build_query(export_sql_query, connection):
        result = export_sql_query.build_query(
            source="TABLE",
            exa_address_list=[
                "127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o="
            ],
        )
        assert (
            result
            == "EXPORT TABLE INTO CSV\nAT 'https://127.18.0.2:8364/YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' PUBLIC KEY 'sha256//YHistZoLhU9+FKoSEHHbNGtC/Ee4KT75DDBO+s5OG8o=' FILE '000.gz'"
        )

    #
    @staticmethod
    def test_load_from_dict(connection):
        ExportQuery.load_from_dict(
            connection=connection, compression=False, params={"delimit": "auto"}
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
        result = export_sql_query._get_export(source="TABLE")
        assert result == expected

    @staticmethod
    @pytest.mark.parametrize(
        "delimit,expected", [("auto", "AUTO"), ("AutO", "AUTO"), (None, None)]
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
        "with_column_names,expected",
        [("with_column_names", "WITH COLUMN NAMES"), (None, None)],
    )
    def test_with_column_names(export_sql_query, with_column_names, expected):
        export_sql_query.with_column_names = with_column_names
        assert export_sql_query._with_column_names == expected
