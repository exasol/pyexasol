import pytest
from integration.import_and_export.helper import select_result


@pytest.fixture
def connection(connection_factory):
    with connection_factory(compression=True) as con:
        yield con


@pytest.fixture
def connection_without_resolving_hostnames(connection_factory):
    with connection_factory(compression=True, resolve_hostnames=False) as con:
        yield con


@pytest.mark.etl
class TestExport:
    @staticmethod
    def test_export_with_column_names(
        connection, fill_table, tmp_path, table_name, all_data
    ):
        actual_filepath = tmp_path / "actual.csv"
        params = {"with_column_names": True}

        connection.export_to_file(
            dst=actual_filepath, query_or_table=table_name, export_params=params
        )

        expected_header = ",".join(all_data.columns) + "\n"
        assert actual_filepath.read_text() == expected_header + all_data.csv_str()

    @staticmethod
    def test_export_without_resolving_hostname(
        connection_without_resolving_hostnames,
        fill_table,
        tmp_path,
        table_name,
        all_data,
    ):
        actual_filepath = tmp_path / "actual.csv"

        connection_without_resolving_hostnames.export_to_file(
            dst=actual_filepath, query_or_table=table_name
        )

        assert actual_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_custom_export_callback(
        connection, fill_table, tmp_path, table_name, all_data
    ):
        actual_filepath = tmp_path / "actual.csv"

        def export_cb(pipe, dst):
            dst.write_bytes(pipe.read())

        connection.export_to_callback(
            callback=export_cb, dst=actual_filepath, query_or_table=table_name
        )

        assert actual_filepath.read_text() == all_data.csv_str()

    @staticmethod
    def test_export_csv_cols(connection, fill_table, tmp_path, table_name, all_data):
        actual_filepath = tmp_path / "actual.csv"
        params = {"csv_cols": ["1..7"]}

        connection.export_to_file(
            dst=actual_filepath, query_or_table=table_name, export_params=params
        )

        assert actual_filepath.read_text() == all_data.csv_str()


@pytest.mark.etl
class TestImport:
    @staticmethod
    def test_without_resolving_hostname(
        connection_without_resolving_hostnames, tmp_path, table_name, all_data
    ):
        filepath = all_data.write_csv(directory=tmp_path)

        connection_without_resolving_hostnames.import_from_file(
            src=filepath, table=table_name
        )

        assert (
            select_result(connection_without_resolving_hostnames)
            == all_data.list_tuple()
        )

    @staticmethod
    def test_import_with_reordered_columns(connection, table_name, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)

        # csv has FIRST_NAME & then LAST_NAME, so we swap those as they're both strings
        # as csv should not require same order as columns defined for table
        columns = [
            "LAST_NAME",
            "FIRST_NAME",
            "REGISTER_DT",
            "LAST_VISIT_TS",
            "IS_GRADUATING",
            "AGE",
            "SCORE",
        ]
        params = {"columns": columns}
        # the data_tuple needs to be resorted afterward
        expected = sorted(
            all_data.list_tuple(selected_columns=columns), key=lambda x: x[0]
        )

        connection.import_from_file(
            src=filepath, table=table_name, import_params=params
        )

        assert select_result(connection) == expected

    @staticmethod
    def test_custom_import_callback(connection, table_name, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)

        def import_cb(pipe, src):
            pipe.write(src.read_bytes())

        connection.import_from_callback(
            callback=import_cb, src=filepath, table=table_name
        )

        assert select_result(connection) == all_data.list_tuple()

    @staticmethod
    def test_skip_rows_in_import(connection, table_name, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)

        offset = 2
        params = {"skip": offset}
        connection.import_from_file(
            src=filepath, table=table_name, import_params=params
        )

        assert select_result(connection) == all_data.list_tuple()[offset:]


@pytest.mark.etl
class TestExportImportRoundTrip:
    @staticmethod
    @pytest.mark.parametrize(
        "params",
        [
            pytest.param({}, id="no_params"),
            pytest.param({"format": "gz"}, id="set_format"),
            pytest.param({"encoding": "WINDOWS-1251"}, id="set_encoding"),
        ],
    )
    def test_to_and_from_csv(
        connection, tmp_path, fill_table, table_name, all_data, params
    ):
        export_file = tmp_path / "export.csv"
        connection.export_to_file(
            dst=export_file, query_or_table=table_name, export_params=params
        )

        connection.import_from_file(
            src=export_file, table=table_name, import_params=params
        )

        assert select_result(connection) == sorted(
            all_data.list_tuple() + all_data.list_tuple(), key=lambda x: x[0]
        )

    @staticmethod
    @pytest.mark.parametrize(
        "params",
        [
            pytest.param(
                {"columns": ["FIRST_NAME", "LAST_NAME", "AGE", "SCORE"]},
                id="set_columns",
            ),
        ],
    )
    def test_with_selected_columns(
        connection,
        tmp_path,
        fill_table,
        table_name,
        all_data,
        reduced_import_data,
        params,
    ):
        export_file = tmp_path / "export.csv"
        connection.export_to_file(
            dst=export_file, query_or_table=table_name, export_params=params
        )

        connection.import_from_file(
            src=export_file, table=table_name, import_params=params
        )

        assert select_result(connection) == sorted(
            all_data.list_tuple() + reduced_import_data.list_tuple(), key=lambda x: x[0]
        )

    @staticmethod
    @pytest.mark.parametrize(
        "params",
        [
            pytest.param(
                {
                    "csv_cols": [
                        "1",
                        "2",
                        "3 FORMAT='DD-MM-YYYY'",
                        "4 FORMAT='YYYY-MM-DD HH24:MI:SS'",
                        "5..7",
                    ]
                },
                id="set_csv_cols",
            ),
        ],
    )
    def test_with_asdf(
        connection,
        tmp_path,
        fill_table,
        table_name,
        all_data,
        reduced_import_data,
        params,
    ):
        export_file = tmp_path / "export.csv"
        connection.export_to_file(
            dst=export_file, query_or_table=table_name, export_params=params
        )

        connection.import_from_file(
            src=export_file, table=table_name, import_params=params
        )

        expected_modifications = [
            (row[0], row[1], row[2], row[3][:-6] + "000000", row[4], row[5], row[6])
            for row in all_data.list_tuple()
        ]

        assert select_result(connection) == sorted(
            all_data.list_tuple() + expected_modifications, key=lambda x: x[0]
        )
