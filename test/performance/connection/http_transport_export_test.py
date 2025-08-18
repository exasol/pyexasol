import pytest

from pyexasol import ExaConnection


@pytest.mark.parametrize(
    "export_method, data_creator",
    [
        pytest.param("import_from_file", "create_csv", id="file"),
    ],
)
def test_export_methods(
    benchmark,
    benchmark_specs,
    connection: ExaConnection,
    request,
    empty_import_into_table,
    export_method: str,
    data_creator: str,
):
    data = request.getfixturevalue(data_creator)

    def func_to_be_measured():
        return getattr(connection, export_method)(
            data,
            table=empty_import_into_table,
            import_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
        )

    benchmark.pedantic(func_to_be_measured, iterations=1, rounds=benchmark_specs.rounds)
    #
    # count_query = f"SELECT count(*) FROM {empty_import_into_table};"
    # count = connection.execute(count_query).fetchval()
    # assert count == benchmark_specs.final_import_data_size
