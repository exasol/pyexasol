from test.integration.import_and_export.conftest import TABLE_NAME

SELECT_QUERY = f"SELECT * FROM {TABLE_NAME} ORDER BY FIRST_NAME ASC;"


def select_result(connection):
    result = connection.execute(SELECT_QUERY)
    return result.fetchall()
