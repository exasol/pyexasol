import os
import shutil
import time

import pytest

import pyexasol


@pytest.fixture
def dev_null():
    with open(os.devnull, "wb") as f:
        yield f


@pytest.fixture
def statement():
    yield "SELECT * FROM USERS LIMIT 1000"


@pytest.fixture
def invalid_statement():
    yield "SELECT * FROM DoesNotExist LIMIT 1000"


@pytest.mark.etl
@pytest.mark.exceptions
def test_sql_error(connection, dev_null, invalid_statement):
    def export_cb(pipe, dst, **kwargs):
        shutil.copyfileobj(pipe, dev_null)

    with pytest.raises(pyexasol.exceptions.ExaQueryError):
        connection.export_to_callback(export_cb, None, invalid_statement)


@pytest.mark.etl
@pytest.mark.exceptions
def test_abort_query(connection, dev_null, statement):
    def export_cb(pipe, dst, **kwargs):
        connection.abort_query()
        # Make sure abort has time to propagate
        time.sleep(1)
        shutil.copyfileobj(pipe, dev_null)

    with pytest.raises(pyexasol.exceptions.ExaError):
        connection.export_to_callback(export_cb, None, statement)


@pytest.mark.etl
@pytest.mark.exceptions
def test_error_in_export_callback(connection, statement):
    error_msg = "Error from callback"

    def export_cb(pipe, dst, **kwargs):
        raise Exception(error_msg)

    with pytest.raises(Exception, match=error_msg):
        connection.export_to_callback(export_cb, None, statement)


@pytest.mark.etl
@pytest.mark.exceptions
def test_error_in_import_callback(connection, statement):
    def import_cb(pipe, src, **kwargs):
        raise Exception()

    with pytest.raises(pyexasol.exceptions.ExaQueryError):
        connection.import_from_callback(import_cb, None, "users")


@pytest.mark.etl
@pytest.mark.exceptions
def test_closed_ws_connection(connection, dev_null, statement):
    def import_cb(pipe, src, **kwargs):
        connection.close(disconnect=False)
        time.sleep(1)
        shutil.copyfileobj(pipe, dev_null)

    with pytest.raises(pyexasol.exceptions.ExaError):
        connection.import_from_callback(import_cb, None, "users")
