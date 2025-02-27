from inspect import cleandoc

import pytest

import pyexasol


@pytest.fixture
def config(dsn, user, password, schema):
    yield cleandoc(
        f"""
    [pyexasol]
    dsn = {dsn}
    user = {user}
    password = {password}
    schema = {schema}
    compression = True
    encryption = False
    socket_timeout = 20
    """
    )


@pytest.fixture
def config_file(tmpdir, config):
    path = tmpdir / "pyexasol.ini"
    with open(path, "w", encoding="utf-8") as f:
        f.write(config)
    yield path


@pytest.mark.configuration
def test_connect_using_config(config_file):
    connection = pyexasol.connect_local_config(
        config_section="pyexasol", config_path=config_file
    )
    result = connection.execute("SELECT 1;")
    expected = [(1,)]
    actual = result.fetchall()
    assert expected == actual
