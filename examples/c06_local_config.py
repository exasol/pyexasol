"""
Local config file
"""

import configparser
import pathlib
import pprint
import tempfile

import examples._config as config
import pyexasol

printer = pprint.PrettyPrinter(indent=4, width=140)

# Generate tmp file with sample config
with tempfile.TemporaryDirectory() as tempdir:
    tempdir = pathlib.Path(tempdir)

    handle = open(tempdir / "test.ini", "w+", encoding="utf-8")
    parser = configparser.ConfigParser()

    parser["test1"] = {
        "dsn": config.dsn,
        "user": config.user,
        "password": config.password,
        "schema": config.schema,
        "compression": True,
        "encryption": True,
        "socket_timeout": 20,
    }

    parser.write(handle)
    handle.seek(0)

    print(handle.read())
    handle.close()

    # Open connection using config file
    C = pyexasol.connect_local_config(
        "test1", config_path=handle.name, websocket_sslopt=config.websocket_sslopt
    )

    # Basic query
    stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
    printer.pprint(stmt.fetchall())
