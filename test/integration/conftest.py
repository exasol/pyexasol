import decimal
import os
import ssl
import subprocess
import uuid
from collections import namedtuple
from inspect import cleandoc
from pathlib import Path

import pytest

import pyexasol
from pyexasol import ExaConnection
from pyexasol_utils.docker_util import DockerDataLoader

_ROOT: Path = Path(__file__).parent
DATA_DIRECTORY = _ROOT / ".." / "data"


@pytest.fixture(scope="session")
def container_name():
    return "db_container_test"


@pytest.fixture(scope="session")
def certificate(tmp_path_factory, container_name) -> Path:
    tmp_dir = tmp_path_factory.mktemp("certificate")
    file_path = tmp_dir / "rootCA.crt"

    command = ["docker", "cp", f"{container_name}:/certificates/rootCA.crt", file_path]
    subprocess.run(
        command,
        check=True,
        text=True,
        capture_output=True,
    )
    return file_path


@pytest.fixture(scope="session")
def db_version(connection_factory):
    with connection_factory() as conn:
        version = conn.exasol_db_version
    return version


@pytest.fixture(scope="session")
def db_major_version(db_version) -> int:
    return db_version.major


@pytest.fixture(scope="session")
def dsn_resolved():
    return os.environ.get("EXAHOST", "localhost:8563")


@pytest.fixture(
    scope="session",
    params=[
        pytest.param(ssl.CERT_NONE, id="NO_CERT", marks=pytest.mark.no_cert),
        pytest.param(ssl.CERT_REQUIRED, id="WITH_CERT", marks=pytest.mark.with_cert),
    ],
)
def certification_type(request):
    if request.param == ssl.CERT_NONE:
        return ssl.CERT_NONE
    return ssl.CERT_REQUIRED


@pytest.fixture(scope="session")
def dsn(certification_type):
    if certification_type == ssl.CERT_NONE:
        return os.environ.get("EXAHOST", "localhost:8563")
    # The host name is different for this case. As it is required to be the same
    # host name that the certificate is signed. This comes from the ITDE.
    return os.environ.get("EXAHOST", "exasol-test-database:8563")


@pytest.fixture(scope="session")
def user():
    return os.environ.get("EXAUID", "SYS")


@pytest.fixture(scope="session")
def password():
    return os.environ.get("EXAPWD", "exasol")


@pytest.fixture(scope="session")
def schema():
    return os.environ.get("EXASCHEMA", "PYEXASOL_TEST")


@pytest.fixture(scope="session")
def websocket_sslopt(certification_type, certificate):
    websocket_dict = {"cert_reqs": certification_type}
    if certification_type == ssl.CERT_REQUIRED:
        websocket_dict["ca_certs"] = certificate
    return websocket_dict


@pytest.fixture(scope="session")
def connection_factory(dsn, user, password, schema, websocket_sslopt):
    def _connection_fixture(**kwargs) -> ExaConnection:
        defaults = {
            "dsn": dsn,
            "user": user,
            "password": password,
            "schema": schema,
            "websocket_sslopt": websocket_sslopt,
        }
        config = {**defaults, **kwargs}
        return pyexasol.connect(**config)

    return _connection_fixture


@pytest.fixture
def connection(connection_factory):
    con = connection_factory()
    yield con
    con.close()


@pytest.fixture
def view(connection, faker):
    name = f"TEST_VIEW_{uuid.uuid4()}"
    name = name.replace("-", "_").upper()
    ddl = f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM users;"
    connection.execute(ddl)
    connection.commit()

    yield name

    delete_stmt = f"DROP VIEW IF EXISTS {name};"
    connection.execute(delete_stmt)
    connection.commit()


@pytest.fixture
def flush_statistics(connection):
    connection.execute("FLUSH STATISTICS TASKS;")
    connection.commit()


@pytest.fixture(scope="session", autouse=True)
def prepare_database(dsn_resolved, user, password, container_name):
    loader = DockerDataLoader(
        dsn=dsn_resolved,
        username=user,
        password=password,
        container_name=container_name,
        data_directory=DATA_DIRECTORY,
    )
    loader.load()


@pytest.fixture
def edge_case_ddl():
    table_name = "edge_case"
    ddl = cleandoc(
        f"""CREATE OR REPLACE TABLE {table_name}
        (
            dec36_0         DECIMAL(36,0),
            dec36_36        DECIMAL(36,36),
            dbl             DOUBLE,
            bl              BOOLEAN,
            dt              DATE,
            ts              TIMESTAMP,
            var100          VARCHAR(100),
            var2000000      VARCHAR(2000000)
        )
        """
    )
    yield table_name, ddl


@pytest.fixture
def edge_cases():
    return {
        "Biggest-Values": {
            "DEC36_0": decimal.Decimal("+" + ("9" * 36)),
            "DEC36_36": decimal.Decimal("+0." + ("9" * 36)),
            "DBL": 1.7e308,
            "BL": True,
            "DT": "9999-12-31",
            "TS": "9999-12-31 23:59:59.999",
            "VAR100": "ひ" * 100,
            "VAR2000000": "ひ" * 2000000,
        },
        "Smallest-Values": {
            "DEC36_0": decimal.Decimal("-" + ("9" * 36)),
            "DEC36_36": decimal.Decimal("-0." + ("9" * 36)),
            "DBL": -1.7e308,
            "BL": False,
            "DT": "0001-01-01",
            "TS": "0001-01-01 00:00:00",
            "VAR100": "",
            "VAR2000000": "ひ",
        },
        "All-Nulls": {
            "DEC36_0": None,
            "DEC36_36": None,
            "DBL": None,
            "BL": None,
            "DT": None,
            "TS": None,
            "VAR100": None,
            "VAR2000000": None,
        },
    }


@pytest.fixture(scope="session")
def expected_reserved_words(db_major_version):
    # fmt: off
    set_shared = {
        "ABSOLUTE", "ACTION", "ADD", "AFTER", "ALL", "ALLOCATE", "ALTER",
        "AND", "ANY", "APPEND", "ARE", "ARRAY", "AS", "ASC", "ASENSITIVE",
        "ASSERTION", "AT", "ATTRIBUTE", "AUTHID", "AUTHORIZATION", "BEFORE",
        "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BIT", "BLOB", "BLOCKED",
        "BOOL", "BOOLEAN", "BOTH", "BY", "BYTE", "CALL", "CALLED",
        "CARDINALITY", "CASCADE", "CASCADED", "CASE", "CASESPECIFIC", "CAST",
        "CATALOG", "CHAIN", "CHAR", "CHARACTER", "CHARACTERISTICS",
        "CHARACTER_SET_CATALOG", "CHARACTER_SET_NAME", "CHARACTER_SET_SCHEMA",
        "CHECK", "CHECKED", "CLOB", "CLOSE", "COALESCE", "COLLATE",
        "COLLATION", "COLLATION_CATALOG", "COLLATION_NAME", "COLLATION_SCHEMA",
        "COLUMN", "COMMIT", "CONDITION", "CONNECTION", "CONNECT_BY_ISCYCLE",
        "CONNECT_BY_ISLEAF", "CONNECT_BY_ROOT", "CONSTANT", "CONSTRAINT",
        "CONSTRAINTS", "CONSTRAINT_STATE_DEFAULT", "CONSTRUCTOR", "CONTAINS",
        "CONTINUE", "CONTROL", "CONVERT", "CORRESPONDING", "CREATE", "CS",
        "CSV", "CUBE", "CURRENT", "CURRENT_DATE", "CURRENT_PATH",
        "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_SESSION",
        "CURRENT_STATEMENT", "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "CURRENT_USER", "CURSOR", "CYCLE", "DATA", "DATALINK", "DATE",
        "DATETIME_INTERVAL_CODE", "DATETIME_INTERVAL_PRECISION", "DAY",
        "DBTIMEZONE", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT",
        "DEFAULT_LIKE_ESCAPE_CHARACTER", "DEFERRABLE", "DEFERRED", "DEFINED",
        "DEFINER", "DELETE", "DEREF", "DERIVED", "DESC", "DESCRIBE",
        "DESCRIPTOR", "DETERMINISTIC", "DISABLE", "DISABLED", "DISCONNECT",
        "DISPATCH", "DISTINCT", "DLURLCOMPLETE", "DLURLPATH", "DLURLPATHONLY",
        "DLURLSCHEME", "DLURLSERVER", "DLVALUE", "DO", "DOMAIN", "DOUBLE",
        "DROP", "DYNAMIC", "DYNAMIC_FUNCTION", "DYNAMIC_FUNCTION_CODE", "EACH",
        "ELSE", "ELSEIF", "ELSIF", "EMITS", "ENABLE", "ENABLED", "END",
        "END-EXEC", "ENDIF", "ENFORCE", "EQUALS", "ERRORS", "ESCAPE",
        "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXIT",
        "EXPORT", "EXTERNAL", "EXTRACT", "FALSE", "FBV", "FETCH", "FILE",
        "FINAL", "FIRST", "FLOAT", "FOLLOWING", "FOR", "FORALL", "FORCE",
        "FORMAT", "FOUND", "FREE", "FROM", "FS", "FULL", "FUNCTION", "GENERAL",
        "GENERATED", "GEOMETRY", "GET", "GLOBAL", "GO", "GOTO", "GRANT",
        "GRANTED", "GROUP", "GROUPING", "GROUPS", "GROUP_CONCAT", "HASHTYPE",
        "HASHTYPE_FORMAT", "HAVING", "HIGH", "HOLD", "HOUR", "IDENTITY", "IF",
        "IFNULL", "IMMEDIATE", "IMPERSONATE", "IMPLEMENTATION", "IMPORT", "IN",
        "INDEX", "INDICATOR", "INNER", "INOUT", "INPUT", "INSENSITIVE",
        "INSERT", "INSTANCE", "INSTANTIABLE", "INT", "INTEGER", "INTEGRITY",
        "INTERSECT", "INTERVAL", "INTO", "INVERSE", "INVOKER", "IS", "ITERATE",
        "JOIN", "KEY_MEMBER", "KEY_TYPE", "LARGE", "LAST", "LATERAL", "LDAP",
        "LEADING", "LEAVE", "LEFT", "LEVEL", "LIKE", "LIMIT", "LISTAGG",
        "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOCATOR", "LOG",
        "LONGVARCHAR", "LOOP", "LOW", "MAP", "MATCH", "MATCHED", "MERGE",
        "METHOD", "MINUS", "MINUTE", "MOD", "MODIFIES", "MODIFY", "MODULE",
        "MONTH", "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW",
        "NEXT", "NLS_DATE_FORMAT", "NLS_DATE_LANGUAGE",
        "NLS_FIRST_DAY_OF_WEEK", "NLS_NUMERIC_CHARACTERS",
        "NLS_TIMESTAMP_FORMAT", "NO", "NOCYCLE", "NOLOGGING", "NONE", "NOT",
        "NULL", "NULLIF", "NUMBER", "NUMERIC", "NVARCHAR", "NVARCHAR2",
        "OBJECT", "OF", "OFF", "OLD", "ON", "ONLY", "OPEN", "OPTION",
        "OPTIONS", "OR", "ORDER", "ORDERING", "ORDINALITY", "OTHERS",
        "OUT", "OUTER", "OUTPUT", "OVER", "OVERLAPS", "OVERLAY", "OVERRIDING",
        "PAD", "PARALLEL_ENABLE", "PARAMETER", "PARAMETER_SPECIFIC_CATALOG",
        "PARAMETER_SPECIFIC_NAME", "PARAMETER_SPECIFIC_SCHEMA", "PARTIAL",
        "PATH", "PERMISSION", "PLACING", "PLUS", "POSITION", "PRECEDING",
        "PREFERRING", "PREPARE", "PRESERVE", "PRIOR", "PRIVILEGES",
        "PROCEDURE", "PROFILE", "QUALIFY", "RANDOM", "RANGE", "READ", "READS",
        "REAL", "RECOVERY", "RECURSIVE", "REF", "REFERENCES", "REFERENCING",
        "REFRESH", "REGEXP_LIKE", "RELATIVE", "RELEASE", "RENAME", "REPEAT",
        "REPLACE", "RESTORE", "RESTRICT", "RESULT", "RETURN",
        "RETURNED_LENGTH", "RETURNED_OCTET_LENGTH", "RETURNS", "REVOKE",
        "RIGHT", "ROLLBACK", "ROLLUP", "ROUTINE", "ROW", "ROWS", "ROWTYPE",
        "SAVEPOINT", "SCHEMA", "SCOPE", "SCOPE_USER", "SCRIPT", "SCROLL",
        "SEARCH", "SECOND", "SECTION", "SECURITY", "SELECT", "SELECTIVE",
        "SELF", "SENSITIVE", "SEPARATOR", "SEQUENCE", "SESSION",
        "SESSIONTIMEZONE", "SESSION_USER", "SET", "SETS", "SHORTINT",
        "SIMILAR", "SMALLINT", "SOME", "SOURCE", "SPACE", "SPECIFIC",
        "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING",
        "SQL_BIGINT", "SQL_BIT", "SQL_CHAR", "SQL_DATE", "SQL_DECIMAL",
        "SQL_DOUBLE", "SQL_FLOAT", "SQL_INTEGER", "SQL_LONGVARCHAR",
        "SQL_NUMERIC", "SQL_PREPROCESSOR_SCRIPT", "SQL_REAL", "SQL_SMALLINT",
        "SQL_TIMESTAMP", "SQL_TINYINT", "SQL_TYPE_DATE", "SQL_TYPE_TIMESTAMP",
        "SQL_VARCHAR", "START", "STATE", "STATEMENT", "STATIC", "STRUCTURE",
        "STYLE", "SUBSTRING", "SUBTYPE", "SYSDATE", "SYSTEM", "SYSTEM_USER",
        "SYSTIMESTAMP", "TABLE", "TEMPORARY", "TEXT", "THEN", "TIME",
        "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TINYINT", "TO",
        "TRAILING", "TRANSACTION", "TRANSFORM", "TRANSFORMS", "TRANSLATION",
        "TREAT", "TRIGGER", "TRIM", "TRUE", "TRUNCATE", "UNDER", "UNION",
        "UNIQUE", "UNKNOWN", "UNLINK", "UNNEST", "UNTIL", "UPDATE", "USAGE",
        "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARCHAR2", "VARRAY",
        "VERIFY", "VIEW", "WHEN", "WHENEVER", "WHERE", "WHILE", "WINDOW",
        "WITH", "WITHIN", "WITHOUT", "WORK", "YEAR", "YES", "ZONE",
    }
    # fmt: on

    if db_major_version == 7:
        return set_shared
    elif db_major_version == 8:
        set_shared.update(["CURRENT_CLUSTER", "CURRENT_CLUSTER_UID"])
        return set_shared


@pytest.fixture(scope="session")
def expected_user_table_column_last_visit_ts(db_major_version):
    timestamp_type = namedtuple("timestamp_type", ["size", "sql_type"])

    if db_major_version == 7:
        return timestamp_type(size=8, sql_type="TIMESTAMP")
    elif db_major_version == 8:
        return timestamp_type(size=16, sql_type="TIMESTAMP(3)")


@pytest.fixture(scope="session")
def expected_user_table_column_info(expected_user_table_column_last_visit_ts):
    return {
        "user_id": {"type": "DECIMAL", "precision": 18, "scale": 0},
        "user_name": {"type": "VARCHAR", "size": 255, "characterSet": "UTF8"},
        "register_dt": {"type": "DATE", "size": 4},
        "last_visit_ts": {
            "type": "TIMESTAMP",
            "withLocalTimeZone": False,
            "size": expected_user_table_column_last_visit_ts.size,
        },
        "is_female": {"type": "BOOLEAN"},
        "user_rating": {"type": "DECIMAL", "precision": 10, "scale": 5},
        "user_score": {"type": "DOUBLE"},
        "status": {"type": "VARCHAR", "size": 50, "characterSet": "UTF8"},
    }
