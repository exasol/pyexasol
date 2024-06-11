import pytest
import pyexasol


@pytest.mark.metadata
def test_wss_protocol_version_supports_nosql_metadata(connection):
    assert connection.protocol_version() >= pyexasol.PROTOCOL_V2


@pytest.mark.metadata
def test_schema_exists(connection, schema):
    assert connection.meta.schema_exists(schema)


@pytest.mark.metadata
def test_schema_does_not_exist(connection):
    schema = "ThisSchemaShouldNotExist"
    assert not connection.meta.schema_exists(schema)


@pytest.mark.metadata
def test_table_exists(connection):
    assert connection.meta.table_exists("users")


@pytest.mark.metadata
def test_table_does_not_exist(connection):
    table = "ThisTableShouldNotExist"
    assert not connection.meta.view_exists(table)


@pytest.mark.metadata
def test_view_exists(connection, view):
    assert connection.meta.view_exists(view)


@pytest.mark.metadata
def test_view_does_not_exist(connection):
    view = "ThisViewShouldNotExist"
    assert not connection.meta.view_exists(view)


@pytest.mark.metadata
def test_list_schemas(connection):
    query = connection.meta.execute_meta_nosql("getSchemas")
    schemas = query.fetchall()
    expected = {"EXA_STATISTICS", "PYEXASOL_TEST", "SYS"}
    actual = {schema["NAME"] for schema in schemas}
    assert actual == expected


@pytest.mark.metadata
def test_list_schemas_with_fitler(connection):
    schemas = connection.meta.execute_meta_nosql(
        "getSchemas", {"schema": "PYEXASOL%"}
    ).fetchall()
    expected = {"PYEXASOL_TEST"}
    actual = {schema["NAME"] for schema in schemas}
    assert actual == expected


@pytest.mark.metadata
def test_list_tables_with_filters(connection):
    schemas = connection.meta.execute_meta_nosql(
        "getTables", {"schema": "PYEXASOL_TEST"}
    ).fetchall()
    expected = {"USERS", "PAYMENTS"}
    actual = {schema["NAME"] for schema in schemas}
    assert actual == expected


@pytest.mark.metadata
def test_list_columns_with_filters(connection):
    query = connection.meta.execute_meta_nosql(
        "getColumns",
        {
            "schema": "PYEXASOL_TEST",
            "table": "USERS",
        },
    )
    expected = {
        ("USER_ID", "DECIMAL(18,0)"),
        ("USER_NAME", "VARCHAR(255) UTF8"),
        ("REGISTER_DT", "DATE"),
        ("LAST_VISIT_TS", "TIMESTAMP"),
        ("IS_FEMALE", "BOOLEAN"),
        ("USER_RATING", "DECIMAL(10,5)"),
        ("USER_SCORE", "DOUBLE"),
        ("STATUS", "VARCHAR(50) UTF8"),
    }
    actual = {(c["NAME"], c["TYPE"]) for c in query.fetchall()}
    assert actual == expected


@pytest.mark.metadata
def test_list_keywords(connection):
    # fmt: off
    expected = {
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
    actual = set(connection.meta.list_sql_keywords())
    assert actual == expected
