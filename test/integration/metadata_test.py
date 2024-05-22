import uuid
import pytest


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


@pytest.mark.metadata
def test_get_columns_without_executing_query(connection):
    statement = 'SELECT a.*, a.user_id + 1 AS next_user_id FROM users a'
    expected = {
        'USER_ID': {'type': 'DECIMAL', 'precision': 18, 'scale': 0},
        'USER_NAME': {'type': 'VARCHAR', 'size': 255, 'characterSet': 'UTF8'},
        'REGISTER_DT': {'type': 'DATE', 'size': 4},
        'LAST_VISIT_TS': {'type': 'TIMESTAMP', 'withLocalTimeZone': False, 'size': 8},
        'IS_FEMALE': {'type': 'BOOLEAN'},
        'USER_RATING': {'type': 'DECIMAL', 'precision': 10, 'scale': 5},
        'USER_SCORE': {'type': 'DOUBLE'},
        'STATUS': {'type': 'VARCHAR', 'size': 50, 'characterSet': 'UTF8'},
        'NEXT_USER_ID': {'type': 'DECIMAL', 'precision': 19, 'scale': 0}
    }
    actual = connection.meta.sql_columns(statement)
    assert actual == expected


@pytest.mark.metadata
def test_schema_exists_returns_true_if_schema_exists(connection, schema):
    assert connection.meta.schema_exists(schema)


@pytest.mark.metadata
def test_schema_exists_returns_false_if_schema_doesnt_exist(connection):
    schema = "THIS_SCHEMA_SHOULD_NOT_EXIST_____"
    assert not connection.meta.schema_exists(schema)


@pytest.mark.metadata
def test_table_exits_returns_true_for_existing_table(connection):
    table = "users"
    assert connection.meta.table_exists(table)


@pytest.mark.metadata
def test_table_exits_returns_false_for_non_existing_table(connection):
    table = "this_talbe_should_not_exist_____"
    assert not connection.meta.table_exists(table)


@pytest.mark.metadata
def test_view_exits_returns_true_for_existing_view(connection, view):
    assert connection.meta.view_exists(view)


@pytest.mark.metadata
def test_view_exits_returns_false_for_non_existing_view(connection):
    view = "this_view_should_not_exist_____"
    assert not connection.meta.view_exists(view)


@pytest.mark.metadata
def test_list_schemas(connection):
    expected = ["PYEXASOL_TEST"]
    actual = [
        schema['SCHEMA_NAME']
        for schema in connection.meta.list_schemas()
    ]
    assert actual == expected


@pytest.mark.metadata
def test_list_schemas_with_filter(connection):
    expected = []
    actual = [
        schema['SCHEMA_NAME']
        for schema in connection.meta.list_schemas(
            schema_name_pattern="FOOBAR%"
        )
    ]
    assert actual == expected


@pytest.mark.metadata
def test_list_tables(connection):
    expected = {"USERS", "PAYMENTS"}
    actual = {
        table['TABLE_NAME']
        for table in connection.meta.list_tables(
            table_schema_pattern='PYEXASOL%'
        )
    }
    assert actual == expected


@pytest.mark.metadata
def test_list_views(connection, view):
    expected = {view}
    actual = {
        view['VIEW_NAME']
        for view in connection.meta.list_views(
            view_schema_pattern='PYEXASOL%'
        )
    }
    assert actual == expected


@pytest.mark.metadata
def test_list_columns(connection):
    expected = {"USER_NAME", "USER_ID", "USER_SCORE", "USER_RATING"}
    actual = {
        columns['COLUMN_NAME']
        for columns in connection.meta.list_columns(
            column_schema_pattern="PYEXASOL%",
            column_table_pattern="USERS%",
            column_name_pattern="%USER%"
        )
    }
    assert actual == expected


@pytest.mark.metadata
def test_list_objects(connection):
    expected = {"USERS"}
    actual = {
        db_object["OBJECT_NAME"]
        for db_object in connection.meta.list_objects(
            object_name_pattern="%USER%",
        )
    }
    assert actual == expected


@pytest.mark.metadata
def test_list_object_sizes(connection):
    expected = [709780]
    actual = [
        db_object["MEM_OBJECT_SIZE"]
        for db_object in connection.meta.list_object_sizes(
            object_name_pattern="USERS%",
            object_type_pattern="TABLE"
        )
    ]
    assert actual == expected


@pytest.mark.metadata
def test_list_indices(connection):
    expected = []
    actual = connection.meta.list_indices(
        index_schema_pattern="PYEXASOL%"
    )
    assert actual == expected


@pytest.mark.metadata
def test_list_keywords(connection):
    expected =  {'ABSOLUTE', 'ACTION', 'ADD', 'AFTER', 'ALL', 'ALLOCATE', 'ALTER', 'AND', 'ANY', 'APPEND', 'ARE', 'ARRAY',
         'AS', 'ASC', 'ASENSITIVE', 'ASSERTION', 'AT', 'ATTRIBUTE', 'AUTHID', 'AUTHORIZATION', 'BEFORE', 'BEGIN',
         'BETWEEN', 'BIGINT', 'BINARY', 'BIT', 'BLOB', 'BLOCKED', 'BOOL', 'BOOLEAN', 'BOTH', 'BY', 'BYTE', 'CALL',
         'CALLED', 'CARDINALITY', 'CASCADE', 'CASCADED', 'CASE', 'CASESPECIFIC', 'CAST', 'CATALOG', 'CHAIN', 'CHAR',
         'CHARACTER', 'CHARACTERISTICS', 'CHARACTER_SET_CATALOG', 'CHARACTER_SET_NAME', 'CHARACTER_SET_SCHEMA',
         'CHECK', 'CHECKED', 'CLOB', 'CLOSE', 'COALESCE', 'COLLATE', 'COLLATION', 'COLLATION_CATALOG',
         'COLLATION_NAME', 'COLLATION_SCHEMA', 'COLUMN', 'COMMIT', 'CONDITION', 'CONNECTION', 'CONNECT_BY_ISCYCLE',
         'CONNECT_BY_ISLEAF', 'CONNECT_BY_ROOT', 'CONSTANT', 'CONSTRAINT', 'CONSTRAINTS',
         'CONSTRAINT_STATE_DEFAULT', 'CONSTRUCTOR', 'CONTAINS', 'CONTINUE', 'CONTROL', 'CONVERT', 'CORRESPONDING',
         'CREATE', 'CS', 'CSV', 'CUBE', 'CURRENT', 'CURRENT_DATE', 'CURRENT_PATH', 'CURRENT_ROLE', 'CURRENT_SCHEMA',
         'CURRENT_SESSION', 'CURRENT_STATEMENT', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'CURSOR',
         'CYCLE', 'DATA', 'DATALINK', 'DATE', 'DATETIME_INTERVAL_CODE', 'DATETIME_INTERVAL_PRECISION', 'DAY',
         'DBTIMEZONE', 'DEALLOCATE', 'DEC', 'DECIMAL', 'DECLARE', 'DEFAULT', 'DEFAULT_LIKE_ESCAPE_CHARACTER',
         'DEFERRABLE', 'DEFERRED', 'DEFINED', 'DEFINER', 'DELETE', 'DEREF', 'DERIVED', 'DESC', 'DESCRIBE',
         'DESCRIPTOR', 'DETERMINISTIC', 'DISABLE', 'DISABLED', 'DISCONNECT', 'DISPATCH', 'DISTINCT',
         'DLURLCOMPLETE', 'DLURLPATH', 'DLURLPATHONLY', 'DLURLSCHEME', 'DLURLSERVER', 'DLVALUE', 'DO', 'DOMAIN',
         'DOUBLE', 'DROP', 'DYNAMIC', 'DYNAMIC_FUNCTION', 'DYNAMIC_FUNCTION_CODE', 'EACH', 'ELSE', 'ELSEIF',
         'ELSIF', 'EMITS', 'ENABLE', 'ENABLED', 'END', 'END-EXEC', 'ENDIF', 'ENFORCE', 'EQUALS', 'ERRORS', 'ESCAPE',
         'EXCEPT', 'EXCEPTION', 'EXEC', 'EXECUTE', 'EXISTS', 'EXIT', 'EXPORT', 'EXTERNAL', 'EXTRACT', 'FALSE',
         'FBV', 'FETCH', 'FILE', 'FINAL', 'FIRST', 'FLOAT', 'FOLLOWING', 'FOR', 'FORALL', 'FORCE', 'FORMAT',
         'FOUND', 'FREE', 'FROM', 'FS', 'FULL', 'FUNCTION', 'GENERAL', 'GENERATED', 'GEOMETRY', 'GET', 'GLOBAL',
         'GO', 'GOTO', 'GRANT', 'GRANTED', 'GROUP', 'GROUPING', 'GROUPS', 'GROUP_CONCAT', 'HASHTYPE',
         'HASHTYPE_FORMAT', 'HAVING', 'HIGH', 'HOLD', 'HOUR', 'IDENTITY', 'IF', 'IFNULL', 'IMMEDIATE',
         'IMPERSONATE', 'IMPLEMENTATION', 'IMPORT', 'IN', 'INDEX', 'INDICATOR', 'INNER', 'INOUT', 'INPUT',
         'INSENSITIVE', 'INSERT', 'INSTANCE', 'INSTANTIABLE', 'INT', 'INTEGER', 'INTEGRITY', 'INTERSECT',
         'INTERVAL', 'INTO', 'INVERSE', 'INVOKER', 'IS', 'ITERATE', 'JOIN', 'KEY_MEMBER', 'KEY_TYPE', 'LARGE',
         'LAST', 'LATERAL', 'LDAP', 'LEADING', 'LEAVE', 'LEFT', 'LEVEL', 'LIKE', 'LIMIT', 'LISTAGG', 'LOCAL',
         'LOCALTIME', 'LOCALTIMESTAMP', 'LOCATOR', 'LOG', 'LONGVARCHAR', 'LOOP', 'LOW', 'MAP', 'MATCH', 'MATCHED',
         'MERGE', 'METHOD', 'MINUS', 'MINUTE', 'MOD', 'MODIFIES', 'MODIFY', 'MODULE', 'MONTH', 'NAMES', 'NATIONAL',
         'NATURAL', 'NCHAR', 'NCLOB', 'NEW', 'NEXT', 'NLS_DATE_FORMAT', 'NLS_DATE_LANGUAGE',
         'NLS_FIRST_DAY_OF_WEEK', 'NLS_NUMERIC_CHARACTERS', 'NLS_TIMESTAMP_FORMAT', 'NO', 'NOCYCLE', 'NOLOGGING',
         'NONE', 'NOT', 'NULL', 'NULLIF', 'NUMBER', 'NUMERIC', 'NVARCHAR', 'NVARCHAR2', 'OBJECT', 'OF', 'OFF',
         'OLD', 'ON', 'ONLY', 'OPEN', 'OPTION', 'OPTIONS', 'OR', 'ORDER', 'ORDERING', 'ORDINALITY', 'OTHERS', 'OUT',
         'OUTER', 'OUTPUT', 'OVER', 'OVERLAPS', 'OVERLAY', 'OVERRIDING', 'PAD', 'PARALLEL_ENABLE', 'PARAMETER',
         'PARAMETER_SPECIFIC_CATALOG', 'PARAMETER_SPECIFIC_NAME', 'PARAMETER_SPECIFIC_SCHEMA', 'PARTIAL', 'PATH',
         'PERMISSION', 'PLACING', 'PLUS', 'POSITION', 'PRECEDING', 'PREFERRING', 'PREPARE', 'PRESERVE', 'PRIOR',
         'PRIVILEGES', 'PROCEDURE', 'PROFILE', 'QUALIFY', 'RANDOM', 'RANGE', 'READ', 'READS', 'REAL', 'RECOVERY',
         'RECURSIVE', 'REF', 'REFERENCES', 'REFERENCING', 'REFRESH', 'REGEXP_LIKE', 'RELATIVE', 'RELEASE', 'RENAME',
         'REPEAT', 'REPLACE', 'RESTORE', 'RESTRICT', 'RESULT', 'RETURN', 'RETURNED_LENGTH', 'RETURNED_OCTET_LENGTH',
         'RETURNS', 'REVOKE', 'RIGHT', 'ROLLBACK', 'ROLLUP', 'ROUTINE', 'ROW', 'ROWS', 'ROWTYPE', 'SAVEPOINT',
         'SCHEMA', 'SCOPE', 'SCOPE_USER', 'SCRIPT', 'SCROLL', 'SEARCH', 'SECOND', 'SECTION', 'SECURITY', 'SELECT',
         'SELECTIVE', 'SELF', 'SENSITIVE', 'SEPARATOR', 'SEQUENCE', 'SESSION', 'SESSIONTIMEZONE', 'SESSION_USER',
         'SET', 'SETS', 'SHORTINT', 'SIMILAR', 'SMALLINT', 'SOME', 'SOURCE', 'SPACE', 'SPECIFIC', 'SPECIFICTYPE',
         'SQL', 'SQLEXCEPTION', 'SQLSTATE', 'SQLWARNING', 'SQL_BIGINT', 'SQL_BIT', 'SQL_CHAR', 'SQL_DATE',
         'SQL_DECIMAL', 'SQL_DOUBLE', 'SQL_FLOAT', 'SQL_INTEGER', 'SQL_LONGVARCHAR', 'SQL_NUMERIC',
         'SQL_PREPROCESSOR_SCRIPT', 'SQL_REAL', 'SQL_SMALLINT', 'SQL_TIMESTAMP', 'SQL_TINYINT', 'SQL_TYPE_DATE',
         'SQL_TYPE_TIMESTAMP', 'SQL_VARCHAR', 'START', 'STATE', 'STATEMENT', 'STATIC', 'STRUCTURE', 'STYLE',
         'SUBSTRING', 'SUBTYPE', 'SYSDATE', 'SYSTEM', 'SYSTEM_USER', 'SYSTIMESTAMP', 'TABLE', 'TEMPORARY', 'TEXT',
         'THEN', 'TIME', 'TIMESTAMP', 'TIMEZONE_HOUR', 'TIMEZONE_MINUTE', 'TINYINT', 'TO', 'TRAILING',
         'TRANSACTION', 'TRANSFORM', 'TRANSFORMS', 'TRANSLATION', 'TREAT', 'TRIGGER', 'TRIM', 'TRUE', 'TRUNCATE',
         'UNDER', 'UNION', 'UNIQUE', 'UNKNOWN', 'UNLINK', 'UNNEST', 'UNTIL', 'UPDATE', 'USAGE', 'USER', 'USING',
         'VALUE', 'VALUES', 'VARCHAR', 'VARCHAR2', 'VARRAY', 'VERIFY', 'VIEW', 'WHEN', 'WHENEVER', 'WHERE', 'WHILE',
         'WINDOW', 'WITH', 'WITHIN', 'WITHOUT', 'WORK', 'YEAR', 'YES', 'ZONE'
    }
    actual = set(connection.meta.list_sql_keywords())
    assert actual == expected
