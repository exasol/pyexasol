import pytest
import pyexasol


@pytest.mark.extensions
class TestExaExtension:

    @pytest.fixture(scope="class")
    def connection(self, dsn, user, password, schema):
        con = pyexasol.connect(
            dsn=dsn,
            user=user,
            password=password,
            schema=schema,
            lower_ident=True
        )
        yield con
        con.close()

    def test_get_columns(self, connection):
        expected = {
            'user_id': {'type': 'DECIMAL', 'precision': 18, 'scale': 0},
            'user_name': {'type': 'VARCHAR', 'size': 255, 'characterSet': 'UTF8'},
            'register_dt': {'type': 'DATE', 'size': 4},
            'last_visit_ts': {'type': 'TIMESTAMP', 'withLocalTimeZone': False, 'size': 8},
            'is_female': {'type': 'BOOLEAN'},
            'user_rating': {'type': 'DECIMAL', 'precision': 10, 'scale': 5},
            'user_score': {'type': 'DOUBLE'},
            'status': {'type': 'VARCHAR', 'size': 50, 'characterSet': 'UTF8'}
        }
        actual = connection.ext.get_columns('users')
        assert expected == actual

    def test_columns_sql(self, connection):
        expected = {
            'user_id': {'type': 'DECIMAL', 'precision': 18, 'scale': 0},
            'user_name': {'type': 'VARCHAR', 'size': 255, 'characterSet': 'UTF8'},
            'register_dt': {'type': 'DATE', 'size': 4},
            'last_visit_ts': {'type': 'TIMESTAMP', 'withLocalTimeZone': False, 'size': 8},
            'is_female': {'type': 'BOOLEAN'},
            'user_rating': {'type': 'DECIMAL', 'precision': 10, 'scale': 5},
            'user_score': {'type': 'DOUBLE'},
            'status': {'type': 'VARCHAR', 'size': 50, 'characterSet': 'UTF8'}
        }
        actual = connection.ext.get_columns_sql('SELECT * FROM users')
        assert expected == actual

    def test_get_sys_columns(self, connection):
        expected = [
            {'name': 'USER_ID', 'type': 'DECIMAL', 'sql_type': 'DECIMAL(18,0)', 'size': 18, 'scale': 0, 'nulls': True,
             'distribution_key': False, 'default': None, 'comment': None},
            {'name': 'USER_NAME', 'type': 'VARCHAR', 'sql_type': 'VARCHAR(255) UTF8', 'size': 255, 'scale': None,
             'nulls': True, 'distribution_key': False, 'default': None, 'comment': None},
            {'name': 'REGISTER_DT', 'type': 'DATE', 'sql_type': 'DATE', 'size': 10, 'scale': None, 'nulls': True,
             'distribution_key': False, 'default': None, 'comment': None},
            {'name': 'LAST_VISIT_TS', 'type': 'TIMESTAMP', 'sql_type': 'TIMESTAMP', 'size': 29, 'scale': None,
             'nulls': True, 'distribution_key': False, 'default': None, 'comment': None},
            {'name': 'IS_FEMALE', 'type': 'BOOLEAN', 'sql_type': 'BOOLEAN', 'size': 1, 'scale': None, 'nulls': True,
             'distribution_key': False, 'default': None, 'comment': None},
            {'name': 'USER_RATING', 'type': 'DECIMAL', 'sql_type': 'DECIMAL(10,5)', 'size': 10, 'scale': 5,
             'nulls': True,
             'distribution_key': False, 'default': None, 'comment': None},
            {'name': 'USER_SCORE', 'type': 'DOUBLE PRECISION', 'sql_type': 'DOUBLE', 'size': 64, 'scale': None,
             'nulls': True, 'distribution_key': False, 'default': None, 'comment': None},
            {'name': 'STATUS', 'type': 'VARCHAR', 'sql_type': 'VARCHAR(50) UTF8', 'size': 50, 'scale': None,
             'nulls': True,
             'distribution_key': False, 'default': None, 'comment': None}
        ]
        actual = connection.ext.get_sys_columns('users')
        assert expected == actual

    def test_get_sys_tables(self, connection):
        expected = [
            {'table_name': 'payments', 'table_schema': 'pyexasol_test', 'table_is_virtual': False,
             'table_has_distribution_key': False, 'table_comment': None},
            {'table_name': 'users', 'table_schema': 'pyexasol_test', 'table_is_virtual': False,
             'table_has_distribution_key': False, 'table_comment': None}
        ]
        actual = connection.ext.get_sys_tables()
        assert expected == actual

    def test_get_sys_views(self, connection):
        expected = []
        actual = connection.ext.get_sys_views()
        assert expected == actual

    def test_get_sys_schemas(self, connection):
        expected = [
            {'schema_comment': None, 'schema_is_virtual': False, 'schema_name': 'pyexasol_test', 'schema_owner': 'sys'}
        ]
        actual = connection.ext.get_sys_schemas()
        assert expected == actual

    def test_get_disk_space(self, connection):
        expected = {
            'free_size',
            'measure_time',
            'occupied_size',
            'occupied_size_percent',
            'total_size'
        }
        actual = set(connection.ext.get_disk_space_usage().keys())
        assert expected == actual

    def test_export_to_pandas_with_dtype(self, connection):
        result = connection.ext.export_to_pandas_with_dtype("users")

        expected = 10_000
        actual = len(result)
        assert actual == expected

        expected = {
            'user_id': 'int64',
            'user_name': 'category',
            'register_dt': 'datetime64[ns]',
            'last_visit_ts': 'datetime64[ns]',
            'is_female': 'category',
            'user_rating': 'float64',
            'user_score': 'float64',
            'status': 'category'
        }
        actual = {column: result[column].dtype.name for column in expected}
        assert actual == expected


    # NOTE: in the future, this test likely needs to be parameterized based on the db version
    #       due to the fact that the keywords will differ across Exasol versions.
    def test_get_reserved_words(self, connection):
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
        actual = set(connection.ext.get_reserved_words())
        assert expected == actual
