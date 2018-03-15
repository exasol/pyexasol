"""
Extension with Exasol-specific helper functions
"""


class ExaExtension(object):
    def __init__(self, connection):
        self.connection = connection
        self.reserved_words = None

    def get_columns(self, object_name):
        """
        Get information about columns of table or view (Websocket format)
        Object name may be passed as tuple to specify custom schema
        """
        return self.get_columns_sql("SELECT * FROM {object_name!i}", {'object_name': object_name})

    def get_columns_sql(self, query, query_params=None):
        """
        Get columns of SQL query without executing it (Websocket format)
        Relies on prepared statement which are closed immediately without execution
        """
        st = self.connection._statement(query, query_params)
        st._prepare()

        columns = st.columns()
        st.close()

        return columns

    def get_sys_columns(self, object_name):
        """
        Get information about columns of table or view (SYS format)
        Object name may be passed as tuple to specify custom schema
        """
        if isinstance(object_name, tuple):
            schema = str(object_name[0]).upper()
            object_name = str(object_name[1]).upper()
        else:
            schema = self.connection.current_schema()
            object_name = str(object_name).upper()

        sql = """
            SELECT c.column_name, c.column_type, c.column_maxsize, c.column_num_scale,
                   c.column_is_nullable, c.column_is_distribution_key, c.column_default,
                   c.column_comment, t.type_name
            FROM EXA_ALL_COLUMNS c
                JOIN EXA_SQL_TYPES t ON (c.column_type_id=t.type_id)
            WHERE c.column_schema={schema}
                AND c.column_table={object_name}
            ORDER BY c.column_ordinal_position
        """

        st = self._execute(sql, {'schema': schema, 'object_name': object_name})
        res = list()

        for r in st:
            res.append({
                'name': r['column_name'],
                'type': r['type_name'],
                'sql_type': r['column_type'],
                'size': r['column_maxsize'],
                'scale': r['column_num_scale'],
                'nulls': r['column_is_nullable'],
                'distribution_key': r['column_is_distribution_key'],
                'default': r['column_default'],
                'comment': r['column_comment'],
            })

        return res

    def get_sys_tables(self, schema=None, table_name_prefix=''):
        """
        Get information about tables in selected schema(SYS format)
        Output may be optionally filtered by table name prefix
        """
        if schema is None:
            schema = self.connection.current_schema()
        else:
            schema = str(schema).upper()

        table_name_prefix = self.connection.format.escape_like(table_name_prefix).upper()

        sql = """
            SELECT *
            FROM EXA_ALL_TABLES
            WHERE table_schema={schema}
                AND table_name LIKE '{table_name_prefix!r}%'
            ORDER BY table_name ASC
        """

        st = self._execute(sql, {'schema': schema, 'table_name_prefix': table_name_prefix})
        res = list()

        for r in st:
            res.append({
                'table_name': r['table_name'].lower() if self.connection.lower_ident else r['table_name'],
                'table_schema': r['table_schema'].lower() if self.connection.lower_ident else r['table_schema'],
                'table_is_virtual': r['table_is_virtual'],
                'table_has_distribution_key': r['table_has_distribution_key'],
                'table_comment': r['table_comment'],
            })

        return res

    def get_sys_views(self, schema=None, view_name_prefix=''):
        """
        Get information about views in selected schema(SYS format)
        Output may be optionally filtered by view name prefix
        """
        if schema is None:
            schema = self.connection.current_schema()
        else:
            schema = str(schema).upper()

        view_name_prefix = self.connection.format.escape_like(view_name_prefix).upper()

        sql = """
            SELECT *
            FROM EXA_ALL_VIEWS
            WHERE view_schema={schema}
                AND view_name LIKE '{view_name_prefix!r}%'
            ORDER BY view_name ASC
        """

        st = self._execute(sql, {'schema': schema, 'view_name_prefix': view_name_prefix})
        res = list()

        for r in st:
            res.append({
                'view_name': r['view_name'].lower() if self.connection.lower_ident else r['view_name'],
                'view_schema': r['view_schema'].lower() if self.connection.lower_ident else r['view_schema'],
                'scope_schema': r['scope_schema'].lower() if self.connection.lower_ident else r['scope_schema'],
                'view_text': r['view_text'],
                'view_comment': r['view_comment'],
            })

        return res

    def get_sys_schemas(self, schema_name_prefix=''):
        """
        Get information about schemas (SYS format)
        Output may be optionally filtered by schema name prefix
        """
        schema_name_prefix = self.connection.format.escape_like(schema_name_prefix).upper()

        sql = """
            SELECT *
            FROM EXA_SCHEMAS
            WHERE schema_name LIKE '{schema_name_prefix!r}%'
            ORDER BY schema_name ASC
        """

        st = self._execute(sql, {'schema_name_prefix': schema_name_prefix})
        res = list()

        for r in st:
            res.append({
                'schema_name': r['schema_name'].lower() if self.connection.lower_ident else r['schema_name'],
                'schema_owner': r['schema_owner'].lower() if self.connection.lower_ident else r['schema_owner'],
                'schema_is_virtual': r['schema_is_virtual'],
                'schema_comment': r['schema_comment'],
            })

        return res

    def get_reserved_words(self):
        """
        Get reserved keywords which cannot be used as identifiers without double-quotes
        Never hard-code this list! It changes with every Exasol versions
        """
        if self.reserved_words is None:
            sql = """
                SELECT keyword
                FROM EXA_SQL_KEYWORDS
                WHERE reserved IS TRUE
                ORDER BY keyword
            """

            self.reserved_words = self._execute(sql).fetchcol()

        return self.reserved_words

    def _execute(self, query, query_params=None):
        st = self.connection._statement(query, query_params)

        st.fetch_dict = True
        st.fetch_mapper = None
        st.lower_ident = True

        st._execute()

        return st
