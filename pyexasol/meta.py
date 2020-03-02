

class ExaMetaData(object):
    """
    This class implements lock-free meta data requests using `/*snapshot execution*/` SQL hint described in IDEA-476
    https://www.exasol.com/support/browse/IDEA-476

    If you still get locks, please make sure to update Exasol server to the latest minor version
    """
    def __init__(self, connection):
        self.connection = connection
        self.sql_keywords = None

    def sql_columns(self, query, query_params=None):
        """
        Get result set columns of SQL query without executing it
        """
        st = self.connection.cls_statement(self.connection, query, query_params, prepare=True)
        columns = st.columns()
        st.close()

        return columns

    def schema_exists(self, schema_name):
        object_name = self.connection.format.default_format_ident_value(schema_name)

        st = self._execute_snapshot("""
            SELECT 1
            FROM sys.exa_schemas
            WHERE schema_name={object_name}
        """, {
            'object_name': object_name,
        })

        return st.rowcount() > 0

    def table_exists(self, table_name):
        if isinstance(table_name, tuple):
            object_schema = self.connection.format.default_format_ident_value(table_name[0])
            object_name = self.connection.format.default_format_ident_value(table_name[1])
        else:
            object_schema = self.connection.current_schema()
            object_name = self.connection.format.default_format_ident_value(table_name)

        st = self._execute_snapshot("""
            SELECT 1
            FROM sys.exa_all_tables
            WHERE table_schema={object_schema}
                AND table_name={object_name}
        """, {
            'object_schema': object_schema,
            'object_name': object_name,
        })

        return st.rowcount() > 0

    def view_exists(self, view_name):
        if isinstance(view_name, tuple):
            object_schema = self.connection.format.default_format_ident_value(view_name[0])
            object_name = self.connection.format.default_format_ident_value(view_name[1])
        else:
            object_schema = self.connection.current_schema()
            object_name = self.connection.format.default_format_ident_value(view_name)

        st = self._execute_snapshot("""
            SELECT 1
            FROM sys.exa_all_views
            WHERE view_schema={object_schema}
                AND view_name={object_name}
        """, {
            'object_schema': object_schema,
            'object_name': object_name,
        })

        return st.rowcount() > 0

    def list_schemas(self, schema_name_pattern='%'):
        st = self._execute_snapshot("""
            SELECT *
            FROM sys.exa_schemas
            WHERE schema_name LIKE {schema_name_pattern}
            ORDER BY schema_name ASC
        """, {
            'schema_name_pattern': schema_name_pattern,
        })

        return st.fetchall()

    def list_tables(self, table_schema_pattern='%', table_name_pattern='%'):
        st = self._execute_snapshot("""
            SELECT *
            FROM sys.exa_all_tables
            WHERE table_schema LIKE {table_schema_pattern}
                AND table_name LIKE {table_name_pattern}
            ORDER BY table_schema ASC, table_name ASC
        """, {
            'table_schema_pattern': table_schema_pattern,
            'table_name_pattern': table_name_pattern,
        })

        return st.fetchall()

    def list_views(self, view_schema_pattern='%', view_name_pattern='%'):
        st = self._execute_snapshot("""
            SELECT *
            FROM sys.exa_all_views
            WHERE view_schema LIKE {view_schema_pattern}
                AND view_name LIKE {view_name_pattern}
            ORDER BY view_schema ASC, view_name ASC
        """, {
            'view_schema_pattern': view_schema_pattern,
            'view_name_pattern': view_name_pattern,
        })

        return st.fetchall()

    def list_columns(self, column_schema_pattern='%', column_table_pattern='%'
                     , column_object_type_pattern='%', column_name_pattern='%'):
        st = self._execute_snapshot("""
            SELECT *
            FROM sys.exa_all_columns
            WHERE column_schema LIKE {column_schema_pattern}
                AND column_table LIKE {column_table_pattern}
                AND column_object_type LIKE {column_object_type_pattern}
                AND column_name LIKE {column_name_pattern}
        """, {
            'column_schema_pattern': column_schema_pattern,
            'column_table_pattern': column_table_pattern,
            'column_object_type_pattern': column_object_type_pattern,
            'column_name_pattern': column_name_pattern,
        })

        return st.fetchall()

    def list_objects(self, object_name_pattern='%', object_type_pattern='%', owner_pattern='%', root_name_pattern='%'):
        st = self._execute_snapshot("""
            SELECT *
            FROM sys.exa_all_objects
            WHERE object_name LIKE {object_name_pattern}
                AND object_type LIKE {object_type_pattern}
                AND owner LIKE {owner_pattern}
                AND root_name LIKE {root_name_pattern}
        """, {
            'object_name_pattern': object_name_pattern,
            'object_type_pattern': object_type_pattern,
            'owner_pattern': owner_pattern,
            'root_name_pattern': root_name_pattern,
        })

        return st.fetchall()

    def list_object_sizes(self, object_name_pattern='%', object_type_pattern='%', owner_pattern='%', root_name_pattern='%'):
        st = self._execute_snapshot("""
            SELECT *
            FROM sys.exa_all_object_sizes
            WHERE object_name LIKE {object_name_pattern}
                AND object_type LIKE {object_type_pattern}
                AND owner LIKE {owner_pattern}
                AND root_name LIKE {root_name_pattern}
        """, {
            'object_name_pattern': object_name_pattern,
            'object_type_pattern': object_type_pattern,
            'owner_pattern': owner_pattern,
            'root_name_pattern': root_name_pattern,
        })

        return st.fetchall()

    def list_indices(self, index_schema_pattern='%', index_table_pattern='%', index_owner_pattern='%'):
        st = self._execute_snapshot("""
            SELECT *
            FROM sys.exa_all_indices
            WHERE index_schema LIKE {index_schema_pattern}
                AND index_table LIKE {index_table_pattern}
                AND index_owner LIKE {index_owner_pattern}
        """, {
            'index_schema_pattern': index_schema_pattern,
            'index_table_pattern': index_table_pattern,
            'index_owner_pattern': index_owner_pattern,
        })

        return st.fetchall()

    def list_sql_keywords(self):
        """
        Get reserved SQL keywords which cannot be used as identifiers without double-quote escaping
        Never hardcode this list! It might change with next Exasol server version without warning
        """
        if not self.sql_keywords:
            st = self._execute_snapshot("""
                SELECT keyword
                FROM EXA_SQL_KEYWORDS
                WHERE reserved IS TRUE
                ORDER BY keyword
            """)

            self.sql_keywords = st.fetchcol()

        return self.sql_keywords

    def _execute_snapshot(self, query, query_params=None):
        """
        Execute query in snapshot transaction mode using SQL hint
        fetch_dict=True is enforced to prevent users from relying on order of columns in system views
        """
        options = {
            'fetch_dict': True,
        }

        return self.connection.cls_statement(self.connection, f"/*snapshot execution*/{query}", query_params, **options)

    def __repr__(self):
        return f'<{self.__class__.__name__} session_id={self.connection.session_id()}>'
