from . import constant
from .exceptions import ExaRuntimeError


class ExaMetaData:
    """
    This class implements lock-free metadata requests using ``/*snapshot execution*/``
    SQL hint, as described further in https://docs.exasol.com/db/latest/database_concepts/snapshot_mode.htm.

    Note:
        If you still get locks, please make sure to update Exasol server to the latest minor version.

    Examples:

        You may access these functions using `.meta` property of connection object.

        >>> C = pyexasol.connect(...)
        ... print(C.meta.sql_columns('SELECT 1 AS id'))
    """

    snapshot_execution_hint = "/*snapshot execution*/"

    def __init__(self, connection):
        self.connection = connection
        self.sql_keywords = None

    def sql_columns(self, query, query_params=None):
        """
        Get result set columns of SQL query without executing it

        Args:
            query:
                SQL query text, possibly with placeholders.
            query_params:
                Values for placeholders.

        Returns:
            Columns of SQL query result without executing it.
            Output format is similar to :meth:`pyexasol.ExaStatement.columns`.
        """
        st = self.connection.cls_statement(
            self.connection, query, query_params, prepare=True
        )
        columns = st.columns()
        st.close()

        return columns

    def schema_exists(self, schema_name):
        """
        Check if schema exists.

        Args:
            schema_name:
                Name of the schema to check.

        Returns:
            ``True`` if the schema exists, otherwise ``False``.
        """
        object_name = self.connection.format.default_format_ident_value(schema_name)

        if self.connection.protocol_version() >= constant.PROTOCOL_V2:
            st = self.execute_meta_nosql(
                "getSchemas",
                {
                    "schema": object_name,
                },
            )
        else:
            st = self.execute_snapshot(
                """
                SELECT 1
                FROM sys.exa_schemas
                WHERE schema_name={object_name}
            """,
                {
                    "object_name": object_name,
                },
            )

        return st.rowcount() > 0

    def table_exists(self, table_name):
        """
        Check if table exists.

        Args:
            table_name:
                Name of the table to check for.
                If schema was not specified, ``current_schema`` is used.

        Returns:
            ``True`` if the table exists, otherwise ``False``.
        """
        if isinstance(table_name, tuple):
            object_schema = self.connection.format.default_format_ident_value(
                table_name[0]
            )
            object_name = self.connection.format.default_format_ident_value(
                table_name[1]
            )
        else:
            object_schema = self.connection.current_schema()
            object_name = self.connection.format.default_format_ident_value(table_name)

        if self.connection.protocol_version() >= constant.PROTOCOL_V2:
            st = self.execute_meta_nosql(
                "getTables",
                {
                    "schema": object_schema,
                    "table": object_name,
                    "tableTypes": ["TABLE"],
                },
            )
        else:
            st = self.execute_snapshot(
                """
                SELECT 1
                FROM sys.exa_all_tables
                WHERE table_schema={object_schema}
                    AND table_name={object_name}
            """,
                {
                    "object_schema": object_schema,
                    "object_name": object_name,
                },
            )

        return st.rowcount() > 0

    def view_exists(self, view_name):
        """
        Check if view exists.

        Args:
            view_name:
                Name of the table to check for.
                If schema was not specified, ``current_schema`` is used.

        Returns:
            ``True`` if the view exists, otherwise ``False``.
        """
        if isinstance(view_name, tuple):
            object_schema = self.connection.format.default_format_ident_value(
                view_name[0]
            )
            object_name = self.connection.format.default_format_ident_value(
                view_name[1]
            )
        else:
            object_schema = self.connection.current_schema()
            object_name = self.connection.format.default_format_ident_value(view_name)

        if self.connection.protocol_version() >= constant.PROTOCOL_V2:
            st = self.execute_meta_nosql(
                "getTables",
                {
                    "schema": object_schema,
                    "table": object_name,
                    "tableTypes": ["VIEW"],
                },
            )
        else:
            st = self.execute_snapshot(
                """
                SELECT 1
                FROM sys.exa_all_views
                WHERE view_schema={object_schema}
                    AND view_name={object_name}
            """,
                {
                    "object_schema": object_schema,
                    "object_name": object_name,
                },
            )

        return st.rowcount() > 0

    def list_schemas(self, schema_name_pattern="%"):
        """
        List Schemas.

        Args:
            schema_name_pattern:
                Schema name or LIKE-pattern to filter on.
                (default: ``'%'``)

        Returns:
            List of schemas from `EXA_SCHEMAS <https://docs.exasol.com/db/latest/sql_references/system_tables/metadata/exa_schemas.htm>`_ system view matching LIKE-pattern.

        Note:
            Patterns are case-sensitive. You may escape LIKE-patterns.
        """
        st = self.execute_snapshot(
            """
            SELECT *
            FROM sys.exa_schemas
            WHERE schema_name LIKE {schema_name_pattern}
            ORDER BY schema_name ASC
        """,
            {
                "schema_name_pattern": schema_name_pattern,
            },
        )

        return st.fetchall()

    def list_tables(self, table_schema_pattern="%", table_name_pattern="%"):
        """
        List Tables.

        Args:
            table_schema_pattern:
                Schema name or LIKE-pattern to filter on.
                (default: ``'%'``)
            table_name_pattern:
                Table name or LIKE-pattern to filter on.
                (default: ``'%'``)

        Returns:
            List of tables from `EXA_ALL_TABLES <https://docs.exasol.com/db/latest/sql_references/system_tables/metadata/exa_all_tables.htm>`_ system view matching LIKE-pattern.

        Note:
            Patterns are case-sensitive. You may escape LIKE-patterns.
        """
        st = self.execute_snapshot(
            """
            SELECT *
            FROM sys.exa_all_tables
            WHERE table_schema LIKE {table_schema_pattern}
                AND table_name LIKE {table_name_pattern}
            ORDER BY table_schema ASC, table_name ASC
        """,
            {
                "table_schema_pattern": table_schema_pattern,
                "table_name_pattern": table_name_pattern,
            },
        )

        return st.fetchall()

    def list_views(self, view_schema_pattern="%", view_name_pattern="%"):
        """
        List Views.

        Args:
            view_schema_pattern:
                Schema name or LIKE-pattern to filter on.
                (default: ``'%'``)
            view_name_pattern:
                Table name or LIKE-pattern to filter on.
                (default: ``'%'``)

        Returns:
            List of views from `EXA_ALL_VIEWS <https://docs.exasol.com/db/latest/sql_references/system_tables/metadata/exa_all_views.htm>`_ system view matching LIKE-pattern.

        Note:
            Patterns are case-sensitive. You may escape LIKE-patterns.
        """
        st = self.execute_snapshot(
            """
            SELECT *
            FROM sys.exa_all_views
            WHERE view_schema LIKE {view_schema_pattern}
                AND view_name LIKE {view_name_pattern}
            ORDER BY view_schema ASC, view_name ASC
        """,
            {
                "view_schema_pattern": view_schema_pattern,
                "view_name_pattern": view_name_pattern,
            },
        )

        return st.fetchall()

    def list_columns(
        self,
        column_schema_pattern="%",
        column_table_pattern="%",
        column_object_type_pattern="%",
        column_name_pattern="%",
    ):
        """
        List Columns.

        Args:
            column_schema_pattern:
                Schema name or LIKE-pattern to filter on.
                (default: ``'%'``)
            column_table_pattern:
                Table name or LIKE-pattern to filter on.
                (default: ``'%'``)
            column_object_type_pattern:
                Object type or LIKE-pattern to filter on.
                (default: ``'%'``)
            column_name_pattern:
                Column name or LIKE-pattern to filter on.
                (default: ``'%'``)

        Returns:
            List of columns from `EXA_ALL_COLUMNS <https://docs.exasol.com/db/latest/sql_references/system_tables/metadata/exa_all_columns.htm>`_ system view matching LIKE-pattern.

        Note:
            Patterns are case-sensitive. You may escape LIKE-patterns.
        """
        st = self.execute_snapshot(
            """
            SELECT *
            FROM sys.exa_all_columns
            WHERE column_schema LIKE {column_schema_pattern}
                AND column_table LIKE {column_table_pattern}
                AND column_object_type LIKE {column_object_type_pattern}
                AND column_name LIKE {column_name_pattern}
        """,
            {
                "column_schema_pattern": column_schema_pattern,
                "column_table_pattern": column_table_pattern,
                "column_object_type_pattern": column_object_type_pattern,
                "column_name_pattern": column_name_pattern,
            },
        )

        return st.fetchall()

    def list_objects(
        self,
        object_name_pattern="%",
        object_type_pattern="%",
        owner_pattern="%",
        root_name_pattern="%",
    ):
        """
        List Objects.

        Args:
            object_name_pattern:
                Object name or LIKE-pattern to filter on.
                (default: ``'%'``)
            object_type_pattern:
                Object type or LIKE-pattern to filter on.
                (default: ``'%'``)
            owner_pattern:
                Owner name or LIKE-pattern to filter on.
                (default: ``'%'``)
            root_name_pattern:
                Root name or LIKE-pattern to filter on.j
                It normally refers to schema name.
                (default: ``'%'``)

        Returns:
            List of objects from `EXA_ALL_OBJECTS <https://docs.exasol.com/db/latest/sql_references/system_tables/metadata/exa_all_objects.htm>`_ system view matching LIKE-pattern.
        Note:
            Patterns are case-sensitive. You may escape LIKE-patterns.
        """
        st = self.execute_snapshot(
            """
            SELECT *
            FROM sys.exa_all_objects
            WHERE object_name LIKE {object_name_pattern}
                AND object_type LIKE {object_type_pattern}
                AND owner LIKE {owner_pattern}
                AND root_name LIKE {root_name_pattern}
        """,
            {
                "object_name_pattern": object_name_pattern,
                "object_type_pattern": object_type_pattern,
                "owner_pattern": owner_pattern,
                "root_name_pattern": root_name_pattern,
            },
        )

        return st.fetchall()

    def list_object_sizes(
        self,
        object_name_pattern="%",
        object_type_pattern="%",
        owner_pattern="%",
        root_name_pattern="%",
    ):
        """
        List Objects with their respective size.

        Args:
            object_name_pattern:
                Object name or LIKE-pattern to filter on.
                (default: ``'%'``)
            object_type_pattern:
                Object type or LIKE-pattern to filter on.
                (default: ``'%'``)
            owner_pattern:
                Owner name or LIKE-pattern to filter on.
                (default: ``'%'``)
            root_name_pattern:
                Root name or LIKE-pattern to filter on.j
                It normally refers to schema name.
                (default: ``'%'``)

        Returns:
            List of objects with sizes from `EXA_ALL_OBJECT_SIZES <https://docs.exasol.com/db/latest/sql_references/system_tables/metadata/exa_all_object_sizes.htm>`_ system view matching LIKE-pattern.
        Note:
            Patterns are case-sensitive. You may escape LIKE-patterns.
        """
        st = self.execute_snapshot(
            """
            SELECT *
            FROM sys.exa_all_object_sizes
            WHERE object_name LIKE {object_name_pattern}
                AND object_type LIKE {object_type_pattern}
                AND owner LIKE {owner_pattern}
                AND root_name LIKE {root_name_pattern}
        """,
            {
                "object_name_pattern": object_name_pattern,
                "object_type_pattern": object_type_pattern,
                "owner_pattern": owner_pattern,
                "root_name_pattern": root_name_pattern,
            },
        )

        return st.fetchall()

    def list_indices(
        self, index_schema_pattern="%", index_table_pattern="%", index_owner_pattern="%"
    ):
        """
        List indicies.

        Args:
            index_schema_pattern:
                Schema name or LIKE-pattern to filter on.
                (default: ``'%'``)
            index_table_pattern:
                Table name or LIKE-pattern to filter on.
                (default: ``'%'``)
            index_owner_pattern:
                Owner name or LIKE-pattern to filter on.
                (default: ``'%'``)

        Returns:
            List of indices with sizes from `EXA_ALL_INDICES <https://docs.exasol.com/db/latest/sql_references/system_tables/metadata/exa_all_indices.htm>`_ system view matching LIKE-pattern.

        Note:
            Patterns are case-sensitive. You may escape LIKE-patterns.
        """
        st = self.execute_snapshot(
            """
            SELECT *
            FROM sys.exa_all_indices
            WHERE index_schema LIKE {index_schema_pattern}
                AND index_table LIKE {index_table_pattern}
                AND index_owner LIKE {index_owner_pattern}
        """,
            {
                "index_schema_pattern": index_schema_pattern,
                "index_table_pattern": index_table_pattern,
                "index_owner_pattern": index_owner_pattern,
            },
        )

        return st.fetchall()

    def list_sql_keywords(self):
        """
        Get reserved SQL keywords which cannot be used as identifiers without double-quote escaping

        Returns:
            List of SQL keywords from `EXA_SQL_KEYWORDS <https://docs.exasol.com/db/latest/sql_references/system_tables/metadata/exa_sql_keywords.htm>`_ system view.

        Warning:
            Never hardcode this list! It might change with next Exasol server version without warning
        Note:
            These keywords cannot be used as identifiers without double quotes.
        """
        if not self.sql_keywords:
            if self.connection.protocol_version() >= constant.PROTOCOL_V2:
                st = self.execute_meta_nosql("getKeywords")

                self.sql_keywords = [
                    r["KEYWORD"] for r in st.fetchall() if r["RESERVED"] is True
                ]
            else:
                st = self.execute_snapshot(
                    """
                    SELECT keyword
                    FROM EXA_SQL_KEYWORDS
                    WHERE reserved IS TRUE
                    ORDER BY keyword
                """
                )

                self.sql_keywords = st.fetchcol()

        return self.sql_keywords

    def execute_snapshot(self, query, query_params=None):
        """
        Execute query in snapshot transaction mode using SQL hint

        Args:
            query:
                SQL query text, possibly with placeholders.
            query_params:
                Values for placeholders.
        Returns:
            :class:`ExaStatement`

        Warning:
            Please do not try to query normal tables with this method. It will fail during creation of indices or statistics objects.

        Note:
            ``fetch_dict=Tru`` is enforced to prevent users from relying on order of columns in system views
        """
        options = {
            "fetch_dict": True,
        }

        return self.connection.cls_statement(
            self.connection,
            f"{self.snapshot_execution_hint}{query}",
            query_params,
            **options,
        )

    def execute_meta_nosql(self, meta_command, meta_params=None):
        """
        Execute no SQL metadata command introduced in Exasol 7.0+

        Args:
            meta_command:
                Metadata command.
            meta_params:
                Parameters for metadata command.

        Returns:
            :class:`ExaStatement`

        Note:
            This feature requires WebSocket protocol v2 or higher

            List of available commands can be found `here <https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV2.md#metadata-related-commands>`_.
        """
        if self.connection.protocol_version() < constant.PROTOCOL_V2:
            raise ExaRuntimeError(
                self.connection,
                "Protocol version 2 is required to execute nosql metadata commands",
            )

        # Security check, prevents execution of dangerous commands if meta_command argument is dynamic
        if meta_command[0:3] != "get":
            raise ExaRuntimeError(
                self.connection, "Meta command name should start with prefix 'get*'"
            )

        options = {
            "fetch_dict": True,
        }

        return self.connection.cls_statement(
            self.connection, meta_command, meta_params, meta_nosql=True, **options
        )

    def __repr__(self):
        return f"<{self.__class__.__name__} session_id={self.connection.session_id()}>"
