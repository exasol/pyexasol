"""
Extension with Exasol-specific helper functions
"""

from typing import Optional

from .exceptions import ExaRuntimeError


class ExaExtension:
    """
    This class extends the functionality of a simple SQL driver to address common Exasol-related problems.

    Tip:
        You may access these functions using `.ext` property of connection object.

    Examples:
        >>> C = pyexasol.connect(...)
        ... print(C.ext.get_disk_space_usage())
    """

    def __init__(self, connection):
        self.connection = connection
        self.reserved_words = None

    def get_columns(self, object_name):
        """
        Get information about columns of table or view.

        Args:
            object_name: Object name may be passed as tuple to specify custom schema.

        Caution:
            **DEPRECATED**, please use ``.meta.sql_columns`` instead.
        """
        object_name = self.connection.format.default_format_ident(object_name)
        return self.get_columns_sql(f"SELECT * FROM {object_name}")

    def get_columns_sql(self, query, query_params=None):
        """
        Get columns of SQL query without executing it.

        Args:
            object_name: Object name may be passed as tuple to specify custom schema.

        Caution:
            **DEPRECATED**, please use ``.meta.sql_columns`` instead.

        Note:
            It relies on a prepared statement that will be closed immediately without execution
        """
        stmt = self.connection.cls_statement(
            self.connection, query, query_params, prepare=True
        )
        columns = stmt.columns()
        stmt.close()

        return columns

    def insert_multi(self, table_name, data, columns=None):
        """
        Insert a small number of rows into a table using a prepared statement.

        Args:
            table_name:
                Target table for INSERT.
            data:
                Source object implementing ``__iter__`` (e.g. list or tuple).
            columns:
                List of column names to specify custom order of columns.

        Tip:
            Compared to ``.import_from_iterable``, this method offers better performance for small data sets of 10,000 rows or fewer.

            * Use ``.import_from_iterable`` for larger data sets and better memory efficiency
            * Use ``.import_from_pandas`` to import from :class:`pandas.DataFrame` regardless of its size
            * Use ``.import_from_parquet`` to import from parquet files
            * Use ``.import_from_polars`` to import from :class:`polars.DataFrame` regardless of its size

            You may use "columns" argument to specify custom order of columns for insertion
            If some columns are not included in this list, ``NULL`` or ``DEFAULT`` value will be used instead

        Note:
            Please note that data should be presented in a row format. You may use ``zip(*data_cols)`` to convert columnar format into row format.
        """

        # Convert possible iterator into list
        data = list(data)

        if len(data) == 0:
            raise ExaRuntimeError(
                self.connection,
                "At least one row of data is required for insert_multi()",
            )

        params = {
            "table_name": self.connection.format.default_format_ident(table_name),
            "columns": "",
            "values": ", ".join(["?"] * len(data[0])),
        }

        if columns:
            params["columns"] = (
                f"({','.join([self.connection.format.default_format_ident(c) for c in columns])})"
            )

        query = "INSERT INTO {table_name!r}{columns!r} VALUES ({values!r})"

        stmt = self.connection.cls_statement(
            self.connection, query, params, prepare=True
        )
        stmt.execute_prepared(data)
        stmt.close()

        return stmt

    def get_sys_columns(self, object_name):
        """
        Get information about columns of table or view (SYS format)

        Args:
            object_name: Object name may be passed as tuple to specify custom schema.

        Caution:
            **DEPRECATED**, please use ``.meta.list_columns`` instead.
        """
        if isinstance(object_name, tuple):
            schema = self.connection.format.default_format_ident_value(object_name[0])
            object_name = self.connection.format.default_format_ident_value(
                object_name[1]
            )
        else:
            schema = self.connection.current_schema()
            object_name = self.connection.format.default_format_ident_value(object_name)

        sql = """/*snapshot execution*/
            SELECT c.column_name, c.column_type, c.column_maxsize, c.column_num_scale,
                   c.column_is_nullable, c.column_is_distribution_key, c.column_default,
                   c.column_comment, t.type_name
            FROM EXA_ALL_COLUMNS c
                JOIN EXA_SQL_TYPES t ON (c.column_type_id=t.type_id)
            WHERE c.column_schema={schema}
                AND c.column_table={object_name}
            ORDER BY c.column_ordinal_position
        """

        st = self._execute(sql, {"schema": schema, "object_name": object_name})
        res = list()

        for r in st:
            res.append(
                {
                    "name": r["column_name"],
                    "type": r["type_name"],
                    "sql_type": r["column_type"],
                    "size": r["column_maxsize"],
                    "scale": r["column_num_scale"],
                    "nulls": r["column_is_nullable"],
                    "distribution_key": r["column_is_distribution_key"],
                    "default": r["column_default"],
                    "comment": r["column_comment"],
                }
            )

        return res

    def get_sys_tables(self, schema=None, table_name_prefix=""):
        """
        Get information about tables in selected schema(SYS format)

        Args:
            schema:
                -
            table_name_prefix:
                Output may be optionally filtered by table name prefix.

        Caution:
            **DEPRECATED**, please use ``.meta.list_tables`` instead.
        """
        if schema is None:
            schema = self.connection.current_schema()
        else:
            schema = self.connection.format.default_format_ident_value(schema)

        table_name_prefix = self.connection.format.default_format_ident_value(
            table_name_prefix
        )
        table_name_prefix = self.connection.format.escape_like(table_name_prefix)

        sql = """/*snapshot execution*/
            SELECT *
            FROM EXA_ALL_TABLES
            WHERE table_schema={schema}
                AND table_name LIKE '{table_name_prefix!r}%'
            ORDER BY table_name ASC
        """

        st = self._execute(
            sql, {"schema": schema, "table_name_prefix": table_name_prefix}
        )
        res = list()

        for r in st:
            res.append(
                {
                    "table_name": (
                        r["table_name"].lower()
                        if self.connection.options["lower_ident"]
                        else r["table_name"]
                    ),
                    "table_schema": (
                        r["table_schema"].lower()
                        if self.connection.options["lower_ident"]
                        else r["table_schema"]
                    ),
                    "table_is_virtual": r["table_is_virtual"],
                    "table_has_distribution_key": r["table_has_distribution_key"],
                    "table_comment": r["table_comment"],
                }
            )

        return res

    def get_sys_views(self, schema: Optional[str] = None, view_name_prefix: str = ""):
        """
        Get information about views in selected schema (SYS format)

        Args:
            schema:
                -
            view_name_prefix:
                Output may be optionally filtered by view name prefix.

        Caution:
            **DEPRECATED**, please use ``.meta.list_views`` instead.
        """
        if schema is None:
            schema = self.connection.current_schema()
        else:
            schema = self.connection.format.default_format_ident_value(schema)

        view_name_prefix = self.connection.format.default_format_ident_value(
            view_name_prefix
        )
        view_name_prefix = self.connection.format.escape_like(view_name_prefix)

        sql = """/*snapshot execution*/
            SELECT *
            FROM EXA_ALL_VIEWS
            WHERE view_schema={schema}
                AND view_name LIKE '{view_name_prefix!r}%'
            ORDER BY view_name ASC
        """

        st = self._execute(
            sql, {"schema": schema, "view_name_prefix": view_name_prefix}
        )
        res = list()

        for r in st:
            res.append(
                {
                    "view_name": (
                        r["view_name"].lower()
                        if self.connection.options["lower_ident"]
                        else r["view_name"]
                    ),
                    "view_schema": (
                        r["view_schema"].lower()
                        if self.connection.options["lower_ident"]
                        else r["view_schema"]
                    ),
                    "scope_schema": (
                        r["scope_schema"].lower()
                        if self.connection.options["lower_ident"]
                        else r["scope_schema"]
                    ),
                    "view_text": r["view_text"],
                    "view_comment": r["view_comment"],
                }
            )

        return res

    def get_sys_schemas(self, schema_name_prefix: str = ""):
        """
        Get information about schemas (SYS format)

        Args:
            schema_name_prefix:
                Output may be optionally filtered by schema name prefix

        Caution:
            **DEPRECATED**, please use ``.meta.list_schemas`` instead.
        """
        schema_name_prefix = self.connection.format.default_format_ident_value(
            schema_name_prefix
        )
        schema_name_prefix = self.connection.format.escape_like(schema_name_prefix)

        sql = """/*snapshot execution*/
            SELECT *
            FROM EXA_SCHEMAS
            WHERE schema_name LIKE '{schema_name_prefix!r}%'
            ORDER BY schema_name ASC
        """

        st = self._execute(sql, {"schema_name_prefix": schema_name_prefix})
        res = list()

        for r in st:
            res.append(
                {
                    "schema_name": (
                        r["schema_name"].lower()
                        if self.connection.options["lower_ident"]
                        else r["schema_name"]
                    ),
                    "schema_owner": (
                        r["schema_owner"].lower()
                        if self.connection.options["lower_ident"]
                        else r["schema_owner"]
                    ),
                    "schema_is_virtual": r["schema_is_virtual"],
                    "schema_comment": r["schema_comment"],
                }
            )

        return res

    def get_reserved_words(self):
        """
        Get reserved keywords which cannot be used as identifiers without double-quotes.

        Caution:
            **DEPRECATED**, please use ``.meta.list_sql_keywords`` instead.

        Warning:
            Never hard-code this list! It changes with every Exasol versions.
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

    def get_disk_space_usage(self):
        """
        Get the disk space usage of the Exasol DB.

        Returns:

            A dict with 4 keys, providing all disk space details.

            .. list-table::
               :header-rows: 1

               * - Key
                 - Description
               * - ``occupied_size``
                 - How much space is occupied (in bytes)
               * - ``free_size``
                 - How much space is available (in bytes)
               * - ``total_size``
                 - occupied_size + free_size
               * - ``occupied_size_percent``
                 - Percentage of occupied disk space (0-100%)

        Note:
            Exasol still lacks a standard function to measure actual disk space usage.
            We are trying to mitigate this problem by creating a custom function.
        """
        sql = """
            SELECT measure_time,
                   (committed_size * redundancy + temp_swap_data) AS occupied_size,
                   (device_size * redundancy + hdd_free) AS total_size
            FROM "$EXA_STATS_DB_SIZE"
            ORDER BY measure_time DESC
            LIMIT 1
        """

        row = self._execute(sql).fetchone()

        if row is None:
            return None

        row["occupied_size"] = int(row["occupied_size"])
        row["total_size"] = int(row["total_size"])

        row["free_size"] = row["total_size"] - row["occupied_size"]
        row["occupied_size_percent"] = round(
            row["occupied_size"] / row["total_size"] * 100, 2
        )

        return row

    def export_to_pandas_with_dtype(self, query_or_table, query_params=None):
        """
        Export to pandas and attempt to guess correct dtypes based on Exasol columns.

        Args:
            query_or_table:
                Query or table to export.
            query_params:
                Additional query parameters.

        Note:
            Since pandas has significantly smaller range of allowed values, this function makes many assumptions.
            Please use it as baseline for your own function for complex cases.

            .. list-table::
               :widths: 25 25
               :header-rows: 1

               * - Exasol Type
                 - Pandas Type
               * - Small decimal
                 - int32
               * - Big decimal
                 - int64
               * - Double
                 - float64
               * - Date, Timestamp
                 - datetime64[ns]
               * - Everything else
                 - category (!)
        """

        if query_params:
            query_or_table = self.connection.format.format(
                query_or_table, **query_params
            )

        if (
            isinstance(query_or_table, tuple)
            or str(query_or_table).strip().find(" ") == -1
        ):
            columns = self.get_columns(query_or_table)
        else:
            columns = self.get_columns_sql(query_or_table)

        params = {
            "names": list(),
            "dtype": dict(),
            "parse_dates": list(),
            "na_values": dict(),
            "infer_datetime_format": True,
            "engine": "c",
            "skip_blank_lines": False,
        }

        for k, c in columns.items():
            params["names"].append(k)

            if c["type"] == "DATE":
                params["dtype"][k] = "object"
                params["na_values"][k] = "0001-01-01"
                params["parse_dates"].append(k)

            elif c["type"] == "TIMESTAMP":
                params["dtype"][k] = "object"
                params["na_values"][k] = "0001-01-01 00:00:00"
                params["parse_dates"].append(k)

            elif c["type"] == "DECIMAL":
                if c["scale"] > 0:
                    params["dtype"][k] = "float64"
                else:
                    if c["precision"] <= 9:
                        params["dtype"][k] = "int32"
                    else:
                        params["dtype"][k] = "int64"

            elif c["type"] == "DOUBLE":
                params["dtype"][k] = "float64"

            else:
                params["dtype"][k] = "category"

        def callback(pipe, dst, **kwargs):
            import pandas

            return pandas.read_csv(pipe, **kwargs)

        return self.connection.export_to_callback(
            callback, None, query_or_table, None, params
        )

    def explain_last(self, details: bool = False):
        """
        Args:
            details (bool):
                - ``False``, the function returns the average (AVG) or maximum (MAX) values aggregated for all Exasol nodes.
                - ``True``, the function returns separate rows for each individual Exasol node, with a column labeled "iproc" representing the node.

        Returns:
            Profiling information for last executed query.

        Note:
            This function should be called immediately after ``execute()``
            ``COMMIT``, ``ROLLBACK`` and ``FLUSH STATISTICS`` queries are ignored.
        Tip:
            Details are useful to detect bad data distribution and imbalanced execution
            If you want to see real values of ``CPU, MEM, HDD, NET`` columns,
            please enable Exasol profiling first with:

            .. code-block:: sql

                ALTER SESSION SET PROFILE = 'ON';

        *Please refer to Exasol User Manuals for explanations about profiling columns.*
        """
        self._execute("FLUSH STATISTICS")

        sql = """
            SELECT part_id /* PyExasol explain_last */
                {iproc_col!r}
                , part_name
                , part_info
                , object_schema
                , object_name
                , object_rows
                , in_rows
                , out_rows
                , duration
                , start_time
                , stop_time
                , cpu
                , mem_peak
                , temp_db_ram_peak
                , hdd_read
                , hdd_write
                , net
                , remarks
            FROM {table_name!q}
            WHERE session_id=CURRENT_SESSION
                AND stmt_id = CURRENT_STATEMENT - {stmt_offset!d}
            ORDER BY {order_by!r}
        """

        params = {
            "iproc_col": ", iproc" if details else "",
            "table_name": (
                "$EXA_PROFILE_DETAILS_LAST_DAY" if details else "$EXA_PROFILE_LAST_DAY"
            ),
            "order_by": "part_id ASC, iproc ASC" if details else "part_id ASC",
            "stmt_offset": 4 if self.connection.attr["autocommit"] else 2,
        }

        return self._execute(sql, params).fetchall()

    def _execute(self, query, query_params=None):
        # Preserve ext-functions output format regardless of current options for user queries
        options = {
            "fetch_dict": True,
            "fetch_mapper": None,
            "lower_ident": True,
        }

        return self.connection.cls_statement(
            self.connection, query, query_params, **options
        )

    def __repr__(self):
        return f"<{self.__class__.__name__} session_id={self.connection.session_id()}>"
