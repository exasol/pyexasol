import collections
import itertools

from . import constant
from .exceptions import ExaRuntimeError


class ExaStatement:
    """
    This class executes and helps to fetch result set of single Exasol SQL statement.

    Warning:
        Unlike typical `Cursor` object, `ExaStatement` is not reusable.

    Note:
        :class:`pyexasol.ExaStatement` may fetch result set rows as ``tuples`` (default)
        or as ``dict`` (set `fetch_dict=True` in connection options).

        :class:`pyexasol.ExaStatement` may use custom data-type mapper during fetching
        (set `fetch_mapper=<func>` in connection options).
        Mapper function accepts two arguments (raw `value` and `dataType` object)
        and returns custom object or value.

        :class:`pyexasol.ExaStatement` fetches big result sets in chunks.
        The size of chunk may be adjusted (set `fetch_size_bytes=<int>` in connection options).

        Public Attributes:
            ``execution_time``:
                Execution time of SQL statement. It is measured by wall-clock time
                of WebSocket request, so real execution time is a bit faster.
    """

    def __init__(
        self,
        connection,
        query=None,
        query_params=None,
        prepare=False,
        meta_nosql=False,
        **options,
    ):
        """
        Args:
            connection:
                -
            query:
                -
            query_params:
                -
            prepare:
                -
            meta_nosql:
                -
            options:
                additonal kwargs
        """
        self.connection = connection

        self.query = query if meta_nosql else self._format_query(query, query_params)
        self.query_params = query_params

        self.fetch_dict = options.get(
            "fetch_dict", self.connection.options["fetch_dict"]
        )
        self.fetch_mapper = options.get(
            "fetch_mapper", self.connection.options["fetch_mapper"]
        )
        self.fetch_size_bytes = options.get(
            "fetch_size_bytes", self.connection.options["fetch_size_bytes"]
        )
        self.lower_ident = options.get(
            "lower_ident", self.connection.options["lower_ident"]
        )

        self.data_zip = zip()
        self.col_names = []
        self.col_types = []

        self.num_columns = 0
        self.num_rows_total = 0
        self.num_rows_chunk = 0

        self.row_count = 0
        self.pos_total = 0
        self.pos_chunk = 0

        self.result_type = None
        self.result_set_handle = None
        self.statement_handle = None
        self.parameter_data = None

        if self.connection.is_closed:
            raise ExaRuntimeError(self.connection, "Exasol connection was closed")

        # Always set last_stmt in connection object regardless of how statement object was created
        self.connection.last_stmt = self

        # This index may not match STMT_ID in system tables due to automatically executed queries (e.g. autocommit)
        self.connection.stmt_count += 1
        self.stmt_idx = self.connection.stmt_count

        self.execution_time = 0
        self.is_closed = False

        if prepare:
            self._prepare()
        elif meta_nosql:
            self._execute_meta_nosql()
        else:
            self._execute()

    def __iter__(self):
        """
        The best way to fetch result set of statement is to use iterator:

        Yields:
            ``tuple`` or ``dict`` depending on ``fetch_dict`` connection option.

        Examples:

            >>> st = pyexasol.execute('SELECT * FROM table')
            ... for row in st:
            ...     print(row)
        """
        return self

    def __next__(self):
        if self.pos_total >= self.num_rows_total:
            if self.result_type != "resultSet":
                raise ExaRuntimeError(
                    self.connection,
                    "Attempt to fetch from statement without result set",
                )

            self._close_result_set_handle()
            raise StopIteration

        if self.pos_chunk >= self.num_rows_chunk:
            self._next_chunk()

        row = next(self.data_zip)

        if self.fetch_mapper:
            row = tuple(map(self.fetch_mapper, row, self.col_types))

        if self.fetch_dict:
            row = dict(zip(self.col_names, row))

        self.pos_total += 1
        self.pos_chunk += 1

        return row

    def fetchone(self):
        """
        Fetches one row of data.

        Returns:
            ``tuple`` or ``dict``.
            ``None`` if all rows were fetched.
        """
        try:
            row = next(self)
        except StopIteration:
            return None

        return row

    def fetchmany(self, size=constant.DEFAULT_FETCHMANY_SIZE):
        """
        Fetch multiple rows.

        Args:
            size:
                Set the specific number of rows to fetch (Default: ``10000``)

        Returns:
            ``list`` of ``tuples`` or ``list`` of ``dict``.
            Empty `list` if all rows were fetched previously.
        """
        return [row for row in itertools.islice(self, size)]

    def fetchall(self):
        """
        Fetches all remaining rows.

        Returns:
            ``list`` of ``tuples`` or ``list`` of ``dict``.
            Empty ``list`` if all rows were fetched previously.

        Warning:
            This function may exhaust available memory.
        """
        return [row for row in self]

    def fetchcol(self):
        """
        Fetches all values from the first column.

        Returns:
            ``list`` of values.
            Empty ``list`` if all rows were fetched previously.
        """
        self.fetch_dict = False
        return [row[0] for row in self]

    def fetchval(self):
        """
        Fetches first column of first row.


        Returns:
            Value, ``None`` if all rows were fetched previously.

        Tip:
            This may be useful for queries returning single value like
            ``SELECT count(*) FROM table``.
        """
        self.fetch_dict = False

        try:
            row = next(self)
        except StopIteration:
            return None

        return row[0]

    def rowcount(self):
        """
        Number of selected/processed rows.

        Returns:
            Total amount of selected rows for statements with result set (``num_rows``).
            Total amount of processed rows for DML queries (``row_count``).
        """
        if self.result_type == "resultSet":
            return self.num_rows_total
        else:
            return self.row_count

    def columns(self):
        """
        Retrieves column information of returned data.

        Returns:
            A ``dict`` with keys as ``column names`` and values as ``dataType`` objects.

        Notes:

            The dict will containt the following data:

            .. list-table::
               :header-rows: 1

               * - Names
                 - Type
                 - Description
               * - type
                 - string
                 - column data type
               * - precision
                 - number
                 - (optional) column precision
               * - scale
                 - number
                 - (optional) column scale
               * - size
                 - number
                 - (optional) maximum size in bytes of a column value
               * - characterSet
                 - string
                 - (optional) character encoding of a text column
               * - withLocalTimeZone
                 - true, false
                 - (optional) specifies if a timestamp has a local time zone
               * - fraction
                 - number
                 - (optional) fractional part of number
               * - srid
                 - number
                 - (optional) spatial reference system identifier
        """
        return dict(zip(self.col_names, self.col_types))

    def column_names(self):
        """List of column names."""
        return self.col_names

    def close(self):
        """
        Closes result set handle if it was opened.

        Warning:
            You won't be able to fetch next chunk of large dataset
            after calling this function, but no other side effects.
        """
        self._close_result_set_handle()
        self._close_statement_handle()

        self.is_closed = True

    def _close_result_set_handle(self):
        if not self.connection.is_closed and self.result_set_handle:
            self.connection.req(
                {
                    "command": "closeResultSet",
                    "resultSetHandles": [self.result_set_handle],
                }
            )

            self.result_set_handle = None

    def _close_statement_handle(self):
        if not self.connection.is_closed and self.statement_handle:
            self.connection.req(
                {
                    "command": "closePreparedStatement",
                    "statementHandle": self.statement_handle,
                }
            )

            self.statement_handle = None

    def _format_query(self, query, query_params):
        query = str(query)

        if query_params is not None:
            query = self.connection.format.format(query, **query_params)

        return query.lstrip(" \n").rstrip(" \n;")

    def _execute(self):
        ret = self.connection.req(
            {
                "command": "execute",
                "sqlText": self.query,
            }
        )

        self.execution_time = self.connection.ws_req_time
        self._init_result_set(ret)

    def _execute_meta_nosql(self):
        meta_params = self.query_params if self.query_params is not None else {}

        if "command" in meta_params:
            raise ExaRuntimeError(
                self.connection,
                "Key 'command' is not allowed as a parameter for meta nosql request",
            )

        if "attributes" in meta_params:
            raise ExaRuntimeError(
                self.connection,
                "Key 'attributes' is not allowed as a parameter for meta nosql request",
            )

        ret = self.connection.req(
            {
                "command": self.query,
                **meta_params,
            }
        )

        self.execution_time = self.connection.ws_req_time
        self._init_result_set(ret)

    def _prepare(self):
        ret = self.connection.req(
            {
                "command": "createPreparedStatement",
                "sqlText": self.query,
            }
        )

        self.statement_handle = ret["responseData"]["statementHandle"]

        if "parameterData" in ret["responseData"]:
            self.parameter_data = ret["responseData"]["parameterData"]

        self._init_result_set(ret)

    def execute_prepared(self, data=None):
        ret = self.connection.req(
            {
                "command": "executePreparedStatement",
                "statementHandle": self.statement_handle,
                "numColumns": (
                    self.parameter_data["numColumns"] if self.parameter_data else 0
                ),
                "numRows": len(data) if data else 0,
                "columns": (
                    self.parameter_data["columns"] if self.parameter_data else []
                ),
                "data": list(zip(*data)) if data else [],
            }
        )

        self.execution_time = self.connection.ws_req_time
        self._init_result_set(ret)

    def _init_result_set(self, ret):
        res = ret["responseData"]["results"][0]

        self.result_type = res["resultType"]

        if self.result_type == "resultSet":
            if "resultSetHandle" in res["resultSet"]:
                self.result_set_handle = res["resultSet"]["resultSetHandle"]

            if "data" in res["resultSet"]:
                self.data_zip = zip(*res["resultSet"]["data"])

            if self.lower_ident:
                self.col_names = [
                    c["name"].lower() for c in res["resultSet"]["columns"]
                ]
            else:
                self.col_names = [c["name"] for c in res["resultSet"]["columns"]]

            self.col_types = [c["dataType"] for c in res["resultSet"]["columns"]]

            self.num_columns = res["resultSet"]["numColumns"]
            self.num_rows_total = res["resultSet"]["numRows"]
            self.num_rows_chunk = res["resultSet"]["numRowsInMessage"]

            self._check_duplicate_col_names()
        elif self.result_type == "rowCount":
            self.row_count = res["rowCount"]
        else:
            raise ExaRuntimeError(
                self.connection, f"Unknown resultType: {self.result_type}"
            )

    def _next_chunk(self):
        ret = self.connection.req(
            {
                "command": "fetch",
                "resultSetHandle": self.result_set_handle,
                "startPosition": self.pos_total,
                "numBytes": self.fetch_size_bytes,
            }
        )

        if "data" in ret["responseData"]:
            self.data_zip = zip(*ret["responseData"]["data"])
        else:
            self.data_zip = zip()

        self.num_rows_chunk = ret["responseData"]["numRows"]
        self.pos_chunk = 0

    def _check_duplicate_col_names(self):
        """
        Exasol allows duplicate names in result sets, but it leads to various problems related to dictionaries
        PyExasol adds additional check to prevent such problems and to allow safe .columns() and fetch_dict=True
        """
        duplicate_col_names = [
            k for (k, v) in collections.Counter(self.col_names).items() if v > 1
        ]

        if duplicate_col_names:
            raise ExaRuntimeError(
                self.connection,
                f'Duplicate column names in result set: {", ".join(duplicate_col_names)}',
            )

    def __repr__(self):
        return f"<{self.__class__.__name__} session_id={self.connection.session_id()} stmt_idx={self.stmt_idx}>"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        """
        close() is being called automatically in order to close handles which
        might be still opened on Exasol server side
        """
        try:
            self.close()
        except Exception:
            pass
