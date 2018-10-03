import itertools
import collections

from .exceptions import ExaRuntimeError
from . import constant


class ExaStatement(object):
    def __init__(self, connection, query, query_params=None, prepare=False, **options):
        self.connection = connection
        self.query = self._format_query(query, query_params)

        self.fetch_dict = options.get('fetch_dict', self.connection.fetch_dict)
        self.fetch_mapper = options.get('fetch_mapper', self.connection.fetch_mapper)
        self.fetch_size_bytes = options.get('fetch_size_bytes', self.connection.fetch_size_bytes)
        self.lower_ident = options.get('lower_ident', self.connection.lower_ident)

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

        # This index may not match STMT_ID in system tables due to automatically executed queries (e.g. autocommit)
        self.connection.stmt_count += 1
        self.stmt_idx = self.connection.stmt_count

        self.execution_time = 0
        self.is_closed = False

        if self.connection.is_closed:
            raise ExaRuntimeError(self.connection, "Exasol connection was closed")

        if prepare:
            self._prepare()
        else:
            self._execute()

    def __iter__(self):
        return self

    def __next__(self):
        if self.pos_total >= self.num_rows_total:
            if self.result_type != 'resultSet':
                raise ExaRuntimeError(self.connection, 'Attempt to fetch from statement without result set')

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
        try:
            row = next(self)
        except StopIteration:
            return None

        return row

    def fetchmany(self, size=constant.DEFAULT_FETCHMANY_SIZE):
        return [row for row in itertools.islice(self, size)]

    def fetchall(self):
        return [row for row in self]

    def fetchcol(self):
        self.fetch_dict = False
        return [row[0] for row in self]

    def fetchval(self):
        self.fetch_dict = False

        try:
            row = next(self)
        except StopIteration:
            return None

        return row[0]

    def rowcount(self):
        if self.result_type == 'resultSet':
            return self.num_rows_total
        else:
            return self.row_count

    def columns(self):
        return dict(zip(self.col_names, self.col_types))

    def column_names(self):
        return self.col_names

    def close(self):
        if self.result_set_handle:
            self.connection.req({
                'command': 'closeResultSet',
                'resultSetHandles': [self.result_set_handle]
            })

        if self.statement_handle:
            self.connection.req({
                'command': 'closePreparedStatement',
                'statementHandle': self.statement_handle,
            })

        self.is_closed = True

    def _format_query(self, query, query_params):
        query = str(query)

        if query_params is not None:
            query = self.connection.format.format(query, **query_params)

        return query.lstrip(' \n').rstrip(' \n;')

    def _execute(self):
        ret = self.connection.req({
            'command': 'execute',
            'sqlText': self.query,
        })

        self.execution_time = self.connection.ws_req_time
        self._init_result_set(ret)

    def _prepare(self):
        ret = self.connection.req({
            'command': 'createPreparedStatement',
            'sqlText': self.query,
        })

        self.statement_handle = ret['responseData']['statementHandle']
        self._init_result_set(ret)

    def _init_result_set(self, ret):
        res = ret['responseData']['results'][0]

        self.result_type = res['resultType']

        if self.result_type == 'resultSet':
            if 'resultSetHandle' in res['resultSet']:
                self.result_set_handle = res['resultSet']['resultSetHandle']
            elif 'data' in res['resultSet']:
                self.data_zip = zip(*res['resultSet']['data'])

            if self.lower_ident:
                self.col_names = [c['name'].lower() for c in res['resultSet']['columns']]
            else:
                self.col_names = [c['name'] for c in res['resultSet']['columns']]

            self.col_types = [c['dataType'] for c in res['resultSet']['columns']]

            self.num_columns = res['resultSet']['numColumns']
            self.num_rows_total = res['resultSet']['numRows']
            self.num_rows_chunk = res['resultSet']['numRowsInMessage']

            self._check_duplicate_col_names()
        elif self.result_type == 'rowCount':
            self.row_count = res['rowCount']
        else:
            raise ExaRuntimeError(self.connection, f'Unknown resultType: {self.result_type}')

    def _next_chunk(self):
        ret = self.connection.req({
            'command': 'fetch',
            'resultSetHandle': self.result_set_handle,
            'startPosition': self.pos_total,
            'numBytes': self.fetch_size_bytes,
        })

        if 'data' in ret['responseData']:
            self.data_zip = zip(*ret['responseData']['data'])
        else:
            self.data_zip = zip()

        self.num_rows_chunk = ret['responseData']['numRows']
        self.pos_chunk = 0

    def _check_duplicate_col_names(self):
        """
        Exasol allows duplicate names in result sets, but it leads to various problems related to dictionaries
        PyEXASOL adds additional check to prevent such problems and to allow safe .columns() and fetch_dict=True
        """
        duplicate_col_names = [k for (k, v) in collections.Counter(self.col_names).items() if v > 1]

        if duplicate_col_names:
            raise ExaRuntimeError(self.connection, f'Duplicate column names in result set: {", ".join(duplicate_col_names)}')

    def __repr__(self):
        return f'<{self.__class__.__name__} session_id={self.connection.session_id()} stmt_idx={self.stmt_idx}>'
