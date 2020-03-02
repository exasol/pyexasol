"""
DB-API 2 compatibility package
https://www.python.org/dev/peps/pep-0249/

It may be used for a short time to test PyEXASOL with existing applications
It is highly recommended to switch to native interface before any production usage

There is no "paramstyle" and no proper error handling
"""

from ..connection import ExaConnection

apilevel = '2.0'
threadsafety = 1
paramstyle = None


def connect(**kwargs):
    if 'autocommit' not in kwargs:
        kwargs['autocommit'] = False

    return DB2Connection(**kwargs)


class DB2Connection(ExaConnection):
    def cursor(self):
        return DB2Cursor(self)


class DB2Cursor(object):
    arraysize = 1

    def __init__(self, connection):
        self.connection = connection
        self.stmt = None

    def execute(self, query):
        self.stmt = self.connection.execute(query)

    def executemany(self, query):
        raise NotSupportedError

    def fetchone(self):
        return self.stmt.fetchone()

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize

        return self.stmt.fetchmany(size)

    def fetchall(self):
        return self.stmt.fetchall()

    def nextset(self):
        raise NotSupportedError

    def setinputsizes(self):
        pass

    def setoutputsize(self):
        pass

    @property
    def description(self):
        cols = []

        for k, v in self.stmt.columns().items():
            cols.append((
                k,
                v.get('type', None),
                v.get('size', None),
                v.get('size', None),
                v.get('precision', None),
                v.get('scale', None),
                True
            ))

        return cols

    @property
    def rowcount(self):
        return self.stmt.rowcount()

    def close(self):
        self.stmt.close()


class NotSupportedError(Exception):
    pass
