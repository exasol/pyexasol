"""
Extend core PyEXASOL classes, add custom logic
In this example we add print_session_id() custom method to all objects
"""

import pyexasol
import _config as config

import collections

import pprint
printer = pprint.PrettyPrinter(indent=4, width=140)


class CustomExaStatement(pyexasol.ExaStatement):
    def print_session_id(self):
        print(f"Statement session_id: {self.connection.session_id()}")


class CustomExaFormatter(pyexasol.ExaFormatter):
    def print_session_id(self):
        print(f"Formatter session_id: {self.connection.session_id()}")


class CustomExaLogger(pyexasol.ExaLogger):
    def print_session_id(self):
        print(f"Logger session_id: {self.connection.session_id()}")


class CustomExaExtension(pyexasol.ExaExtension):
    def print_session_id(self):
        print(f"Extension session_id: {self.connection.session_id()}")


class CustomExaMetaData(pyexasol.ExaMetaData):
    def print_session_id(self):
        print(f"MetaData session_id: {self.connection.session_id()}")


class CustomExaConnection(pyexasol.ExaConnection):
    # Set custom sub-classes here
    cls_statement = CustomExaStatement
    cls_formatter = CustomExaFormatter
    cls_logger = CustomExaLogger
    cls_extension = CustomExaExtension
    cls_meta = CustomExaMetaData

    def __init__(self, **kwargs):
        if 'custom_param' in kwargs:
            print(f"Custom connection parameter: {kwargs['custom_param']}")
            del kwargs['custom_param']

        super().__init__(**kwargs)


C = CustomExaConnection(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema,
                        custom_param='test custom param!')

stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")

# Access overloaded objects
stmt.print_session_id()
C.format.print_session_id()
C.logger.print_session_id()
C.ext.print_session_id()
C.meta.print_session_id()

C.close()

print("Return result rows as named tuples")


class NamedTupleExaStatement(pyexasol.ExaStatement):
    def __next__(self):
        row = super().__next__()
        return self.cls_row(*row)

    def _init_result_set(self, res):
        super()._init_result_set(res)
        self.cls_row = collections.namedtuple('Row', self.col_names)


class NamedTupleExaConnection(pyexasol.ExaConnection):
    cls_statement = NamedTupleExaStatement


C = NamedTupleExaConnection(dsn=config.dsn, user=config.user, password=config.password, schema=config.schema)
stmt = C.execute("SELECT * FROM users ORDER BY user_id LIMIT 5")
print(stmt.fetchone())
print(stmt.fetchone())

C.close()
