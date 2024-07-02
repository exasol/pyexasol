"""
This module provides `PEP-249`_ a DBAPI compliant Connection and Cursor protocol definition.
(see also `PEP-249-connection`_ and `PEP-249-cursor`_)

.. _PEP-249-connection: https://peps.python.org/pep-0249/#connection-objects
.. _PEP-249-cursor: https://peps.python.org/pep-0249/#cursor-objects
"""

from typing import Protocol


class Connection(Protocol):
    """
    Defines a connection protocol based on `connection-objects`_.

    .. connection-objects: https://peps.python.org/pep-0249/#connection-objects
    """

    def connect(self):
        """
        Connect to the database.

        Attention:
            Addition not required by PEP-249.
        """

    def close(self):
        """
        Close the connection now (rather than whenever .__del__() is called).

        The connection will be unusable from this point forward; an Error (or subclass)
        exception will be raised if any operation is attempted with the connection.
        The same applies to all cursor objects trying to use the connection.
        Note that closing a connection without committing the changes first will cause
        an implicit rollback to be performed.
        """

    def commit(self):
        """
        Commit any pending transaction to the database.

        Note:
            If the database supports an auto-commit feature, this must be initially off.
            An interface method may be provided to turn it back on. Database modules
            that do not support transactions should implement this method with
            void functionality.
        """

    def rollback(self):
        """
        This method is optional since not all databases provide transaction support.

        In case a database does provide transactions this method causes the database
        to roll back to the start of any pending transaction. Closing a connection
        without committing the changes first will cause an implicit rollback
        to be performed.
        """

    def cursor(self):
        """
        Return a new Cursor Object using the connection.

        If the database does not provide a direct cursor concept, the module will have
        to emulate cursors using other means to the extent needed
        by this specification.
        """


class Cursor(Protocol):
    """
    Defines a protocol which is compliant with `cursor-objects`_.

    .. cursor-objects: https://peps.python.org/pep-0249/#cursor-objects
    """

    @property
    def arraysize(self):
        """
        This read/write attribute specifies the number of rows to fetch
        at a time with .fetchmany().

        It defaults to 1, meaning to fetch a single row at a time.
        Implementations must observe this value with respect to the .fetchmany() method,
        but are free to interact with the database a single row at a time.
        It may also be used in the implementation of .executemany().
        """

    @property
    def description(self):
        """
        This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

            * name
            * type_code
            * display_size
            * internal_size
            * precision
            * scale
            * null_ok

        The first two items (name and type_code) are mandatory, the other five
        are optional and are set to None if no meaningful values can be provided.

        This attribute will be None for operations that do not return rows or if
        the cursor has not had an operation invoked via the .execute*() method yet.
        """

    @property
    def rowcount(self):
        """
        This read-only attribute specifies the number of rows that the last .execute*()
        produced (for DQL statements like SELECT) or affected (for DML statements
        like UPDATE or INSERT).

        The attribute is -1 in case no .execute*() has been performed on the cursor or
        the rowcount of the last operation cannot be determined by the interface.

        .. note::

            Future versions of the DB API specification could redefine the latter case
            to have the object return None instead of -1.
        """

    def callproc(self, procname, parameters):
        """
        Call a stored database procedure with the given name.
        (This method is optional since not all databases provide stored procedures)

        The sequence of parameters must contain one entry for each argument that the
        procedure expects. The result of the call is returned as a modified copy of
        the input sequence. Input parameters are left untouched, output and
        input/output parameters replaced with possibly new values.

        The procedure may also provide a result set as output. This must then be
        made available through the standard .fetch*() methods.
        """

    def close(self):
        """
        Close the cursor now (rather than whenever __del__ is called).

        The cursor will be unusable from this point forward; an Error (or subclass)
        exception will be raised if any operation is attempted with the cursor.
        """

    def execute(self, operation, parameters=None):
        """
        Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound to
        variables in the operation. Variables are specified in a database-specific
        notation (see the module’s paramstyle attribute for details).

        A reference to the operation will be retained by the cursor.
        If the same operation object is passed in again, then the cursor can optimize
        its behavior. This is most effective for algorithms where the same operation
        is used, but different parameters are bound to it (many times).

        For maximum efficiency when reusing an operation, it is best to use the
        .setinputsizes() method to specify the parameter types and sizes ahead of time.
        It is legal for a parameter to not match the predefined information;
        the implementation should compensate, possibly with a loss of efficiency.

        The parameters may also be specified as list of tuples to e.g. insert multiple
        rows in a single operation, but this kind of usage is deprecated: .executemany()
        should be used instead.

        Return values are not defined.
        """

    def executemany(self, operation, seq_of_parameters):
        """
        Prepare a database operation (query or command) and then execute it against all
        parameter sequences or mappings found in the sequence seq_of_parameters.

        Modules are free to implement this method using multiple calls to the .execute()
        method or by using array operations to have the database process the sequence
        as a whole in one call.

        Use of this method for an operation which produces one or more result sets
        constitutes undefined behavior, and the implementation is permitted
        (but not required) to raise an exception when it detects that a result set
        has been created by an invocation of the operation.

        The same comments as for .execute() also apply accordingly to this method.

        Return values are not defined.
        """

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence, or None
        when no more data is available.

        An Error (or subclass) exception is raised if the previous call to .execute*()
        did not produce any result set or no call was issued yet.
        """

    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences
        (e.g. a list of tuples).

        An empty sequence is returned when no more rows are available. The number of
        rows to fetch per call is specified by the parameter. If it is not given,
        the cursor’s arraysize determines the number of rows to be fetched. The method
        should try to fetch as many rows as indicated by the size parameter.
        If this is not possible due to the specified number of rows not being available,
        fewer rows may be returned.

        An Error (or subclass) exception is raised if the previous call to .execute*()
        did not produce any result set or no call was issued yet.

        Note there are performance considerations involved with the size parameter.
        For optimal performance, it is usually best to use the .arraysize attribute.
        If the size parameter is used, then it is best for it to retain the same value
        from one .fetchmany() call to the next.
        """

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a sequence of
        sequences (e.g. a list of tuples).

        Note that the cursor’s arraysize attribute can affect the performance of this
        operation. An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        """

    def nextset(self):
        """
        This method will make the cursor skip to the next available set, discarding any
        remaining rows from the current set.
        (This method is optional since not all databases support multiple result sets)

        If there are no more sets, the method returns None. Otherwise, it returns a true
        value and subsequent calls to the .fetch*() methods will return rows from the
        next result set.

        An Error (or subclass) exception is raised if the previous call to .execute*()
        did not produce any result set or no call was issued yet.
        """

    def setinputsizes(self, sizes):
        """
        This can be used before a call to .execute*() to predefine memory areas for the
        operation’s parameters.

        sizes is specified as a sequence — one item for each input parameter. The item
        should be a Type Object that corresponds to the input that will be used, or it
        should be an integer specifying the maximum length of a string parameter. If the
        item is None, then no predefined memory area will be reserved for that column
        (this is useful to avoid predefined areas for large inputs).

        This method would be used before the .execute*() method is invoked.

        Implementations are free to have this method do nothing and users are free
        to not use it.
        """

    def setoutputsizes(self, size, column):
        """
        Set a column buffer size for fetches of large columns (e.g. LONGs, BLOBs, etc.).

        The column is specified as an index into the result sequence. Not specifying
        the column will set the default size for all large columns in the cursor.
        This method would be used before the .execute*() method is invoked.

        Implementations are free to have this method do nothing and users are free
        to not use it.
        """
