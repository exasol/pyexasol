Executing SQL Scripts
=====================

Use :meth:`pyexasol.ExaConnection.execute_sql_script` to execute a string containing
multiple SQL statements.

.. code-block:: python

    statements = connection.execute_sql_script("""
        CREATE OR REPLACE TABLE example (value INT);
        INSERT INTO example VALUES 1;
        INSERT INTO example VALUES 2;
        SELECT COUNT(*) FROM example;
    """)

    row_count = statements[-1].fetchval()

The method executes the statements sequentially and returns a list of
:class:`pyexasol.ExaStatement` objects in execution order. Each returned statement
represents an already executed statement. You can fetch result rows from statements
that produce a result set or inspect row counts for DML statements.

Script Splitting Rules
----------------------

Statements are separated by semicolons. Semicolons inside the following constructs
do not split statements:

* string literals
* quoted SQL identifiers
* line comments
* block comments
* Exasol script bodies

Exasol script bodies are terminated by a standalone ``/`` line.

.. code-block:: python

    connection.execute_sql_script("""
        CREATE OR REPLACE LUA SCALAR SCRIPT answer()
            RETURNS INT AS

        function run(ctx)
            local value = 1;
            return value + 41;
        end
        /

        SELECT answer();
    """)

Limitations
-----------

SQL scripts do not support query parameters. Use :meth:`pyexasol.ExaConnection.execute`
for parameterized single statements.

If one statement fails, the original exception is raised and the remaining statements
are not executed. Statements executed before the failure are not rolled back
automatically, and their :class:`pyexasol.ExaStatement` objects are not returned.

Inside Exasol script bodies, a standalone ``/`` line terminates the body even if the
line appears inside a language-specific string literal or expression. This matches
the delimiter convention used by Exasol SQL clients.
