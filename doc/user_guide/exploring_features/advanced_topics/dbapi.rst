Using DB-API 2.0
================

PyExasol provides a `PEP 249-compatible DB-API 2.0
<https://peps.python.org/pep-0249/>`__ adapter in the ``exasol.driver.websocket.dbapi2``
module. The adapter does not strictly follow the specification. This page
introduces the adapter, explains the reasons for its intentional differences,
and describes alternative drivers when they are a better fit for your application.

The `SQLAlchemy-Exasol dialect <https://github.com/exasol/sqlalchemy-exasol>`__
uses this adapter to connect SQLAlchemy applications to Exasol.

Alternatives
------------

Exasol WebSocket Driver
^^^^^^^^^^^^^^^^^^^^^^^

The ``pyexasol`` package includes this DB-API 2.0 adapter in the
``exasol.driver.websocket.dbapi2`` module. However, using ``pyexasol``
directly will generally yield better performance when utilizing Exasol in an
OLAP manner, which is likely the typical use case.

In specific scenarios, the DB-API 2.0 interface can be advantageous or even
necessary, particularly when integrating with database-agnostic frameworks.

TurboODBC
^^^^^^^^^

`TurboODBC <https://github.com/blue-yonder/turbodbc>`__ offers an alternative
ODBC-based, DBAPI2-compatible driver, which supports the Exasol database.

Pyodbc
^^^^^^

`Pyodbc <https://github.com/mkleehammer/pyodbc>`__ provides an ODBC-based,
DBAPI2-compatible driver. For further details, please refer to the
`wiki <https://github.com/mkleehammer/pyodbc/wiki>`__.

Rationale
---------

PEP-249 was originally created for general purpose OLTP row store databases running on a
single server: SQLite, MySQL, PostgreSQL, MSSQL, Oracle, etc.

It does not work very well for OLAP columnar databases (like Exasol) running on multiple
servers because it was never designed for this purpose. Despite both OLTP DBMS and OLAP
DBMS using SQL for communication, the foundation and usage patterns are completely
different.

When people use DB-API 2.0 drivers, they tend to skip manuals and automatically apply
OLTP usage patterns without even realizing how much they lose in terms of performance
and efficiency.

A good example is `TurboODBC <https://github.com/blue-yonder/turbodbc>`__. Very few
know that it is possible to fetch data as
`NumPy arrays <https://turbodbc.readthedocs.io/en/latest/pages/advanced_usage.html#numpy-support>`__
and as `Apache Arrow <https://turbodbc.readthedocs.io/en/latest/pages/advanced_usage.html#apache-arrow-support>`__.

Minor intentional incompatibilities with DB-API 2.0 force users to look through the
manual and to learn about :ref:`Best Practices` of getting the job done.

Usage Notes
-----------

.. code-block:: python

    from exasol.driver.websocket.dbapi2 import connect

    connection = connect(dsn='', username='sys', password='exasol', schema='TEST')

    with connection.cursor() as cursor:
        cursor.execute("SELECT 1;")

See the :ref:`Using DB-API 2.0 <using_dbapi2_example>` example for a longer usage example.

Session Date/Time Formats
^^^^^^^^^^^^^^^^^^^^^^^^^

Before executing a user statement, the WebSocket DBAPI checks the active session
values of ExaParameters``NLS_DATE_FORMAT`` and ``NLS_TIMESTAMP_FORMAT``. The DBAPI
currently supports only the exact formats listed below. Format-model matching is
case-sensitive.

The supported values are represented by the following definitions in the DBAPI
connection implementation:

.. literalinclude:: ../../../../exasol/driver/websocket/_connection.py
   :language: python
   :start-after: # Start: supported session date/time formats included in the DBAPI documentation.
   :end-before: # End: supported session date/time formats included in the DBAPI documentation.

Statements containing ``ALTER SESSION`` are allowed so applications can change
these settings. If either format is unsupported, execution raises one
``InterfaceError`` containing all invalid parameters, their supported formats,
and an example ``ALTER SESSION`` statement showing where to insert a supported
format.

Exasol Specific Problems with DB-API 2.0
----------------------------------------

- Default ``autocommit=off`` prevents indexes from being stored permanently on disk for ``SELECT`` statements;
- Default ``autocommit=off`` may hold transaction for a long time (e.g., opened connection in an IPython notebook);
- Python object creation and destruction overhead is very significant when you process large amounts of data;
- Functions ``fetchmany()`` and ``executemany()`` have significant additional overhead related to JSON serialization;
- Exasol WebSocket protocol provides more information about columns than is normally available in ``.description`` property of ``cursor``;

We also wanted to discourage:

- "Drop-in" replacements of other Exasol drivers without reading the manual;
- Usage of OLTP-oriented ORM (e.g., SQLAlchemy, Django);

Unlike common OLTP databases, each OLAP database is very unique. It is important to understand implementation details and features of the specific database and to build applications around those features. Generalization of any kind and the "copy-paste" approach may lead to abysmal performance in trivial cases.

Ideas for Migration
-------------------

Find ``cursor()`` calls:

.. code-block:: python

    cur = C.cursor()
    cur.execute('SELECT * FROM table')
    data = cur.fetchall()

Replace with:

.. code-block:: python

    st = C.execute('SELECT * FROM table')
    data = st.fetchall()

Find ``.description``

.. code-block:: python

    columns = list(map(str.lower, next(zip(*cur.description))))

Replace with:

.. code-block:: python

    columns = st.column_names()

Find all reads into pandas:

.. code-block:: python

    cur.execute('SELECT * FROM table')
    pandas.DataFrame(cur.fetchall(), columns=columns)

Replace with:

.. code-block:: python

    C.export_to_pandas('SELECT * FROM table')
