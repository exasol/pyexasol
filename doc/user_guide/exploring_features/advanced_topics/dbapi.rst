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

PEP 249 was originally created for general purpose OLTP row store databases running on a
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

.. _WebSocket API data type definition: https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV3.md#data-types-type-names-and-properties

Fetching Data
^^^^^^^^^^^^^

The WebSocket DBAPI converts fetched values with the ``fetch_mapper`` connection
option. The Python types you receive are determined by the default mapper;
applications can replace it with a custom mapper.

Default Types
~~~~~~~~~~~~~

The default ``fetch_mapper`` is
:func:`pyexasol.data_types.convert_websocket_to_python`. It converts values to
Python types according to the result column's WebSocket metadata. To align more
closely with PEP 249, it returns values as Python types whenever possible. Some
values were already returned as Python types before version 3.0; version 3.0
made this behavior more consistent.

.. note::
    * ``NULL`` values are returned as ``None`` for all types.
    * Nanosecond precision in ``TIMESTAMP`` values is truncated to Python's
      microsecond precision.

.. list-table:: WebSocket DBAPI result value types
   :header-rows: 1

   * - Exasol type
     - Version < 3.0
     - Version >= 3.0
   * - ``BOOLEAN``
     - ``bool``
     - ``bool``
   * - ``CHAR``
     - ``str``
     - ``str``
   * - ``DATE``
     - ``datetime.date``
     - ``datetime.date``
   * - ``DECIMAL(p, 0)``
     - ``int``
     - ``int``
   * - ``DECIMAL(p, s)`` where ``s > 0``
     - ``str``
     - ``decimal.Decimal``
   * - ``DOUBLE``
     - ``float``
     - ``float``
   * - ``GEOMETRY``
     - ``str``
     - ``str``
   * - ``HASHTYPE``
     - ``str``
     - ``str``
   * - ``INTERVAL DAY TO SECOND``
     - ``str``
     - ``ExaTimeDelta``
   * - ``INTERVAL YEAR TO MONTH``
     - ``str``
     - ``str``
   * - ``TIMESTAMP``
     - ``str``
     - ``datetime.datetime``
   * - ``TIMESTAMP WITH LOCAL TIME ZONE``
     - ``str``
     - ``datetime.datetime``
   * - ``VARCHAR``
     - ``str``
     - ``str``

Custom Mapper
~~~~~~~~~~~~~

Instead of the default mapper described above, applications can provide a custom
mapper through the ``fetch_mapper`` connection option. The mapper receives two
positional arguments: the raw scalar value and the column's WebSocket ``dataType``
metadata. See the `WebSocket API data type definition`_. Use the metadata's
``type`` value to determine how to convert the value.

.. code-block:: python

    from pyexasol.data_types import WebSocketDataType

    def fetch_mapper(value, data_type):
        if data_type["type"] == WebSocketDataType.Decimal.value:
            return float(value)
        return value

.. note::
    When using a custom ``fetch_mapper``, the application is responsible for
    supporting the session date/time formats required by that mapper. The DBAPI
    only validates the session formats when the default mapper is active.

Inserting Data
^^^^^^^^^^^^^^

The WebSocket DBAPI converts and validates the parameter values passed to
``executemany`` with the ``insert_mapper`` connection option. The Python types
accepted per column type are determined by the default mapper; applications can
replace it with a custom mapper.

Default Types
~~~~~~~~~~~~~

The default ``insert_mapper`` is
:func:`pyexasol.data_types.convert_python_to_websocket`. It converts each value
according to the target column's WebSocket metadata. Before anything is sent to
the database, ``executemany`` checks every row and raises:

* a ``ProgrammingError`` if a row has a different number of values than the
  statement has columns. The error gives the row number and both counts.
* a ``DataError`` if a value's Python type is not accepted for its column type.
  The error gives the row number, the column location, the column type and the
  allowed types.

Both errors are raised for the first invalid entry only, so other invalid
entries are not reported. If you get one of these errors, check your data
directly to make sure that all rows have the expected number of values and
that each value has a type accepted for its column.

.. note::
    * ``None`` is accepted for all types.
    * Only the types are validated. Precision, scale and length are not checked
      and remain the responsibility of the database.
    * A naive ``datetime.datetime`` is assumed to be exactly the time the
      database should store and is inserted in ISO format. A timezone-aware
      ``datetime.datetime`` for a ``DATE``, ``TIMESTAMP`` or
      ``TIMESTAMP WITH LOCAL TIME ZONE`` column raises a ``DataError``, because
      these columns have no timezone and the offset cannot be sent. Convert the
      value explicitly first, e.g. with
      ``value.astimezone(datetime.UTC).replace(tzinfo=None)`` for a
      ``TIMESTAMP``.
    * Values bound to a ``VARCHAR`` column must be ``str``. Before version 4.0,
      any value was silently converted with ``str()``, which can hide a mismatch
      between the values and the columns they are inserted into. Convert values
      explicitly with ``str(...)`` where a string is desired.

.. list-table:: WebSocket DBAPI accepted parameter value types
   :header-rows: 1

   * - Exasol type
     - Version < 4.0
     - Version >= 4.0
   * - ``BOOLEAN``
     - ``bool``
     - ``bool``, ``int`` (``0``/``1``), ``str`` (e.g. ``'true'``)
   * - ``CHAR``
     - ``str``
     - ``str``
   * - ``DATE``
     - ``datetime.date``, ``datetime.datetime``, ``str``
     - ``datetime.date``, naive ``datetime.datetime``, ``str``
   * - ``DECIMAL(p, s)``
     - ``decimal.Decimal``, ``float``, ``int``, ``str``
     - ``decimal.Decimal``, ``float``, ``int``, ``str``
   * - ``DOUBLE``
     - any value accepted by ``float()``
     - ``float``, ``int``, ``decimal.Decimal``, ``str`` (parsable)
   * - ``GEOMETRY``
     - ``str``
     - ``str``
   * - ``HASHTYPE``
     - ``str``
     - ``str``
   * - ``INTERVAL DAY TO SECOND``
     - ``str``
     - ``datetime.timedelta``, ``ExaTimeDelta``, ``str``
   * - ``INTERVAL YEAR TO MONTH``
     - ``str``
     - ``str``
   * - ``TIMESTAMP``
     - ``datetime.datetime``, ``str``
     - naive ``datetime.datetime``, ``str``
   * - ``TIMESTAMP WITH LOCAL TIME ZONE``
     - ``datetime.datetime``, ``str``
     - naive ``datetime.datetime``, ``str``
   * - ``VARCHAR``
     - any value (converted with ``str()``)
     - ``str``

Custom Mapper
~~~~~~~~~~~~~

Instead of the default mapper described above, applications can provide a custom
mapper through the ``insert_mapper`` connection option. The mapper receives two
positional arguments: the Python parameter value and the column's WebSocket
``dataType`` metadata, and returns the value to send. See the
`WebSocket API data type definition`_. Use the metadata's ``type`` value to
determine how to convert the value.

.. code-block:: python

    from pyexasol.data_types import WebSocketDataType

    def insert_mapper(value, data_type):
        if data_type["type"] == WebSocketDataType.String.value:
            return str(value)
        return value

The mapper's output must still be a type accepted by
``validate_value_for_websocket`` for the column (e.g. ``float`` for ``DOUBLE``,
``str`` for ``VARCHAR``), otherwise a ``DataError`` is raised. This validation
gives more specific type errors than the database would.

.. note::
    When using a custom ``insert_mapper``, ``executemany`` no longer validates
    the session's date and timestamp formats. ``execute`` still validates them
    unless a custom ``fetch_mapper`` is also provided, so both mappers must be
    custom for the formats to be changeable. The application is then responsible
    for ensuring that the custom mappers support the session's formats.

Session Date/Time Formats
^^^^^^^^^^^^^^^^^^^^^^^^^

The default mappers depend on the session's ``NLS_DATE_FORMAT`` and
``NLS_TIMESTAMP_FORMAT``, which are listed in ``EXA_PARAMETERS``. The WebSocket
DBAPI therefore validates both values, but only when the relevant default mapper
is active:

* Before executing a user statement, if the default ``fetch_mapper`` is active.
* Before calling ``executemany``, if the default ``insert_mapper`` is active.

The DBAPI currently supports only the exact formats listed below. Format-model matching is
case-sensitive.

The supported values are represented by the following definitions in the
WebSocket-to-Python conversion module:

.. literalinclude:: ../../../../pyexasol/data_types/websocket_to_python.py
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
