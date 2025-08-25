.. _Best Practices:

Best Practices
==============

This page explains how to use PyExasol with maximum efficiency.

Enable Compression for WiFi Connections
---------------------------------------
Wireless network bandwidth is usually the main bottleneck for laptops. ``Compression`` flag enables zlib compression both for common fetching and for :ref:`importing_and_exporting_data`. It may improve overall performance by a factor of 4-8x.

.. code-block:: python

    C = pyexasol.connect(... , compression=True)

Use HTTP Transport for Big Volumes of Data
------------------------------------------
It is okay to use common fetching for small data sets up to 1M of records.

For large data sets, you should always consider :ref:`variants` (``export_*`` and ``import_*`` functions). It scales well and prevents creation and destruction of intermediate Python objects.

.. code-block:: python

    pd = C.export_to_pandas('SELECT * FROM table')
    C.export_to_file('my_file.csv', 'SELECT * FROM table')

    C.import_from_pandas(pd, 'table')
    C.import_from_file('my_file.csv', 'table')

Prefer Iterator Syntax to Fetch Result Sets
-------------------------------------------
Iterator syntax is much shorter and easier to use. Also, there is no need to check for ``None`` or empty list ``[]`` to detect end of result set.

.. code-block:: python

    stmt = C.execute('SELECT * FROM table')

    for row in stmt:
        print(row)

Avoid Using INSERT Prepared Statement to Import Raw Values in SQL
-----------------------------------------------------------------
PyExasol supports INSERT prepared statements via ``.ext.insert_multi()`` function. It works for small data sets and may provide some performance benefits.

However, it is strongly advised to use more efficient ``IMPORT`` command and HTTP transport instead. It has a small overhead to initiate the communication, but large data sets will be transferred and processed much faster. It is also more CPU and memory efficient.

You may use ``import_from_iterable()`` to insert data from list of rows.

.. code-block:: python

    data = [
        (1, 'John'),
        (2, 'Gill'),
        (3, 'Ben')
    ]

    C.import_from_iterable(data, 'table')

Please note: if you want to INSERT single row only into Exasol, you're probably doing something wrong. It is advised to use row-based databases (MySQL, PostgreSQL, etc) to track status of ETL jobs, etc.

Always Specify Full Connection String for Exasol Cluster
--------------------------------------------------------
Unlike standard WebSocket Python driver, PyExasol supports full connection strings and node redundancy. For example, connection string `myexasol1..5:8563` will be expanded as:

::

    myexasol1:8563
    myexasol2:8563
    myexasol3:8563
    myexasol4:8563
    myexasol5:8563

PyExasol tries to connect to random node from this list. If it fails, it tries to connect to another random node. The main benefits of this approach are:

- Multiple connections are evenly distributed across the whole cluster;
- If one or more nodes are down, but the cluster is still operational due to redundancy, users will be able to connect without any problems or random error messages;

Consider Faster JSON-Parsing Libraries
--------------------------------------
PyExasol defaults to standard `json <https://docs.python.org/3/library/json.html>` library for best compatibility. It is sufficient for the majority of use-cases. However, if you are unhappy with HTTP transport, and you wish to load large amounts of data using standard fetching, we highly recommend trying faster JSON libraries.

`rapidjson <https://github.com/python-rapidjson/python-rapidjson>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Rapidjson provides significant performance boost and is well maintained by creators. PyExasol defaults to ``number_mode=NM_NATIVE``. Exasol server wraps big decimals with quotes and returns as strings, so it should be a safe option.

``json_lib=[rapidjson]``

``pip install pyexasol[rapidjson]``

`ujson  <https://github.com/ultrajson/ultrajson>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ujson provides great performance in our internal tests. It was abandoned by maintainers for a while, but now it is supported once again.

``json_lib=[ujson]``

``pip install pyexasol[ujson]``

`orjson  <https://github.com/ijl/orjson>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Orjson is the fastest modern JSON library.

``json_lib=[orjson]``

``pip install pyexasol[orjson]``

You may try any other json library. All you need to do is to overload ``_init_json()`` method in ``ExaConnection``.

Use ``.meta`` Functions to Perform Lock-Free Meta Data Requests
---------------------------------------------------------------
It is quite common for Exasol system views to become locked by DML statements, which prevents clients from retrieving metadata.

In order to mitigate this problem, Exasol provided special SQL hint described in `Snapshot Mode <https://docs.exasol.com/db/latest/database_concepts/snapshot_mode.htm>__` which is available in latest versions. It does not require user to enable "snapshot transaction" mode for the whole session. Currently this is the best way to access metadata using WebSocket protocol.

Also, it is possible to get SQL result set column structure without executing the actual query. This method relies on prepared statements and it is also free from locks.

Few examples:

.. code-block:: python

    # Get SQL result set column structure without executing the actual query
    C.sql_columns('SELECT user_id, user_name FROM users')

    # Get list of tables matching specified LIKE-pattern
    C.list_tables('MY_SCHEMA', 'USER_%')

    # Get list of views matching specified LIKE-pattern
    C.list_views('MY_SCHEMA', 'USER_VIEW_%')

Consider What Information is Logged from Exceptions
---------------------------------------------------------------
Depending on the initial query, sensitive information may be present in the returned exception.
It is the responsibility of the user to correctly handle this sensitive information, i.e. not save or pass it verbatim into their logging or other services.

What not to do:

.. code-block:: python

    try:
        stmt = C.execute('CREATE USER "MY_USER" IDENTIFIED BY "my_secret_password"')
        # create same user with different password; raises exception
        stmt = C.execute('CREATE USER "MY_USER" IDENTIFIED BY "my_secret_password2"')
    except pyexasol.ExaQueryError as e:
        logging.error(e)

>>>
ERROR:root:
(
    message     =>  user name MY_USER conflicts with another user or role name (Session: 1825312482065121280)
    dsn         =>  localhost:8563
    user        =>  sys
    schema      =>
    session_id  =>  1825312482065121280
    code        =>  42500
    # query contains sensitive information
    query       =>  CREATE USER "MY_USER" IDENTIFIED BY "my_secret_password"
)

What to do:

.. code-block:: python

    try:
        stmt = C.execute('CREATE USER "MY_USER" IDENTIFIED BY "my_secret_password"')
        stmt = C.execute('CREATE USER "MY_USER" IDENTIFIED BY "my_secret_password2"')
    except pyexasol.ExaQueryError as e:
        params = e.get_params_for_print()
        # Mindfully, select which parameter(s) you want to log or include in subsequently raised exceptions.
        logging.error(f"CREATE USER failed to execute in session_id={params['session_id']}")

>>>
ERROR:root:CREATE USER failed to execute in session_id=1825312482065121280
