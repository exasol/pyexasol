Getting Started
===============

Welcome to PyExasol
-------------------

PyExasol is the officially supported Python connector that revolutionizes how users
interact with Exasol databases. This powerful tool is specifically designed to handle
massive volumes of data with efficiency, providing a performance boost over traditional ODBC/JDBC solutions.

Why Choose PyExasol?
--------------------
* Easy and fast access to Exasol from Python
* Bulk import and export from/to pandas and polars to Exasol
* Exasol UDF debugging support

Prerequisites
-------------

- Exasol >= 7.1
- Python >= 3.9

.. _optional_dependencies:

Optional Dependencies
^^^^^^^^^^^^^^^^^^^^^

- ``orjson`` is required for ``json_lib=orjson`` to improve JSON parsing performance
- ``pandas`` is required for :ref:`http_transport` functions working with :class:`pandas.DataFrame`
- ``polars`` is required for :ref:`http_transport` functions working with :class:`polars.DataFrame`
- ``pproxy`` is used in the :ref:`examples` to test an HTTP proxy
- ``rapidjson`` is required for ``json_lib=rapidjson`` to improve JSON parsing performance
- ``ujson`` is required for ``json_lib=ujson`` to improve JSON parsing performance


Installing
----------

`PyExasol is distributed through PyPI <https://pypi.org/project/pyexasol/>`__. It can be installed via pip, poetry, or any other compatible dependency management tool:

.. code-block:: bash

   pip install pyexasol

To install with optional dependencies, use:

.. code-block:: bash

   pip install pyexasol[<optional-package-name>]

For a list of optional dependencies, see :ref:`optional_dependencies`.

First Steps
-----------

For a user's first steps, it is recommended to try out running basic queries and exporting data from an Exasol table well-known Python packages, like pandas or polars.

.. note::
    These examples are written assuming a newly installed or otherwise safe-to-test
    Exasol database. If that is not the case, it is recommended, in particular with the
    export examples, to check the :ref:`API` and your to-be-queried table to ensure that
    the output you will receive is as desired (and not, i.e. millions of rows).

Run basic query
^^^^^^^^^^^^^^^
.. note::
    For more options when running a basic query, check out :class:`pyexasol.ExaStatement`,
    which is the returned object from :func:`C.execute()`.


.. code-block:: python

    import pyexasol

    C = pyexasol.connect(dsn='<host:port>', user='sys', password='exasol')
    stmt = C.execute("SELECT * FROM EXA_ALL_USERS")

    # to fetch 1 row
    print(stmt.fetchone())

    # to fetch n=3 rows
    print(stmt.fetchmany(3))

    # to fetch all remaining rows
    print(stmt.fetchall())

    C = pyexasol.connect(dsn='<host:port>', user='sys', password='exasol')
    stmt = C.execute("SELECT * FROM EXA_ALL_USERS")

    # to iterate through all rows
    for row in stmt:
        print(row)

Export data into a DataFrame
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using pandas
""""""""""""

.. code-block:: python

    # pip install pyexasol[pandas]
    import pyexasol

    C = pyexasol.connect(dsn='<host:port>', user='sys', password='exasol', compression=True)
    df = C.export_to_pandas("SELECT * FROM EXA_ALL_USERS")
    print(df.head())

Using polars
""""""""""""

.. code-block:: python

    # pip install pyexasol[polars]
    import pyexasol

    C = pyexasol.connect(dsn='<host:port>', user='sys', password='exasol', compression=True)
    df = C.export_to_polars("SELECT * FROM EXA_ALL_USERS")
    print(df.head())

Diving Deeper
-------------

The PyExasol documentation covers many topics at different levels of experience:

* For configuring usage of PyExasol, see :ref:`configuration`.
* For more useful starting tips and examples, see :ref:`exploring_features` and, in particular, the :ref:`examples` page.
* For an overview of the API, check out the :ref:`API` page.
* As a user's needs with PyExasol become more advanced, check out the :ref:`advanced_topics`.
