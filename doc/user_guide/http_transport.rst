.. _http_transport:

HTTP Transport
==============

The main purpose of HTTP transport is to reduce massive fetching overhead associated with large data sets (1M+ rows). It uses native Exasol commands `EXPORT` and `IMPORT` specifically designed to move large amounts of data. Data is transferred using CSV format with optional zlib compression.

This is a powerful tool which helps to bypass the creation of intermediate Python objects altogether and dramatically increases performance.

PyExasol offloads HTTP communication and decompression to a separate thread using the `threading`_ module. The main thread deals with a simple `pipe`_ opened in binary mode.

You may specify a custom `callback` function to read or write from the raw pipe and to apply complex logic. Use `callback_params` to pass additional parameters to the `callback` function (e.g. options for pandas).

You may also specify `import_params` or `export_params` to alter the `IMPORT` or `EXPORT` query and modify the CSV data stream.

.. _threading: https://docs.python.org/3/library/threading.html
.. _pipe: https://docs.python.org/3/library/os.html#os.pipe

For further details, see:

- `EXPORT <https://docs.exasol.com/db/latest/sql/export.htm>`_
- `IMPORT <https://docs.exasol.com/db/latest/sql/import.htm>`_
- `CHANGELOG: TLS Certificate Verification for Loader File Connections <https://exasol.my.site.com/s/article/Changelog-content-16273>`_

Pre-defined Functions
---------------------

Using pandas
^^^^^^^^^^^^

Export
""""""""""""""""

Export data from Exasol into a `pandas.DataFrame`. You may use the `callback_params` argument to pass custom options for the pandas `read_csv`_ function.

.. _read_csv: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

.. code-block:: python

    # Read from SQL
    pd = C.export_to_pandas("SELECT * FROM users")

    # Read from table
    pd = C.export_to_pandas("users")

Import
""""""

Import data from `pandas.DataFrame` into an Exasol table. You may use the `callback_params` argument to pass custom options for the pandas `to_csv`_ function.

.. _to_csv: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html

.. code-block:: python

    C.import_from_pandas(pd, "users")

Using parquet
^^^^^^^^^^^^^

Import
""""""

Import data from parquet files (`pyarrow.parquet.Table`) into an Exasol table. You may use the `callback_params` argument to pass custom options for the pyarrow.csv `WriteOptions`_ class.

.. _WriteOptions: https://arrow.apache.org/docs/python/generated/pyarrow.csv.WriteOptions.html

.. code-block:: python

    C.import_from_parquet("<local_path>/*.parquet", "users")

Using an Iterable
^^^^^^^^^^^^^^^^^

Import from a List
""""""""""""""""""

.. code-block:: python

    my_list = [
        (1, 'Bob', False, '2018-01-01'),
        (2, 'Gill', True, '2018-02-01'),
    ]

    C.import_from_iterable(my_list, "users")

Import from a Generator
"""""""""""""""""""""""

This function is suitable for very big INSERTS as long as the generator returns rows one-by-one and does not run out of memory.

.. code-block:: python

    def my_generator():
        for i in range(5):
            yield (i, 'Bob', True, '2017-01-01')

    C.import_from_iterable(my_generator(), "users")

Using a File
^^^^^^^^^^^^

Export
""""""

Export data from Exasol into a file, path object, or file-like object opened in binary mode. You may export to process `STDOUT` using `sys.stdout.buffer`.

.. code-block:: python

    # Export from file defined with string path
    C.export_to_file('my_file.csv', "users")

    # Export into STDOUT
    C.export_to_file(sys.stdout.buffer, "users")

Import
""""""

Import data from a file, path object, or file-like object opened in binary mode. You may import from process `STDIN` using `sys.stdin.buffer`.

.. code-block:: python

    # Import from file defined with string path
    C.import_from_file('/test/my_file.csv', "users")

    # Import from path object
    C.import_from_file(pathlib.Path('/test/my_file.csv'), "users")

    # Import from opened file
    file = open('/test/my_file.csv', 'rb')
    C.import_from_file(file, "users")
    file.close()

    # Import from STDIN
    C.import_from_file(sys.stdin.buffer, "users")


Parameters
----------

Please refer to the Exasol User Manual to learn more about `IMPORT` and `EXPORT` parameters.

import_params
^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1

   * - Name
     - Example
     - Description
   * - `column_separator`
     - `,`
     - Column separator for CSV
   * - `column_delimiter`
     - `"`
     - Column delimiter for CSV (quoting)
   * - `columns`
     - `['id', 'name']`
     - List of table columns in the data source, useful if the column order of data source does not match the column order of Exasol table
   * - `csv_cols`
     - `["1..5", "6 FORMAT='999.99'", "8"]`
     - List of CSV columns with optional `numeric`_ or `date`_ formats
   * - `row_separator`
     - `LF`
     - Row separator for CSV (line-endings)
   * - `encoding`
     - `UTF8`
     - File encoding
   * - `with_column_names`
     - `True`
     - Add column names as the first line, useful for Pandas
   * - `null`
     - `\N`
     - Custom `NULL` value
   * - `delimit`
     - `AUTO`
     - Delimiter mode: `AUTO`, `ALWAYS`, `NEVER`
   * - `format`
     - `gz`
     - Import file or stream compressed with `gz`, `bzip2`, `zip`
   * - `comment`
     - `This is a query description`
     - Add a comment before the beginning of the query

.. _numeric: https://docs.exasol.com/db/latest/sql_references/formatmodels.htm#Numericformatmodels
.. _date: https://docs.exasol.com/db/latest/sql_references/formatmodels.htm#Datetimeformatmodels

export_params
^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1

   * - Name
     - Example
     - Description
   * - `column_separator`
     - `,`
     - Column separator for CSV
   * - `column_delimiter`
     - `"`
     - Column delimiter for CSV (quoting)
   * - `columns`
     - `['id', 'name']`
     - List of table columns, useful to reorder table columns during export from table
   * - `csv_cols`
     - `["1..5", "6 FORMAT='999.99'", "8"]`
     - List of CSV columns with optional `numeric`_ or `date`_ formats
   * - `row_separator`
     - `LF`
     - Row separator for CSV (line-endings)
   * - `encoding`
     - `UTF8`
     - File encoding
   * - `skip`
     - `1`
     - How many first rows to skip, useful for skipping header
   * - `null`
     - `\N`
     - Custom `NULL` value
   * - `trim`
     - `TRIM`
     - Trim mode: `TRIM`, `RTRIM`, `LTRIM`
   * - `format`
     - `gz`
     - Export file or stream compressed with `gz`, `bzip2`, `zip`
   * - `comment`
     - `This is a query description`
     - Add a comment before the beginning of the query

The `comment` parameter
^^^^^^^^^^^^^^^^^^^^^^^

For any `export_*` or `import_*` call, you can add a comment that will be inserted before the beginning of the query.

This can be used for profiling and auditing. Example:

.. code-block:: python

    C.import_from_file('/test/my_file.csv', 'users', import_params={'comment': '''
    This comment will be inserted before the query.
    This query is importing user from CSV.
    '''})

The comment is inserted as a block comment (`/* <comment> */`). Thus, the block comment closing sequence (`*/`) is forbidden in the provided comment.

Write Custom EXPORT / IMPORT Functions
--------------------------------------

A full collection of pre-defined callback functions is available in ``callback.py`` module.

Example of a callback exporting into a basic Python list.

.. code-block:: python

    # Define callback function
    def export_to_list(pipe, dst, **kwargs):
        wrapped_pipe = io.TextIOWrapper(pipe, newline='\n')
        reader = csv.reader(wrapped_pipe, lineterminator='\n', **kwargs)

        return [row for row in reader]

    # Run EXPORT using the defined callback function
    C.export_to_callback(export_to_list, None, 'my_table')

Example of a callback importing from pandas into an Exasol table.

.. code-block:: python

    df = <pandas.DataFrame>

    def import_from_pandas(pipe, src, **kwargs):
        wrapped_pipe = io.TextIOWrapper(pipe, newline='\n')
        return src.to_csv(wrapped_pipe, header=False, index=False, quoting=csv.QUOTE_NONNUMERIC, **kwargs)

    # Run IMPORT using the defined callback function
    C.export_from_callback(import_from_pandas, df, 'my_table')
