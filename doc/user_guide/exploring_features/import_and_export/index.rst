.. _importing_and_exporting_data:

Importing and Exporting Data
============================

.. toctree::
    :maxdepth: 2

    parameters
    http_transport


PyExasol provides many :ref:`variants` to import data to and export
data from an Exasol database. To provide a consistent API, these variant have simple
interfaces that follow the same pattern:

* utilize the same inputs for generating the ``IMPORT`` and ``EXPORT`` statements.
  See :ref:`parameters`.
* use the :ref:`http_transport` to reduce the fetching overhead associated with large
  data sets.
* when the functionality of an external API is used (e.g. pandas), the variant will have
  an argument ``callback_params`` so that a user can define custom parameters to the
  required transformation function.

For more advanced users, check out the documentation on :ref:`http_transport_parallel`
to parallelize importing or exporting data.

.. _variants:

Variants
--------
File
^^^^^

Export
""""""
See :meth:`pyexasol.ExaConnection.export_to_file`. This method supports exporting data
from an Exasol database into a file, path object, file-like object opened in binary
mode, or to a process ``STDOUT`` using ``sys.stdout.buffer``.

.. code-block:: python

    # Export from file defined with string path
    C.export_to_file('my_file.csv', "users")

    # Export into STDOUT
    C.export_to_file(sys.stdout.buffer, "users")


Import
""""""
See :meth:`pyexasol.ExaConnection.import_from_file`. This method supports importing
data from a file, path object, file-like object opened in binary mode, or from a process
``STDIN`` using ``sys.stdin.buffer``.

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


Iterable
^^^^^^^^

Export
""""""
See :meth:`pyexasol.ExaConnection.export_to_list`.

.. code-block:: python

    # Read from SQL
    export_list = C.export_to_list("SELECT * FROM users")

    # Read from table
    export_list = C.export_to_list("users")


Import
""""""
See :meth:`pyexasol.ExaConnection.import_from_iterable`.

.. code-block:: python

        my_list = [
        (1, 'Bob', False, '2018-01-01'),
        (2, 'Gill', True, '2018-02-01'),
    ]

    C.import_from_iterable(my_list, "users")

    # This is suitable for very big INSERTS as long as the generator returns rows
    # one-by-one and does not run out of memory.

    def my_generator():
        for i in range(5):
            yield (i, 'Bob', True, '2017-01-01')

    C.import_from_iterable(my_generator(), "users")

.. _pandas_export_import:

Pandas
^^^^^^

Export
""""""
See :meth:`pyexasol.ExaConnection.export_to_pandas`.

.. code-block:: python

    # Read from SQL
    pd = C.export_to_pandas("SELECT * FROM users")

    # Read from table
    pd = C.export_to_pandas("users")


Import
""""""
See :meth:`pyexasol.ExaConnection.import_from_pandas`.

.. code-block:: python

    C.import_from_pandas(pd, "users")

.. _parquet_export_import:

Parquet
^^^^^^^

Import
""""""
See :meth:`pyexasol.ExaConnection.import_from_parquet`.

.. code-block:: python

    from pathlib import Path

    # list[Path]: list of specific parquet files to load
    C.import_from_parquet([Path("local_path/test.parquet"], "users")

    # Path: can be either a file or directory. If it's a directory,
    # all files matching this pattern *.parquet will be processed.
    C.import_from_parquet(Path("local_path/test.parquet", "users")

    # string: representing a filepath which already contains a glob pattern
    C.import_from_parquet("local_path/*.parquet", "users")

.. _polars_export_import:

Polars
^^^^^^

Export
""""""
See :meth:`pyexasol.ExaConnection.export_to_polars`.

.. code-block:: python

    # Read from SQL
    df = C.export_to_polars("SELECT * FROM users")

    # Read from table
    df = C.export_to_polars("users")


Import
""""""
See :meth:`pyexasol.ExaConnection.import_from_polars`.

.. code-block:: python

    C.import_from_polars(df, "users")


Write a Custom Variant
^^^^^^^^^^^^^^^^^^^^^^

.. note::
    A full collection of pre-defined callback functions is available in the
    :py:mod:`pyexasol.callback` module. Their usage by the
    :class:`pyexasol.ExaConnection` class is documented in the :ref:`Variants` section.

Export
""""""

Example of a callback exporting into a basic Python list.

.. code-block:: python

    # Define callback function
    def export_to_list(pipe, dst, **kwargs):
        wrapped_pipe = io.TextIOWrapper(pipe, newline='\n')
        reader = csv.reader(wrapped_pipe, lineterminator='\n', **kwargs)

        return [row for row in reader]

    # Run EXPORT using the defined callback function
    C.export_to_callback(export_to_list, None, 'my_table')


Import
""""""
Example of a callback importing from Pandas into Exasol.

.. code-block:: python

    df = <pandas.DataFrame>

    def import_from_pandas(pipe, src, **kwargs):
        wrapped_pipe = io.TextIOWrapper(pipe, newline='\n')
        return src.to_csv(wrapped_pipe, header=False, index=False, quoting=csv.QUOTE_NONNUMERIC, **kwargs)

    # Run IMPORT using the defined callback function
    C.import_from_callback(import_from_pandas, df, 'my_table')
