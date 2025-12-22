.. _parameters:

Exasol Parameters
=================

* The :ref:`export_params`, are defined for all ``export_to_*`` :ref:`Variants`
  to generate the ``EXPORT`` statement.
* The :ref:`import_params` are defined for all ``import_from_*`` :ref:`Variants`
  to generate the ``IMPORT`` statement.

Please refer to the Exasol User Manual to learn more about:

* `EXPORT query parameters <https://docs.exasol.com/db/latest/sql/export.htm>`__
* `IMPORT query parameters <https://docs.exasol.com/db/latest/sql/import.htm>`__
* `CHANGELOG: TLS Certificate Verification for Loader File Connections <https://exasol.my.site.com/s/article/Changelog-content-16273>`__

.. _export_params:

``export_params``
-----------------

These parameters are given as a dictionary, like ``export_params = { "name1": "value1", "name2": ["value2", "value3"]}``.
This dictionary is passed into the :ref:`Variants` for a selected ``export_to_*``
method. When the code is executed, these are validated using the :class:`pyexasol.http_transport.ExportQuery`.


.. list-table::
   :header-rows: 1

   * - Name
     - Example
     - Description
   * - column_delimiter
     - "
     - Column delimiter for CSV (quoting)
   * - column_separator
     - ,
     - Column separator for CSV
   * - columns
     - ["id", "name"]
     - List of table columns, useful to reorder table columns during export from table
   * - comment
     - This is a query description
     - Add a comment before the beginning of the query. See :ref:`comment`.
   * - csv_cols
     - ["1..5", "6 FORMAT='999.99'", "8"]
     - List of CSV columns with optional `numeric`_ or `date`_ formats
   * - delimit
     - AUTO
     - Delimiter mode: ``AUTO``, ``ALWAYS``, ``NEVER``
   * - encoding
     - UTF8
     - File encoding
   * - format
     - gz
     - Export file or stream compressed with ``gz``, ``bzip2``, ``zip``
   * - null
     - \N
     - Custom `NULL` value
   * - row_separator
     - LF
     - Row separator for CSV (line-endings)
   * - with_column_names
     - True
     - Add column names as the first line, which may be useful for external APIs (e.g. pandas).
       The default value for this is False, except for `export_to_pandas`,
       `export_to_parquet`, and `export_to_polars` where it is set to True.

.. _import_params:

``import_params``
-----------------

These parameters are given as a dictionary, like ``import_params = { "name1": "value1", "name2": ["value2", "value3"]}``.
This dictionary is passed into the :ref:`Variants` for a selected ``import_from_*``
method. When the code is executed, these are validated using the :class:`pyexasol.http_transport.ImportQuery`.

.. list-table::
   :header-rows: 1

   * - Name
     - Example
     - Description
   * - column_delimiter
     - "
     - Column delimiter for CSV (quoting)
   * - column_separator
     - ,
     - Column separator for CSV
   * - columns
     - ["id", "name"]
     - List of table columns in the data source, useful if the column order of data source does not match the column order of Exasol table
   * - comment
     - This is a query description
     - Add a comment before the beginning of the query.  See :ref:`comment`.
   * - csv_cols
     - ["1..5", "6 FORMAT='999.99'", "8"]
     - List of CSV columns with optional `numeric`_ or `date`_ formats
   * - encoding
     - UTF8
     - File encoding
   * - format
     - gz
     - Import file or stream compressed with ``gz``, ``bzip2``, ``zip``
   * - null
     - \N
     - Custom NULL value
   * - row_separator
     - LF
     - Row separator for CSV (line-endings)
   * - skip
     - 1
     - How many first rows to skip, useful for skipping header
   * - trim
     - TRIM
     - Trim mode: ``TRIM``, ``RTRIM``, ``LTRIM``


.. _numeric: https://docs.exasol.com/db/latest/sql_references/formatmodels.htm#Numericformatmodels
.. _date: https://docs.exasol.com/db/latest/sql_references/formatmodels.htm#Datetimeformatmodels


.. _comment:

Comment
-------

For any ``export_*`` or ``import_*`` call, you can add a comment that will be inserted before the beginning of the query.

This can be used for profiling and auditing. Example:

.. code-block:: python

    C.import_from_file('/test/my_file.csv', 'users', import_params={'comment': '''
    This comment will be inserted before the query.
    This query is importing user from CSV.
    '''})

The comment is inserted as a block comment (``/* <comment> */``). The block comment closing sequence (``*/``) is forbidden in the comment.
