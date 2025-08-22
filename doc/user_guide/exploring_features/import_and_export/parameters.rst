.. _parameters:

Exasol Parameters
=================

* The :ref:`import_params` are defined for all ``import_from_*`` :ref:`Variants` and
  to generate the ``IMPORT`` statement.
* The :ref:`export_params`, are defined for all ``export_to_*`` :ref:`Variants`, and
  to generate the ``EXPORT`` statement.

Please refer to the Exasol User Manual to learn more about:

* `IMPORT query parameters <https://docs.exasol.com/db/latest/sql/import.htm>`
* `EXPORT query parameters <https://docs.exasol.com/db/latest/sql/export.htm>`
* `CHANGELOG: TLS Certificate Verification for Loader File Connections <https://exasol.my.site.com/s/article/Changelog-content-16273>`_

.. _import_params:

``import_params``
-----------------

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
     - Add column names as the first line, which may be useful for external APIs (e.g. pandas)
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


.. _export_params:

``export_params``
-----------------

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

The `comment` parameter, for adding comments to queries
-------------------------------------------------------

For any `export_*` or `import_*` call, you can add a comment that will be inserted before the beginning of the query.

This can be used for profiling and auditing. Example:

.. code-block:: python

    C.import_from_file('/test/my_file.csv', 'users', import_params={'comment': '''
    This comment will be inserted before the query.
    This query is importing user from CSV.
    '''})

The comment is inserted as a block comment (`/* <comment> */`). The block comment closing sequence (`*/`) is forbidden in the comment.
