Examples
========

Preparation
-----------

Basic preparation steps are required to see examples in action.

1. Install PyEXASOL with *optional dependencies*.
2. Download `PyEXASOL source code <https://github.com/exasol/pyexasol/archive/master.zip>`_ and unzip it.
3. Make sure Exasol is installed and dedicated schema for testing is created. You may use free `Exasol Community Edition <https://www.exasol.com/portal/display/DOWNLOAD/Free+Trial>`_ for testing purposes.
4. Open ``/examples/`` directory and edit file ``_config.py``. Input your Exasol credentials.
5. Run script to prepare data set for testing:

    .. code-block:: bash

        python examples/a00_prepare.py

That's all. Now you may run examples in any order like common python scripts. E.g.:

    .. code-block:: bash

        python examples/a01_basic.py

Examples of core functions
--------------------------

Basic
+++++

.. literalinclude:: ../../examples/a01_basic.py
    :language: python
    :caption: minimal code to create connection and run query
    
Fetching Tuples
+++++++++++++++

.. literalinclude:: ../../examples/a02_fetch_tuple.py
    :language: python
    :caption: all methods of fetching result set returning tuples
    
Fetching Dictionaries
+++++++++++++++++++++

.. literalinclude:: ../../examples/a03_fetch_dict.py
    :language: python
    :caption: all methods of fetching result set returning dictionaries
    
Custom Data Type Mapper
+++++++++++++++++++++++

.. literalinclude:: ../../examples/a04_fetch_mapper.py
    :language: python
    :caption: apply custom data type mapper for fetching
    
SQL Formatting
++++++++++++++

.. literalinclude:: ../../examples/a05_formatting.py
    :language: python
    :caption: SQL text formatting
    
Transaction Management
++++++++++++++++++++++

.. literalinclude:: ../../examples/a06_transaction.py
    :language: python
    :caption: transaction management, autocommit
    
Error Handling
++++++++++++++

.. literalinclude:: ../../examples/a07_exceptions.py
    :language: python
    :caption: error handling for basic SQL queries
    
Extension Functions
+++++++++++++++++++

.. literalinclude:: ../../examples/a08_ext.py
    :language: python
    :caption: extension functions to help with common problems outside the scope of the database driver
    
Abort Query
+++++++++++

.. literalinclude:: ../../examples/a09_abort_query.py
    :language: python
    :caption: abort running query from a separate thread
    
Context Manager
+++++++++++++++

.. literalinclude:: ../../examples/a10_context_manager.py
    :language: python
    :caption: use WITH clause for ExaConnection and ExaStatement objects
    
Insert Multiple Rows
++++++++++++++++++++

.. literalinclude:: ../../examples/a11_insert_multi.py
    :language: python
    :caption: INSERT a small number of rows using prepared statements instead of HTTP transport
    
Metadata Requests
+++++++++++++++++

.. literalinclude:: ../../examples/a12_meta.py
    :language: python
    :caption: lock-free meta data requests
    
No-SQL Metadata
+++++++++++++++

.. literalinclude:: ../../examples/a13_meta_nosql.py
    :language: python
    :caption: no-SQL metadata commands introduced in Exasol v7.0+
    
Examples of HTTP transport
--------------------------

Pandas DataFrame
++++++++++++++++

.. literalinclude:: ../../examples/b01_pandas.py
    :language: python
    :caption: IMPORT / EXPORT to and from pandas.DataFrame
    
Other Methods
+++++++++++++

.. literalinclude:: ../../examples/b02_import_export.py
    :language: python
    :caption: other methods of IMPORT / EXPORT
    
Parallel Export
+++++++++++++++

.. literalinclude:: ../../examples/b03_parallel_export.py
    :language: python
    :caption: multi-process HTTP transport for EXPORT
    
Parallel Import
+++++++++++++++

.. literalinclude:: ../../examples/b04_parallel_import.py
    :language: python
    :caption: multi-process HTTP transport for IMPORT
    
Parallel Export/Import
++++++++++++++++++++++

.. literalinclude:: ../../examples/b05_parallel_export_import.py
    :language: python
    :caption: multi-process HTTP transport for EXPORT followed by IMPORT
    
HTTP Transport Errors
+++++++++++++++++++++

.. literalinclude:: ../../examples/b06_http_transport_errors.py
    :language: python
    :caption: various ways to break HTTP transport and handle errors
    
Examples of misc functions
--------------------------

Connection Redundancy
+++++++++++++++++++++

.. literalinclude:: ../../examples/c01_redundancy.py
    :language: python
    :caption: connection redundancy, handling of missing nodes
    
Edge Cases
++++++++++

.. literalinclude:: ../../examples/c02_edge_case.py
    :language: python
    :caption: storing and fetching biggest and smallest values for data types available in Exasol
    
DB-API 2.0 Compatibility
++++++++++++++++++++++++

.. literalinclude:: ../../examples/c03_db2_compat.py
    :language: python
    :caption: DB-API 2.0 compatibility wrapper
    
SSL Encryption
++++++++++++++

.. literalinclude:: ../../examples/c04_encryption.py
    :language: python
    :caption: SSL-encrypted WebSocket connection and HTTP transport
    
Custom Session Parameters
+++++++++++++++++++++++++

.. literalinclude:: ../../examples/c05_session_params.py
    :language: python
    :caption: passing custom session parameters client_name, client_version, etc.
    
Local Config File
+++++++++++++++++

.. literalinclude:: ../../examples/c06_local_config.py
    :language: python
    :caption: connect using local config file
    
Profiling
+++++++++

.. literalinclude:: ../../examples/c07_profiling.py
    :language: python
    :caption: last query profiling
    
Snapshot Transactions
+++++++++++++++++++++

.. literalinclude:: ../../examples/c08_snapshot_transactions.py
    :language: python
    :caption: snapshot transactions mode, which may help with metadata locking problems
    
UDF Script Output
+++++++++++++++++

.. literalinclude:: ../../examples/c09_script_output.py
    :language: python
    :caption: run query with UDF script and capture output (may not work on a local laptop)
    
Class Extension
+++++++++++++++

.. literalinclude:: ../../examples/c10_overload.py
    :language: python
    :caption: extend core PyEXASOL classes to add custom logic
    
Quoted Identifiers
++++++++++++++++++

.. literalinclude:: ../../examples/c11_quote_ident.py
    :language: python
    :caption: enable quoted identifiers for import_*, export_* and other relevant functions
    
Thread Safety
+++++++++++++

.. literalinclude:: ../../examples/c12_thread_safety.py
    :language: python
    :caption: built-in protection from accessing connection object from multiple threads simultaneously
    
DSN Parsing
+++++++++++

.. literalinclude:: ../../examples/c13_dsn_parsing.py
    :language: python
    :caption: parsing of complex connection strings and catching relevant exceptions
    
HTTP Proxy
++++++++++

.. literalinclude:: ../../examples/c14_http_proxy.py
    :language: python
    :caption: connection via HTTP proxy
    
Garbage Collection
++++++++++++++++++

.. literalinclude:: ../../examples/c15_garbage_collection.py
    :language: python
    :caption: detect potential garbage collection problems due to cross-references
    
Examples of JSON libraries used for fetching
--------------------------------------------

RapidJSON
+++++++++

.. literalinclude:: ../../examples/j01_rapidjson.py
    :language: python
    :caption: JSON library rapidjson
    
UJSON
+++++

.. literalinclude:: ../../examples/j02_ujson.py
    :language: python
    :caption: JSON library ujson
    
ORJSON
++++++

.. literalinclude:: ../../examples/j03_orjson.py 
    :language: python
    :caption: JSON library orjson
