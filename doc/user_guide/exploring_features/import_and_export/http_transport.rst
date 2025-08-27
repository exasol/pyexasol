.. _http_transport:

HTTP Transport
==============

The main purpose of HTTP transport is to reduce massive fetching overhead associated
with large data sets (1M+ rows). It uses native Exasol commands ``EXPORT`` and ``IMPORT``
specifically designed to move large amounts of data. Data is transferred using streamed
CSV file(s) with optional zlib compression. This is a powerful tool which helps to
bypass the creation of intermediate Python objects altogether and dramatically increases
performance.

PyExasol offloads HTTP communication and decompression to a separate thread using the
`threading`_ module. The main thread deals with a simple `pipe`_ opened in binary mode.
For more details, check out the implementation of :func:`pyexasol.http_transport`.


.. _threading: https://docs.python.org/3/library/threading.html
.. _pipe: https://docs.python.org/3/library/os.html#os.pipe

.. _http_transport_parallel:

Parallel
--------

.. note::
    Please check out the documentation :ref:`pyexasol_parallelism` for PyExasol.

It is possible to run :ref:`http_transport` in parallel. The workload may be
distributed across multiple CPU cores and even across multiple servers.

Overview of How it Works
^^^^^^^^^^^^^^^^^^^^^^^^

1. A parent process opens main connection to Exasol and spawns multiple child processes.
2. Each child process:

  * connects to an individual Exasol node using the :func:`pyexasol.http_transport` function
  * gets an internal Exasol address (``ipaddr:port/public_key`` string) using the ``.exa_address`` property
  * sends its internal Exasol address to the parent process

3. The parent process collects a list of internal Exasol addresses from its child
   processes and runs either `export_parallel()`` or ``import_parallel()`` function to execute SQL query.
4. Each child process runs a callback function and reads or sends a chunk of data from or to Exasol.
5. Parent process waits for the SQL query and child processes to finish.

.. image:: /_static/parallel_export.png

Please note that PyExasol does not provide any specific way to send internal Exasol
address strings from child processes to parent process. You are free to choose your own
way of inter-process communication. For example, you may use
`multiprocessing.Pipe <https://docs.python.org/3/library/multiprocessing.html?highlight=Pipes#exchanging-objects-between-processes>`__.

Examples
^^^^^^^^

- :ref:`b03_parallel_export.py <parallel_export>` for EXPORT;
- :ref:`b04_parallel_import.py <parallel_import>` for IMPORT;
- :ref:`b05_parallel_export_import.py <parallel_export_and_import>` for EXPORT followed by IMPORT using the same child processes;

Example of EXPORT query executed in Exasol
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is how the complete query looks from Exasol's perspective.

.. code-block:: sql

    EXPORT my_table INTO CSV
    AT 'http://27.1.0.30:33601' FILE '000.csv'
    AT 'http://27.1.0.31:41733' FILE '001.csv'
    AT 'http://27.1.0.32:45014' FILE '002.csv'
    AT 'http://27.1.0.33:42071' FILE '003.csv'
    AT 'http://27.1.0.34:36669' FILE '004.csv'
    AT 'http://27.1.0.35:36794' FILE '005.csv'
    WITH COLUMN HEADERS
    ;
