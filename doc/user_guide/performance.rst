Performance Tests
=================

Performance of database drivers depends on many factors. Results may vary depending on hardware, network, settings and data set properties. I strongly suggest making your own performance tests before making any important decisions.

In this sample test I want to compare:

- `PyODBC <https://github.com/mkleehammer/pyodbc>`_
- `TurbODBC <https://github.com/blue-yonder/turbodbc>`_
- PyEXASOL

I use Badoo production Exasol cluster for testing:

- 20 nodes
- 800+ CPU cores with hyper-threading
- 14 Tb of RAM
- 10 Gb private network connections
- 1 Gb public network connections

I run three different types of tests:

- Fetching "low random" data set using server in the same data center
- Fetching "high random" data set using server in the same data center
- Fetching data set using local laptop behind VPN and Wifi network (slow network)

I use the default number of rows in the test table: 10 million rows, mixed data types.

I measure total rounded execution time in seconds using the `time` command in bash.

Results
-------

.. list-table::
   :header-rows: 1

   * - Test
     - Low random
     - High random
     - Slow network
   * - PyODBC - fetchall
     - 106
     - 107
     - -
   * - TurbODBC - fetchall
     - 56
     - 55
     - -
   * - PyEXASOL - fetchall
     - 32
     - 39
     - 126
   * - PyEXASOL - fetchall+zlib
     - -
     - -
     - 92
   * - TurbODBC - fetchallnumpy
     - 15
     - 15
     - -
   * - TurbODBC - fetchallarrow
     - 14
     - 14
     - -
   * - PyEXASOL - export_to_pandas
     - 11
     - 21
     - 77
   * - PyEXASOL - export_to_pandas+zlib
     - 28
     - 53
     - 29
   * - PyEXASOL - export_parallel
     - 5
     - 7
     - -

Conclusions
-----------

1. PyODBC performance is trash (no surprise).
2. PyEXASOL standard fetching is faster than TurbODBC, but it happens mostly due to fewer ops with Python objects and due to zip() magic.
3. TurbODBC optimized fetching as numpy or arrow is very efficient and consistent, well done!
4. PyEXASOL export to pandas performance may vary depending on the randomness of the data set. It highly depends on pandas CSV reader.
5. PyEXASOL fetch and export with ZLIB compression is very good for slow network scenarios, but it is bad for fast networks.
6. PyEXASOL parallel export beats everything else because it fully utilizes multiple CPU cores.

How to Run Your Own Test
------------------------

I strongly encourage you to run your own performance tests. You may use test scripts provided with PyEXASOL as the starting point. Make sure to use your production Exasol cluster for tests. Please do not use Exasol running in Docker locally, it eliminates the whole point of testing.

1. Install PyODBC, TurbODBC, PyEXASOL, pandas.
2. Install Exasol ODBC driver.
3. Download `PyEXASOL source code <https://github.com/exasol/pyexasol/archive/master.zip>`_ and unzip it.
4. Open `/performance/` directory and edit the file `_config.py`. Input your Exasol credentials, set table name, and other settings. Set the path to the ODBC driver.
5. (Optional) Run the script to prepare the data set for testing:

   .. code-block:: bash

      python 00_prepare.py

That's all. Now you may run examples in any order like common Python scripts. E.g.:

.. code-block:: bash

   time python 03_pyexasol_fetch.py
