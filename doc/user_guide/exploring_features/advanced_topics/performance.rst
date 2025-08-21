Runtime Comparisons
===================

The performance of a database driver depends on many factors: the hardware used, the network used, the properties of the test dataset, etc. It is strongly suggest to do your own performance tests before making any important decisions.

In this sample test, the following are compared:

- `PyODBC <https://github.com/mkleehammer/pyodbc>`_
- `TurbODBC <https://github.com/blue-yonder/turbodbc>`_
- PyExasol

For testing, an Exasol cluster with the following specifications was used:

- 20 nodes
- 800+ CPU cores with hyper-threading
- 14 Tb of RAM
- 10 Gb private network connections
- 1 Gb public network connections

Three different scenarios were evaluated for each of the database drivers:

- Fetching **low random** data set using server in the same data center
- Fetching **high random** data set using server in the same data center
- Fetching data set using local laptop behind VPN and Wifi network (**slow network**)

For each of the scenarios, there were 10 million rows in the test table with mixed data types. The bash command ``time`` was used to measure the total execution duration in seconds.

Results
-------

.. note::

    All results are recorded in seconds.

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
   * - PyExasol - fetchall
     - 32
     - 39
     - 126
   * - PyExasol - fetchall+zlib
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
   * - PyExasol - export_to_pandas
     - 11
     - 21
     - 77
   * - PyExasol - export_to_pandas+zlib
     - 28
     - 53
     - 29
   * - PyExasol - export_parallel
     - 5
     - 7
     - -

Conclusions
-----------

1. PyODBC's performance is poor (no surprise).
2. PyExasol standard fetching is faster than TurbODBC, but it happens mostly due to fewer ops with Python objects and due to zip() magic.
3. TurbODBC optimized fetching as numpy or arrow is very efficient and consistent, well done!
4. PyExasol export to pandas performance may vary depending on the randomness of the data set. It highly depends on pandas CSV reader.
5. PyExasol fetch and export with ZLIB compression is very good for slow network scenarios, but it is bad for fast networks.
6. PyExasol parallel export beats everything else because it fully utilizes multiple CPU cores.

How to Run Your Own Test
------------------------

It is strongly encouraged that you run your own performance measurements. You may use the scripts provided with PyExasol as the starting point. Make sure to use your production Exasol cluster for measurements. Please do not use Exasol running in Docker locally; it eliminates the whole point of evaluating the performance.

1. Install PyODBC, TurbODBC, PyExasol, pandas.
2. Install Exasol ODBC driver.
3. Download the `PyExasol source code <https://github.com/exasol/pyexasol/>`__.
4. Open `/performance/` directory and edit the file `_config.py`. Input your Exasol credentials, set the table name, and other settings. Set the path to the ODBC driver.
5. (Optional) Run the script to prepare the data set for testing:

   .. code-block:: bash

      python 00_prepare.py

That's all. Now you may run examples in any order like common Python scripts. E.g.:

.. code-block:: bash

   time python 03_pyexasol_fetch.py
