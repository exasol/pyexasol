Parallelism in PyEXASOL
=======================

Fundamentals:
-------------

- 1 Exasol session can run only 1 SQL query in parallel.
- 1 PyEXASOL connection equals 1 Exasol session.
- `threadsafety <https://www.python.org/dev/peps/pep-0249/#threadsafety>`_ level of PyEXASOL is `1` (threads may share the module, but not connections).

Best practices
--------------

In practice, it means you have two possible options to achieve parallelism:

1. Start multiple independent processes using `multiprocessing`, `subprocess` or similar modules. Each process should open its own PyEXASOL connection **after start**.
2. Use :ref:`http_transport_parallel` to run 1 SQL query, but read or write actual data using multiple processes.

All other options are inefficient or prone to errors.

Known problems when trying to use other options
------------------------------------------------

- Re-using one PyEXASOL connection in multiple threads will cause an exception.
- Opening multiple PyEXASOL connections in multiple threads will work, but you will experience a bottleneck in data processing. Your script will be bound by 1 CPU core due to GIL.
- Re-using one PyEXASOL connection in multiple processes will fail due to SSL context going out of sync.

Parallelism limitations
-----------------------

Normally Exasol server can only run 100 queries in parallel. But the practical limit is much lower.

It is recommended to avoid running more than 20-30 queries in parallel to improve performance. If your system experiences sudden bursts of activity, it is recommended to add a basic "queue" or a "proxy" as a system in the middle between clients and Exasol server. It will help to spread the workload and reduce the complexity of resource management for Exasol server. Which will lead to better performance overall.
