Parallelism
===========

Fundamentals:
-------------

- 1 Exasol session can run only 1 SQL query in parallel.
- 1 PyExasol connection equals 1 Exasol session.
- `threadsafety <https://peps.python.org/pep-0249/#threadsafety>`__ level of PyExasol is `1` (threads may share the module, but not connections).

Best Practices
--------------

In practice, it means there are two possible options to achieve parallelism:

1. Start multiple independent processes using ``multiprocessing``, ``subprocess``, or similar modules. Each process should open its own PyExasol connection **after start**.
2. Use :ref:`http_transport_parallel` to run 1 SQL query, but read or write actual data using multiple processes.

All other options have been found to be inefficient or prone to errors.

Known Problems When Trying Other Options
----------------------------------------

- Re-using one PyExasol connection in multiple threads will cause an exception.
- Opening multiple PyExasol connections in multiple threads will work, but you will experience a bottleneck in data processing. Your script will be bound by 1 CPU core due to GIL.
- Re-using one PyExasol connection in multiple processes will fail due to SSL context going out of sync.

Parallelism Limitations
-----------------------

Normally, an Exasol server can only run 100 queries in parallel, but the practical limit is much lower.

It is recommended to avoid running more than 20-30 queries in parallel to improve performance. If your system experiences sudden bursts of activity, it is recommended to add a basic "queue" or a "proxy" as a system in the middle between clients and Exasol server. It will help to spread the workload and reduce the complexity of resource management for Exasol server. Which will lead to better performance overall.
