Snapshot Transactions
=====================

At this moment, Exasol supports only one transaction isolation level: ``SERIALIZABLE``.

This is good for data consistency, but it increases the probability of transactional conflicts. For more information, see:

- `Transaction System <https://exasol.my.site.com/s/article/Transaction-System?language=en_US>`_
- `WAIT FOR COMMIT on SELECT statement <https://exasol.my.site.com/s/article/WAIT-FOR-COMMIT-on-SELECT-statement?language=en_US>`_

The most common locking problem is related to metadata selects from system views (tables, column, object sizes, etc.). JDBC and ODBC drivers provide special non-blocking calls for common metadata requests: getTables(), getColumns(). But there are no such calls for WebSocket drivers.

The only way to access metadata in non-blocking manner with PyExasol is an internal feature called "Snapshot Transactions".
For information, see:
* `Transaction Management <https://docs.exasol.com/db/latest/database_concepts/transaction_management.htm>`__
* `Snapshot mode <https://docs.exasol.com/db/latest/database_concepts/snapshot_mode.htm>`__


Recommended Usage Pattern
-------------------------

If you want to read metadata without locks, and if strict transaction integrity is not an issue, please do the following:

1. Open new connection with option ``snapshot_transactions=True``. Use this connection to read metadata from system views only.
2. Open another connection in normal mode and use it for everything else.

Please see ``c08_snapshot_transactions.py`` for a common locking scenario solved by this feature.
