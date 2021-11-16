# Snapshot transactions

At this moment Exasol supports only one transaction isolation level: SERIALIZABLE.

This is good for data consistency, but it increases probability of transactions conflicts. You may read more about it here:

- [SOL-135](https://www.exasol.com/support/browse/SOL-135)
- [SOL-214](https://www.exasol.com/support/browse/SOL-214)

The most common locking problem is related to metadata selects from system views (tables, column, object sizes, etc.). JDBC and ODBC drivers provide special non-blocking calls for common metadata requests: getTables(), getColumns(). But there are no such calls for WebSocket drivers.

The only way to access metadata in non-blocking manner with PyEXASOL is an internal feature called "Snapshot Transactions". Details are limited, but we managed to find out a few things:

1. ExaPlus client uses snapshot transactions to access system views in separate META-session;
2. Snapshot transactions are read-only. Connection will crash instantly on any write attempt;
3. In this mode Exasol returns last snapshot of accessed objects instead of locking in `WAIT FOR COMMIT` state;

### Recommended usage pattern

If you want to read metadata without locks, and if strict transaction integrity is not an issue, please do the following:

1. Open new connection with option `snapshot_transactions=True`. Use this connection to read metadata from system views only.
2. Open another connection in normal mode and use it for everything else.

Follow this pattern and you should be fine.

Please see [example_23](/examples/c08_snapshot_transactions.py) for common locking scenario solved by this feature.
