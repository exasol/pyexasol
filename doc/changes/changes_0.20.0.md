# 0.20.0 - 2021-06-17

- Send `snapshotTransactionsEnabled` attribute only if set explicitly in connection option, prepare for Exasol 7.1 release.
- Default `snapshot_transaction` connection options is now `None` (database default), previously it was `False`.

