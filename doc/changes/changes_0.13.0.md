# 0.13.0 - 2020-05-30

- Added optional [`disconnect`](https://github.com/exasol/websocket-api/blob/master/docs/commands/disconnectV1.md) command executed during `.close()`. It is now enabled by default , but can be disabled with explicit `.close(disconnect=False)` to revert to original behaviour;
- Added `csv_cols` to **HTTP transport parameters**. It allows to skip some columns in CSV and adjust numeric and date format during IMPORT and EXPORT. It is still recommended to implement your own data transformation layer, since `csv_cols` capabilities are limited;
