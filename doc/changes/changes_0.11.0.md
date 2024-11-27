# 0.11.0 - 2020-01-25

## ExaConnection

- Reworked `close()` method. It is now sending basic `OP_CLOSE` WebSocket frame instead of `disconnect` command.
- Method `close()` is now called implicitly during destruction of `ExaConnection` object to terminate IDLE session and free resources on Exasol server side immediately.
- `ExaFormatter`, `ExaExtension`, `ExaLogger` objects now have [weak reference](https://docs.python.org/3/library/weakref.html) to the main `ExaConnection` object. It helps to prevent circular reference problem which stopped `ExaConnection` object from being processed by Python garbage collector.
- Connection will be closed automatically after receiving `WebSocketException` and raising `ExaCommunicationError`. It should prevent connection from being stuck in invalid state.

