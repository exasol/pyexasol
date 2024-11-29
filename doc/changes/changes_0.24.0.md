# 0.24.0 - 2022-02-12

- **Encryption** is now enabled by default both for WebSocket and HTTP transport. Non-encrypted connections will be disabled in Exasol 8.0.

It may introduce some extra CPU overhead. If it becomes a problem, you may still disable encryption explicitly by setting `encryption=False`.

