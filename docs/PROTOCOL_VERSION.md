# WebSocket protocol versions

## TL;DR (updated on 2021-09-27, since 0.21.0)

If you have Exasol v6.1.9+, 6.2.5+ or v7+, do nothing.

If you have an older Exasol version, and you get error message:

> Could not create WebSocket protocol version

please add connection option `protocol_version=pyexasol.PROTOCOL_V1` explicitly.

## Explanation

Exasol has the concept of "protocol version" which is used to extend the functionality of new database drivers without breaking the backwards compatibility with older database drivers.

Exasol v6.x supports WebSocket protocol version [`1`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md) only.

Exasol v7.0+ supports WebSocket protocol versions [`1`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md), [`2`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV2.md).

Exasol v7.1+ supports WebSocket protocol versions [`1`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md), [`2`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV2.md), [`3`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV3.md).

---

When client (e.g. pyexasol) opens the connection with Exasol server, it sends the requested protocol version during the authorisation. Exasol server may or may not support the requested protocol version. If Exasol server does not support the requested version, it will downgrade the protocol version automatically. You may check the actual protocol version of connection using function [`.protocol_version()`](/docs/REFERENCE.md#protocol_version).

However, the "downgrade" behaviour was introduced in the latest minor Exasol v6 versions only (`6.2.5`, `6.1.9`). All prior versions, including the whole `6.0.x` branch, will raise an exception if `protocol_version=2` or higher was requested. Please add connection option `protocol_version=1` explicitly if it happens.
