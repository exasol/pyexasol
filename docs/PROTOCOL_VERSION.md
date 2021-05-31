# WebSocket protocol versions

## TL;DR

If you have Exasol v6.x, do nothing.

If you have Exasol v7.0+, you may add the extra connection option `protocol_version=pyexasol.PROTOCOL_V2`. It will improve the performance of some `.meta` functions. Also, it will allow you to use [no SQL metadata commands](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV2.md#metadata-related-commands) via [`.meta.execute_meta_nosql()`](/docs/REFERENCE.md#execute_meta_nosql) function.

If you have Exasol v7.1+ and use connection option `access_token` or `refresh_token`, default protocol version will be upgraded to `pyexasol.PROTOCOL_V3` automatically.

## Explanation

Exasol has the concept of "protocol version" which is used to extend the functionality of new database drivers without breaking the backwards compatibility with older database drivers.

Exasol v6.x supports WebSocket protocol version [`1`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md) only.

Exasol v7.0+ supports WebSocket protocol versions [`1`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md), [`2`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV2.md).

Exasol v7.1+ supports WebSocket protocol versions [`1`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md), [`2`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV2.md), [`3`](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV3.md).

---

When client (e.g. pyexasol) opens the connection with Exasol server, it sends the requested protocol version during the authorisation. Exasol server may or may not support the requested protocol version. If Exasol server does not support the requested version, it will downgrade the protocol version automatically. You may check the actual protocol version of connection using function [`.protocol_version()`](/docs/REFERENCE.md#protocol_version).

However, the "downgrade" behaviour was introduced in the latest minor Exasol v6 versions only (`6.2.5`, `6.1.9`). All prior versions, including the whole `6.0.x` branch, will raise the exception if `protocol_version=2` was requested.

Since most customers will be using Exasol v6 for a long time, the default `protocol_version` of PyEXASOL will remain `1` for 6 to 12 months after the official release of Exasol v7.0.

If you have Exasol v7.0+ and you want to use new [no SQL metadata commands](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV2.md#metadata-related-commands), please set the `protocol_version=pyexasol.PROTOCOL_V2` connection option explicitly.

If you use one of OpenID connection options (`access_token` or `refresh_token`), the default protocol version will be upgraded to `pyexasol.PROTOCOL_V3` automatically.
