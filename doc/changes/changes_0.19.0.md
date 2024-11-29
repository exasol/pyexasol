# 0.19.0 - 2021-05-31

- Added connection options `access_token` and `refresh_token` to support OpenID Connect in [WebSocket Protocol V3](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV3.md).
- PyEXASOL default protocol version will be upgraded to `3` if connection option `access_token` or `refresh_token` were used.

