# Unreleased

## Summary

* Proxy handling now follows `websocket-client` 1.9.0 behavior more closely.
  If you rely on an HTTP proxy, set `http_proxy` explicitly in
  `pyexasol.connect(...)` or via environment variables such as
  `HTTP_PROXY`/`HTTPS_PROXY`.
  If you use a local test proxy on `localhost` or `127.0.0.1`, make sure the
  proxy process is fully started before opening the Exasol connection.
  The old implicit loopback bypass behavior is no longer guaranteed by the
  underlying WebSocket client.

## Security Issues

* #331: Resolved vulnerabilities by re-locking `poetry.lock` and updated `exasol-toolbox` to 7.0.0
* #337: Resolved vulnerabilities by relocking `poetry.lock`

## Refactoring

* #333: Updated to `exasol-toolbox` version 8.0.0
* #335: Added `export` plugin to `pyproject.toml` for `exasol-toolbox` usage
* #338: Update to `exasol-toolbox` version 8.1.1
* #343: Removed down-pinning on `packaging` which follows CalVer versioning
