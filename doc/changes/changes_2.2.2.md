# 2.2.2 - 2026-06-23

## Summary

In this patch release, the down-pinning on `packaging` and `websocket-client` have been
removed.

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| cryptography | GHSA-537c-gmf6-5ccf | 46.0.7 | 48.0.1 |
| gitpython | CVE-2026-42215 | 3.1.46 | 3.1.47 |
| gitpython | CVE-2026-42284 | 3.1.46 | 3.1.47 |
| gitpython | CVE-2026-44244 | 3.1.46 | 3.1.49 |
| gitpython | GHSA-mv93-w799-cj2w | 3.1.46 | 3.1.50 |
| idna | PYSEC-2026-215 | 3.11 | 3.15 |
| msgpack | GHSA-6v7p-g79w-8964 | 1.1.2 | 1.2.1 |
| pip | PYSEC-2026-196 | 26.0.1 | 26.1.2 |
| pip | CVE-2026-3219 | 26.0.1 | 26.1 |
| pip | CVE-2026-6357 | 26.0.1 | 26.1 |
| pyarrow | PYSEC-2026-113 | 23.0.0 | 23.0.1 |
| tornado | CVE-2026-49854 | 6.5.5 | 6.5.6 |
| tornado | CVE-2026-49853 | 6.5.5 | 6.5.6 |
| tornado | CVE-2026-49855 | 6.5.5 | 6.5.6 |
| tornado | GHSA-pw6j-qg29-8w7f | 6.5.5 | 6.5.7 |
| ujson | CVE-2026-44660 | 5.12.0 | 5.12.1 |
| ujson | CVE-2026-54911 | 5.12.0 | 5.13.0 |
| urllib3 | PYSEC-2026-142 | 2.6.3 | 2.7.0 |
| urllib3 | PYSEC-2026-142 | 2.6.3 | 2.7.0 |
| urllib3 | PYSEC-2026-141 | 2.6.3 | 2.7.0 |

* #331: Resolved vulnerabilities by re-locking `poetry.lock` and updated `exasol-toolbox` to 7.0.0
* #337: Resolved vulnerabilities by relocking `poetry.lock`

## Refactoring

* #333: Updated to `exasol-toolbox` version 8.0.0
* #335: Added `export` plugin to `pyproject.toml` for `exasol-toolbox` usage
* #338: Update to `exasol-toolbox` version 8.1.1
* #343: Removed down-pinning on `packaging` which follows CalVer versioning and on `websocket-client`

## Dependency Updates

### `main`

* Updated dependency `cryptography:46.0.7` to `49.0.0`
* Updated dependency `orjson:3.11.7` to `3.11.9`
* Updated dependency `packaging:25.0` to `26.2`
* Updated dependency `polars:1.37.1` to `1.41.2`
* Updated dependency `pyarrow:23.0.0` to `24.0.0`
* Updated dependency `ujson:5.12.0` to `5.13.0`
* Updated dependency `websocket-client:1.8.0` to `1.9.0`

### `dev`

* Updated dependency `exasol-integration-test-docker-environment:5.0.0` to `6.2.0`
* Updated dependency `exasol-toolbox:6.4.0` to `9.0.0`
* Updated dependency `pytest:9.0.3` to `9.1.1`