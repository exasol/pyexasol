## Dependencies

- Exasol >= 6
- Python >= 3.6
- websocket-client >= 0.47
- rsa

## Optional dependencies

- `pandas` is required for [HTTP transport](/docs/HTTP_TRANSPORT.md) functions working with data frames;
- `pyopenssl` is required for HTTP transport with [encryption](/docs/ENCRYPTION.md) to generate "ad-hoc" certificates;
- `websocket-client` may take advantage of `numpy` and `wsaccel` to improve `send()` method performance;

## Installation with optional dependencies

```
pip install pyexasol[pandas,encrypt]
```
