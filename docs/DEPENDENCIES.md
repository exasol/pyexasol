## Core dependencies

- Exasol >= 6.2
- Python >= 3.6
- websocket-client >= 1.0.1
- pyopenssl
- rsa

## Optional dependencies

- `pandas` is required for [HTTP transport](/docs/HTTP_TRANSPORT.md) functions working with data frames;
- `ujson` is required for `json_lib=usjon` to improve json parsing performance;
- `rapidjson` is required for `json_lib=rapidjson` to improve json parsing performance;
- `orjson` is required for `json_lib=orjson` to improve json parsing performance;
- `pproxy` is used in examples to test HTTP proxy;

## Installation with optional dependencies

```
pip install pyexasol[pandas,ujson,rapidjson,orjson,examples]
```
