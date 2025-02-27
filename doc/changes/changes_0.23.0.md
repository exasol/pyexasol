# 0.23.0 - 2021-11-19

- Added [`orjson`](https://github.com/ijl/orjson) as possible option for `jsob_lib` connection parameter.
- Default `indent` for JSON debug output is now 2 (was 4) for more compact representation.
- `ensure_ascii` is now `False` (was `True`) for better readability and smaller payload size.
- Fixed JSON examples, `fetch_mapper` is now set correctly for second data set.

