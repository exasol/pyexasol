# Unreleased

## Features

* #208: Added `ExaConnection.import_from_parquet` which can import data from parquet
  file(s)
* #234: Added `ExaConnection.export_to_parquet` which can export data to parquet file(s)
* #220: Added performance tests & checks for http_transport export & import methods via
  nox sessions `performance:test` and `performance:check`
