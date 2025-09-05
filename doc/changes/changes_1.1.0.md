# 1.1.0 - 2025-09-05

This release adds support for the reserved word "nocertcheck" as fingerprint value, which disables the certificate check when establishing a connection.
Besides the release fixes a bug for `ImportQuery` and `ExportQuery`. 

## Bugfix

* #241: Switched checks for `ImportQuery` and `ExportQuery` to test more explicitly if None (instead of truthy assumptions).

## Features
* #235: Support NOCERTCHECK as fingerprint
