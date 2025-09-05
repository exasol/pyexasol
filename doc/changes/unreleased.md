# Unreleased

This release adds support for the reserved word "nocertcheck" as fingerprint value, which disables the certificate check when establishing a connection.

## Bugfix

* #241: Switched checks for `ImportQuery` and `ExportQuery` to test more explicitly if None (instead of truthy assumptions).

## Features
* #235: Support NOCERTCHECK as fingerprint
