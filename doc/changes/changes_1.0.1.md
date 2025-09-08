# 1.0.1 - 2025-09-03

Users have historically been able to provide the server fingerprint for verification
when using `pyexasol.connect`. In PyExasol `1.0.0`, strict certificate verification was
turned on as the default of `pyexasol.connect`. While this is the desired behavior, it
added the inconvenience to users using fingerprint verification to modify the
`websocket_sslopt` of their `pyexasol.connect`. With this bugfix, we disable strict
certificate verification by default when a fingerprint is provided to the `dsn` argument
of `pyexasol.connect`; thus, with PyExasol `1.0.1`, users providing a fingerprint
should not need to alter their usage of `pyexasol.connect`.

## Bugfix

* #232: Disabled strict certificate check when a fingerprint is provided in the ``dsn``
  argument for ``pyexasol.connect``
