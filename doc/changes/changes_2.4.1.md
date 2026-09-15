# 2.4.1 - 2026-09-15

## Summary

This patch release improves WebSocket DB-API error handling by passing through
the original PyExasol error messages and making the exception classification
more specific. Connection and communication failures are reported as
`OperationalError`, while cursor or connection lifecycle misuse is reported as
`InterfaceError`. These exceptions remain compatible with existing handlers
that catch their common `Error` base class.

It also tightens DSN parsing: the optional TLS fingerprint must appear before the
optional port, using the form `hostname[/fingerprint][:port]`
(for example, `localhost/1234:8563`). A DSN using the reversed form, such as
`localhost:8563/1234`, is now rejected with a DSN parsing error instead of silently
treating the fingerprint as part of the hostname.

## Bugfixes

* #360: Switched `exasol.driver` to adhere to the DBAPI per PEP-249 and pass on messages
* #237: Fixed the DSN parser accepting a fingerprint placed after the port (e.g. `localhost:8563/1234`) as part of the hostname
