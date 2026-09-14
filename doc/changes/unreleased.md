# Unreleased

## Summary

This patch release improves WebSocket DB-API error handling by passing through
the original PyExasol error messages and making the exception classification
more specific. Connection and communication failures are reported as
`OperationalError`, while cursor or connection lifecycle misuse is reported as
`InterfaceError`. These exceptions remain compatible with existing handlers
that catch their common `Error` base class.

## Bugfix

* #360: Switched `exasol.driver` to adhere to the DBAPI per PEP-249 and pass on messages
