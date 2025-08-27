.. _protocol_version:

WebSocket Protocol Version
==========================

PyExasol is based on the `WebSocket protocol <https://github.com/exasol/websocket-api>`__.

Exasol has the concept of "protocol version" which is used to extend the functionality
of new database drivers without breaking the backwards compatibility with older database drivers.

When a client (e.g. PyExasol) opens the connection with the Exasol server, it sends the
requested protocol version during the authorisation. Exasol server may or may not
support the requested protocol version. If an Exasol server does not support the requested
version, it will downgrade the protocol version automatically. To check the actual protocol
version of connection, use :meth:`pyexasol.ExaConnection.protocol_version`.

For a list of Websocket protocol versions supported by specific Exasol DB versions, see
`Supported Versions <https://github.com/exasol/websocket-api/blob/master/README.md#supported-versions>`__.

If a user is using an older Exasol DB version like ``6.25`` and has an error like
``Could not create WebSocket protocol version``, modify the ``protocol_version`` parameter
in :func:`pyexasol.connect` or :class:`pyexasol.ExaConnection` to a supported version.
