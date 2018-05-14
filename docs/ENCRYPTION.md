# Encryption

If you have to use strong encryption due to compliance and regulations, the best option is to create dedicated secured tunnel. Production Exasol instances should never be exposed to the internet.

However, it is possible to enable SSL encryption in PyEXASOL similar to standard ODBC / JDBC drivers. This option is significantly less secure, but it might be useful in some cases.

### How to enable encryption?

1. Install PyEXASOL with extra `[encrypt]`. It adds dependency for [pyOpenSSL](https://github.com/pyca/pyopenssl).
```
pip install pyexasol[encrypt]
```

2. Create connection with argument `encryption=True`. It enables SSL encryption both for WebSocket communication and for [HTTP transport](/docs/HTTP_TRANSPORT.md).

### Certificate verification?

- Exasol uses self-signed SSL certificate for secured WebSocket communication (wss://).
- Exasol does not verify certificates for encrypted connections during IMPORT / EXPORT commands. It is mentioned in manual.

PyEXASOL can only follow this behaviour and **disable certificate verification** on client side. PyEXASOL creates temporary "ad-hoc" certificates and private keys for every call of HTTP transport and deletes them immediately after initialisation of SSLContext.

Disabled verification makes it possible to perform MITM attack on encrypted connection.
