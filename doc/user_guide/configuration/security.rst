Security
========

PyExasol works with different Exasol database variants: on-premise, SAAS, and, for
testing purposes, Docker-based. Each of these have shared and unique
:ref:`authentication` methods and require a
:ref:`TLS/SSL certificate setup <certificate_validation>`.
Throughout this guide on security, an overview of the security features of PyExasol is
provided, as well as examples.

.. _changed_defaults:

Changed Defaults Over Time
**************************

As Exasol constantly improves its built-in and recommended security measures, so too is the
default security of PyExasol improved in subsequent versions. These improvements affect
all functionality which rely upon the :func:`pyexasol.connect` function (and the class
which it wraps, :class:`pyexasol.ExaConnection`).

+------------------+----------------------------+---------------------------+
| PyExasol Version | Encryption                 | Certificate Validation    |
+==================+============================+===========================+
| < 0.24.0         | unencrypted                | not validated             |
+------------------+----------------------------+---------------------------+
| >= 0.24.0        | :octicon:`check` encrypted | not validated             |
+------------------+----------------------------+---------------------------+
| >= 1.0.0         | :octicon:`check` encrypted | :octicon:`check` validated|
+------------------+----------------------------+---------------------------+

Thus, the current defaults, in relation to the :func:`pyexasol.connect` are:

* **Encryption** - To use SSL to encrypt client-server communications for WebSocket &
  HTTP Transport, the ``encryption`` parameter is set to ``True``.
* **Certificate Validation** - When :func:`pyexasol.connect`
  is executed with ``websocket_sslopt=None``, then this is effectively mapped to
  ``websocket_sslopt={"cert_reqs": ssl.CERT_REQUIRED}``. For more details, see
  :ref:`certificate_validation`.


.. _authentication:

Authentication
**************

For the various Exasol DBs, there are different ways to set up and use the access
credentials for a connection made with the :func:`pyexasol.connect` function.

+------------------+------------------------------+----------------------------------------+
| Exasol DB        | Setting Credentials          | PyExasol parameters                    |
+==================+==============================+========================================+
| on-premise       | `on-premise authentication`_ | * ``user``                             |
|                  |                              | * ``password``                         |
+------------------+------------------------------+----------------------------------------+
| SAAS             | `SAAS authentication`_       | * ``user``                             |
|                  |                              | * ``access_token`` or ``refresh_token``|
+------------------+------------------------------+----------------------------------------+
| Docker (testing) | `Docker authentication`_     | * ``user``                             |
|                  |                              | * ``password``                         |
+------------------+------------------------------+----------------------------------------+

.. _on-premise authentication: https://docs.exasol.com/db/latest/sql/create_user.htm
.. _SAAS authentication: https://docs.exasol.com/saas/administration/access_mngt/access_management.htm#Databaseaccessmanagement
.. _Docker authentication: https://github.com/exasol/docker-db?tab=readme-ov-file#connecting-to-the-database



#. Connect to Exasol on-premise or Docker

   .. code-block:: python

      pyexasol.connect(dsn='myexasol:8563'
                       , user='user'
                       , password='password')


#. Connect to Exasol SAAS (TLS encryption is REQUIRED for SAAS):

   .. code-block:: python

      pyexasol.connect(dsn='abc.cloud.exasol.com:8563'
                       , user='user'
                       , refresh_token='token'
                       )

   .. code-block:: python

      pyexasol.connect(dsn='myexasol:8563'
                       , user='user'
                       , access_token='personal_access_token'
                       )



Transport Layer Security (TLS)
******************************

Similar to other Exasol connectors, PyExasol is compatible with using TLS cryptographic
protocol. As a part of the TLS handshake, the drivers require the SSL/TLS certificate
used by Exasol to be validated. This is the standard practice that increases the security of
connections by preventing man-in-the-middle attacks.

Please check out the following documentation for user-friendly tutorials on TLS from Exasol:

* `An introduction to TLS <https://github.com/exasol/tutorials/blob/1.0.0/tls/doc/tls_introduction.md>`__
* `TLS at Exasol <https://github.com/exasol/tutorials/blob/1.0.0/tls/doc/tls_with_exasol.md>`__
* `TLS in UDFs tutorial <https://github.com/exasol/tutorials/blob/1.0.0/tls/doc/tls_in_udfs.md>`__

For technical articles made by Exasol relating to TLS, please see:

- `Database connection encryption at Exasol <https://exasol.my.site.com/s/article/Database-connection-encryption-at-Exasol/>`__
- `CHANGELOG: TLS for all Exasol drivers <https://exasol.my.site.com/s/article/Changelog-content-6507>`__
- `CHANGELOG: Database accepts only TLS connections <https://exasol.my.site.com/s/article/Changelog-content-16927>`__
- `Generating TLS files yourself to avoid providing a fingerprint <https://exasol.my.site.com/s/article/Generating-TLS-files-yourself-to-avoid-providing-a-fingerprint/>`__
- `TLS connection fails <https://exasol.my.site.com/s/article/TLS-connection-fails>`__


Fingerprint Verification
------------------------
Similar to JDBC / ODBC drivers, PyExasol supports fingerprint certificate validation.

.. code-block:: python

  fingerprint = "135a1d2dce102de866f58267521f4232153545a075dc85f8f7596f57e588a181"
  pyexasol.connect(dsn=f'myexasol/{fingerprint}:8563'
                   , user='user'
                   , password='password'
                   )


.. _certificate_validation:

Certification Validation
------------------------

As further discussed in
`Certificate and Certificate Agencies <https://github.com/exasol/tutorials/blob/1.0.0/tls/doc/tls_introduction.md#certificates-and-certification-agencies>`__,
there are three kinds of certificates:

* ones from a public Certificate Authority (CA)
* ones from a private CA
* ones that are self-signed

Before using a certificate for certificate validation, your IT Admin should ensure that
whatever certificate your Exasol instance uses is the most secure. Exasol on-premise
uses a default certificate which should be replaced with one provided by your
organization. For further context, see
`Incoming TLS Connections <https://github.com/exasol/tutorials/blob/1.0.0/tls/doc/tls_with_exasol.md#incoming-tls-connections>`__
and `TLS Certificate Instructions <https://docs.exasol.com/db/latest/administration/on-premise/access_management/tls_certificate.htm>`__.

- Exasol running on-premise uses self-signed SSL certificate by default. You may generate a proper SSL certificate and upload it using .
- Exasol SAAS running in the cloud uses proper certificate generated by public certificate authority. It does not require any extra setup.
- Exasol Docker uses self-signed SSL certificate by default. You may generate a proper SSL certificate and submit it for use via the ConfD API. More details are available on:

   - `GitHub for Exasol Docker <https://github.com/exasol/docker-db>`_
   - `ConfD API <https://docs.exasol.com/db/latest/confd/confd.htm>`_
   - `confd_client cert_update <https://docs.exasol.com/db/latest/confd/jobs/cert_update.htm>`_

Setup
^^^^^

In order to validate a certificate which will be provided in your PyExasol connection
(see :ref:`Certificate Handling in PyExasol <certificate_in_pyexasol>`),
you will need to have the certificate setup for your PyExasol usage: either on your
:ref:`client_machine` or :ref:`inside_a_udf`.

.. _client_machine:

Client machine
""""""""""""""

#. Public CA
    * The certificate should already be in the operating system truststore of the client machine.
#. Private CA (Corporate CA)
    * Your IT should add it to operating system truststore of the client machine.
#. Self-signed Certificate
    * Your IT should add it to operating system truststore of the client machine.
        1. DBA needs to fetch the certificate from the Exasol Cluster.
        2. Client Machine Admin needs to add it to the  operating system truststore.
    * Or, in case of a unprivileged user and the user can access the certificate of the Exasol database you can specify the certificate during connect.
    * For testing with a local DB you can disable the certificate validation (however, this should **NEVER** be used for production).

.. _inside_a_udf:

Inside a UDF
""""""""""""

#. Public CA
    * The certificate should already be in the operating system truststore of the client machine.
#. Private CA (Corporate CA)
    * Your DBA should upload the certificate to BucketFS and you should pass it to the connect inside of the UDF.
        * Note: The operating system truststore is part of the SLC and can only be changed during SLC creation.
          While you run a UDF, the operating system truststore is read-only.
#. Self-signed Certificate
    * Your DBA or you should upload the certificate to BucketFS and you should pass it to the connect inside of the UDF.
        * Note: The operating system truststore is part of the SLC and can only be changed during SLC creation.
          While you run a UDF, the operating system truststore is read-only.
    * For testing with a local DB you can disable the certificate validation (however, this should **NEVER** be used for production).

.. _certificate_in_pyexasol:

Handling in PyExasol
^^^^^^^^^^^^^^^^^^^^

Passing into the Connection
"""""""""""""""""""""""""""

This is how an unprivileged user can specify the certificate when making the connection.

   .. code-block:: python

      pyexasol.connect(dsn='myexasol:8563'
                       , user='user'
                       , password='password'
                       , websocket_sslopt={
                          "cert_reqs": ssl.CERT_REQUIRED,
                          "ca_certs": '/path/to/rootCA.crt',
                       })

Disabling Certificate Validation
""""""""""""""""""""""""""""""""

This should only be used when testing with a local DB and **never** be used for production.

   .. code-block:: python

      pyexasol.connect(dsn='myexasol:8563'
                       , user='user'
                       , password='password'
                       , websocket_sslopt={"cert_reqs": ssl.CERT_NONE})


.. _security_examples:
