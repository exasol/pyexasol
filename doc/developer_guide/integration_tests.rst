Integration Tests
=================

Starting and Stopping the Database
----------------------------------

You can start and stop a test database manually using ``nox -s db:start`` and
``nox -s db:stop``.

Exasol Docker DB images often have a size of multiple GB and download may take
significant time depending on your network connection's bandwidth.

Also starting the Docker container may take a bit before the tests themselves
start executing.

After the tests have been run, the database will be shut down again.

Exasol Version
--------------

The default database version used by the integration tests, is the highest one
found in the `Python Toolbox <ptb_>`_ (PTB) defaults:

.. literalinclude:: ../../noxconfig.py
  :caption: ``noxconfig.py``
  :language: python
  :start-at: DEFAULT_DB_VERSION =
  :lines: 1

Please note that the supported database versions also dependend on the ones
supported by `Integration Test Docker Environment <itde_>`_ (ITDE). You might
not be able to use a database version not yet supported by the ITDE.

.. _ptb: https://github.com/exasol/python-toolbox
.. _itde: https://github.com/exasol/integration-test-docker-environment/tree/main/docker_db_config_template

Using a Custom Docker DB
------------------------

You can use a custom Docker DB, e.g. started manually, by setting the
following environment variables:

.. code-block:: shell

    export EXAHOST=some-host:11234
    export EXASOL_DOCKER_CONTAINER=my_container_name

``EXAHOST`` designates a `ODBC Data Source Name <dsn_>`_ (DSN), identifying
the database connection incl. hostname (or ip-address), and port.

This allows you using Exasol versions not yet supported by ITDE.

.. _dsn: https://en.wikipedia.org/wiki/Data_source_name

Runing the Integration Tests
----------------------------

.. code-block:: shell

    nox -s test:integration

Passing additional arguments to pytest works the same as for the unit tests.

The large parquet tests can be excluded from an integration test run using
the ``parquet_slow`` marker:

.. code-block:: shell

    nox -s test:integration -- -m "not parquet_slow"

Certificate Validation
----------------------

By default, local integration test runs only execute the `ssl.CERT_NONE`
variant, so they do not require a certificate setup.

The following command line enables the certificate-verification variant
locally:

.. code-block:: shell

    poetry run -- nox -s test:integration -- --with-cert

Continuous integration validates both certificate modes.

.. attention::

    If you are running Docker inside of a VM and are running the integration tests
    with `ssl.CERT_REQUIRED`, inside your VM map the `exasol-test-database` to the
    associated IP address. This mapping is required for hostname resolution and
    also the hostname needs to match the one named in the certificate.

    .. code-block:: shell

        echo "127.0.0.1 exasol-test-database" | sudo tee -a /etc/hosts
