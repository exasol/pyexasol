.. _developer_guide:

:octicon:`tools` Developer Guide
================================

This guide explains how to develop PyExasol and run tests.

Initial Setup
+++++++++++++

Create a virtual environment and install dependencies:

.. code-block:: shell

    poetry install --all-extras

Run the following command to enter the virtual environment:

.. code-block:: shell

    poetry shell

Once you have set up all dependencies and activated the poetry shell, all further tasks for development should be available via the task runner ``nox``. To see all available tasks, run the following command: ``nox --list``.

To execute a specific task, run ``nox -s <task name>``. For example, ``nox -s test:unit``.

In our CI, PyExasol is checked for various formatting & type checks with nox tasks.
To run these more easily locally, we've added a `.pre-commit-config.yaml`,
which you can activate with `pre-commit <https://pre-commit.com/>`_

    .. code-block:: shell

        poetry run -- pre-commit install --hook-type pre-commit --hook-type pre-push

Running tests
++++++++++++++

Unit Tests
----------

.. code-block:: shell

    nox -s test:unit

If you want to forward additional arguments to pytest, you need to pass ``--`` to indicate the end of the argument vector for the nox command. For example:

.. code-block:: shell

   nox -s test::unit -- -k meta

Integration Tests
-----------------

.. attention::

   As a Docker container with a test databases needs to be started for the integration tests, it may take a bit before the tests themselves start executing. After the tests have been run, the database will be shut down again.

   If you are running Docker inside of a VM and are running the integration tests with `ssl.CERT_REQUIRED`, inside your VM map the `exasol-test-database` to the associated IP address.
   This mapping is required due to how the certificate was created & for hostname resolution.

    .. code-block:: shell

        echo "127.0.0.1 exasol-test-database" | sudo tee -a /etc/hosts

.. important::

    To (temporarily) skip integration tests that require `ssl.CERT_REQUIRED`, you can deselect those
    tests by using:


    .. code-block:: shell

        poetry run -- nox -s test:integration -- -m "not with_cert"


.. code-block:: shell

    nox -s test:integration

Passing additional arguments to pytest works the same as for the unit tests.

DB
--
If you manually run some tests or want to try something out, you can start and stop the database manually using ``nox -s db:start`` and ``nox -s db:stop``.

Preparing & Triggering a Release
--------------------------------

The `exasol-toolbox` provides nox tasks to semi-automate the release process:

.. code-block:: python

    # prepare a release
    nox -s release:prepare -- --type {major,minor,patch}

    # trigger a release
    nox -s release:trigger

For further information, please refer to the `exasol-toolbox`'s page `How to Release
<https://exasol.github.io/python-toolbox/main/user_guide/how_to_release.html>`_.
