.. _developer_guide:

:octicon:`tools` Developer Guide
================================

This guide explains how to develop ``pyexasol`` and run tests.

Initial Setup
+++++++++++++

Create a virtual environment and install dependencies:

.. code-block:: shell

    poetry install --all-extras

Run the following command to enter the virtual environment:

.. code-block:: shell

    poetry shell

Once you have set up all dependencies and activated the poetry shell, all further tasks for development should be available via the task runner ``nox``. To see all available tasks, run the following command: ``nox --list``.

To execute a specific task, run ``nox -s <taskname>``. For example, ``nox -s test:unit``.

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

.. code-block:: shell

    nox -s test:integration

Passing additional arguments to pytest works the same as for the unit tests.

DB
--
If you manually run some tests or want to try something out, you can start and stop the database manually using ``nox -s db:start`` and ``nox -s db:stop``.
