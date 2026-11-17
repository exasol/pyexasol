.. include:: <isopub.txt>
.. _developer_guide:

:octicon:`tools` Developer Guide
================================

.. toctree::
    :maxdepth: 2
    :hidden:

    developer_guide/import_export_threading
    developer_guide/integration_tests
    developer_guide/performance_tests

This guide explains how to develop PyExasol and run tests.

Initial Setup
+++++++++++++

Switch to a supported Python version:

.. code-block:: shell

    poetry env use python3.13

Create a virtual environment and install dependencies:

.. code-block:: shell

    poetry install --all-extras

Run the following command to enter the virtual environment:

.. code-block:: shell

    eval "$(poetry env activate)"

Once you have set up all dependencies and activated the poetry shell, all further tasks for development should be available via the task runner ``nox``. To see all available tasks, run the following command: ``nox --list``.

To execute a specific task, run ``nox -s <task name>``. For example, ``nox -s test:unit``.

In our CI, PyExasol is checked for various formatting & type checks with nox tasks.
To run these more easily locally, we've added a `.pre-commit-config.yaml`,
which you can activate with `pre-commit <https://pre-commit.com/>`_

.. code-block:: shell

   poetry run -- pre-commit install --hook-type pre-commit --hook-type pre-push

Unit Tests
++++++++++

.. code-block:: shell

    nox -s test:unit

If you want to forward additional arguments to pytest, you need to pass ``--``
to indicate the end of the argument vector for the nox command. For example:

.. code-block:: shell

   nox -s test::unit -- -k meta

Preparing & Triggering a Release
++++++++++++++++++++++++++++++++

The `exasol-toolbox` provides nox tasks to semi-automate the release process:

.. code-block:: python

    # prepare a release
    nox -s release:prepare -- --type {major,minor,patch}

    # trigger a release
    nox -s release:trigger

For further information, please refer to the `exasol-toolbox`'s page `Creating a Release
<https://exasol.github.io/python-toolbox/main/user_guide/features/creating_a_release.html>`_.
