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

Performance Tests
-----------------

The CI executes and compares the results of the performance tests in `performance-checks.yml`,
using two nox sessions:

* ``performance:test`` - this uses the ``pytest-benchmark`` to capture the results
  of the **current CI run** in ``test/performance/.benchmarks/0002_performance.json``
* ``performance:check`` - this utilizes scipy's implementation of the Wilcoxon
  signed-rank test to determine if the results in the **current CI run** are statistically
  similar to the **saved reference results** in ``test/performance/.benchmarks/0001_performance.json``.
  If a test is not present in both the **current CI run** and **saved reference results**,
  this is noted as an error.

If the results of executing ``nox -s performance:check`` breaks a build, this requires
manual checks from a developer:

#. When the CI run was executed, the file generated from executing the performance tests
is uploaded to the GitHub Action as an artifact, ``performance-python${{ python-version }}-exasol${{ exasol-version}}``.
The developer should retrieve this file and compare the results to the **saved reference results**.
#. If the changed results can be explained by significant differences in the CI runner
or expected changes in the implementation, then the developer would overwrite the **saved reference results**
with the GitHub Action artifact.

.. note::

    As the results of performance tests can vary by machine & the active environment on said machine,
    the performance tests are benchmarked against the CI runner, and the reference results are saved
    in ``test/performance/.benchmarks/0001_performance.json`` of the repository.
    Naturally, this makes the results dependent upon the consistency & stability of GitHub's runners.
    While it may not cover all use cases, the results saved by the ``nox -s performance:test`` include
    the machine information, which may help developers determine if the observed shift in performance results
    could have resulted from differences in the past and current execution of the performance tests
    due to apparent macroscopic runner differences.

DB
--
If you manually run some tests or want to try something out, you can start and stop the database manually using ``nox -s db:start`` and ``nox -s db:stop``.

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
