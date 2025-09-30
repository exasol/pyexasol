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

.. note::
    If you manually run some integration or performance tests or want to try something out,
    you can start and stop a test database manually using ``nox -s db:start`` and
    ``nox -s db:stop``.

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

   As a Docker container with a test database needs to be started for the integration tests, it may take a bit before the tests themselves start executing. After the tests have been run, the database will be shut down again.

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

.. attention::

   For the performance tests, a Docker container with a test database needs to be started.
   This can be done with ``poetry run -- nox -s db:start``.

   As these tests are meant to measure the performance of functions, the tests
   are expected to take a bit to set up and execute. If you are running the tests
   for the first time, it is recommend that you modify the throughput data values
   to a smaller value; this can be done by modifying :attr:`BenchmarkSpecifications.target_data_size`.

To execute the performance tests, you need to additionally execute:

.. code-block:: shell

    poetry install --with performance

This installs `pytest-benchmark <https://pypi.org/project/pytest-benchmark/>__`,
which provides a pytest fixture ``benchmark`` to re-run tests a specified number of times,
to capture how long each execution took to run, and to calculate various statistical
values for comparative purposes.

.. note::
    ``pytest-benchmark`` is not included in the ``dev`` dependencies due to an odd
    correlation observed when executing the integration tests. This was observed
    with ``pytest-benchmark`` version 5.1.0 and its dependency ``py-cpuinfo`` 9.0.0.
    Namely, when executing the integration tests with Python 3.11, we saw that the
    tests in ``transaction_test.py`` consistently failed. This was not observed
    for other Python versions and could not be correlated with other changes at the
    time of investigation.

Updating the Benchmark JSON File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To ensure that PyExasol's functional performance does not degrade over time, the
performance tests are executed in the CI, which provides semi-homogeneous runners, and
their results are compared in the workflow ``performance-checks.yml`` per pull request.
If the comparison fails, a developer should scrutinize the changes to determine the
cause and, if deemed relevant, update the
``test/performance/.benchmarks/0001_performance.json`` with results
saved from the CI run (i.e. from the relevant artifact):

* Is it a new test?

  * Then, add it to the JSON file.

* Was a test removed?

  * If we meant to remove it, remove it from the JSON file.

* What change made that impacted the tests?

  * If it's a change we intended and cannot improve upon, then alter the corresponding
    benchmark results in the JSON file.

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
