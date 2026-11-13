Performance Test
================

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
