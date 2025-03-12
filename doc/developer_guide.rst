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

Creating a Release
++++++++++++++++++

Prepare the Release
-------------------

To prepare for a release, a pull request with the following parameters needs to be created:

- Updated version numbers
- Updated the changelog

This can be achieved by running the following command:

.. code-block:: shell

   nox -s release:prepare -- <major>.<minor>.<patch>

Replace `<major>`, `<minor>`, and `<patch>` with the appropriate version numbers.
Once the PR is successfully merged, the release can be triggered (see next section).

Triggering the Release
----------------------

To trigger a release, a new tag must be pushed to GitHub. For further details, see `.github/workflows/ci-cd.yml`.

1. Create a local tag with the appropriate version number:

    .. code-block:: shell

        git tag x.y.z

2. Push the tag to GitHub:

    .. code-block:: shell

        git push origin x.y.z


What to do if the release failed?
---------------------------------

The release failed during pre-release checks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. Delete the local tag

    .. code-block:: shell

        git tag -d x.y.z

#. Delete the remote tag

    .. code-block:: shell

        git push --delete origin x.y.z

#. Fix the issue(s) which lead to the failing checks
#. Start the release process from the beginning


One of the release steps failed (Partial Release)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. Check the Github action/workflow to see which steps failed
#. Finish or redo the failed release steps manually

.. note:: Example

    **Scenario**: Publishing of the release on Github was successfully but during the PyPi release, the upload step got interrupted.

    **Solution**: Manually push the package to PyPi




