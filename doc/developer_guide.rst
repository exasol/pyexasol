.. _developer_guide:

:octicon:`tools` Developer Guide
================================

This guide explains how to develop `pyexasol` and run tests.

Initial Setup
+++++++++++++

Create a virtual environment and install dependencies::

  poetry install --all-extras

Run the following to enter the virtual environment::

  poetry shell

Running Integration Tests
+++++++++++++++++++++++++

To run integration tests first start a local database::

  nox -s db-start

Then you can run tests as usual with `pytest`.
