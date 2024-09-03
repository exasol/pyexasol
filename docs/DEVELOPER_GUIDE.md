# Developer Guide

This guide explains how to develop pytest and run tests.

## Initial Setup

Create a virtual environment and install dependencies:

```sh
poetry install --all-extras
```

Run the following to enter the virtual environment:

```sh
poetry shell
```

## Running Integration Tests

To run integration tests first start a local database:

```sh
nox -s db-start
```

Then you can run tests as usual with `pytest`.
