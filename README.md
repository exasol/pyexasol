<h1 align="center">PyExasol</h1>
<p align="center">
<a href="https://github.com/exasol/pyexasol/actions/workflows/pr-merge.yml">
    <img src="https://github.com/exasol/pyexasol/actions/workflows/pr-merge.yml/badge.svg?branch=master" alt="Continuous Integration (master)">
</a>
<a href="https://anaconda.org/conda-forge/pyexasol">
    <img src="https://anaconda.org/conda-forge/pyexasol/badges/version.svg" alt="Anaconda">
</a>
<a href="https://pypi.org/project/pyexasol/">
    <img src="https://img.shields.io/pypi/v/pyexasol" alt="PyPi Package">
</a>
<a href="https://pypi.org/project/pyexasol/">
    <img src="https://img.shields.io/pypi/dm/pyexasol" alt="Downloads">
</a>
<a href="https://pypi.org/project/pyexasol/">
    <img src="https://img.shields.io/pypi/pyversions/pyexasol" alt="Supported Python Versions">
</a>
</p>

PyExasol is the officially supported Python connector for [Exasol](https://www.exasol.com). It helps to handle massive volumes of data commonly associated with this DBMS.

You may expect significant performance improvement over ODBC in a single process scenario involving pandas, parquet, or polars.

PyExasol provides an [API](https://exasol.github.io/pyexasol/master/api.html) to read & write multiple data streams in parallel using separate processes, which is necessary to fully utilize hardware and achieve linear scalability. With PyExasol you are no longer limited to a single CPU core.

---
* Documentation: [https://exasol.github.io/pyexasol/](https://exasol.github.io/pyexasol/index.html)
* Source Code: [https://github.com/exasol/pyexasol](https://github.com/exasol/pyexasol)
---

## PyExasol Main Concepts

- Based on [WebSocket protocol](https://github.com/exasol/websocket-api);
- Optimized for minimum overhead;
- Easy integration with pandas, parquet, and polars via HTTP transport;
- Compression to reduce network bottleneck;


## System Requirements

- Exasol >= 7.1
- Python >= 3.10

## Getting Started

Check out PyExasol's [Getting Started](https://exasol.github.io/pyexasol/master/user_guide/getting_started.html) page for your first steps.

## Developers
* Created by [Vitaly Markov](https://www.linkedin.com/in/markov-vitaly/), 2018 — 2022
* Maintained by [Exasol](https://www.exasol.com) 2023 — Today
