Getting Started
===============


Installing
----------

`PyExasol is distributed through PyPI <https://pypi.org/project/pyexasol/>`__. It can be installed via pip, poetry, or any other compatible dependency management tool:

.. code-block:: bash

   pip install pyexasol

To install with optional dependencies, use:

.. code-block:: bash

   pip install pyexasol[<optional-package-name>]

For a list of optional dependencies, see :ref:`optional_dependencies`.


Prerequisites
-------------

- Exasol >= 7.1
- Python >= 3.9
- websocket-client >= 1.0.1
- rsa

.. _optional_dependencies:

Optional Dependencies
^^^^^^^^^^^^^^^^^^^^^

- ``orjson`` is required for ``json_lib=orjson`` to improve JSON parsing performance
- ``pandas`` is required for :ref:`http_transport` functions working with :class:`pandas.DataFrame`
- ``polars`` is required for :ref:`http_transport` functions working with :class:`polars.DataFrame`
- ``pproxy`` is used in the :ref:`examples` to test an HTTP proxy
- ``rapidjson`` is required for ``json_lib=rapidjson`` to improve JSON parsing performance
- ``ujson`` is required for ``json_lib=ujson`` to improve JSON parsing performance
