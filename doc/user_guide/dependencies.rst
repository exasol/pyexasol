Dependencies
============

Core dependencies
+++++++++++++++++

- Exasol >= 7.1
- Python >= 3.9
- websocket-client >= 1.0.1
- rsa

Optional dependencies
+++++++++++++++++++++

- ``orjson`` is required for ``json_lib=orjson`` to improve JSON parsing performance
- ``pandas`` is required for :ref:`http_transport` functions working with :class:`pandas.DataFrame`
- ``parquet`` is required for :ref:`http_transport` functions working with :class:`pyarrow.parquet.Table`
- ``polars`` is required for :ref:`http_transport` functions working with :class:`polars.DataFrame`
- ``pproxy`` is used in examples to test HTTP proxy
- ``rapidjson`` is required for ``json_lib=rapidjson`` to improve JSON parsing performance
- ``ujson`` is required for ``json_lib=ujson`` to improve JSON parsing performance

Installation of optional dependencies
+++++++++++++++++++++++++++++++++++++

.. code-block:: bash

   # to install all optional dependencies
   pip install pyexasol[all]
   # to install a particular optional dependency
   pip install pyexasol[<reference_to_package>]
