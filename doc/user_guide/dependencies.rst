Dependencies
============

Core dependencies
+++++++++++++++++

- Exasol >= 6.2
- Python >= 3.6
- websocket-client >= 1.0.1
- rsa

Optional dependencies
+++++++++++++++++++++

- ``pandas`` is required for :ref:`http_transport` functions working with pandas data frames
- ``polars`` is required for :ref:`http_transport` functions working with polars data frames
- ``ujson`` is required for ``json_lib=ujson`` to improve JSON parsing performance
- ``rapidjson`` is required for ``json_lib=rapidjson`` to improve JSON parsing performance
- ``orjson`` is required for ``json_lib=orjson`` to improve JSON parsing performance
- ``pproxy`` is used in examples to test HTTP proxy;

Installation with optional dependencies
+++++++++++++++++++++++++++++++++++++++

.. code-block:: bash

   pip install pyexasol[pandas,ujson,rapidjson,orjson,examples]
