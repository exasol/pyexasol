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

- ``pandas`` is required for :ref:`http_transport` functions working with data frames
- ``ujson`` is required for ``json_lib=ujson`` to improve JSON parsing performance
- ``rapidjson`` is required for ``json_lib=rapidjson`` to improve JSON parsing performance
- ``orjson`` is required for ``json_lib=orjson`` to improve JSON parsing performance
- ``pproxy`` is used in examples to test HTTP proxy;

Installation with optional dependencies
+++++++++++++++++++++++++++++++++++++++

.. code-block:: bash

   pip install pyexasol[pandas,ujson,rapidjson,orjson,examples]
