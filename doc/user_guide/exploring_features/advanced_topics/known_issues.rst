Known Issues
============

``ssl.SSLError: Underlying socket connection gone (_ssl.c:...)``
------------------------------------------------------------------

This error can occur in rare cases, for additional details see `GH-Issue #108 <https://github.com/exasol/pyexasol/issues/108>`_
and ``pyexasol.connection.ExaConnection:__del__``.
The root cause of this issue usually stems from a connection not being properly closed before program exit (interpreter shutdown).

Example Output:

.. code-block:: shell

    Exception ignored in: <function ConnectionWrapper.__del__ at 0x7fa7c9799990>
    Traceback (most recent call last):
      File ".../test_case.py", line 14, in __del__
      File ".../lib/python3.10/site-packages/pyexasol/connection.py", line 456, in close
      File ".../lib/python3.10/site-packages/pyexasol/connection.py", line 524, in req
      File ".../lib/python3.10/site-packages/websocket/_core.py", line 285, in send
      File ".../lib/python3.10/site-packages/websocket/_core.py", line 313, in send_frame
      File ".../lib/python3.10/site-packages/websocket/_core.py", line 527, in _send
      File ".../lib/python3.10/site-packages/websocket/_socket.py", line 172, in send
      File ".../lib/python3.10/site-packages/websocket/_socket.py", line 149, in _send
      File "/usr/lib/python3.10/ssl.py", line 1206, in send
    ssl.SSLError: Underlying socket connection gone (_ssl.c:2326)
