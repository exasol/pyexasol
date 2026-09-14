Import and Export Threading
===========================

The :doc:`user guide's import and export overview
<../user_guide/exploring_features/import_and_export/index>` describes the
public behavior and links to further usage details. This page complements it
with an implementation-oriented view of the threads and transport components
behind callback-based ``IMPORT`` and ``EXPORT`` operations. The diagrams make
the ownership, data flow, and error-propagation relationships easier to trace
when developing or debugging PyExasol.

The callback variants of ``IMPORT`` and ``EXPORT`` use three cooperating threads.
The calling thread runs the user callback, while an HTTP thread transfers data
and a SQL thread executes the database statement. The HTTP thread and SQL thread
are workers created by :meth:`ExaConnection.import_from_callback` or
:meth:`ExaConnection.export_to_callback`.

.. mermaid::

    flowchart TD
        subgraph callback_group["Callback"]
            callback["Calling thread"]
            pipe["OS pipe"]

            callback <--> |"reads or writes data"| pipe
        end

        subgraph http_group["HTTP thread"]
            http["ExaHttpThread"]
        end

        subgraph sql_group["SQL thread"]
            sql["ExaSQLImportThread or<br/>ExaSQLExportThread"]
        end

        db[("Exasol database")]

        pipe <--> http
        http <--> |"HTTP(S) transport"| db
        sql --> |"executes IMPORT or EXPORT"| db
        sql -.-> |"uses address and terminates on failure"| http

HTTP Thread Internals
---------------------

The HTTP thread owns and runs a TCP Server (``ExaTCPServer``). The server uses an
HTTP request handler (``ExaHttpRequestHandler``) to move data between Exasol and the pipe.

.. mermaid::

    flowchart TD
        subgraph http_group["HTTP thread"]
            http["ExaHttpThread"] --> |"owns and runs"| server["ExaTCPServer"]
            server --> handler["ExaHttpRequestHandler"]
        end

        handler <--> pipe["OS pipe"]
        handler <--> |"HTTP(S) request"| db[("Exasol database")]

Responsibilities
----------------

* **Calling thread:** Starts the workers and runs the user callback. For an
  ``IMPORT``, the callback writes data to the pipe. For an ``EXPORT``, it reads
  data from the pipe.
* **HTTP thread:** Owns a TCP server (``ExaTCPServer``) and runs its request loop. The
  server passes the request to a request handler (``ExaHttpRequestHandler``), which moves
  the streamed data through the pipe.
* **SQL thread:** Builds and executes the ``IMPORT`` or ``EXPORT`` statement. The
  statement contains the address exposed by the TCP server, so Exasol can
  connect to the HTTP thread. The SQL thread also keeps a reference to the HTTP
  thread and terminates it when the SQL operation fails.

The pipe separates the callback from the network transport. This lets the
callback work with a file-like binary stream without needing to know about the
socket or the HTTP request.

Data Flow
---------

The direction through the HTTP thread depends on the operation:

* For ``EXPORT``, Exasol sends an HTTP ``PUT`` request to the TCP server.
  The request handler writes the received data to the pipe, and the callback
  reads that data from the pipe.
* For ``IMPORT``, Exasol sends an HTTP ``GET`` request to the TCP server.
  The callback writes data to the pipe, while the request handler reads it and
  sends it to Exasol.

In both cases, the SQL thread runs concurrently with the callback and the HTTP
thread. The coordinator waits for the workers, joins the HTTP thread first, and
then joins the SQL thread so that a SQL failure can stop the HTTP worker before
cleanup waits for it.

Understanding Failures
----------------------

An exception can originate in the callback, the HTTP transport, or the SQL
operation. Since the components share a data path and depend on one another,
one failure can cause a secondary failure elsewhere. The callback API collects
the exceptions observed during cleanup in ``ExaImportError`` or
``ExaExportError``.

For example, if ``ExaTCPServer.handle_request()`` raises a transport exception,
``ExaHttpThread.run()`` stores it in ``http_thread.exc``. The SQL operation may
also fail because its ``IMPORT`` or ``EXPORT`` transport was interrupted. Both
failures can therefore appear in the aggregated exception. Conversely, if the
SQL thread fails first, it terminates the HTTP thread; the HTTP thread may then
exit without an independent exception.

For the public behavior and callback variants, see :doc:`the user guide
<../user_guide/exploring_features/import_and_export/index>` and
:doc:`HTTP Transport <../user_guide/exploring_features/import_and_export/http_transport>`.
