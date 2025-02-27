# 0.25.0 - 2022-08-18

- HTTP transport callbacks are now executed inside a context manager for read or write pipe. It guarantees that pipe will be closed in the main thread regardless of successful execution -OR- exception in callback function.  It should help to prevent certain edge cases with pipes on Windows, when pipe `.close()` can block if called in unexpected order.
- HTTP transport "server" termination was simplified. Now it always closes "write" end of pipe first, followed by "read" end of pipe.
- Attempt to fix GitHub action SSL errors.

