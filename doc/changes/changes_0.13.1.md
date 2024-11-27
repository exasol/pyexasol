# 0.13.1 - 2020-06-02

- Re-throw `BrokenPipeError` (and other sub-classes of `ConnectionError`) as `ExaCommunicationError`. This type of errors might not be handled in WebSocket client library in certain cases.

