# 0.23.1 - 2021-11-21

- Improved termination logic for HTTP transport thread while handling an exception. Order of closing pipes now depends on type of callback (EXPORT or IMPORT). It should help to prevent unresponsive infinite loop on Windows.
- Improved parallel HTTP transport examples with better exception handling.
- Removed comment about `if __name__ == '__main__':` being required for Windows OS only. Multiprocessing on macOS uses `spawn` method in the most recent Python versions, so it is no longer unique.
- `pyopenssl` is now a hard dependency, which is required for encrypted HTTP transport to generate an "ad-hoc" certificate. Encryption will be enabled by default for SAAS Exasol in future.

