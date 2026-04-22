# 2.2.1 - 2026-04-22

## Summary

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency   | Vulnerability  | Affected | Fixed in |
|--------------|----------------|----------|----------|
| cryptography | CVE-2026-39892 | 46.0.6   | 46.0.7   |
| pygments     | CVE-2026-4539  | 2.19.2   | 2.20.0   |
| pytest       | CVE-2025-71176 | 8.4.2    | 9.0.3    |

## Security

* #329: Increased allowed `pytest` range to `>=7.0.0,<10"` and relocked cryptography,
  pygments, and pytest

## Dependency Updates

### `main`

* Updated dependency `cryptography:46.0.6` to `46.0.7`

### `dev`

* Updated dependency `exasol-toolbox:6.1.1` to `6.4.0`
* Updated dependency `pytest:8.4.2` to `9.0.3`
