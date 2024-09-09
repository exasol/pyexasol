# Design

This document contains background information on various design decisions, which will help current and future maintainers and developers better assess and evaluate potential changes and adjustments to these decisions.

## Automatic Resolution and Randomization of Connection Addresses

By default pyexasol resolves host names to IP addresses, randomly shuffles the IP addresses and tries to connect until connection succeeds. This has the following reasons:

* This will ensure that if at least one hostname is unavailable, an exception will be raised. Otherwise, an exception will occur only when "random" selects a broken hostname, leading to unpredictable errors in production.

* When you have a very large cluster with a growing number of nodes, it makes sense to put all nodes under one hostname, like `myexasol.mlan`, instead of having separate hostnames like `myexasol1..64.mlan`, especially when the number constantly changes. In this case, redundancy will not work properly if the hostname is not resolved beforehand, as we do not know if it points to a single address or multiple addresses.

* For redundancy, we do not want to try the same IP address twice. To our knowledge, this cannot be guaranteed if we do not connect by IP.
