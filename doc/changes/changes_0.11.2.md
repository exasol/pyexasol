# 0.11.2 - 2020-01-27

## ExaConnection

- Added option `client_os_username` to specify custom client OS username. Previously username was always detected automatically with `getpass.getuser()`, but it might not work in some environments, like OpenShift.

