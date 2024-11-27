# 0.23.3 - 2021-12-03

- SSL certificate verification is now enabled when used with `access_token` or `refresh_token` connection options.
- Updated documentation regarding **encryption**.

OpenID tokens are used to connect to Exasol SAAS clusters, which are available using public internet address. Unlike Exasol clusters running "on-premises" and secured by corporate VPNs and private networks, SAAS clusters are at risk of MITM attacks. SSL certificates must be verified in such conditions.

Exasol SAAS certificates are properly configured using standard certificate authority, so no extra configuration is required.

