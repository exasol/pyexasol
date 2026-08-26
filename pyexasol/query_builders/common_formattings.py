from re import match

from packaging.version import Version


class TransportEndpoint:
    """Build SQL for a transport endpoint."""

    @staticmethod
    def build_endpoint_clause(
        endpoint_address: str,
        exasol_db_version: Version | None,
        encryption: bool,
    ) -> str:
        """
        Build an ``AT`` endpoint clause.

        Args:
            endpoint_address: A transport endpoint address.
            exasol_db_version: The Exasol database version, if available.
            encryption: ``True`` if the connection uses TLS encryption; otherwise,
                ``False``.

        Returns:
            An ``AT`` clause containing the endpoint URL and, when required, its
            TLS public key.

        Raises:
            ValueError: If ``endpoint_address`` is invalid or a required public key is
                missing.
        """
        ip_address_port, public_key = TransportEndpoint._parse_endpoint_address(
            endpoint_address
        )
        url_prefix = TransportEndpoint._get_url_prefix(encryption)
        endpoint_clause = f"AT '{url_prefix}{ip_address_port}'"

        if TransportEndpoint._is_tls_public_key_required(
            exasol_db_version=exasol_db_version, encryption=encryption
        ):
            if not public_key:
                raise ValueError(
                    "Public key is required to be in the 'endpoint_address' for "
                    "encrypted connections with Exasol DB >= 8.32.0"
                )
            endpoint_clause += f" PUBLIC KEY 'sha256//{public_key}'"

        return endpoint_clause

    @staticmethod
    def _get_url_prefix(encryption: bool) -> str:
        """
        Return the URL scheme needed for an Exasol connection.

        Args:
            encryption: ``True`` if the connection uses TLS encryption; otherwise,
                ``False``.

        Returns:
            ``https://`` for encrypted connections; otherwise, ``http://``.
        """
        if encryption:
            return "https://"
        return "http://"

    @staticmethod
    def _is_tls_public_key_required(
        exasol_db_version: Version | None, encryption: bool
    ) -> bool:
        """
        Determine whether an encrypted connection requires a TLS public key.

        TLS (Transport Layer Security) encrypts network traffic and authenticates
        the server. Exasol 8.32.0 introduced certificate verification for import and
        export connections, requiring the SHA-256 fingerprint of the server
        certificate's public key when encryption is enabled. See the Exasol database
        changelog: https://docs.exasol.com/db/latest/changelogs/21747.htm

        Args:
            exasol_db_version: The Exasol database version, if available.
            encryption: ``True`` if the connection uses TLS encryption; otherwise,
                ``False``.

        Returns:
            ``True`` if the connection requires a TLS public key; otherwise, ``False``.
        """
        return (
            exasol_db_version is not None
            and exasol_db_version >= Version("8.32.0")
            and encryption
        )

    @staticmethod
    def _parse_endpoint_address(endpoint_address: str) -> tuple[str, str | None]:
        """
        Parse a database endpoint address into its address and optional public key.

        Supported formats are:

            ip_address:port
            ip_address:port/public_key

        The public key must be a base64-encoded SHA-256 hash.

        Args:
            endpoint_address: A transport endpoint address.

        Returns:
            A tuple containing the ``ip_address:port`` and the optional public key.

        Raises:
            ValueError: If ``endpoint_address`` does not match a supported format.
        """
        pattern = r"^([\d\.]+:\d+)(?:\/([a-zA-Z0-9_\-+\/]+=))?$"
        address_match = match(pattern, endpoint_address)
        if address_match is None:
            raise ValueError(
                f"Could not split endpoint_address {endpoint_address} into known components"
            )

        ip_address, public_key = address_match.groups()
        if not public_key:
            return ip_address, None
        return ip_address, public_key
