from re import match

from packaging.version import Version


class ExasolEndpoint:
    """Build SQL for an Exasol transport endpoint."""

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
    def _parse_exa_address(exa_address: str) -> tuple[str, str | None]:
        """
        Parse an Exasol endpoint address into its address and optional public key.

        Supported formats are:

            ip_address:port
            ip_address:port/public_key

        The public key must be a base64-encoded SHA-256 hash.

        Args:
            exa_address: An Exasol endpoint address.

        Returns:
            A tuple containing the ``ip_address:port`` and the optional public key.

        Raises:
            ValueError: If ``exa_address`` does not match a supported format.
        """
        pattern = r"^([\d\.]+:\d+)(?:\/([a-zA-Z0-9_\-+\/]+=))?$"
        address_match = match(pattern, exa_address)
        if address_match is None:
            raise ValueError(
                f"Could not split exa_address {exa_address} into known components"
            )

        ip_address, public_key = address_match.groups()
        if not public_key:
            return ip_address, None
        return ip_address, public_key
