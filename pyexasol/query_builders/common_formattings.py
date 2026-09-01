from dataclasses import dataclass
from enum import Enum
from re import match

from packaging.version import Version

from pyexasol.database_versions import MIN_VERSION_FOR_TLS_PUBLIC_KEY


class StringEnum(str, Enum):
    """Enum whose string representation is its underlying value."""

    def __str__(self) -> str:
        return self.value

    def __format__(self, format_spec: str) -> str:
        return format(self.value, format_spec)

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            normalized_value = value.upper()
            for member in cls:
                if member.value.upper() == normalized_value:
                    return member
        return None


@dataclass(frozen=True)
class TransportEndpoint:
    """Build SQL for a transport endpoint."""

    database_version: Version | None
    encryption: bool

    def build_endpoint_clause(self, endpoint_address: str) -> str:
        """
        Build an ``AT`` endpoint clause.

        Args:
            endpoint_address: A transport endpoint address.

        Returns:
            An ``AT`` clause containing the endpoint URL and, when required, its
            TLS public key.

        Raises:
            ValueError: If ``endpoint_address`` is invalid or a required public key is
                missing.
        """
        ip_address_port, public_key = self._parse_endpoint_address(endpoint_address)

        public_key_clause = ""
        if self.is_tls_public_key_required:
            if not public_key:
                raise ValueError(
                    "Public key is required to be in the 'endpoint_address' for "
                    "encrypted connections with Exasol database version >= "
                    f"{MIN_VERSION_FOR_TLS_PUBLIC_KEY.version}"
                )
            public_key_clause = f" PUBLIC KEY 'sha256//{public_key}'"

        return f"AT '{self.url_prefix}{ip_address_port}'{public_key_clause}"

    @property
    def url_prefix(self) -> str:
        """
        Return the URL scheme needed for a connection.

        Returns:
            ``https://`` for encrypted connections; otherwise, ``http://``.
        """
        if self.encryption:
            return "https://"
        return "http://"

    @property
    def is_tls_public_key_required(self) -> bool:
        """
        Determine whether an encrypted connection requires a TLS public key.

        Returns:
            ``True`` if the connection requires a TLS public key; otherwise, ``False``.
        """
        return (
            MIN_VERSION_FOR_TLS_PUBLIC_KEY.is_supported_by(self.database_version)
            and self.encryption
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
            raise ValueError(f"Could not parse 'endpoint_address' {endpoint_address}")

        ip_address, public_key = address_match.groups()
        if not public_key:
            return ip_address, None
        return ip_address, public_key
