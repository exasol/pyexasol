from dataclasses import dataclass
from enum import Enum
from re import match
from typing import Annotated

from packaging.version import Version
from pydantic import AfterValidator

from pyexasol.database_versions import MIN_VERSION_FOR_TLS_PUBLIC_KEY

COLUMN_NUMBER_OR_RANGE = r"\d+|\d+\.\.\d+"


def reject_string_as_iterable(value: object) -> object:
    """Reject strings before Pydantic converts iterables into generators."""
    if isinstance(value, str):
        raise ValueError("must be an iterable, not a single string.")
    return value


def validate_comment(comment: str | None) -> str | None:
    """Validate that a comment can be safely embedded in a SQL comment."""
    if comment is None:
        return comment
    if "*/" in comment:
        raise ValueError(f"'comment' {comment} must not contain '*/'")
    return f"/*{comment}*/"


Comment = Annotated[str | None, AfterValidator(validate_comment)]


def join_query_lines(*query_lines: str | None) -> str:
    """Join non-empty SQL query lines with newline separators."""
    return "\n".join(filter(None, query_lines))


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

    def build_endpoint_clause(
        self, endpoint_address: str, connection_parameters: dict | None = None
    ) -> str:
        """
        Build an ``AT`` endpoint clause.

        Args:
            endpoint_address: A transport endpoint address.
            connection_parameters: Optional connection parameters appended to the
                endpoint URL, such as Parquet import options.

        Returns:
            An ``AT`` clause containing the endpoint URL, optional connection
            parameters, and, when required, its TLS public key.

        Raises:
            ValueError: If ``endpoint_address`` is invalid or a required public key is
                missing.
        """
        ip_address_port, public_key = self._parse_endpoint_address(endpoint_address)
        connection_clause = self._build_connection_clause(connection_parameters)
        public_key_clause = self._build_public_key_clause(public_key)

        return (
            f"AT '{self.url_prefix}{ip_address_port}{connection_clause}'"
            f"{public_key_clause}"
        )

    def _build_public_key_clause(self, public_key: str | None) -> str:
        """
        Build the TLS public-key clause when required by the connection.

        Args:
            public_key: The base64-encoded SHA-256 public-key fingerprint, or
                ``None`` when no public key was provided.

        Returns:
            A ``PUBLIC KEY`` clause when encryption is enabled and the database
            version requires public-key verification; otherwise, an empty string.

        Raises:
            ValueError: If public-key verification is required but ``public_key``
                is missing.
        """
        if self.encryption and MIN_VERSION_FOR_TLS_PUBLIC_KEY.is_supported_by(
            self.database_version
        ):
            if not public_key:
                raise ValueError(
                    "Public key is required to be in the 'endpoint_address' for "
                    "encrypted connections with Exasol database version >= "
                    f"{MIN_VERSION_FOR_TLS_PUBLIC_KEY.version}"
                )

            return f" PUBLIC KEY 'sha256//{public_key}'"

        return ""

    @staticmethod
    def _build_connection_clause(connection_parameters: dict | None) -> str:
        """
        Build a connection-string clause from optional connection parameters.

        Args:
            connection_parameters: A mapping of connection-parameter names to
                values, or ``None`` when no parameters should be appended.

        Returns:
            A semicolon-delimited connection clause, such as
            ``;MaxConnections=2;MaxConcurrentReads=1``, or an empty string when
            ``connection_parameters`` is ``None`` or empty.
        """
        if connection_parameters is None:
            return ""

        return "".join(
            f";{parameter_name}={parameter_value}"
            for parameter_name, parameter_value in connection_parameters.items()
        )

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
