import pytest
from packaging.version import Version

from pyexasol.query_builders.common_formattings import (
    MIN_DATABASE_VERSION_FOR_TLS_PUBLIC_KEY,
    TransportEndpoint,
)


class TestTransportEndpoint:
    @staticmethod
    @pytest.mark.parametrize(
        "database_version",
        (Version("7.1.19"), MIN_DATABASE_VERSION_FOR_TLS_PUBLIC_KEY, None),
    )
    def test_build_endpoint_clause_without_encryption(database_version):
        endpoint_clause = TransportEndpoint(
            database_version=database_version, encryption=False
        ).build_endpoint_clause(
            endpoint_address="127.18.0.2:8156",
        )
        assert endpoint_clause == "AT 'http://127.18.0.2:8156'"

    @staticmethod
    @pytest.mark.parametrize("database_version", (Version("7.1.19"), None))
    def test_build_endpoint_clause_with_encryption_below_min_database_version(
        database_version,
    ):
        endpoint_clause = TransportEndpoint(
            database_version=database_version, encryption=True
        ).build_endpoint_clause(
            endpoint_address="127.18.0.2:8156/tfdCUbrFQxEBTtrD9yet67fwCQMlxNVGqIdagPXvnlM=",
        )
        assert endpoint_clause == "AT 'https://127.18.0.2:8156'"

    @staticmethod
    @pytest.mark.parametrize(
        "database_version", (MIN_DATABASE_VERSION_FOR_TLS_PUBLIC_KEY,)
    )
    def test_build_endpoint_clause_with_encryption_at_min_database_version(
        database_version,
    ):
        endpoint_clause = TransportEndpoint(
            database_version=database_version, encryption=True
        ).build_endpoint_clause(
            endpoint_address="127.18.0.2:8156/tfdCUbrFQxEBTtrD9yet67fwCQMlxNVGqIdagPXvnlM=",
        )
        assert endpoint_clause == (
            "AT 'https://127.18.0.2:8156' PUBLIC KEY "
            "'sha256//tfdCUbrFQxEBTtrD9yet67fwCQMlxNVGqIdagPXvnlM='"
        )

    @staticmethod
    def test_build_endpoint_clause_raises_exception():
        with pytest.raises(ValueError, match="Public key is required to be in"):
            TransportEndpoint(
                database_version=MIN_DATABASE_VERSION_FOR_TLS_PUBLIC_KEY,
                encryption=True,
            ).build_endpoint_clause(
                endpoint_address="127.18.0.2:8156",
            )

    @staticmethod
    @pytest.mark.parametrize(
        "encryption,expected",
        [
            (False, "http://"),
            (True, "https://"),
        ],
    )
    def test_get_url_prefix(encryption, expected):
        url_prefix = TransportEndpoint(
            database_version=None, encryption=encryption
        ).url_prefix
        assert url_prefix == expected

    @staticmethod
    @pytest.mark.parametrize(
        "db_version,encryption,expected",
        [
            pytest.param(
                Version("7.1.19"), False, False, id="lower_version_without_encryption"
            ),
            pytest.param(
                Version("7.1.19"), True, False, id="lower_version_with_encryption"
            ),
            pytest.param(
                MIN_DATABASE_VERSION_FOR_TLS_PUBLIC_KEY,
                True,
                True,
                id="equal_version_with_encryption",
            ),
            pytest.param(
                MIN_DATABASE_VERSION_FOR_TLS_PUBLIC_KEY,
                False,
                False,
                id="equal_version_without_encryption",
            ),
            pytest.param(None, False, False, id="no_db_version_without_encryption"),
            pytest.param(None, True, False, id="no_db_version_with_encryption"),
        ],
    )
    def test_is_tls_public_key_required(db_version, encryption, expected):
        result = TransportEndpoint(
            database_version=db_version, encryption=encryption
        ).is_tls_public_key_required
        assert result == expected

    @staticmethod
    @pytest.mark.parametrize(
        "ip_address, public_key",
        [
            pytest.param(
                "127.18.0.2:8156",
                "tfdCUbrFQxEBTtrD9yet67fwCQMlxNVGqIdagPXvnlM=",
                id="ip",
            ),
            pytest.param(
                "127.18.0.2:8364",
                None,
                id="url_without_public_key",
            ),
        ],
    )
    def test_parse_endpoint_address(ip_address: str, public_key: str):
        endpoint_address = f"{ip_address}"
        if public_key:
            endpoint_address = f"{ip_address}/{public_key}"
        result = TransportEndpoint(
            database_version=None, encryption=False
        )._parse_endpoint_address(endpoint_address)
        assert result[0] == ip_address
        assert result[1] == public_key

    @staticmethod
    @pytest.mark.parametrize(
        "endpoint_address",
        [
            pytest.param(
                "127.18.0.2:8364/YHistZoLhU9+FKoSEH", id="incomplete_public_key"
            ),
            pytest.param("127.18.0.2/64:8364", id="cidr_notation"),
            pytest.param("localhost:1729", id="localhost"),
        ],
    )
    def test_parse_endpoint_address_raises_exception(endpoint_address: str):
        with pytest.raises(ValueError, match="Could not parse 'endpoint_address'"):
            TransportEndpoint(
                database_version=None, encryption=False
            )._parse_endpoint_address(endpoint_address)
