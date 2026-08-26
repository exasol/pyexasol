import pytest
from packaging.version import Version

from pyexasol.query_builders.common_formattings import TransportEndpoint


class TestTransportEndpoint:
    @staticmethod
    @pytest.mark.parametrize(
        "encryption,expected",
        [
            (False, "http://"),
            (True, "https://"),
        ],
    )
    def test_get_url_prefix(encryption, expected):
        url_prefix = TransportEndpoint._get_url_prefix(encryption=encryption)
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
                Version("8.32.0"), True, True, id="equal_version_with_encryption"
            ),
            pytest.param(
                Version("8.32.0"), False, False, id="equal_version_without_encryption"
            ),
            pytest.param(None, False, False, id="no_db_version_without_encryption"),
            pytest.param(None, True, False, id="no_db_version_with_encryption"),
        ],
    )
    def test_is_tls_public_key_required(db_version, encryption, expected):

        result = TransportEndpoint._is_tls_public_key_required(
            exasol_db_version=db_version, encryption=encryption
        )
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
        result = TransportEndpoint._parse_endpoint_address(endpoint_address)
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
    def test__parse_endpoint_address_raises_exception(endpoint_address: str):
        with pytest.raises(ValueError, match="Could not split endpoint_address"):
            TransportEndpoint._parse_endpoint_address(endpoint_address)
