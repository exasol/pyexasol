import pytest
from packaging.version import Version

from pyexasol.database_versions import MIN_VERSION_FOR_TLS_PUBLIC_KEY
from pyexasol.query_builders.common_formattings import (
    StringEnum,
    TransportEndpoint,
)


class ExampleStringEnum(StringEnum):
    LOWER = "lower"
    UPPER = "UPPER"


class TestStringEnum:
    @staticmethod
    @pytest.mark.parametrize(
        "member,expected",
        [
            (ExampleStringEnum.LOWER, "lower"),
            (ExampleStringEnum.UPPER, "UPPER"),
        ],
    )
    def test_behaves_as_string(member, expected):
        assert member == expected
        assert isinstance(member, str)

    @staticmethod
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("LOWER", ExampleStringEnum.LOWER),
            ("upper", ExampleStringEnum.UPPER),
        ],
    )
    def test_matches_values_case_insensitively(value, expected):
        assert ExampleStringEnum(value) is expected

    @staticmethod
    def test_rejects_unsupported_value():
        with pytest.raises(
            ValueError, match="'invalid' is not a valid ExampleStringEnum"
        ):
            ExampleStringEnum("invalid")


class TestBuildPublicKeyClause:
    @staticmethod
    @pytest.mark.parametrize(
        "database_version,encryption",
        [
            pytest.param(
                Version("7.1.19"),
                False,
                id="lower_version_without_encryption",
            ),
            pytest.param(
                Version("7.1.19"),
                True,
                id="lower_version_with_encryption",
            ),
            pytest.param(
                MIN_VERSION_FOR_TLS_PUBLIC_KEY.version,
                False,
                id="equal_version_without_encryption",
            ),
            pytest.param(None, False, id="no_db_version_without_encryption"),
            pytest.param(None, True, id="no_db_version_with_encryption"),
        ],
    )
    def test_returns_empty_string_for_non_required_key(database_version, encryption):
        transport_endpoint = TransportEndpoint(
            database_version=database_version, encryption=encryption
        )
        clause = transport_endpoint._build_public_key_clause("public-key")
        assert clause == ""

    @staticmethod
    def test_returns_clause_for_required_key():
        transport_endpoint = TransportEndpoint(
            database_version=MIN_VERSION_FOR_TLS_PUBLIC_KEY.version,
            encryption=True,
        )
        clause = transport_endpoint._build_public_key_clause("public-key")

        assert clause == " PUBLIC KEY 'sha256//public-key'"

    @staticmethod
    def test_raises_without_required_key():
        transport_endpoint = TransportEndpoint(
            database_version=MIN_VERSION_FOR_TLS_PUBLIC_KEY.version,
            encryption=True,
        )

        with pytest.raises(ValueError, match="Public key is required to be in"):
            transport_endpoint._build_public_key_clause(None)


class TestTransportEndpoint:
    @staticmethod
    @pytest.mark.parametrize(
        "database_version",
        (Version("7.1.19"), MIN_VERSION_FOR_TLS_PUBLIC_KEY.version, None),
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
        "database_version", (MIN_VERSION_FOR_TLS_PUBLIC_KEY.version,)
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
        transport_endpoint = TransportEndpoint(
            database_version=MIN_VERSION_FOR_TLS_PUBLIC_KEY.version,
            encryption=True,
        )
        with pytest.raises(ValueError, match="Public key is required to be in"):
            transport_endpoint.build_endpoint_clause(
                endpoint_address="127.18.0.2:8156",
            )

    @staticmethod
    def test_build_endpoint_clause_with_connection_parameters():
        endpoint_clause = TransportEndpoint(
            database_version=None, encryption=False
        ).build_endpoint_clause(
            endpoint_address="127.18.0.2:8156",
            connection_parameters={"MaxConnections": 1, "MaxConcurrentReads": 1},
        )
        assert endpoint_clause == (
            "AT 'http://127.18.0.2:8156;MaxConnections=1;MaxConcurrentReads=1'"
        )

    @staticmethod
    def test_build_connection_parameters_with_empty_dict_returns_empty_string():
        assert TransportEndpoint._build_connection_clause({}) == ""

    @staticmethod
    def test_build_connection_parameters_with_none_returns_empty_string():
        assert TransportEndpoint._build_connection_clause(None) == ""

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
        transport_endpoint = TransportEndpoint(database_version=None, encryption=False)
        with pytest.raises(ValueError, match="Could not parse 'endpoint_address'"):
            transport_endpoint._parse_endpoint_address(endpoint_address)
