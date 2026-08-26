import pytest
from packaging.version import Version

from pyexasol.query_builders.common_formattings import ExasolEndpoint


class TestExasolEndpoint:
    @staticmethod
    @pytest.mark.parametrize(
        "encryption,expected",
        [
            (False, "http://"),
            (True, "https://"),
        ],
    )
    def test_get_url_prefix(encryption, expected):
        url_prefix = ExasolEndpoint._get_url_prefix(encryption=encryption)
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

        result = ExasolEndpoint._is_tls_public_key_required(
            exasol_db_version=db_version, encryption=encryption
        )
        assert result == expected
