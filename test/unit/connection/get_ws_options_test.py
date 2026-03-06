from ssl import (
    CERT_NONE,
    CERT_REQUIRED,
)


class TestGetWsOptions:
    """
    To create the websocket connection, specific parameters are passed into it.
    In PyExasol, users may connect:
        1) using the default behavior where certificate verification is required
           - The certificate being verified would be automatically retrieved from an
             OS-specific truststore.
        2) by providing a fingerprint in their DSN
          - The fingerprint is extracted from the DSN and used for fingerprint
            verification. As a convenience, PyExasol automatically turns off the
            default of certificate verification.
       3) by providing a certificate stored in another location.
         - As it is currently coded, users must add
           `websocket_sslopt["cert_reqs"] = CERT_REQUIRED` for certificate
           verification to be turned on. This is because the code directly takes the
           value passed into `websocket_sslopt`. Otherwise, this behaves similar to 1).
       4) by turning off certificate verification by passing a case-insensitive
          `nocertcheck` as a fingerprint in their DSN.
          - Unlike 2), this only deactivates certificate verification; the
            `nocertcheck` value is not passed onto the websocket connection. The
            motivation for adding this is that customers are used to this experience
            from other drivers, like the
            `JDBC driver <https://docs.exasol.com/db/latest/connect_exasol/drivers/jdbc.htm>`__.
          - Customers have been explicitly warned that this is a security risk and
            that this value should ONLY be used in testing environments.

    Each of these has slightly different defaults that have been decided upon based on
    Exasol's security guidelines. These are explicitly tested here to ensure that the
    defaults do not wander over time without explicit intention.
    """

    @staticmethod
    def test_default_with_certificate_in_truststore(
        mock_exaconnection_factory, recwarn
    ):
        connection = mock_exaconnection_factory()
        result = connection._get_ws_options(fingerprint=None)

        assert result == {
            "timeout": 10,
            "skip_utf8_validation": True,
            "enable_multithread": True,
            "sslopt": {"cert_reqs": CERT_REQUIRED},
        }
        # To help (new) users of PyExasol, a warning message is emitted to make it clear
        # that the default behavior is to require a certificate from
        # an OS-specific truststore and to verify it. This makes it easier for a user
        # to debug the issue if the websocket connection returns an exception,
        # indicating that a connection could not be made with the provided credentials.
        assert len(recwarn.list) == 1
        assert "From PyExasol version ``1.0.0``," in str(recwarn.list[0].message)

    @staticmethod
    def test_verification_with_fingerprint(mock_exaconnection_factory, recwarn):
        fingerprint = "7BBBF74F1F2B993BB81FF5F795BCA2340CC697B8DEFEB768DD6BABDF13FB2F05"
        dsn = f"localhost/{fingerprint}:8563"

        connection = mock_exaconnection_factory(dsn=dsn)
        result = connection._get_ws_options(fingerprint=fingerprint)

        assert result == {
            "timeout": 10,
            "skip_utf8_validation": True,
            "enable_multithread": True,
            "sslopt": {"cert_reqs": CERT_NONE},
        }
        assert len(recwarn.list) == 0

    @staticmethod
    def test_verification_with_certificate(mock_exaconnection_factory, recwarn):
        # If websocket_sslopt is defined, like here, this is propagated as is without
        # any checks for this function. This means that a customer must provide
        # `websocket_sslopt["cert_reqs"]= CERT_REQUIRED` to activate certificate
        # verification.

        websocket_sslopt = {"cert_reqs": CERT_REQUIRED, "ca_certs": "rootCA.crt"}

        connection = mock_exaconnection_factory(websocket_sslopt=websocket_sslopt)
        result = connection._get_ws_options(fingerprint=None)

        assert result == {
            "timeout": 10,
            "skip_utf8_validation": True,
            "enable_multithread": True,
            "sslopt": websocket_sslopt,
        }
        assert len(recwarn.list) == 0

    @staticmethod
    def test_verification_with_fingerprint_nocertcheck(
        mock_exaconnection_factory, recwarn
    ):
        fingerprint = "nocertcheck"
        dsn = f"localhost/{fingerprint}:8563"

        connection = mock_exaconnection_factory(dsn=dsn)
        result = connection._get_ws_options(fingerprint=fingerprint)

        assert result == {
            "timeout": 10,
            "skip_utf8_validation": True,
            "enable_multithread": True,
            "sslopt": {"cert_reqs": CERT_NONE},
        }
        assert len(recwarn.list) == 0
