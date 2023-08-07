import pytest
import src.converter as converter


def test_convert_warehouse_credentials_raises_Error_on_null_credential():
    with pytest.raises(converter.NullCredentialsError):
        converter.convert_warehouse_credentials(
            None, "port", "schema", "user", "password"
        )

    with pytest.raises(converter.NullCredentialsError):
        converter.convert_warehouse_credentials(
            "hostname", None, "schema", "user", "password"
        )

    with pytest.raises(converter.NullCredentialsError):
        converter.convert_warehouse_credentials(
            "hostname", "port", None, "user", "password"
        )

    with pytest.raises(converter.NullCredentialsError):
        converter.convert_warehouse_credentials(
            "hostname", "port", "schema", None, "password"
        )

    with pytest.raises(converter.NullCredentialsError):
        converter.convert_warehouse_credentials(
            "hostname", "port", "schema", "user", None
        )


def test_convert_warehouse_credentials_returns_valid_json():
    example = '{"hostname": "banana", "port": "1234", "schema": "example'
    example += 'schema", "user": "exampleuser", "password": "examplepassword"}'

    assert (
        converter.convert_warehouse_credentials(
            "banana", "1234", "exampleschema", "exampleuser", "examplepassword"
        )
        == example
    )


def test_convert_ingestion_credentials_raises_Error_on_null_credential():
    with pytest.raises(converter.NullCredentialsError):
        converter.convert_ingestion_credentials(
            None, "port", "db", "username", "password"
        )

    with pytest.raises(converter.NullCredentialsError):
        converter.convert_ingestion_credentials(
            "hostname", None, "db", "username", "password"
        )

    with pytest.raises(converter.NullCredentialsError):
        converter.convert_ingestion_credentials(
            "hostname", "port", None, "username", "password"
        )

    with pytest.raises(converter.NullCredentialsError):
        converter.convert_ingestion_credentials(
            "hostname", "port", "db", None, "password"
        )

    with pytest.raises(converter.NullCredentialsError):
        converter.convert_ingestion_credentials(
            "hostname", "port", "db", "username", None
        )


def test_convert_ingestion_warehouse_returns_valid_json():
    example = '{"hostname": "examplehostname", "port": "1234", '
    example += '"db": "exampledb", "username": "exampleusername", '
    example += '"password": "examplepassword"}'
    assert (
        converter.convert_ingestion_credentials(
            "examplehostname", "1234", "exampledb",
            "exampleusername", "examplepassword"
        )
        == example
    )
