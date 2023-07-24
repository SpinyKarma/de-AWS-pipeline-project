import json


class NullCredentialsError(Exception):
    """
    Raised when a required credential is None
    """

    pass


def convert_warehouse_credentials(port, schema, user, password):
    """
    Returns a string: the JSON representation of our credentials
    """

    if port is None:
        raise NullCredentialsError("port")

    if schema is None:
        raise NullCredentialsError("schema")

    if user is None:
        raise NullCredentialsError("user")

    if password is None:
        raise NullCredentialsError("password")

    credentials = {"port": port, "schema": schema, "user": user, "password": password}

    return json.dumps(credentials)


def convert_ingestion_credentials(hostname, port, db, username, password):
    """
    Returns a string: the JSON representation of our credentials
    """
    if hostname is None:
        raise NullCredentialsError("hostname")

    if port is None:
        raise NullCredentialsError("port")

    if db is None:
        raise NullCredentialsError("db")

    if username is None:
        raise NullCredentialsError("username")

    if password is None:
        raise NullCredentialsError("password")

    credentials = {
        "hostname": hostname,
        "port": port,
        "db": db,
        "username": username,
        "password": password,
    }

    return json.dumps(credentials)
