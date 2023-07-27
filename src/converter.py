import json

'''
All functions in this module are used to covert the passed arguments into a
credentials JSON for upload to AWS secrets-manager.
'''


class NullCredentialsError(Exception):
    """Raised when a required credential is None."""

    pass


def convert_warehouse_credentials(hostname, port, schema, user, password):
    """
    Returns a string: the JSON representation of our credentials.
    Uses the format for the warehouse credentials.
    """

    if hostname is None:
        raise NullCredentialsError("hostname")

    if port is None:
        raise NullCredentialsError("port")

    if schema is None:
        raise NullCredentialsError("schema")

    if user is None:
        raise NullCredentialsError("user")

    if password is None:
        raise NullCredentialsError("password")

    credentials = {
        "hostname": hostname,
        "port": port,
        "schema": schema,
        "user": user,
        "password": password
    }

    return json.dumps(credentials)


def convert_ingestion_credentials(hostname, port, db, username, password):
    """
    Returns a string: the JSON representation of our credentials.
    Uses the format for the ingestion credentials.
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
