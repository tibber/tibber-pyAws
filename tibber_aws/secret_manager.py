import base64
import json
import logging

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

_LOGGER = logging.getLogger(__name__)

DEFAULT_REGION_NAME = "eu-west-1"


def get_secret_parser(secret_name, region_name=DEFAULT_REGION_NAME):
    try:
        secret = get_secret(secret_name, region_name)
    except (ClientError, NoCredentialsError):
        _LOGGER.error("No credentials")
        return {}
    if secret is None:
        return {}
    return json.loads(secret)


def get_secret(secret_name, region_name=DEFAULT_REGION_NAME):

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    # Decrypts secret using the associated KMS CMK.
    # Depending on whether the secret is a string or binary, one of these fields will be populated.
    if "SecretString" in get_secret_value_response:
        return get_secret_value_response["SecretString"]
    return base64.b64decode(get_secret_value_response["SecretBinary"])
