import asyncio
import json
import logging
import os
from urllib.parse import urlparse

import aiohttp
import async_timeout
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.session import Session

_LOGGER = logging.getLogger(__name__)

CREDS = Session().get_credentials()
LAMBDA_ENDPOINT_BASE = 'https://lambda.eu-west-1.amazonaws.com/2015-03-31/functions'
LAMBDA_TIMEOUT = 120


async def invoke(func_name, payload, aiohttp_session, retries=3):
    """Used to invoke lambda functions async."""

    def create_signed_headers(_url, _payload):
        host_segments = urlparse(_url).netloc.split('.')
        service = host_segments[0]
        region = host_segments[1]
        try:
            data = json.dumps(_payload)
        except TypeError:
            _LOGGER.error("Failed to convert to json, %s, %s",
                          _payload.get("deviceId", ""), _payload, exc_info=True)
            raise
        request = AWSRequest(method='POST',
                             url=_url,
                             data=data)
        SigV4Auth(CREDS, service, region).add_auth(request)
        return dict(request.headers.items())

    url = os.path.join(LAMBDA_ENDPOINT_BASE, func_name, 'invocations')
    signed_headers = create_signed_headers(url, payload)

    def log(msg, retry):
        if retry > 1:
            _LOGGER.warning(msg)
            return
        _LOGGER.error(msg)

    for retry in range(retries, 0, -1):
        try:
            with async_timeout.timeout(LAMBDA_TIMEOUT):
                try:
                    async with aiohttp_session.post(url,
                                                    json=payload,
                                                    headers=signed_headers) as response:
                        if response.status != 200:
                            msg = await response.json()
                            log(f"Error getting data from {func_name}, resp code: {response.status}, {msg}", retry)
                            continue
                        return await response.json()
                except aiohttp.client_exceptions.ClientConnectorError:
                    log("ClientConnectorError", retry)
                    continue
        except asyncio.TimeoutError:
            log("Timed out", retry)
            continue

    _LOGGER.error("Error getting data from %s", func_name)
    return {}
