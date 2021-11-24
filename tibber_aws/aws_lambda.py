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
LAMBDA_ENDPOINT_BASE = "https://lambda.eu-west-1.amazonaws.com/2015-03-31/functions"
LAMBDA_TIMEOUT = 120


def create_signed_headers(url, payload):
    host_segments = urlparse(url).netloc.split(".")
    service = host_segments[0]
    region = host_segments[1]
    request = AWSRequest(method="POST", url=url, data=payload)
    SigV4Auth(CREDS, service, region).add_auth(request)
    return dict(request.headers.items())


def log(msg, retry):
    if retry > 1:
        _LOGGER.warning(msg)
        return
    _LOGGER.error(msg)


async def invoke(
    func_name, payload, aiohttp_session: aiohttp.ClientSession, retries=3, timeout=LAMBDA_TIMEOUT
):
    """Used to invoke lambda functions async."""
    url = os.path.join(LAMBDA_ENDPOINT_BASE, func_name, "invocations")
    data = json.dumps(payload)
    signed_headers = create_signed_headers(url, data)

    for retry in range(retries, 0, -1):
        try:
            with async_timeout.timeout(timeout):
                try:
                    async with aiohttp_session.post(
                        url, data=data, headers=signed_headers, timeout=aiohttp.ClientTimeout(total=timeout)
                    ) as response:
                        if response.status != 200:
                            msg = await response.json()
                            log(
                                f"Error getting data from {func_name}, resp code: {response.status}, {msg}",
                                retry,
                            )
                            continue
                        return await response.json()
                except aiohttp.client_exceptions.ClientConnectorError:
                    log("ClientConnectorError", retry)
                    continue
        except asyncio.TimeoutError:
            _LOGGER.error("Timed out %s", func_name)
            return {}

    _LOGGER.error(
        "Error getting data from %s. %s", func_name, payload.get("deviceId", "")
    )
    return {}
