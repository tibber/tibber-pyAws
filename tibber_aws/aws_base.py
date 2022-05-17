import asyncio
import logging
from contextlib import AsyncExitStack

from aiobotocore.session import get_session
from aiobotocore.config import AioConfig

_LOGGER = logging.getLogger(__name__)


def get_aiosession():
    return get_session()


class AwsBase:
    def __init__(self, service_name, region_name="eu-west-1", max_pool_connections=10) -> None:
        self._client = None
        self._context_stack = AsyncExitStack()
        self._region_name = region_name
        self._service_name = service_name
        self._init_lock = asyncio.Lock()
        self._max_pool_connections = max_pool_connections

    async def close(self) -> None:
        await self._context_stack.aclose()
        self._client = None

    async def init_client_if_required(self, session=None) -> None:
        async with self._init_lock:
            if self._client is not None:
                return

            config = AioConfig(
                max_pool_connections=self._max_pool_connections,
            )

            session = session or get_aiosession()
            self._client = await self._context_stack.enter_async_context(
                session.create_client(self._service_name, region_name=self._region_name, config=config)
            )
