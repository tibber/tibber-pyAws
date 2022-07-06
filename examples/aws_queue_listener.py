import asyncio
from typing import Awaitable, Callable

from aiobotocore.session import get_session

from tibber_aws.aws_queue_listener import SqsListener, SqsMessage


async def handle_test_message(msg: SqsMessage):
    print("Handling message")
    print(msg.message)


async def main():
    listener = SqsListener(
        queue_url="test-py-aws-queue",
        message_handler=handle_test_message,
    )

    await listener.run()


asyncio.run(main())
