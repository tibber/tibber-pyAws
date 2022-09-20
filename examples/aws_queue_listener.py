import asyncio
from typing import Awaitable, Callable

from tibber_aws.aws_queue_listener import SqsListener, SqsMessage


async def handle_test_message(msg: SqsMessage):
    print("Handling message")
    print(msg.message)


def create_message_handler() -> Callable[[SqsMessage], Awaitable]:
    """Create message handler."""

    async def message_handler(message: SqsMessage):
        if message.subject == "YOUR SUBJECT":
            return await handle_test_message(message)
        return True

    return message_handler


async def main():

    listener = SqsListener(
        queue_url="test-py-aws-queue",
        message_handler=create_message_handler(),
    )

    await listener.run()


asyncio.run(main())
