import asyncio

from aiobotocore.session import get_session

from tibber_aws.aws_queue_listener import (AwsQueueListener, SqsMessage,
                                           create_queue_with_subscription)


async def handle_test_message(msg: SqsMessage):
    print("Handling message")
    print(msg.message)


async def main():
    try:
        queue_url = await create_queue_with_subscription(
            "test-py-aws-queue", "test-py-aws-topic"
        )

        session = get_session()
        handlers = {"Test Message": handle_test_message}
        async with session.create_client("sqs", "eu-west-1") as client:
            queue_listener = AwsQueueListener(client, queue_url, handlers)
            await queue_listener.run()

    except KeyboardInterrupt:
        pass


asyncio.run(main())
