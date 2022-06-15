import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable

from aiobotocore.session import get_session

_LOGGER = logging.getLogger()

REGION_NAME = "eu-west-1"


@dataclass
class SqsMessage:
    type: str
    message_id: str
    topic_arn: str
    subject: str
    message: dict
    timestamp: str
    signature_version: str
    signature: str
    signing_cert_url: str
    unsubscribe_url: str


@dataclass
class MessageHandle:
    message_id: str
    body: dict
    receipt_handle: str
    receive_count: int


class SqsListener:
    """SqsListener."""

    def __init__(
        self,
        queue_url: str,
        message_handler: Callable[[SqsMessage], Awaitable],
        max_num_msgs=1,
        wait_time_seconds=2,
        max_retry_count=3,
    ):
        self._queue_url = queue_url
        self._message_handler = message_handler
        self._max_num_msgs = max_num_msgs
        self._wait_time_seconds = wait_time_seconds
        self._max_retry_count = max_retry_count

    async def run(self):
        async def process_message(sqs_client, msg_handle: MessageHandle):
            if msg_handle.receive_count > self._max_retry_count:
                _LOGGER.error(
                    "ReceiptHandle:\n%s\nbody:\n%s\nhas received %s times, will be deleted",
                    msg_handle.receipt_handle,
                    msg_handle.body,
                    msg_handle.receive_count,
                )
                await sqs_client.delete_message(
                    QueueUrl=self._queue_url,
                    ReceiptHandle=msg_handle.receipt_handle,
                )
                return
            try:
                body = msg_handle.body
                await self._message_handler(
                    SqsMessage(
                        type=body.get("Type"),
                        message_id=body.get("MessageId"),
                        topic_arn=body.get("TopicArn"),
                        subject=body.get("Subject"),
                        message=json.loads(body.get("Message")),
                        timestamp=body.get("Timestamp"),
                        signature_version=body.get("SignatureVersion"),
                        signature=body.get("Signature"),
                        signing_cert_url=body.get("SigningCertURL"),
                        unsubscribe_url=body.get("UnsubscribeURL"),
                    )
                )
                # TODO: Possible optimization:
                # Bulk delete:
                # entries = [
                #     {
                #         "Id": message.message_id,
                #         "ReceiptHandle": message.receipt_handle,
                #     }
                #     for message in msg_handles
                # ]
                # await sqs_client.delete_message_batch(
                #     QueueUrl=self._queue_url, Entries=entries
                # )
                #  - Let all the messages process until completion before stating to delete
                await sqs_client.delete_message(
                    QueueUrl=self._queue_url, ReceiptHandle=msg_handle.receipt_handle
                )
            except Exception as e:
                _LOGGER.exception(e)

        session = get_session()
        async with session.create_client("sqs", region_name=REGION_NAME) as sqs_client:
            while True:
                response = await sqs_client.receive_message(
                    QueueUrl=self._queue_url,
                    MaxNumberOfMessages=self._max_num_msgs,
                    WaitTimeSeconds=self._wait_time_seconds,
                    AttributeNames=["ApproximateReceiveCount"],
                )
                msg_handles = [
                    MessageHandle(
                        message_id=msg.get("MessageId"),
                        body=json.loads(msg["Body"]),
                        receipt_handle=msg["ReceiptHandle"],
                        receive_count=int(msg["Attributes"]["ApproximateReceiveCount"]),
                    )
                    for msg in response.get("Messages", [])
                ]
                if msg_handles:
                    running_tasks = []
                    for msg_handle in msg_handles:
                        if msg_handle is None:
                            continue
                        running_tasks.append(
                            asyncio.ensure_future(
                                process_message(sqs_client, msg_handle)
                            )
                        )

                    if running_tasks:
                        tasks_completed, _ = await asyncio.wait(
                            running_tasks, return_when=asyncio.FIRST_COMPLETED
                        )
                        for task in tasks_completed:
                            running_tasks.remove(task)
