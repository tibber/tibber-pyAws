"""Sqs listener."""
import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from aiobotocore.session import get_session

_LOGGER = logging.getLogger(__name__)


@dataclass
class SqsMessage:
    """Sqs message."""

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
    """Message handle"""

    message_id: str
    body: dict
    receipt_handle: str
    receive_count: int


@dataclass
class SqsListenerOptions:
    """Sqs listener options."""

    max_num_msgs: int = 1
    wait_time_seconds: int = 2
    max_retry_count: int = 3
    region_name: str = "eu-west-1"

    def __post_init__(self):
        # Aws supports fetching of max 10 messages at once
        if self.max_num_msgs < 1 or self.max_num_msgs > 10:
            raise ValueError("max_num_msgs must be between 1 and 10")


class SqsListener:
    """SqsListener."""

    def __init__(
        self,
        queue_url: str,
        message_handler: Callable[[SqsMessage], Awaitable],
        options: Optional[SqsListenerOptions] = None,
    ):
        self._queue_url = queue_url
        self._message_handler = message_handler
        self._options = options or SqsListenerOptions()
        self._session = get_session()

    async def run(self):
        """run sqs listener."""

        async def process_message(sqs_client, msg_handle: MessageHandle):
            if msg_handle.receive_count > self._options.max_retry_count:
                # TODO: Change to warning?
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
                if await self._message_handler(
                    SqsMessage(
                        type=body["Type"],
                        message_id=body["MessageId"],
                        topic_arn=body["TopicArn"],
                        subject=body["Subject"],
                        message=json.loads(body["Message"]),
                        timestamp=body["Timestamp"],
                        signature_version=body["SignatureVersion"],
                        signature=body["Signature"],
                        signing_cert_url=body["SigningCertURL"],
                        unsubscribe_url=body["UnsubscribeURL"],
                    )
                ):
                    await sqs_client.delete_message(
                        QueueUrl=self._queue_url,
                        ReceiptHandle=msg_handle.receipt_handle,
                    )
            except Exception:  # pylint: disable=broad-except
                _LOGGER.error("Error in process_message", exc_info=True)

        running_tasks = []

        async with self._session.create_client(
            "sqs", region_name=self._options.region_name
        ) as sqs:
            while True:
                num_msgs = self._options.max_num_msgs - len(running_tasks)
                response = await sqs.receive_message(
                    QueueUrl=self._queue_url,
                    MaxNumberOfMessages=num_msgs,
                    WaitTimeSeconds=self._options.wait_time_seconds,
                    AttributeNames=["ApproximateReceiveCount"],
                )
                for msg in response.get("Messages", []):
                    msg_handle = MessageHandle(
                        message_id=msg.get("MessageId"),
                        body=json.loads(msg["Body"]),
                        receipt_handle=msg["ReceiptHandle"],
                        receive_count=int(msg["Attributes"]["ApproximateReceiveCount"]),
                    )
                    running_tasks.append(
                        asyncio.ensure_future(process_message(sqs, msg_handle))
                    )

                if running_tasks:
                    tasks_completed, _ = await asyncio.wait(
                        running_tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in tasks_completed:
                        running_tasks.remove(task)
