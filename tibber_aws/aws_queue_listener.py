import asyncio
import json
import logging
import time
from dataclasses import dataclass

from aiobotocore.session import get_session

_LOGGER = logging.getLogger(__name__)


@dataclass
class SqsMessage:
    type: str
    message_id: str
    topic_arn: str
    subject: str
    message: str
    timestamp: str
    signature_version: str
    signature: str
    signing_cert_url: str
    unsubscribe_url: str

    @staticmethod
    def from_json(msg):
        return SqsMessage(
            type=msg.get("Type"),
            message_id=msg.get("MessageId"),
            topic_arn=msg.get("TopicArn"),
            subject=msg.get("Subject"),
            message=msg.get("Message"),
            timestamp=msg.get("Timestamp"),
            signature_version=msg.get("SignatureVersion"),
            signature=msg.get("Signature"),
            signing_cert_url=msg.get("SigningCertURL"),
            unsubscribe_url=msg.get("UnsubscribeURL"),
        )


class MessageHandle:
    def __init__(self, msg):
        self._msg = msg

    @property
    def body(self):
        return self._msg["Body"]

    @property
    def receipt_handle(self):
        return self._msg["ReceiptHandle"]


async def json_message_processor(msg_handle, handlers: dict):
    message = SqsMessage.from_json(json.loads(msg_handle.body))
    await handlers[message.subject](message)


class AwsQueueListener:
    def __init__(
        self,
        sqs_client,
        queue_url: str,
        handlers: dict,
        max_num_msgs=1,
        wait_time_seconds=2,
    ):
        self._sqs_client = sqs_client
        self._queue_url = queue_url
        self._handlers = handlers
        self._max_num_msgs = max_num_msgs
        self._wait_time_seconds = wait_time_seconds

    async def run(self):
        async def process_message(msg_handle):
            try:
                await json_message_processor(msg_handle, self._handlers)
                await self.delete_message(msg_handle)
            except Exception as e:
                _LOGGER.exception(e)

        while True:
            msg_handles = await self.receive_message()
            if msg_handles:
                running_tasks = []
                for msg_handle in msg_handles:
                    if msg_handle is None:
                        continue
                    running_tasks.append(
                        asyncio.ensure_future(process_message(msg_handle))
                    )

                if running_tasks:
                    tasks_completed, _ = await asyncio.wait(
                        running_tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in tasks_completed:
                        running_tasks.remove(task)

    async def receive_message(self) -> list[MessageHandle]:
        response = await self._sqs_client.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=self._max_num_msgs,
            WaitTimeSeconds=self._wait_time_seconds,
        )
        return [MessageHandle(msg) for msg in response.get("Messages", [])]

    async def delete_message(self, msg_handle: MessageHandle) -> None:
        await self._sqs_client.delete_message(
            QueueUrl=self._queue_url, ReceiptHandle=msg_handle.receipt_handle
        )


async def create_queue_with_subscription(
    queue_name, topic_name, region_name="eu-west-1"
):
    session = get_session()
    async with session.create_client("sqs", region_name) as sqs:
        async with session.create_client("sns", region_name) as sns:
            response = await sqs.create_queue(QueueName=queue_name)
            queue_url = response["QueueUrl"]
            attr_response = await sqs.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["All"]
            )

            queue_attributes = attr_response.get("Attributes")
            queue_arn = queue_attributes.get("QueueArn")

            if "Policy" in queue_attributes:
                policy = json.loads(queue_attributes["Policy"])
            else:
                policy = {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "Sid" + str(int(time.time())),
                            "Effect": "Allow",
                            "Principal": {"AWS": "*"},
                            "Action": ["sqs:SendMessage", "sqs:ReceiveMessage"],
                        }
                    ],
                }

            statement = policy.get("Statement", [{}])[0]
            statement["Resource"] = statement.get("Resource", queue_arn)
            statement["Condition"] = statement.get("Condition", {})
            statement["Condition"]["StringLike"] = statement["Condition"].get(
                "StringLike", {}
            )
            source_arn = statement["Condition"]["StringLike"].get("aws:SourceArn", [])
            if not isinstance(source_arn, list):
                source_arn = [source_arn]

            response = await sns.create_topic(Name=topic_name)
            topic_arn = response["TopicArn"]

            if topic_arn not in source_arn:
                source_arn.append(topic_arn)
                statement["Condition"]["StringLike"]["aws:SourceArn"] = source_arn
                policy["Statement"] = statement
                await sqs.set_queue_attributes(
                    QueueUrl=queue_url, Attributes={"Policy": json.dumps(policy)}
                )

            await sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

    return queue_url
