"""SQS Queue object to subscribe queue to topic, produce and consume messages.

#TODO: Shouldn't have to subscribe to an SNS topic to get messages from SQS.

"""
import json
from typing import List, Optional
import logging
import time

from .aws_base import AwsBase, get_aiosession, AioSession
from types_aiobotocore_sqs.client import SQSClient
from types_aiobotocore_sqs.type_defs import SendMessageResultTypeDef

_LOGGER = logging.getLogger(__name__)


class MessageHandle:
    def __init__(self, msg):
        self._msg = msg

    @property
    def body(self):
        return self._msg["Body"]

    @property
    def receipt_handle(self):
        return self._msg["ReceiptHandle"]


class Queue(AwsBase):
    """Queue object to publish and consume messages from AWS SQS, async."""

    _client: SQSClient

    def __init__(self, queue_name, region_name="eu-west-1") -> None:
        self._queue_name = queue_name
        self.queue_url: Optional[str] = None
        super().__init__("sqs", region_name)

    async def subscribe_topic(self, topic_name: str, session: AioSession = None) -> None:
        """Subscribe `Queue` to a topic.

        :param topic_name: name of the topic to subscribe to
        :type topic_name: str
        :param session: the aiobotocore session to use, defaults to None
        :type session: AioSession, optional
        """
        session = session or get_aiosession()
        await self.init_client_if_required(session)

        response = await self._client.create_queue(QueueName=self._queue_name)
        self.queue_url = response["QueueUrl"]
        attr_response = await self._client.get_queue_attributes(QueueUrl=self.queue_url, AttributeNames=["All"])

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

        statement["Condition"]["StringLike"] = statement["Condition"].get("StringLike", {})
        source_arn = statement["Condition"]["StringLike"].get("aws:SourceArn", [])
        if not isinstance(source_arn, list):
            source_arn = [source_arn]

        sns = await self._context_stack.enter_async_context(session.create_client("sns", region_name=self._region_name))
        response = await sns.create_topic(Name=topic_name)
        topic_arn = response["TopicArn"]

        if topic_arn not in source_arn:
            source_arn.append(topic_arn)
            statement["Condition"]["StringLike"]["aws:SourceArn"] = source_arn
            policy["Statement"] = statement
            await self._client.set_queue_attributes(QueueUrl=self.queue_url, Attributes={"Policy": json.dumps(policy)})

        await sns.subscribe(TopicArn=topic_arn, Protocol=self._service_name, Endpoint=queue_arn)
        await sns.close()

    async def receive_message(self, num_msgs: int = 1) -> List[MessageHandle]:
        """
        Consume messages from the queue.

        :param num_msgs: max number of messages to receive. Default 1
        :type num_msgs: int
        :return: Message in the form of a message handle
        :rtype: list[MessageHandle]
        """
        if self.queue_url is None:
            _LOGGER.error("No subscribed queue")
            return [None]
        response = await self._client.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=num_msgs)
        res = []
        for msg in response.get("Messages", []):
            res.append(MessageHandle(msg))
        return res

    async def delete_message(self, msg_handle: MessageHandle) -> None:
        """Delete a message from the queue.

        :param msg_handle: Message handle to delete, will delete based on `msg_handle.receipt_handle`
        :type msg_handle: MessageHandle
        """
        await self._client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=msg_handle.receipt_handle)

    async def send_message(self, message: dict, **kwargs) -> Optional[SendMessageResultTypeDef]:
        """Publish a message to the queue.

        Example of returned metadata dict:
        {'MD5OfMessageBody': '83956d1d4f05f535d55c9895ff593550',
            'MessageId': 'd9654e21-f92b-4d27-bcb5-b27e1dc18839',
            'ResponseMetadata': {'HTTPHeaders': {'content-length': '378',
                                                'content-type': 'text/xml',
                                      'date': 'Tue, 20 Sep 2022 07:00:38 GMT',
                                      'x-amzn-requestid': '7202509e-f09f-5082-8aea-0b3d97c92220'},
                      'HTTPStatusCode': 200,
                      'RequestId': '7202509e-f09f-5082-8aea-0b3d97c92220',
                      'RetryAttempts': 0}}

        :param message: Message to publish, must be serializable to JSON
        :type message: dict
        :return: sent message metadata
        :rtype: SendMessageResultTypeDef
        """
        if self.queue_url is None:
            _LOGGER.error("No subscribed queue, call subscribe_topic first")
            return
        await self.init_client_if_required()
        resp: SendMessageResultTypeDef = await self._client.send_message(
            QueueUrl=self.queue_url, MessageBody=json.dumps(message), **kwargs
        )
        return resp
