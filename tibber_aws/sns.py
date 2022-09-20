import dataclasses
import json
from abc import ABC
from datetime import datetime

import boto3
from aiobotocore.session import get_session

from .aws_base import AwsBase


class Topic(AwsBase):
    def __init__(self, topic_name, region_name="eu-west-1"):
        super().__init__("sns", region_name)
        self._topic_arn = (
            boto3.resource(self._service_name, region_name)
            .create_topic(Name=topic_name)
            .arn
        )

    async def publish(self, subject, message):
        await self.init_client_if_required()
        await self._client.publish(
            TargetArn=self._topic_arn,
            Message=json.dumps({"default": json.dumps(message)}),
            Subject=subject,
            MessageStructure="json",
        )


class EventJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


class SnsEvent(ABC):
    subject: str

    def to_json(self, indent=None):
        return json.dumps(self, cls=EventJSONEncoder, indent=indent)


class SnsPublisher:
    def __init__(self, topic_type_mapping: dict):
        self._topic_type_mapping = topic_type_mapping

    async def publish(self, event: SnsEvent):
        session = get_session()
        async with session.create_client("sns") as sns:
            print(
                "Publishing event to SNS: {} with subject '{}' to topic '{}'".format(
                    event.to_json(indent=2),
                    event.subject,
                    self._topic_type_mapping[type(event)],
                )
            )
            # TODO:
            # await sns.publish(
            #     TopicArn=self._topic_type_mapping[type(event)],
            #     Message=event.to_json(),
            #     Subject=event.subject,
            # )
