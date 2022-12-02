"""
Publish SnsEvent to SNS topic
"""

import dataclasses
import datetime
import json

from aiobotocore.session import get_session


class EventJSONEncoder(json.JSONEncoder):
    """JSON encoder for dataclasses."""

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


@dataclasses.dataclass
class SnsEvent:
    """Base class for SNS events."""

    subject: str

    def to_json(self, indent=None):
        """Return JSON representation of the event."""
        return json.dumps(self, cls=EventJSONEncoder, indent=indent)


class SnsClient:
    """
    Publisher that publishes to SNS

    :param topic_type_mapping: Mapping of event types to SNS topics

    Example:
        @dataclass
        class MySnsEvent(SnsEvent):
            homeId: str
            subject: str = "My SNS subject"

        # create type mapping
        type_mapping = {
            EstimatedAboveThresholdEvent: config["sns_topic"],
        }

        # Create sns client
        async with SnsClient(type_mapping) as sns_publisher:

            # Publish event
            await sns_publisher.publish(
                MySnsEvent(
                    homeId="123",
                )
            )

    """

    def __init__(self, topic_type_mapping: dict):
        """
        :param topic_type_mapping: Mapping of event types to SNS topics
        """
        self._topic_type_mapping = topic_type_mapping
        self._session = get_session()

    async def __aenter__(self):
        """Gives the client a session if it has none when entering an async block"""
        self._sns_client = await self._session.create_client("sns").__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Closes the client when exiting an async block"""
        await self._sns_client.__aexit__(exc_type, exc, tb)

    async def publish(self, sns_event: SnsEvent):
        """Publish SnsEvent to SNS topic"""
        await self._sns_client.publish(
            TopicArn=self._topic_type_mapping[type(sns_event)],
            Message=sns_event.to_json(),
            Subject=sns_event.subject,
        )
