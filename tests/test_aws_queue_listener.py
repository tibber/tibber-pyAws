import json

import pytest

from tibber_aws.aws_queue_listener import (MessageHandle, SqsMessage,
                                           json_message_processor)


class TestQueue:
    async def delete_message(self, msg_handle):
        print("Deleted", msg_handle)
        pass


test_queue = TestQueue()


@pytest.mark.asyncio
async def test_json_message_processor():
    async def test_handler(message: SqsMessage):
        assert message.subject == "Test Message"
        assert json.loads(message.message)["Data"] == "Hello World"
        assert message.message_id == "1f00b2a8-cfaa-58b6-852c-e83e24249122"

    body = {
        "Type": "Notification",
        "MessageId": "1f00b2a8-cfaa-58b6-852c-e83e24249122",
        "TopicArn": "arn:aws:sns:eu-west-1:945084044763:test-py-aws-topic",
        "Subject": "Test Message",
        "Message": '{\n  "Data": "Hello World"\n}',
        "Timestamp": "2022-02-28T15:25:02.462Z",
        "SignatureVersion": "1",
        "Signature": "mdimGxOWejY+PLhumoZct7Tcsv13oFrutBwTm59BvbvbaDFgHfdHLkEKr9XlGPkKo85Ub/eo4ccIgf9PBoJf0I9EQlDaGw4niiKp722D6MzY4nRoutbyazNBgobyb0Cp5hsmTBSVmiGSeU5M2IS5Jo7AXhl8oA4DXLZii2vu/LLOS9rM9YrNrdf82an7FohhLzFRki46AwJwSOKpYQn5EktB9r7KhGEs49WKzpHQ6NGFzGJVosoJCjICknkiMLpQGXm7ssnI1l06quNN0R46tP/Snq3hhNzeYMVEC2huRQNVpdS4nDPp3tpYfTNjhzGOs+q5ZcUpWDtAoxBpaYG5gA==",
        "SigningCertURL": "https://sns.eu-west-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",
        "UnsubscribeURL": "https://sns.eu-west-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-west-1:945084044763:test-py-aws-topic:583d9061-7e8e-4def-9a93-451f0cf30e19",
    }
    msg = {"Body": json.dumps(body)}
    msg_handle = MessageHandle(msg)
    handlers = {"Test Message": test_handler}
    await json_message_processor(msg_handle, handlers)
