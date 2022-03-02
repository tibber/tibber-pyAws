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

# {
#     "Messages": [
#         {
#             "MessageId": "09b970fb-7af0-48a3-a1ee-f5e80f63c535",
#             "ReceiptHandle": "AQEBuRklebnwHLXMXLaXbmzUQfY9sDm6l6uZOfA1Yfit4GlLONYbsIB7/8bgwAcSb7AuUqqLKF5Ho/WW2wtmvP7DndCMwtBzFUeIq5csZMsVGh+XhyhXECzyz2E4i5zYL+w8728gmb/NsH4oUmp5oHSukQ5w8gIzoznFEA8w5wPuRb7Lua065Ezr++pDKtQgF/Ab6AvuGuY0oPCyeJGZKZTQ4V0pPr02lhabNugJjb4AFao1kiUiGaeltUkUPayGQLr/8XX8UXCyfiWi9ScYBjiAsHMi6e8ta8hfYJ55Kx8k+gNRKwNp0FqTx0n88LthlkCgC0eVwQL2muv94Ift2MrvU9DVw73O5GmF57WxTF8y51N7A+nzD1y++hR8NggkJjGlCxHI1f+L9mKoCelL546LFw==",
#             "MD5OfBody": "fe74d9deca8a94d36d327e217c8aa0c6",
#             "Body": '{\n  "Type" : "Notification",\n  "MessageId" : "1f00b2a8-cfaa-58b6-852c-e83e24249122",\n  "TopicArn" : "arn:aws:sns:eu-west-1:945084044763:test-py-aws-topic",\n  "Subject" : "\\"Test Message\\"",\n  "Message" : "{\\n  \\"Data\\": \\"Hello World\\"\\n}",\n  "Timestamp" : "2022-02-28T15:25:02.462Z",\n  "SignatureVersion" : "1",\n  "Signature" : "mdimGxOWejY+PLhumoZct7Tcsv13oFrutBwTm59BvbvbaDFgHfdHLkEKr9XlGPkKo85Ub/eo4ccIgf9PBoJf0I9EQlDaGw4niiKp722D6MzY4nRoutbyazNBgobyb0Cp5hsmTBSVmiGSeU5M2IS5Jo7AXhl8oA4DXLZii2vu/LLOS9rM9YrNrdf82an7FohhLzFRki46AwJwSOKpYQn5EktB9r7KhGEs49WKzpHQ6NGFzGJVosoJCjICknkiMLpQGXm7ssnI1l06quNN0R46tP/Snq3hhNzeYMVEC2huRQNVpdS4nDPp3tpYfTNjhzGOs+q5ZcUpWDtAoxBpaYG5gA==",\n  "SigningCertURL" : "https://sns.eu-west-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",\n  "UnsubscribeURL" : "https://sns.eu-west-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-west-1:945084044763:test-py-aws-topic:583d9061-7e8e-4def-9a93-451f0cf30e19"\n}',
#         }
#     ],
#     "ResponseMetadata": {
#         "RequestId": "9f32bc5a-9ec7-5f3e-8707-e6c3fe82ec5b",
#         "HTTPStatusCode": 200,
#         "HTTPHeaders": {
#             "x-amzn-requestid": "9f32bc5a-9ec7-5f3e-8707-e6c3fe82ec5b",
#             "date": "Wed, 02 Mar 2022 12:07:33 GMT",
#             "content-type": "text/xml",
#             "content-length": "2071",
#         },
#         "RetryAttempts": 0,
#     },
# }


{
    "Type": "Notification",
    "MessageId": "bc97c1ac-d514-5019-a4d0-9b35b1cb24ef",
    "TopicArn": "arn:aws:sns:eu-west-1:945084044763:test-py-aws-topic",
    "Subject": "Test Message",
    "Message": "Hello World",
    "Timestamp": "2022-03-02T12:27:11.628Z",
    "SignatureVersion": "1",
    "Signature": "JDdZ2eyRxXl7QA7JfboWU3KqSvuVwkJf2Eyl6eAc2bbLFvSGe0Iuc2Vfa5csvlZCfagrWQ+PrmNxoWQVbfK88LLgr69xqYw1eTLL+79BCrMAbZPq7Zq2xCPwG1z78O6fWlDxoJABbVTvokWy286B3iXblG3ZYdlEJelebxjRxccaLUqdQ7Zh9tK36fpbI4sXMJam71vKCplC/mQgxBq9TAF1vfv+A89oJ+glpBWRXMhrdQshB2T66sm/M03I9TVV/6j1/A3p4mAXa89QrczAB8zTVuSx7DwM7v+uZwXXqkKedxCflRApdV0kchioXCEXe7FzRPcQRAmFrWvlQTI2Ig==",
    "SigningCertURL": "https://sns.eu-west-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",
    "UnsubscribeURL": "https://sns.eu-west-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-west-1:945084044763:test-py-aws-topic:583d9061-7e8e-4def-9a93-451f0cf30e19",
}
