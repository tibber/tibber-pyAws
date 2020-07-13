import datetime
import logging
import zlib

import aiobotocore
import botocore

_LOGGER = logging.getLogger(__name__)

STATE_NOT_EXISTING = "not_existing"
STATE_OK = "ok"
STATE_PRECONDITION_FAILED = "precondition_failed"


class S3Bucket:
    def __init__(self, bucket_name, region_name="eu-west-1"):
        self._bucket_name = bucket_name
        self._region_name = region_name
        session = aiobotocore.get_session()
        self.client = session.create_client("s3", region_name=region_name)

    async def load_data(self, key, if_unmodified_since=None):
        if if_unmodified_since is None:
            if_unmodified_since = datetime.datetime(1900, 1, 1)
        try:
            res = await (
                await self.client.get_object(
                    Bucket=self._bucket_name,
                    Key=key,
                    IfUnmodifiedSince=if_unmodified_since,
                )
            )["Body"].read()
        except self.client.exceptions.NoSuchKey:
            return None, STATE_NOT_EXISTING
        except botocore.exceptions.ClientError as exp:
            if "PreconditionFailed" in str(exp):
                return None, STATE_PRECONDITION_FAILED
            raise
        if len(key) > 3 and key[-3:] == ".gz":
            content = zlib.decompressobj(zlib.MAX_WBITS | 16).decompress(res)
            return content, STATE_OK
        return res.decode("utf-8"), STATE_OK

    async def store_data(self, key, data):
        if len(key) > 3 and key[-3:] == ".gz":
            compressor = zlib.compressobj(wbits=zlib.MAX_WBITS | 16)
            body = compressor.compress(data)
            body += compressor.flush()
        else:
            body = data
        try:
            resp = await self.client.put_object(
                Bucket=self._bucket_name, Key=key, Body=body
            )
        except self.client.exceptions.NoSuchBucket:
            await self.client.create_bucket(Bucket=self._bucket_name,
                                            CreateBucketConfiguration={'LocationConstraint': self._region_name})
            return await self.store_data(key, data)
        return resp

    async def close(self):
        await self.client.close()
