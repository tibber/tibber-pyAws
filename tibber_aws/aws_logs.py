import logging
from dataclasses import dataclass
from datetime import datetime as dt
import json
from .aws_base import AwsBase
from types_aiobotocore_logs.client import CloudWatchLogsClient

logger = logging.getLogger("aws_logs")


@dataclass
class CloudWatchLogEvent:
    """An AWS CloudWatch log event (a log line)"""
    logStreamName: str
    timestamp: int
    message: str
    ingestionTime: int
    eventId: str

    def __post_init__(self):
        self.timestamp_dt = dt.fromtimestamp(self.timestamp / 1000)
        self.ingestionTime_dt = dt.fromtimestamp(self.ingestionTime / 1000)
        self.message = json.loads(self.message)


class Logs(AwsBase):
    """CloudWatch Logs client to query AWS logs from a log stream."""
    _client: CloudWatchLogsClient

    def __init__(self, region_name="eu-west-1") -> None:
        super().__init__("logs", region_name)

    async def close(self):
        await self._client.close()

    async def _get_log_events_raw(
        self, event: str, log_group: str, start_time: dt, end_time: dt, extra_filter: dict = None, **kwargs
    ) -> dict:
        """Make a call to `CloudWatchLogsClient.filter_log_events` with optional filtering"""
        await self.init_client_if_required()

        properties_to_match = {"event": event}
        if extra_filter is not None:
            properties_to_match.update(extra_filter)
        pattern = " && ".join([f'( $.{k} = "{v}" )' for k, v in properties_to_match.items()])
        pattern = f"{{ {pattern} }}"

        log_events = await self._client.filter_log_events(
            logGroupName=log_group,
            startTime=int(start_time.timestamp() * 1000),
            filterPattern=pattern,
            endTime=int(end_time.timestamp() * 1000),
            **kwargs
        )
        logger.info(
            "Found %s log events searching for pattern=%s in log_group=%s, with responseMetadata=%s",
            len(log_events["events"]), pattern, log_group, log_events.get("responseMetadata"))
        return log_events

    async def get_log_events(
        self,
        event: str,
        log_group: str,
        start_time: dt,
        end_time: dt,
        extra_filter: dict[str, str] = None,
        max_recursion: int = 10,
        **kwargs
    ) -> list[CloudWatchLogEvent]:
        """Retrieve log messages for a specific log event in a log group with potential extra filters

        Example:
        .. code-block:: python
            param_filter = {"body.price_area": "NL", "body.vehicle_type": "easee charger"}
            log_events = await log_client.get_log_events(
                event="Request",
                log_group="prod-hem-api",
                start_time=datetime.datetime(2022,12,7),
                end_time=datetime.datetime(2022,12,15),
                extra_filter=param_filter)

        :param event: name of the event to search for
        :type event: str
        :param log_group: name of the log group to search in
        :type log_group: str
        :param start_time: start of interval for events
        :type start_time: datetime.datetime
        :param end_time: end of interval for events
        :type end_time: datetime.datetime
        :param extra_filter: name of property mapped to the name it so
        :type extra_filter: dict
        :param max_recursion: Limit how many fetches to make when the stream was not depleated. This happens
        when not all records fit in `limit`, defaults to 10000 or the size limit of 1MB.
        :type max_recursion: int
        :return: The events from `log_group` response matching the filter.
        :rtype: list[CloudWatchLogEvent]
        """
        log_events = await self._get_log_events_raw(event, log_group, start_time, end_time, extra_filter, **kwargs)
        events = [CloudWatchLogEvent(**e) for e in log_events.get("events", [])]
        next_token = log_events.get("nextToken")
        i = 1
        while next_token is not None:
            logger.info("More logs to fetch... Using nextToken, %s/%s", i, max_recursion)
            log_events = await self._get_log_events_raw(
                event=event,
                log_group=log_group,
                start_time=start_time,
                end_time=end_time,
                extra_filter=extra_filter,
                **kwargs,
            )
            events += [CloudWatchLogEvent(**e) for e in log_events.get("events", [])]
            next_token = log_events.get("nextToken")
            if i >= max_recursion and next_token is not None:
                logger.warning(
                    "Hit max recursion: %s, more to be fetched. Increase `max_recursion` to get everything.",
                    max_recursion,
                )
                break
            i += 1

        return events


async def main():
    log_client = Logs()
    await log_client.init_client_if_required()
    param_filter = {"body.price_area": "NL", "body.vehicle_type": "easee charger"}
    log_events = await log_client.get_log_events(
        event="Request",
        log_group="prod-hem-api",
        start_time=dt(2022, 12, 10),
        end_time=dt(2022, 12, 15),
        extra_filter=param_filter,
        max_recursion=20)

    await log_client.close()
    return log_events


if __name__ == "__main__":
    import asyncio
    import sys

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - [%(filename)s:%(lineno)d] %(message)s")
    handler.setFormatter(formatter)
    logging.basicConfig(level=logging.getLevelName("DEBUG"), handlers=[handler])

    all_events = asyncio.run(main())
    print(all_events[0])
