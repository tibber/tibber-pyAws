import logging
from dataclasses import dataclass
import datetime
import json
from tibber_aws.aws_base import AwsBase
from typing import Dict, List, Optional, Union
from types_aiobotocore_logs.client import (
    CloudWatchLogsClient, FilterLogEventsResponseTypeDef, GetLogEventsResponseTypeDef)

logger = logging.getLogger("aws_logs")
UTC = datetime.timezone.utc


@dataclass
class CloudWatchLogEvent:
    """An AWS CloudWatch base log event (a log line)"""
    timestamp: int
    message: str
    ingestionTime: int

    def __post_init__(self):
        self.timestamp_dt = datetime.datetime.fromtimestamp(self.timestamp / 1000, tz=UTC)
        self.ingestionTime_dt = datetime.datetime.fromtimestamp(self.ingestionTime / 1000, tz=UTC)
        self._jsonmsg = None

    @property
    def jsonmsg(self) -> Optional[dict]:
        """Get message as parsed JSON if possible. Otherwise None"""
        if self._jsonmsg is None:
            try:
                self._jsonmsg = json.loads(self.message)
            except ValueError:
                self._jsonmsg = None
        return self._jsonmsg


@dataclass
class CloudWatchFilteredLogEvent(CloudWatchLogEvent):
    """An AWS CloudWatch log event as a result from a filtering query."""
    logStreamName: str
    eventId: str


class Logs(AwsBase):
    """CloudWatch Logs client to query AWS logs from a log stream.

    Example:
    ```python
    >>> log_client = Logs()
    >>> await log_client.init_client_if_required()
    >>> param_filter = {"body.price_area": "NL", "body.vehicle_type": "easee charger"}

    >>> structlog_events = await log_client.get_structlog_events(
    ...    event="Request",
    ...    log_group="prod-hem-api",
    ...    start_time=dt(2022, 12, 10),
    ...    end_time=dt(2022, 12, 15),
    ...    extra_filter=param_filter,
    ...    max_recursion=1)

    >>> raw_filtered_events = await log_client.filter_log_events(
    ...    logGroupName="prod-hem-api",
    ...    filterPattern='{ $.event = "Request" && $.body.price_area = "NL" && $.body.vehicle_type = "easee charger" }',
    ...    startTime=int(dt(2022, 12, 10).timestamp() * 1000),
    ...    endTime=int(dt(2022, 12, 15).timestamp() * 1000),
    ...    limit=10_000,)

    >>> raw_all_events = await log_client.get_log_events(
    ...    logGroupName="prod-hem-api",
    ...    logStreamName="380/tibber_hem_api/0419e686df8846d38b61cb6e0ac99763",
    ...    startTime=int(dt(2022, 12, 10).timestamp() * 1000),
    ...    endTime=int(dt(2022, 12, 15).timestamp() * 1000),
    ...    limit=10_000,)

    >>> all_events = [CloudWatchLogEvent(*e) for e in raw_all_events["events"]]
    >>> filtered_events = [CloudWatchFilteredLogEvent(*e) for e in raw_filtered_events["events"]]
    ```

    """
    _client: CloudWatchLogsClient

    def __init__(self, region_name="eu-west-1") -> None:
        super().__init__("logs", region_name)

    async def get_log_events(
        self,
        logGroupName: str,
        logStreamName: str,
        startTime: int = None,
        endTime: int = None,
        nextToken: str = None,
        limit: int = None,
        startFromHead: bool = None
    ) -> GetLogEventsResponseTypeDef:
        """Lists log events from the specified log group. Exposing native method.

        Convert the returned events with:

        ```python
        >>> log_events = logs.get_log_events(...)
        >>> cwle_events = [CloudWatchLogEvent(**e) for e in log_events["events"]]
        ```

        :param logGroupName: The name of the log group to search.
        :type logGroupName: str
        :param logStreamName: The name of the log stream.
        :type logStreamName: str
        :param startTime: The start of the time range, expressed as the number of milliseconds
        after Jan 1, 1970 00:00:00 UTC. Events with a timestamp before this time are not returned.
        :type startTime: int, optional
        :param endTime: The end of the time range, expressed as the number of milliseconds
        after Jan 1, 1970 00:00:00 UTC. Events with a timestamp later than this time are not returned.
        :type endTime: int, optional
        :param nextToken: he token for the next set of events to return. (You received this token from a previous call)
        :type nextToken: str, optional
        :param limit:  The maximum number of events to return. The default is 10,000 events.
        :type limit: int, optional
        :param startFromHead: If the value is true, the earliest log events are returned first. If the value is false,
        the latest log events are returned first. The default value is false. If you are using a previous
        nextForwardToken value as the nextToken in this operation, you must specify true for startFromHead.
        :type startFromHead: bool, optional
        :return: typed dict with a list of events and some metadata
        :rtype: GetLogEventsResponseTypeDef
        """
        kwargs = dict(
            logGroupName=logGroupName,
            logStreamName=logStreamName,
            startTime=startTime,
            endTime=endTime,
            nextToken=nextToken,
            limit=limit,
            startFromHead=startFromHead)
        used_kwargs = {  # Drop None kwargs to use the built in defaults
            name: param
            for name, param in kwargs.items()
            if param is not None
        }
        return await self._client.get_log_events(**used_kwargs)

    async def filter_log_events(
            self,
            logGroupName: str,
            logGroupIdentifier: str = None,
            logStreamNames: List[str] = None,
            logStreamNamePrefix: str = None,
            startTime: int = None,
            endTime: int = None,
            filterPattern: str = None,
            nextToken: str = None,
            limit: int = None,
            unmask: bool = None) -> FilterLogEventsResponseTypeDef:
        """Filter log events for a specific log-group. Exposing native method.

        Convert the returned events with:

        ```python
        >>> log_events = logs.filter_log_events(...)
        >>> cwfle_events = [CloudWatchFilteredLogEvent(**e) for e in log_events["events"]]
        ```

        :param logGroupName: The name of the log group to search.
        :type logGroupName: str
        :param logGroupIdentifier: Specify either the name or ARN of the log group to view log events from.
        If the log group is in a source account and you are using a monitoring account,
        you must use the log group ARN. If you specify values for both logGroupName and logGroupIdentifier,
        the action returns an InvalidParameterException error.
        :type logGroupIdentifier: str, optional
        :param logStreamNames: Filters the results to only logs from the log streams in this list. If you
        specify a value for both logStreamNamePrefix and logStreamNames, the action
        returns an InvalidParameterException error.
        :type logStreamNames: List[str], optional
        :param logStreamNamePrefix: Filters the results to include only events from log streams that have names
        starting with this prefix. If you specify a value for both logStreamNamePrefix and logStreamNames,
        but the value for logStreamNamePrefix does not match any log stream names specified in logStreamNames,
        the action returns an InvalidParameterException error.
        :type logStreamNamePrefix: str, optional
        :param startTime: The start of the time range, expressed as the number of milliseconds
        after Jan 1, 1970 00:00:00 UTC. Events with a timestamp before this time are not returned.
        :type startTime: int, optional
        :param endTime: The end of the time range, expressed as the number of milliseconds
        after Jan 1, 1970 00:00:00 UTC. Events with a timestamp later than this time are not returned.
        :type endTime: int, optional
        :param filterPattern: The filter pattern to use. If not provided, all events are matched
        :type filterPattern: str, optional
        :param nextToken: he token for the next set of events to return. (You received this token from a previous call)
        :type nextToken: str, optional
        :param limit:  The maximum number of events to return. The default is 10,000 events.
        :type limit: int, optional
        :param unmask: Specify true to display the log event fields with all sensitive data unmasked and visible.
        The default is false. To use this operation with this parameter, you must be signed into an account with
        the logs:Unmask permission.
        :type unmask: bool, optional
        :return: typed dict with:
            searchedLogStreams (list) -- always empty, legacy
            nextToken (string) -- The token to use when requesting the next set of items. Token expires after 24 hours.
            events (list[dict]) -- The matched events. Each event contains a dict which represents a matched event.
                logStreamName (string) -- The name of the log stream to which this event belongs.
                timestamp (integer) -- The time the event occurred, expressed as millisecond unix timestamp.
                message (string) -- The data contained in the log event.
                ingestionTime (integer) -- The time the event was ingested, expressed as millisecond unix timestamp.
                eventId (string) -- The ID of the event.
        :rtype: FilterLogEventsResponseTypeDef
        """
        kwargs = dict(
            logGroupName=logGroupName,
            logGroupIdentifier=logGroupIdentifier,
            logStreamNames=logStreamNames,
            logStreamNamePrefix=logStreamNamePrefix,
            startTime=startTime,
            endTime=endTime,
            filterPattern=filterPattern,
            nextToken=nextToken,
            limit=limit,
            unmask=unmask)
        used_kwargs = {  # Drop None kwargs to use the built in defaults
            name: param
            for name, param in kwargs.items()
            if param is not None
        }
        return await self._client.filter_log_events(**used_kwargs)

    async def _filter_structlog_events(
        self,
        event: str,
        log_group: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        extra_filter: Dict[str, Union[str, float, int]] = None,
        **kwargs
    ) -> FilterLogEventsResponseTypeDef:
        """Make a call to `CloudWatchLogsClient.filter_log_events` with optional filtering assuming the logs
        are structlog formats and have the property `event`.
        """
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

    async def get_structlog_events(
        self,
        event: str,
        log_group: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        extra_filter: Dict[str, str] = None,
        max_recursion: int = 10,
        **kwargs
    ) -> List[CloudWatchFilteredLogEvent]:
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
        :param extra_filter: name of property mapped to the value it should match. All have to be met (&&).
        :type extra_filter: dict
        :param max_recursion: Limit how many fetches to make when the stream was not depleated. This happens
        when not all records fit in `limit`, defaults to 10000 or the size limit of 1MB.
        :type max_recursion: int
        :return: The events from `log_group` response matching the filter.
        :rtype: list[CloudWatchLogEvent]
        """
        log_events = await self._filter_structlog_events(event, log_group, start_time, end_time, extra_filter, **kwargs)
        events = [CloudWatchFilteredLogEvent(**e) for e in log_events.get("events", [])]
        next_token = log_events.get("nextToken")
        i = 1
        while next_token is not None:
            logger.info("More logs to fetch... Using nextToken, %s/%s", i, max_recursion)
            log_events = await self._filter_structlog_events(
                event=event,
                log_group=log_group,
                start_time=start_time,
                end_time=end_time,
                extra_filter=extra_filter,
                **kwargs,
            )
            events += [CloudWatchFilteredLogEvent(**e) for e in log_events.get("events", [])]
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
    structlog_events = await log_client.get_structlog_events(
        event="Request",
        log_group="prod-hem-api",
        start_time=datetime.datetime(2022, 12, 10),
        end_time=datetime.datetime(2022, 12, 15),
        extra_filter=param_filter,
        max_recursion=1)
    raw_filtered_events = await log_client.filter_log_events(
        logGroupName="prod-hem-api",
        filterPattern='{ $.event = "Request" && $.body.price_area = "NL" && $.body.vehicle_type = "easee charger" }',
        startTime=int(datetime.datetime(2022, 12, 10).timestamp() * 1000),
        endTime=int(datetime.datetime(2022, 12, 15).timestamp() * 1000),
        limit=10_000,)
    raw_all_events = await log_client.get_log_events(
        logGroupName="prod-hem-api",
        logStreamName="380/tibber_hem_api/0419e686df8846d38b61cb6e0ac99763",
        startTime=int(datetime.datetime(2022, 12, 10).timestamp() * 1000),
        endTime=int(datetime.datetime(2022, 12, 15).timestamp() * 1000),
        limit=10_000,)
    raw_trading_fundamentals_events = await log_client.get_log_events(
        logGroupName="prod-trading-fundamental-data",
        logStreamName="prod/tibber_trading_fundamental_data/9076218fbfbb4599bb9ed66d822e37d7",
        startTime=int(datetime.datetime(2022, 12, 10).timestamp() * 1000),
        endTime=int(datetime.datetime(2022, 12, 15).timestamp() * 1000),
        limit=10_000,)

    all_events = [CloudWatchLogEvent(**e) for e in raw_all_events["events"]]
    filtered_events = [CloudWatchFilteredLogEvent(**e) for e in raw_filtered_events["events"]]
    all_fundamental_events = [CloudWatchLogEvent(**e) for e in raw_trading_fundamentals_events["events"]]

    logger.info("Filtered event json: %s", filtered_events[0].jsonmsg)
    logger.info("All events event json: %s", all_events[0].jsonmsg)
    logger.info("structlog event json: %s", structlog_events[0].jsonmsg)
    logger.info("fundamental event json: %s", all_fundamental_events[0].jsonmsg)

    await log_client.close()
    logger.critical("Complete")
    return structlog_events


if __name__ == "__main__":
    import asyncio
    import sys

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - [%(filename)s:%(lineno)d] %(message)s")
    handler.setFormatter(formatter)
    logging.basicConfig(level=logging.getLevelName("DEBUG"), handlers=[handler])

    all_struct_events = asyncio.run(main())
    print(all_struct_events[0])
