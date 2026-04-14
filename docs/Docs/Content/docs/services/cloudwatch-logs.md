---
title: CloudWatch Logs
description: CloudWatch Logs emulation — log groups, streams, events, metric filters, subscription filters, queries.
order: 15
section: Services
---

# CloudWatch Logs

MicroStack's CloudWatch Logs handler supports log groups and streams, structured event storage, metric filters, subscription filters, destinations, and CloudWatch Logs Insights queries.

## Supported Operations

- CreateLogGroup, DeleteLogGroup, DescribeLogGroups
- CreateLogStream, DeleteLogStream, DescribeLogStreams
- PutLogEvents, GetLogEvents, FilterLogEvents
- PutRetentionPolicy, DeleteRetentionPolicy
- PutMetricFilter, DeleteMetricFilter, DescribeMetricFilters
- PutSubscriptionFilter, DeleteSubscriptionFilter, DescribeSubscriptionFilters
- PutDestination, DeleteDestination, DescribeDestinations, PutDestinationPolicy
- StartQuery, GetQueryResults, StopQuery
- TagLogGroup, UntagLogGroup, ListTagsLogGroup
- TagResource, UntagResource, ListTagsForResource

## Usage

```csharp
var logs = new AmazonCloudWatchLogsClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonCloudWatchLogsConfig { ServiceURL = "http://localhost:4566" });

// Create log group and stream
await logs.CreateLogGroupAsync(new CreateLogGroupRequest
{
    LogGroupName = "/myapp/backend",
});

await logs.CreateLogStreamAsync(new CreateLogStreamRequest
{
    LogGroupName = "/myapp/backend",
    LogStreamName = "instance-1",
});

// Write log events
await logs.PutLogEventsAsync(new PutLogEventsRequest
{
    LogGroupName = "/myapp/backend",
    LogStreamName = "instance-1",
    LogEvents =
    [
        new InputLogEvent { Timestamp = DateTime.UtcNow, Message = "Application started" },
        new InputLogEvent { Timestamp = DateTime.UtcNow, Message = "Listening on port 8080" },
    ],
});

// Read log events
var events = await logs.GetLogEventsAsync(new GetLogEventsRequest
{
    LogGroupName = "/myapp/backend",
    LogStreamName = "instance-1",
});

foreach (var e in events.Events)
{
    Console.WriteLine(e.Message);
}
```

## Filtering Events

```csharp
// Filter events matching a pattern
var filtered = await logs.FilterLogEventsAsync(new FilterLogEventsRequest
{
    LogGroupName = "/myapp/backend",
    FilterPattern = "ERROR",
});

foreach (var e in filtered.Events)
{
    Console.WriteLine($"[{e.Timestamp}] {e.Message}");
}
```

## Retention Policies

```csharp
// Set log group retention to 7 days
await logs.PutRetentionPolicyAsync(new PutRetentionPolicyRequest
{
    LogGroupName = "/myapp/backend",
    RetentionInDays = 7,
});
```
