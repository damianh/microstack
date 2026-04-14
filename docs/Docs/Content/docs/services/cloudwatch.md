---
title: CloudWatch Metrics
description: CloudWatch Metrics emulation — metrics, alarms (standard and composite), dashboards, and alarm state management.
order: 26
section: Services
---

# CloudWatch Metrics

MicroStack emulates Amazon CloudWatch Metrics, supporting custom metric ingestion, metric queries, standard and composite alarms, dashboards, and alarm state transitions. Both the classic Query/XML protocol and the Smithy RPC v2 CBOR protocol are accepted, so all AWS SDK versions work without configuration changes.

## Supported Operations

PutMetricData, GetMetricStatistics, GetMetricData, ListMetrics, PutMetricAlarm, PutCompositeAlarm, DescribeAlarms, DescribeAlarmsForMetric, DescribeAlarmHistory, DeleteAlarms, EnableAlarmActions, DisableAlarmActions, SetAlarmState, TagResource, UntagResource, ListTagsForResource, PutDashboard, GetDashboard, DeleteDashboards, ListDashboards

## Usage

```csharp
var client = new AmazonCloudWatchClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonCloudWatchConfig
    {
        RegionEndpoint = RegionEndpoint.USEast1,
        ServiceURL = "http://localhost:4566",
    });

// Publish custom metrics
await client.PutMetricDataAsync(new PutMetricDataRequest
{
    Namespace = "MyApp",
    MetricData =
    [
        new MetricDatum
        {
            MetricName = "RequestCount",
            Value = 42.0,
            Unit = StandardUnit.Count,
            Dimensions = [new Dimension { Name = "API", Value = "/users" }],
        },
        new MetricDatum
        {
            MetricName = "Latency",
            Value = 123.5,
            Unit = StandardUnit.Milliseconds,
        },
    ],
});

// List metrics in a namespace
var listResp = await client.ListMetricsAsync(new ListMetricsRequest
{
    Namespace = "MyApp",
});
Console.WriteLine(listResp.Metrics.Count); // 2

// Query metric statistics
var now = DateTimeOffset.UtcNow;
var statsResp = await client.GetMetricStatisticsAsync(new GetMetricStatisticsRequest
{
    Namespace = "MyApp",
    MetricName = "Latency",
    Period = 60,
    StartTime = now.AddMinutes(-10).UtcDateTime,
    EndTime   = now.AddMinutes(10).UtcDateTime,
    Statistics = [Statistic.Average, Statistic.Sum, Statistic.Maximum],
});
Console.WriteLine(statsResp.Datapoints[0].Average); // 123.5
```

## Alarms

```csharp
// Create a metric alarm
await client.PutMetricAlarmAsync(new PutMetricAlarmRequest
{
    AlarmName = "high-latency",
    MetricName = "Latency",
    Namespace = "MyApp",
    Statistic = Statistic.Average,
    Period = 60,
    EvaluationPeriods = 1,
    Threshold = 500.0,
    ComparisonOperator = ComparisonOperator.GreaterThanThreshold,
    AlarmActions = ["arn:aws:sns:us-east-1:000000000000:my-alarm-topic"],
    AlarmDescription = "Alert when p50 latency exceeds 500ms",
});

// Describe alarms
var alarms = await client.DescribeAlarmsAsync(new DescribeAlarmsRequest
{
    AlarmNames = ["high-latency"],
});
Console.WriteLine(alarms.MetricAlarms[0].StateValue); // INSUFFICIENT_DATA

// Manually set alarm state (useful in tests)
await client.SetAlarmStateAsync(new SetAlarmStateRequest
{
    AlarmName = "high-latency",
    StateValue = StateValue.ALARM,
    StateReason = "Simulated alarm for test",
});

// Create a composite alarm
await client.PutCompositeAlarmAsync(new PutCompositeAlarmRequest
{
    AlarmName = "composite-alarm",
    AlarmRule = "ALARM(high-latency)",
    AlarmDescription = "Composite alarm for high latency",
});
```

:::aside{type="note" title="CBOR protocol support"}
CloudWatch accepts both the classic Query/XML protocol and the newer Smithy RPC v2 CBOR format. MicroStack handles both transparently — no SDK configuration is required.
:::
