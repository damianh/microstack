using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.CloudWatchLogs;

internal sealed partial class CloudWatchLogsServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("log-group", "Log groups"), new("destination", "Destinations"),
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == "logs" ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != "logs") return [];
        lock (_lock)
        {
            return _logGroups.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => GroupNode(x.Key, x.Value.Arn))
                .Concat(_destinations.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                    .Select(x => DestinationNode(x.Key, x.Value.Arn))).ToArray();
        }
    }

    private AdminNode GroupNode(string name, string arn) =>
        AdminData.Node("log-group", name, name, arn) with
        {
            ReadFields = () => ReadGroupFields(name),
            ChildKinds =
            [
                new("log-stream", "Log streams") { IsRoot = false },
                new("subscription-filter", "Subscription filters") { IsRoot = false },
                new("metric-filter", "Metric filters") { IsRoot = false },
                new("tag", "Tags") { IsRoot = false },
            ],
            ReadChildren = () => ReadGroupChildren(name),
        };

    private IReadOnlyList<AdminField> ReadGroupFields(string name)
    {
        lock (_lock)
        {
            if (!_logGroups.TryGetValue(name, out var group)) return [];
            return
            [
                AdminData.Field("Created", Millis(group.CreationTime), format: "datetime"),
                AdminData.Field("Retention days", group.RetentionInDays?.ToString()),
                AdminData.Field("Stored bytes",
                    group.Streams.Values.Sum(s => s.Events.Sum(e => (long)e.Message.Length)).ToString(), format: "bytes"),
            ];
        }
    }

    private IEnumerable<AdminNode> ReadGroupChildren(string name)
    {
        lock (_lock)
        {
            if (!_logGroups.TryGetValue(name, out var group)) return [];
            var nodes = group.Streams.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => StreamNode(name, x.Key)).ToList();
            nodes.AddRange(group.SubscriptionFilters.Values.OrderBy(x => x.FilterName, StringComparer.Ordinal)
                .Select(SubscriptionNode));
            nodes.AddRange(_metricFilters.Values.Where(x => x.LogGroupName == name)
                .OrderBy(x => x.FilterName, StringComparer.Ordinal).Select(MetricFilterNode));
            nodes.AddRange(group.Tags.OrderBy(x => x.Key, StringComparer.Ordinal).Select(x => TagNode(x.Key, x.Value)));
            return nodes;
        }
    }

    private AdminNode StreamNode(string groupName, string streamName) =>
        AdminData.Node("log-stream", streamName, streamName) with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_logGroups.TryGetValue(groupName, out var group) ||
                        !group.Streams.TryGetValue(streamName, out var stream)) return [];
                    return
                    [
                        AdminData.Field("Created", Millis(stream.CreationTime), format: "datetime"),
                        AdminData.Field("First event", Millis(stream.FirstEventTimestamp), format: "datetime"),
                        AdminData.Field("Last event", Millis(stream.LastEventTimestamp), format: "datetime"),
                        AdminData.Field("Last ingestion", Millis(stream.LastIngestionTime), format: "datetime"),
                        AdminData.Field("Event count", stream.Events.Count.ToString()),
                    ];
                }
            },
            ChildKinds = [new("log-event", "Log events") { IsRoot = false }],
            ReadChildren = () => ReadEvents(groupName, streamName),
        };

    private IEnumerable<AdminNode> ReadEvents(string groupName, string streamName)
    {
        lock (_lock)
        {
            if (!_logGroups.TryGetValue(groupName, out var group) ||
                !group.Streams.TryGetValue(streamName, out var stream)) return [];
            return stream.Events.Select((entry, index) =>
                AdminData.Node("log-event", $"{entry.Timestamp}:{entry.IngestionTime}:{index}",
                    Millis(entry.Timestamp) ?? entry.Timestamp.ToString()) with
                {
                    ReadFields = () =>
                    [
                        AdminData.Field("Timestamp", Millis(entry.Timestamp), format: "datetime"),
                        AdminData.Field("Ingestion time", Millis(entry.IngestionTime), format: "datetime"),
                    ],
                    ReadContent = () => AdminData.Text(entry.Message),
                }).ToArray();
        }
    }

    private AdminNode SubscriptionNode(SubscriptionFilter filter) =>
        AdminData.Node("subscription-filter", filter.FilterName, filter.FilterName, status: "configured") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Pattern", filter.FilterPattern),
                AdminData.Field("Destination ARN", filter.DestinationArn),
                AdminData.Field("Role ARN", filter.RoleArn),
                AdminData.Field("Distribution", filter.Distribution),
                AdminData.Field("Created", Millis(filter.CreationTime), format: "datetime"),
            ],
            ReadConnections = () =>
            {
                lock (_lock)
                {
                    if (string.IsNullOrEmpty(filter.DestinationArn)) return [];
                    var destination = _destinations.Items.FirstOrDefault(x => x.Value.Arn == filter.DestinationArn);
                    return destination.Value is not null
                        ? [new("Destination", "configured", "logs", [new("destination", destination.Key)])]
                        : [new(filter.DestinationArn, "configured")];
                }
            },
        };

    private static AdminNode MetricFilterNode(MetricFilter filter) =>
        AdminData.Node("metric-filter", filter.FilterName, filter.FilterName, status: "configured") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Pattern", filter.FilterPattern),
                AdminData.Field("Created", Millis(filter.CreationTime), format: "datetime"),
            ],
            ReadContent = () => AdminData.Json(new Dictionary<string, object?>
            {
                ["metricTransformations"] = filter.MetricTransformations
                    .Select(x => (object?)x.ToDictionary(y => y.Key, y => y.Value)).ToArray(),
            }),
        };

    private static AdminNode TagNode(string key, string value) =>
        AdminData.Node("tag", key, key) with { ReadFields = () => [AdminData.Field("Value", value)] };

    private AdminNode DestinationNode(string name, string arn) =>
        AdminData.Node("destination", name, name, arn) with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_destinations.TryGetValue(name, out var destination)) return [];
                    return
                    [
                        AdminData.Field("Target ARN", destination.TargetArn),
                        AdminData.Field("Role ARN", destination.RoleArn),
                        AdminData.Field("Created", Millis(destination.CreationTime), format: "datetime"),
                    ];
                }
            },
            ReadConnections = () =>
            {
                lock (_lock)
                    return _destinations.TryGetValue(name, out var destination) &&
                           !string.IsNullOrEmpty(destination.TargetArn)
                        ? [new(destination.TargetArn, "configured")] : [];
            },
        };

    private static string? Millis(long? value) =>
        value is null ? null : AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds(value.Value));
}
