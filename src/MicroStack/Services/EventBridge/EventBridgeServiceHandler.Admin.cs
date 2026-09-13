using System.Globalization;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.EventBridge;

internal sealed partial class EventBridgeServiceHandler : IAdminResourceSource
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("event-buses", "Event buses"),
        new("rules", "Rules"),
        new("targets", "Targets"),
        new("archives", "Archives"),
        new("replays", "Replays"),
        new("connections", "Connections"),
        new("api-destinations", "API destinations"),
        new("endpoints", "Endpoints"),
        new("partner-event-sources", "Partner event sources")
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == "events" ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != "events")
            return [];

        var account = AccountContext.GetAccountId();
        Dictionary<string, object?>[] buses;
        Dictionary<string, object?>[] rules;
        Dictionary<string, Dictionary<string, object?>[]> targets;
        Dictionary<string, object?>[] archives;
        Dictionary<string, object?>[] replays;
        Dictionary<string, object?>[] connections;
        Dictionary<string, object?>[] destinations;
        Dictionary<string, object?>[] endpoints;
        Dictionary<string, string>[] partnerSources;
        Dictionary<string, object?>? defaultPolicy;
        lock (_lock)
        {
            buses = _eventBuses.Values
                .Where(bus => GetText(bus, "Name") != "default"
                    && ArnAccount(GetText(bus, "Arn")) == account)
                .Select(CloneMap).ToArray();
            rules = _rules.Values.Select(CloneMap).ToArray();
            targets = _targets.Items.ToDictionary(
                item => item.Key,
                item => item.Value.Select(CloneMap).ToArray(),
                StringComparer.Ordinal);
            archives = _archives.Values.Select(CloneMap).ToArray();
            replays = _replays.Values.Select(CloneMap).ToArray();
            connections = _connections.Values.Select(CloneMap).ToArray();
            destinations = _apiDestinations.Values.Select(CloneMap).ToArray();
            endpoints = _endpoints.Values.Select(CloneMap).ToArray();
            partnerSources = _partnerEventSources.Values
                .Where(source => source.GetValueOrDefault("Account") == account)
                .Select(source => new Dictionary<string, string>(source, StringComparer.Ordinal)).ToArray();
            defaultPolicy = _eventBusPolicies.TryGetValue("default", out var policy) ? CloneMap(policy) : null;
        }

        var defaultBus = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Name"] = "default",
            ["Arn"] = $"arn:aws:events:{Region}:{account}:event-bus/default",
            ["Description"] = "",
            ["Policy"] = defaultPolicy
        };
        buses = [defaultBus, .. buses];

        return buses.Select(bus => BusNode(bus, rules, targets))
            .Concat(archives.Select(ArchiveNode))
            .Concat(replays.Select(ReplayNode))
            .Concat(connections.Select(ConnectionNode))
            .Concat(destinations.Select(DestinationNode))
            .Concat(endpoints.Select(EndpointNode))
            .Concat(partnerSources.Select(PartnerSourceNode))
            .ToArray();
    }

    private static AdminNode BusNode(
        Dictionary<string, object?> bus,
        IReadOnlyList<Dictionary<string, object?>> rules,
        IReadOnlyDictionary<string, Dictionary<string, object?>[]> targets)
    {
        var name = GetText(bus, "Name");
        var arn = GetText(bus, "Arn");
        var busRules = rules.Where(rule => GetText(rule, "EventBusName", "default") == name).ToArray();
        return AdminData.Node("event-buses", arn, name, arn, "active") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Description", GetText(bus, "Description")),
                AdminData.Field("Rules", busRules.Length.ToString(CultureInfo.InvariantCulture)),
                AdminData.Field("Created", Epoch(bus, "CreationTime"), format: "datetime"),
                AdminData.Field("Last modified", Epoch(bus, "LastModifiedTime"), format: "datetime")
            ],
            ReadChildren = () => busRules.Select(rule => RuleNode(rule, targets)).ToArray(),
            ReadContent = bus.TryGetValue("Policy", out var policy) && policy is not null
                ? () => AdminData.Json(policy)
                : null
        };
    }

    private static AdminNode RuleNode(
        Dictionary<string, object?> rule,
        IReadOnlyDictionary<string, Dictionary<string, object?>[]> targets)
    {
        var name = GetText(rule, "Name");
        var bus = GetText(rule, "EventBusName", "default");
        var key = RuleKey(name, bus);
        var ruleTargets = targets.GetValueOrDefault(key) ?? [];
        var pattern = GetText(rule, "EventPattern");
        return AdminData.Node("rules", GetText(rule, "Arn"), name,
            GetText(rule, "Arn"), GetText(rule, "State")) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Event bus", bus),
                AdminData.Field("Description", GetText(rule, "Description")),
                AdminData.Field("Schedule", GetText(rule, "ScheduleExpression")),
                AdminData.Field("Role ARN", GetText(rule, "RoleArn")),
                AdminData.Field("Targets", ruleTargets.Length.ToString(CultureInfo.InvariantCulture))
            ],
            ReadChildren = () => ruleTargets.Select(TargetNode).ToArray(),
            ReadContent = string.IsNullOrWhiteSpace(pattern) ? null : () => AdminData.JsonText(pattern),
            ReadConnections = () => ruleTargets.SelectMany(TargetConnections).ToArray()
        };
    }

    private static AdminNode TargetNode(Dictionary<string, object?> target)
    {
        var id = GetText(target, "Id");
        var arn = GetText(target, "Arn");
        return AdminData.Node("targets", id, id, arn, "configured") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Target ARN", arn),
                AdminData.Field("Role ARN", GetText(target, "RoleArn")),
                AdminData.Field("Input path", GetText(target, "InputPath"))
            ],
            ReadContent = () => AdminData.Json(target),
            ReadConnections = () => TargetConnections(target)
        };
    }

    private static IReadOnlyList<AdminConnection> TargetConnections(Dictionary<string, object?> target)
    {
        var arn = GetText(target, "Arn");
        if (string.IsNullOrEmpty(arn))
            return [];
        var mapped = MapArn(arn);
        return mapped is null
            ? [new(arn, "targets", State: "configured")]
            : [new(mapped.Value.Label, "targets", mapped.Value.Service,
                [new(mapped.Value.Kind, arn)], State: "configured")];
    }

    private static AdminNode ArchiveNode(Dictionary<string, object?> archive)
    {
        var name = GetText(archive, "ArchiveName");
        var arn = GetText(archive, "ArchiveArn");
        var pattern = GetText(archive, "EventPattern");
        return AdminData.Node("archives", arn, name, arn, GetText(archive, "State")) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Description", GetText(archive, "Description")),
                AdminData.Field("Event source ARN", GetText(archive, "EventSourceArn")),
                AdminData.Field("Retention days", GetText(archive, "RetentionDays")),
                AdminData.Field("Event count", GetText(archive, "EventCount")),
                AdminData.Field("Size bytes", GetText(archive, "SizeBytes")),
                AdminData.Field("Created", Epoch(archive, "CreationTime"), format: "datetime")
            ],
            ReadContent = string.IsNullOrWhiteSpace(pattern) ? null : () => AdminData.JsonText(pattern),
            ReadConnections = () => ArnConnections(GetText(archive, "EventSourceArn"), "archives-from")
        };
    }

    private static AdminNode ReplayNode(Dictionary<string, object?> replay)
    {
        var name = GetText(replay, "ReplayName");
        var arn = GetText(replay, "ReplayArn");
        var source = GetText(replay, "EventSourceArn");
        var destination = replay.TryGetValue("Destination", out var value)
            && value is Dictionary<string, object?> map ? GetText(map, "Arn") : "";
        return AdminData.Node("replays", arn, name, arn, GetText(replay, "State")) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Description", GetText(replay, "Description")),
                AdminData.Field("Event source ARN", source),
                AdminData.Field("Destination ARN", destination),
                AdminData.Field("Started", Epoch(replay, "ReplayStartTime"), format: "datetime")
            ],
            ReadConnections = () => ArnConnections(source, "replays-from")
                .Concat(ArnConnections(destination, "replays-to")).ToArray()
        };
    }

    private static AdminNode ConnectionNode(Dictionary<string, object?> connection)
    {
        var name = GetText(connection, "Name");
        var arn = GetText(connection, "ConnectionArn");
        return AdminData.Node("connections", arn, name, arn, GetText(connection, "ConnectionState")) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Authorization type", GetText(connection, "AuthorizationType")),
                AdminData.Field("Authentication", "configured", sensitive: true),
                AdminData.Field("Description", GetText(connection, "Description")),
                AdminData.Field("Created", Epoch(connection, "CreationTime"), format: "datetime"),
                AdminData.Field("Last authorized", Epoch(connection, "LastAuthorizedTime"), format: "datetime")
            ]
        };
    }

    private static AdminNode DestinationNode(Dictionary<string, object?> destination)
    {
        var name = GetText(destination, "Name");
        var arn = GetText(destination, "ApiDestinationArn");
        var connectionArn = GetText(destination, "ConnectionArn");
        return AdminData.Node("api-destinations", arn, name, arn,
            GetText(destination, "ApiDestinationState")) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Invocation endpoint", GetText(destination, "InvocationEndpoint"), format: "uri"),
                AdminData.Field("HTTP method", GetText(destination, "HttpMethod")),
                AdminData.Field("Rate limit", GetText(destination, "InvocationRateLimitPerSecond")),
                AdminData.Field("Connection ARN", connectionArn)
            ],
            ReadConnections = () => string.IsNullOrEmpty(connectionArn) ? []
                : [new("Connection", "authorized-by", "events",
                    [new("connections", connectionArn)], State: "configured")]
        };
    }

    private static AdminNode EndpointNode(Dictionary<string, object?> endpoint)
    {
        var name = GetText(endpoint, "Name");
        var arn = GetText(endpoint, "Arn");
        return AdminData.Node("endpoints", arn, name, arn, GetText(endpoint, "State")) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Description", GetText(endpoint, "Description")),
                AdminData.Field("Endpoint URL", GetText(endpoint, "EndpointUrl"), format: "uri"),
                AdminData.Field("Role ARN", GetText(endpoint, "RoleArn")),
                AdminData.Field("Created", Epoch(endpoint, "CreationTime"), format: "datetime")
            ],
            ReadContent = endpoint.TryGetValue("RoutingConfig", out var routing) && routing is not null
                ? () => AdminData.Json(routing) : null
        };
    }

    private static AdminNode PartnerSourceNode(IReadOnlyDictionary<string, string> source)
    {
        var name = source.GetValueOrDefault("Name", "");
        var arn = source.GetValueOrDefault("EventSourceArn", "");
        return AdminData.Node("partner-event-sources", arn, name, arn, "ACTIVE") with
        {
            ReadFields = () => [AdminData.Field("Account", source.GetValueOrDefault("Account"))]
        };
    }

    private static IReadOnlyList<AdminConnection> ArnConnections(string arn, string relation)
    {
        if (string.IsNullOrEmpty(arn))
            return [];
        var mapped = MapArn(arn);
        return mapped is null ? [new(arn, relation, State: "configured")]
            : [new(mapped.Value.Label, relation, mapped.Value.Service,
                [new(mapped.Value.Kind, arn)], State: "configured")];
    }

    private static (string Service, string Kind, string Label)? MapArn(string arn) =>
        arn.StartsWith("arn:aws:sqs:", StringComparison.Ordinal) ? ("sqs", "queues", "Queue")
        : arn.StartsWith("arn:aws:sns:", StringComparison.Ordinal) ? ("sns", "topics", "Topic")
        : arn.StartsWith("arn:aws:lambda:", StringComparison.Ordinal) ? ("lambda", "functions", "Function")
        : arn.StartsWith("arn:aws:states:", StringComparison.Ordinal) ? ("stepfunctions", "state-machines", "State machine")
        : arn.Contains(":event-bus/", StringComparison.Ordinal) ? ("events", "event-buses", "Event bus")
        : arn.Contains(":archive/", StringComparison.Ordinal) ? ("events", "archives", "Archive")
        : null;

    private static string ArnAccount(string arn)
    {
        var parts = arn.Split(':');
        return parts.Length > 4 ? parts[4] : "";
    }

    private static string GetText(IReadOnlyDictionary<string, object?> value, string key, string fallback = "") =>
        value.TryGetValue(key, out var item) && item is not null
            ? Convert.ToString(item, CultureInfo.InvariantCulture) ?? fallback : fallback;

    private static string? Epoch(IReadOnlyDictionary<string, object?> value, string key)
    {
        if (!value.TryGetValue(key, out var item) || item is null
            || !double.TryParse(Convert.ToString(item, CultureInfo.InvariantCulture),
                NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds))
            return null;
        return AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds((long)(seconds * 1000)));
    }

    private static Dictionary<string, object?> CloneMap(Dictionary<string, object?> source) =>
        source.ToDictionary(item => item.Key, item => CloneValue(item.Value), StringComparer.Ordinal);

    private static object? CloneValue(object? value) => value switch
    {
        Dictionary<string, object?> map => CloneMap(map),
        List<Dictionary<string, object?>> maps => maps.Select(map => (object?)CloneMap(map)).ToArray(),
        List<object?> list => list.Select(CloneValue).ToArray(),
        string or bool or byte or sbyte or short or ushort or int or uint or long or ulong
            or float or double or decimal or null => value,
        _ => value.ToString()
    };
}
