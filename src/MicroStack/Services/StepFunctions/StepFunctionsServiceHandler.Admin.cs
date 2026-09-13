using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.StepFunctions;

internal sealed partial class StepFunctionsServiceHandler : IAdminResourceSource, IAdminRelationshipSource
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("state-machines", "State machines"),
        new("executions", "Executions") { IsRoot = false },
        new("history-events", "History events") { IsRoot = false },
        new("activities", "Activities"),
        new("activity-tasks", "Activity tasks") { IsRoot = false }
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == "stepfunctions" ? AdminKinds : [];

    public AdminRelationshipSnapshot GetAdminRelationshipSnapshot()
    {
        var resources = new List<AdminRelationshipResource>();
        AddResources(_stateMachines, "state-machines", "stateMachineArn");
        AddResources(_activities, "activities", "activityArn");
        return new(resources, []);

        void AddResources(
            AccountScopedDictionary<string, Dictionary<string, object?>> source, string kind, string arnKey)
        {
            foreach (var item in source.Items.OrderBy(item => item.Key, StringComparer.Ordinal).ToArray())
            {
                lock (item.Value)
                {
                    var arn = GetString(item.Value, arnKey) ?? item.Key;
                    resources.Add(new([new(kind, arn)], GetString(item.Value, "name") ?? arn,
                        GetString(item.Value, "status"), arn));
                }
            }
        }
    }

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != "stepfunctions")
            return [];

        var machines = Snapshot(_stateMachines);
        var executions = Snapshot(_executions);
        var activities = Snapshot(_activities);
        var taskSnapshots = new Dictionary<string, Dictionary<string, object?>[]>(StringComparer.Ordinal);
        foreach (var item in _activityTasks.Items)
        {
            lock (item.Value)
            {
                taskSnapshots[item.Key] = item.Value.Select(DeepCopyDict).ToArray();
            }
        }

        return machines.Select(machine => StateMachineNode(machine, executions))
            .Concat(activities.Select(activity => ActivityNode(activity, taskSnapshots)))
            .ToArray();
    }

    private static Dictionary<string, object?>[] Snapshot(
        AccountScopedDictionary<string, Dictionary<string, object?>> source)
    {
        var values = source.Values.ToArray();
        var result = new Dictionary<string, object?>[values.Length];
        for (var index = 0; index < values.Length; index++)
        {
            lock (values[index])
                result[index] = DeepCopyDict(values[index]);
        }
        return result;
    }

    private static AdminNode StateMachineNode(
        Dictionary<string, object?> machine,
        IReadOnlyList<Dictionary<string, object?>> executions)
    {
        var arn = GetString(machine, "stateMachineArn") ?? "";
        var name = GetString(machine, "name") ?? arn;
        var definition = GetString(machine, "definition") ?? "{}";
        var children = executions
            .Where(execution => GetString(execution, "stateMachineArn") == arn).ToArray();
        return AdminData.Node("state-machines", arn, name, arn,
            GetString(machine, "status")) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Type", GetString(machine, "type")),
                AdminData.Field("Role ARN", GetString(machine, "roleArn")),
                AdminData.Field("Created", Iso(machine, "creationDate"), format: "datetime"),
                AdminData.Field("Executions", children.Length.ToString(CultureInfo.InvariantCulture))
            ],
            ChildKinds = [AdminKinds[1]],
            ReadChildren = () => children.Select(ExecutionNode).ToArray(),
            ReadContent = () => SafeJsonText(definition),
            ReadConnections = () => DefinitionConnections(definition)
        };
    }

    private static AdminNode ExecutionNode(Dictionary<string, object?> execution)
    {
        var arn = GetString(execution, "executionArn") ?? "";
        var name = GetString(execution, "name") ?? arn;
        var events = GetList(execution, "events")
            .OfType<Dictionary<string, object?>>().Select(DeepCopyDict).ToArray();
        return AdminData.Node("executions", arn, name, arn,
            GetString(execution, "status")) with
        {
            ReadFields = () =>
            [
                AdminData.Field("State machine ARN", GetString(execution, "stateMachineArn")),
                AdminData.Field("Started", Iso(execution, "startDate"), format: "datetime"),
                AdminData.Field("Stopped", Iso(execution, "stopDate"), format: "datetime"),
                AdminData.Field("History events", events.Length.ToString(CultureInfo.InvariantCulture))
            ],
            ChildKinds = [AdminKinds[2]],
            ReadChildren = () => events.Select(HistoryEventNode).ToArray(),
            ReadContent = () => AdminData.Json(new Dictionary<string, object?>
            {
                ["input"] = GetString(execution, "input"),
                ["output"] = AdminValue(execution.GetValueOrDefault("output"))
            }, sensitive: true)
        };
    }

    private static AdminNode HistoryEventNode(Dictionary<string, object?> historyEvent)
    {
        var id = GetString(historyEvent, "id") ?? "";
        var type = GetString(historyEvent, "type") ?? "Event";
        return AdminData.Node("history-events", id, $"{id} · {type}", status: type) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Type", type),
                AdminData.Field("Timestamp", Iso(historyEvent, "timestamp"), format: "datetime")
            ],
            ReadContent = () => AdminData.Json(AdminValue(historyEvent), sensitive: true)
        };
    }

    private static AdminNode ActivityNode(
        Dictionary<string, object?> activity,
        IReadOnlyDictionary<string, Dictionary<string, object?>[]> taskSnapshots)
    {
        var arn = GetString(activity, "activityArn") ?? "";
        var name = GetString(activity, "name") ?? arn;
        var tasks = taskSnapshots.GetValueOrDefault(arn) ?? [];
        return AdminData.Node("activities", arn, name, arn, "active") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Created", Iso(activity, "creationDate"), format: "datetime"),
                AdminData.Field("Pending tasks", tasks.Length.ToString(CultureInfo.InvariantCulture))
            ],
            ChildKinds = [AdminKinds[4]],
            ReadChildren = () => tasks.Select(ActivityTaskNode).ToArray()
        };
    }

    private static AdminNode ActivityTaskNode(Dictionary<string, object?> task)
    {
        var token = GetString(task, "taskToken") ?? "";
        var key = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(token)));
        return AdminData.Node("activity-tasks", key, $"Task {key[..12]}", status: "pending") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Task token", token, sensitive: true)
            ],
            ReadContent = () => AdminData.Text(GetString(task, "input") ?? "{}", "application/json", sensitive: true)
        };
    }

    private static IReadOnlyList<AdminConnection> DefinitionConnections(string definition)
    {
        try
        {
            using var document = JsonDocument.Parse(definition);
            var arns = new HashSet<string>(StringComparer.Ordinal);
            FindResources(document.RootElement, arns);
            return arns.Select(ResourceConnection).ToArray();
        }
        catch (JsonException)
        {
            return [];
        }
    }

    private static AdminContent SafeJsonText(string value)
    {
        try
        {
            return AdminData.JsonText(value);
        }
        catch (JsonException)
        {
            return AdminData.Unavailable("The retained state machine definition is not valid JSON.",
                "application/json", Encoding.UTF8.GetByteCount(value));
        }
    }

    private static void FindResources(JsonElement element, HashSet<string> resources)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in element.EnumerateObject())
            {
                if (property.Value.ValueKind == JsonValueKind.String
                    && (property.NameEquals("Resource")
                        || property.Name.EndsWith("Arn", StringComparison.Ordinal)))
                {
                    var value = property.Value.GetString();
                    if (!string.IsNullOrEmpty(value))
                        resources.Add(value);
                }
                FindResources(property.Value, resources);
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in element.EnumerateArray())
                FindResources(item, resources);
        }
    }

    private static AdminConnection ResourceConnection(string resource)
    {
        if (resource.StartsWith("arn:aws:states:::", StringComparison.Ordinal))
            return new(resource, "uses-integration", State: "configured");
        if (resource.StartsWith("arn:aws:sqs:", StringComparison.Ordinal))
            return new("Queue", "invokes", "sqs", [new("queues", resource)], State: "configured");
        if (resource.StartsWith("arn:aws:sns:", StringComparison.Ordinal))
            return new("Topic", "invokes", "sns", [new("topics", resource)], State: "configured");
        if (resource.StartsWith("arn:aws:lambda:", StringComparison.Ordinal))
            return new("Function", "invokes", "lambda", [new("functions", resource)], State: "configured");
        if (resource.Contains(":stateMachine:", StringComparison.Ordinal))
            return new("State machine", "invokes", "stepfunctions",
                [new("state-machines", resource)], State: "configured");
        return new(resource, "invokes", State: "configured");
    }

    private static string? Iso(IReadOnlyDictionary<string, object?> value, string key)
    {
        var text = value.TryGetValue(key, out var item) ? item?.ToString() : null;
        return DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal, out var timestamp)
            ? AdminData.IsoUtc(timestamp) : null;
    }

    private static object? AdminValue(object? value) => value switch
    {
        Dictionary<string, object?> map => map.ToDictionary(
            item => item.Key, item => AdminValue(item.Value), StringComparer.Ordinal),
        List<object?> list => list.Select(AdminValue).ToArray(),
        List<Dictionary<string, object?>> maps => maps.Select(map => AdminValue(map)).ToArray(),
        _ => value
    };
}
