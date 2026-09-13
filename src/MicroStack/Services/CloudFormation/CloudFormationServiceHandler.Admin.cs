using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.CloudFormation;

internal sealed partial class CloudFormationServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds = [new("stack", "Stacks")];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == "cloudformation" ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != "cloudformation") return [];
        lock (_lock)
        {
            return _stacks.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => StackNode(x.Key, GetString(x.Value.GetValueOrDefault("StackId")),
                    GetString(x.Value.GetValueOrDefault("StackStatus")))).ToArray();
        }
    }

    private AdminNode StackNode(string name, string arn, string status) =>
        AdminData.Node("stack", name, name, arn, status) with
        {
            ReadFields = () => ReadStackFields(name),
            ReadChildren = () => ReadStackChildren(name),
        };

    private IReadOnlyList<AdminField> ReadStackFields(string name)
    {
        lock (_lock)
        {
            if (!_stacks.TryGetValue(name, out var stack)) return [];
            return
            [
                AdminData.Field("Stack ID", GetString(stack.GetValueOrDefault("StackId"))),
                AdminData.Field("Status", GetString(stack.GetValueOrDefault("StackStatus"))),
                AdminData.Field("Status reason", GetString(stack.GetValueOrDefault("StackStatusReason"))),
                AdminData.Field("Description", GetString(stack.GetValueOrDefault("Description"))),
                AdminData.Field("Created", GetString(stack.GetValueOrDefault("CreationTime")), format: "datetime"),
                AdminData.Field("Last updated", GetString(stack.GetValueOrDefault("LastUpdatedTime")), format: "datetime"),
            ];
        }
    }

    private IEnumerable<AdminNode> ReadStackChildren(string name)
    {
        lock (_lock)
        {
            if (!_stacks.TryGetValue(name, out var stack)) return [];
            var children = new List<AdminNode>();
            foreach (var (logicalId, value) in GetDict(stack.GetValueOrDefault("_resources"))
                         .OrderBy(x => x.Key, StringComparer.Ordinal))
            {
                var resource = GetDict(value);
                var snapshot = ProjectDictionary(resource);
                var physicalId = GetString(resource.GetValueOrDefault("PhysicalResourceId"));
                var type = GetString(resource.GetValueOrDefault("ResourceType"));
                var resourceStatus = GetString(resource.GetValueOrDefault("ResourceStatus"));
                children.Add(AdminData.Node("resource", logicalId, logicalId, status: resourceStatus) with
                {
                    ReadFields = () =>
                    [
                        AdminData.Field("Logical ID", logicalId),
                        AdminData.Field("Physical ID", physicalId),
                        AdminData.Field("Type", type),
                        AdminData.Field("Status", resourceStatus),
                    ],
                    ReadContent = () => AdminData.Json(snapshot),
                });
            }

            var stackId = GetString(stack.GetValueOrDefault("StackId"));
            if (_stackEvents.TryGetValue(stackId, out var events))
            {
                foreach (var ev in events.OrderByDescending(x => x.GetValueOrDefault("Timestamp"), StringComparer.Ordinal))
                {
                    var snapshot = ev.ToDictionary(x => x.Key, x => (object?)x.Value, StringComparer.Ordinal);
                    var id = ev.GetValueOrDefault("EventId", "");
                    var logicalId = ev.GetValueOrDefault("LogicalResourceId", id);
                    var eventStatus = ev.GetValueOrDefault("ResourceStatus");
                    var timestamp = ev.GetValueOrDefault("Timestamp");
                    var type = ev.GetValueOrDefault("ResourceType");
                    var reason = ev.GetValueOrDefault("ResourceStatusReason");
                    children.Add(AdminData.Node("event", id, logicalId, status: eventStatus) with
                    {
                        ReadFields = () =>
                        [
                            AdminData.Field("Timestamp", timestamp, format: "datetime"),
                            AdminData.Field("Resource type", type),
                            AdminData.Field("Reason", reason),
                        ],
                        ReadContent = () => AdminData.Json(snapshot),
                    });
                }
            }

            foreach (var value in GetList(stack.GetValueOrDefault("Outputs")))
            {
                var output = GetDict(value);
                var key = GetString(output.GetValueOrDefault("OutputKey"));
                var outputValue = GetString(output.GetValueOrDefault("OutputValue"));
                var description = GetString(output.GetValueOrDefault("Description"));
                var export = GetString(output.GetValueOrDefault("ExportName"));
                var snapshot = ProjectDictionary(output);
                children.Add(AdminData.Node("output", key, key) with
                {
                    ReadFields = () =>
                    [
                        AdminData.Field("Value", outputValue),
                        AdminData.Field("Description", description),
                        AdminData.Field("Export", export),
                    ],
                    ReadContent = () => AdminData.Json(snapshot),
                });
            }

            var templateBody = GetString(stack.GetValueOrDefault("_template_body"));
            children.Add(AdminData.Node("template", "template", "Template") with
            {
                ReadContent = () => AdminData.JsonText(templateBody),
            });
            return children;
        }
    }

    private static Dictionary<string, object?> ProjectDictionary(Dictionary<string, object?> source) =>
        source.Where(x => !x.Key.StartsWith('_'))
            .ToDictionary(x => x.Key, x => ProjectValue(x.Value), StringComparer.Ordinal);

    private static object? ProjectValue(object? value) => value switch
    {
        Dictionary<string, object?> dictionary => ProjectDictionary(dictionary),
        Dictionary<string, string> dictionary =>
            dictionary.ToDictionary(x => x.Key, x => (object?)x.Value, StringComparer.Ordinal),
        List<object?> list => list.Select(ProjectValue).ToArray(),
        _ => value,
    };
}
