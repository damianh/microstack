using System.Globalization;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Waf;

internal sealed partial class WafServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("web-acl", "Web ACLs"),
        new("ip-set", "IP sets"),
        new("rule-group", "Rule groups")
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) => AdminKinds;

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        lock (_lock)
        {
            var nodes = _webAcls.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => ResourceNode("web-acl", x.Key, x.Value)).ToList();
            nodes.AddRange(_ipSets.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => ResourceNode("ip-set", x.Key, x.Value)));
            nodes.AddRange(_ruleGroups.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => ResourceNode("rule-group", x.Key, x.Value)));
            return nodes;
        }
    }

    private AdminNode ResourceNode(string kind, string id, Dictionary<string, object?> snapshot) =>
        AdminData.Node(kind, id, Text(snapshot, "Name") ?? id, Text(snapshot, "ARN"),
            "Active") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!TryResource(kind, id, out var resource)) return [];
                    var fields = new List<AdminField>
                    {
                        AdminData.Field("Description", Text(resource, "Description")),
                        AdminData.Field("Scope", Text(resource, "Scope")),
                        AdminData.Field("Capacity", Scalar(resource, "Capacity")),
                        AdminData.Field("Tags", Tags(Text(resource, "ARN")))
                    };
                    if (kind == "ip-set")
                    {
                        fields.Add(AdminData.Field("IP address version", Text(resource, "IPAddressVersion")));
                        fields.Add(AdminData.Field("Addresses", SafeAddresses(resource.GetValueOrDefault("Addresses"))));
                    }
                    else
                    {
                        fields.Add(AdminData.Field("Rule count", Count(resource.GetValueOrDefault("Rules"))));
                    }
                    return fields;
                }
            },
            ReadConnections = kind == "web-acl"
                ? () =>
                {
                    lock (_lock)
                    {
                        if (!TryResource(kind, id, out var resource) ||
                            Text(resource, "ARN") is not { } arn)
                            return [];
                        return _associations.Items.Where(x => x.Value == arn)
                            .OrderBy(x => x.Key, StringComparer.Ordinal)
                            .Select(x => new AdminConnection(x.Key, "associated-resource"))
                            .ToArray();
                    }
                }
                : null
        };

    private bool TryResource(string kind, string id,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Dictionary<string, object?>? resource)
    {
        return kind switch
        {
            "web-acl" => _webAcls.TryGetValue(id, out resource),
            "ip-set" => _ipSets.TryGetValue(id, out resource),
            "rule-group" => _ruleGroups.TryGetValue(id, out resource),
            _ => Missing(out resource)
        };
    }

    private static bool Missing(out Dictionary<string, object?>? resource)
    {
        resource = null;
        return false;
    }

    private string Tags(string? arn)
    {
        if (arn is null || !_wafTags.TryGetValue(arn, out var tags)) return "";
        return string.Join(", ", tags.Select(t =>
            $"{t.GetValueOrDefault("Key")}={t.GetValueOrDefault("Value")}"));
    }

    private static string? Text(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) ? item as string : null;

    private static string? Scalar(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) && item is not null
            ? Convert.ToString(item, CultureInfo.InvariantCulture)
            : null;

    private static string Count(object? value) => value switch
    {
        ICollection<object?> collection => collection.Count.ToString(CultureInfo.InvariantCulture),
        JsonElement { ValueKind: JsonValueKind.Array } element =>
            element.GetArrayLength().ToString(CultureInfo.InvariantCulture),
        _ => "0"
    };

    private static string SafeAddresses(object? value)
    {
        return value switch
        {
            IEnumerable<object?> values => string.Join(", ", values.OfType<string>()),
            JsonElement { ValueKind: JsonValueKind.Array } element =>
                string.Join(", ", element.EnumerateArray()
                    .Where(x => x.ValueKind == JsonValueKind.String).Select(x => x.GetString())),
            _ => ""
        };
    }
}
