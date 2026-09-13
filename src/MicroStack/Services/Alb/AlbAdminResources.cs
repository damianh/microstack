using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using MicroStack.Services.ApiGateway;

namespace MicroStack.Services.Alb;

internal sealed partial class AlbServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("load-balancers", "Load balancers"), new("listeners", "Listeners") { IsRoot = false },
        new("rules", "Rules") { IsRoot = false }, new("target-groups", "Target groups"),
        new("targets", "Targets") { IsRoot = false },
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == "alb" || serviceId == ServiceName ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != "alb" && serviceId != ServiceName)
            return [];
        lock (_lock)
        {
            return
            [
                .. _lbs.Items.ToArray().Select(CreateLoadBalancerNode),
                .. _tgs.Items.ToArray().Select(CreateTargetGroupNode),
            ];
        }
    }

    private AdminNode CreateLoadBalancerNode(KeyValuePair<string, Dictionary<string, object>> pair)
    {
        var (arn, lb) = pair;
        var node = ObjectNode("load-balancers", arn,
            NetworkingAdminData.Get(lb, "LoadBalancerName", arn), lb, arn,
            NetworkingAdminData.Get(lb, "State"));
        return node with
        {
            ReadContent = () => ResourceConfiguration(arn, lb, _lbAttrs),
            ChildKinds = [AdminKinds[1]],
            ReadChildren = () =>
            {
                lock (_lock)
                    return _listeners.Items
                        .Where(x => NetworkingAdminData.Get(x.Value, "LoadBalancerArn") == arn)
                        .Select(CreateListenerNode).ToArray();
            },
        };
    }

    private AdminNode CreateListenerNode(KeyValuePair<string, Dictionary<string, object>> pair)
    {
        var (arn, listener) = pair;
        var protocol = NetworkingAdminData.Get(listener, "Protocol");
        var port = NetworkingAdminData.Get(listener, "Port");
        return ObjectNode("listeners", arn, $"{protocol}:{port}", listener, arn) with
        {
            ChildKinds = [AdminKinds[2]],
            ReadChildren = () =>
            {
                lock (_lock)
                    return _rules.Items
                        .Where(x => NetworkingAdminData.Get(x.Value, "ListenerArn") == arn)
                        .Select(CreateRuleNode).ToArray();
            },
            ReadConnections = () =>
            [
                new("Load balancer", "listener-of", ServiceName,
                    [new("load-balancers", NetworkingAdminData.Get(listener, "LoadBalancerArn"))]),
                .. TargetGroupConnections(listener, "DefaultActions"),
            ],
        };
    }

    private AdminNode CreateRuleNode(KeyValuePair<string, Dictionary<string, object>> pair)
    {
        var (arn, rule) = pair;
        return ObjectNode("rules", arn,
            $"Priority {NetworkingAdminData.Get(rule, "Priority")}", rule, arn) with
        {
            ReadConnections = () =>
            [
                new("Listener", "rule-of", ServiceName,
                    [new("listeners", NetworkingAdminData.Get(rule, "ListenerArn"))]),
                .. TargetGroupConnections(rule, "Actions"),
            ],
        };
    }

    private AdminNode CreateTargetGroupNode(KeyValuePair<string, Dictionary<string, object>> pair)
    {
        var (arn, targetGroup) = pair;
        var node = ObjectNode("target-groups", arn,
            NetworkingAdminData.Get(targetGroup, "TargetGroupName", arn), targetGroup, arn);
        return node with
        {
            ReadContent = () => ResourceConfiguration(arn, targetGroup, _tgAttrs),
            ChildKinds = [AdminKinds[4]],
            ReadChildren = () =>
            {
                lock (_lock)
                {
                    if (!_targets.TryGetValue(arn, out var targets))
                        return [];
                    return targets.ToArray().Select(target =>
                    {
                        var id = NetworkingAdminData.Get(target, "Id");
                        var port = NetworkingAdminData.Get(target, "Port");
                        return ObjectNode("targets", NetworkingAdminData.Composite(id, port),
                            string.IsNullOrEmpty(port) ? id : $"{id}:{port}", target);
                    }).ToArray();
                }
            },
            ReadConnections = () =>
            {
                var arns = targetGroup.TryGetValue("LoadBalancerArns", out var value)
                    && value is IEnumerable<string> items ? items.ToArray() : [];
                return arns.Select(lbArn => new AdminConnection(
                    "Load balancer", "registered-with", ServiceName,
                    [new("load-balancers", lbArn)])).ToArray();
            },
        };
    }

    private AdminContent ResourceConfiguration(
        string arn, Dictionary<string, object> resource,
        AccountScopedDictionary<string, List<Dictionary<string, string>>> attributes)
    {
        lock (_lock)
        {
            var value = new Dictionary<string, object?>(resource
                .Select(x => new KeyValuePair<string, object?>(x.Key, x.Value)));
            if (attributes.TryGetValue(arn, out var attrs))
                value["Attributes"] = attrs.ToArray();
            if (_tags.TryGetValue(arn, out var tags))
                value["Tags"] = tags.ToArray();
            return NetworkingAdminData.Json(value);
        }
    }

    private static IReadOnlyList<AdminConnection> TargetGroupConnections(
        Dictionary<string, object> source, string property)
    {
        if (!source.TryGetValue(property, out var actionsValue)
            || actionsValue is not IEnumerable<Dictionary<string, object>> actions)
            return [];
        return actions
            .Where(action => action.TryGetValue("TargetGroupArn", out var value) && value is string)
            .Select(action => new AdminConnection("Target group", "forwards-to", "elasticloadbalancing",
                [new("target-groups", (string)action["TargetGroupArn"])]))
            .ToArray();
    }

    private static AdminNode ObjectNode(
        string kind, string id, string name, Dictionary<string, object> value,
        string? arn = null, string? status = null) =>
        AdminData.Node(kind, id, name, arn, status) with
        {
            ReadFields = () => NetworkingAdminData.Fields(
                value.Select(x => new KeyValuePair<string, object?>(x.Key, x.Value))),
            ReadContent = () => NetworkingAdminData.Json(value),
        };
}
