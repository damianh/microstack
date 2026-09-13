using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;
using MicroStack.Services.ApiGateway;

namespace MicroStack.Services.ServiceDiscovery;

internal sealed partial class ServiceDiscoveryServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("namespaces", "Namespaces"), new("services", "Services") { IsRoot = false },
        new("instances", "Instances") { IsRoot = false },
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == ServiceName ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != ServiceName)
            return [];
        lock (_lock)
            return _namespaces.Items.ToArray().Select(CreateNamespaceNode).ToArray();
    }

    private AdminNode CreateNamespaceNode(KeyValuePair<string, SdNamespace> pair)
    {
        var ns = pair.Value;
        return AdminData.Node("namespaces", ns.Id, ns.Name, ns.Arn, ns.Type) with
        {
            ReadFields = () => NetworkingAdminData.Fields(ns.ToDict()),
            ReadContent = () => NetworkingAdminData.Json(ns.ToDict()),
            ChildKinds = [AdminKinds[1]],
            ReadChildren = () =>
            {
                lock (_lock)
                    return _services.Items.Where(x => x.Value.NamespaceId == ns.Id)
                        .Select(CreateServiceNode).ToArray();
            },
            ReadConnections = () =>
            {
                var zoneId = HostedZoneId(ns);
                var exists = zoneId is not null
                    && ((IAdminResourceSource)_route53).GetAdminResources("route53")
                        .Any(x => x.Resource.Key.Id == zoneId);
                return !exists
                    ? []
                    :
                    [
                        new("Route 53 hosted zone", "backed-by", "route53",
                            [new("hosted-zones", zoneId!)]),
                    ];
            },
        };
    }

    private AdminNode CreateServiceNode(KeyValuePair<string, SdService> pair)
    {
        var service = pair.Value;
        return AdminData.Node("services", service.Id, service.Name, service.Arn) with
        {
            ReadFields = () => NetworkingAdminData.Fields(service.ToDict()),
            ReadContent = () => NetworkingAdminData.Json(service.ToDict()),
            ChildKinds = [AdminKinds[2]],
            ReadChildren = () =>
            {
                lock (_lock)
                {
                    if (!_instances.TryGetValue(service.Id, out var instances))
                        return [];
                    return instances.ToArray().Select(instance =>
                        CreateInstanceNode(service.Id, instance.Value)).ToArray();
                }
            },
            ReadConnections = () =>
            [
                new("Namespace", "belongs-to", ServiceName,
                    [new("namespaces", service.NamespaceId)]),
            ],
        };
    }

    private static AdminNode CreateInstanceNode(string serviceId, SdInstance instance) =>
        AdminData.Node("instances", instance.Id, instance.Id) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Id", instance.Id),
                .. instance.Attributes.OrderBy(x => x.Key, StringComparer.Ordinal)
                    .Select(x => AdminData.Field(x.Key, x.Value)),
            ],
            ReadContent = () => NetworkingAdminData.Json(instance.ToDict()),
            ReadConnections = () =>
            [
                new("Service", "registered-with", "servicediscovery",
                    [new("services", serviceId)]),
            ],
        };

    private static string? HostedZoneId(SdNamespace ns)
    {
        if (ns.Properties is null
            || !ns.Properties.TryGetValue("DnsProperties", out var dnsValue)
            || dnsValue is not Dictionary<string, object?> dns
            || !dns.TryGetValue("HostedZoneId", out var zoneValue))
            return null;
        return zoneValue?.ToString();
    }
}
