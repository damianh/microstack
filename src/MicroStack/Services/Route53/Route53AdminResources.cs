using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;
using MicroStack.Services.ApiGateway;

namespace MicroStack.Services.Route53;

internal sealed partial class Route53ServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("hosted-zones", "Hosted zones"), new("record-sets", "Record sets"),
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == ServiceName ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != ServiceName)
            return [];
        lock (_lock)
            return _zones.Items.ToArray().Select(CreateZoneNode).ToArray();
    }

    private AdminNode CreateZoneNode(KeyValuePair<string, HostedZone> pair)
    {
        var zone = pair.Value;
        return AdminData.Node("hosted-zones", zone.Id, zone.Name,
            $"arn:aws:route53:::hostedzone/{zone.Id}") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Id", zone.Id),
                AdminData.Field("Name", zone.Name),
                AdminData.Field("Private", zone.Private ? "true" : "false"),
                AdminData.Field("Comment", zone.Comment),
                AdminData.Field("CallerReference", zone.CallerReference),
            ],
            ReadChildren = () =>
            {
                lock (_lock)
                {
                    if (!_records.TryGetValue(zone.Id, out var records))
                        return [];
                    return records.ToArray().Select(record => CreateRecordNode(zone.Id, record)).ToArray();
                }
            },
        };
    }

    private AdminNode CreateRecordNode(string zoneId, RecordSet record)
    {
        var id = NetworkingAdminData.Composite(record.Name, record.Type, record.SetIdentifier ?? "");
        var aliasZone = record.AliasTarget?.HostedZoneId?
            .TrimStart('/').Replace("hostedzone/", "", StringComparison.Ordinal);
        var hasLocalAlias = !string.IsNullOrEmpty(aliasZone) && _zones.ContainsKey(aliasZone);
        return AdminData.Node("record-sets", id, $"{record.Name} {record.Type}") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Name", record.Name),
                AdminData.Field("Type", record.Type),
                AdminData.Field("TTL", record.Ttl),
                AdminData.Field("SetIdentifier", record.SetIdentifier),
                AdminData.Field("Weight", record.Weight?.ToString()),
                AdminData.Field("Region", record.Region),
                AdminData.Field("Failover", record.Failover),
                AdminData.Field("MultiValueAnswer", record.MultiValueAnswer?.ToString().ToLowerInvariant()),
                AdminData.Field("HealthCheckId", record.HealthCheckId),
                AdminData.Field("Values", record.ResourceRecords is null
                    ? record.AliasTarget?.DnsName
                    : string.Join(", ", record.ResourceRecords)),
            ],
            ReadContent = () => NetworkingAdminData.Json(new Dictionary<string, object?>
            {
                ["Name"] = record.Name,
                ["Type"] = record.Type,
                ["TTL"] = record.Ttl,
                ["SetIdentifier"] = record.SetIdentifier,
                ["Weight"] = record.Weight,
                ["Region"] = record.Region,
                ["Failover"] = record.Failover,
                ["MultiValueAnswer"] = record.MultiValueAnswer,
                ["ResourceRecords"] = record.ResourceRecords,
                ["AliasTarget"] = record.AliasTarget is null ? null : new Dictionary<string, object?>
                {
                    ["HostedZoneId"] = record.AliasTarget.HostedZoneId,
                    ["DNSName"] = record.AliasTarget.DnsName,
                    ["EvaluateTargetHealth"] = record.AliasTarget.EvaluateTargetHealth,
                },
                ["HealthCheckId"] = record.HealthCheckId,
            }),
            ReadConnections = !hasLocalAlias ? null : () =>
                [
                    new("Alias hosted zone", "aliases", "route53",
                        [new("hosted-zones", aliasZone!)]),
                ],
        };
    }
}
