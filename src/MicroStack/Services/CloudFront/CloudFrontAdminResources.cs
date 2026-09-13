using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;
using MicroStack.Services.ApiGateway;

namespace MicroStack.Services.CloudFront;

internal sealed partial class CloudFrontServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("distributions", "Distributions"), new("invalidations", "Invalidations"),
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == ServiceName ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != ServiceName)
            return [];
        lock (_lock)
            return _distributions.Items.ToArray().Select(CreateDistributionNode).ToArray();
    }

    private AdminNode CreateDistributionNode(KeyValuePair<string, CfDistribution> pair)
    {
        var distribution = pair.Value;
        return AdminData.Node("distributions", distribution.Id,
            string.IsNullOrEmpty(distribution.DomainName) ? distribution.Id : distribution.DomainName,
            distribution.Arn, distribution.Status) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Id", distribution.Id),
                AdminData.Field("DomainName", distribution.DomainName),
                AdminData.Field("Status", distribution.Status),
                AdminData.Field("Enabled", distribution.Enabled ? "true" : "false"),
                AdminData.Field("ETag", distribution.ETag),
                AdminData.Field("LastModifiedTime", distribution.LastModifiedTime),
            ],
            ReadContent = () => AdminData.Text(distribution.ConfigXml, "application/xml"),
            ReadChildren = () =>
            {
                lock (_lock)
                {
                    if (!_invalidations.TryGetValue(distribution.Id, out var invalidations))
                        return [];
                    return invalidations.ToArray().Select(CreateInvalidationNode).ToArray();
                }
            },
        };
    }

    private static AdminNode CreateInvalidationNode(CfInvalidation invalidation) =>
        AdminData.Node("invalidations", invalidation.Id, invalidation.Id, status: invalidation.Status) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Id", invalidation.Id),
                AdminData.Field("Status", invalidation.Status),
                AdminData.Field("CreateTime", invalidation.CreateTime),
                AdminData.Field("CallerReference", invalidation.CallerReference),
                AdminData.Field("Paths", string.Join(", ", invalidation.PathItems)),
            ],
            ReadContent = () => NetworkingAdminData.Json(new Dictionary<string, object?>
            {
                ["Id"] = invalidation.Id,
                ["Status"] = invalidation.Status,
                ["CreateTime"] = invalidation.CreateTime,
                ["CallerReference"] = invalidation.CallerReference,
                ["Paths"] = invalidation.PathItems,
            }),
        };
}
