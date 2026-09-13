using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Acm;

internal sealed partial class AcmServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds = [new("certificate", "Certificates")];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) => AdminKinds;

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        lock (_lock)
        {
            return _certificates.Items
                .OrderBy(x => x.Value.DomainName, StringComparer.Ordinal)
                .Select(x => CertificateNode(x.Key, x.Value))
                .ToArray();
        }
    }

    private AdminNode CertificateNode(string arn, AcmCertificate snapshot) =>
        AdminData.Node("certificate", arn, snapshot.DomainName, arn, snapshot.Status) with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_certificates.TryGetValue(arn, out var cert)) return [];
                    return
                    [
                        AdminData.Field("Domain name", cert.DomainName),
                        AdminData.Field("Type", cert.Type),
                        AdminData.Field("Validation method", cert.ValidationMethod),
                        AdminData.Field("Created", cert.CreatedAt, format: "datetime"),
                        AdminData.Field("Issued", cert.IssuedAt, format: "datetime"),
                        AdminData.Field("Not before", cert.NotBefore, format: "datetime"),
                        AdminData.Field("Not after", cert.NotAfter, format: "datetime"),
                        AdminData.Field("Subject alternative names", string.Join(", ", cert.SubjectAlternativeNames)),
                        AdminData.Field("In use by", string.Join(", ", cert.InUseBy)),
                        AdminData.Field("Tags", string.Join(", ", cert.Tags.Select(t =>
                            $"{t.GetValueOrDefault("Key")}={t.GetValueOrDefault("Value")}")))
                    ];
                }
            }
        };
}
