using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Sts;

internal sealed partial class StsServiceHandler
{
    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) => [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId) => [];

    public string? GetAdminNotice(string serviceId) =>
        serviceId == "sts"
            ? "STS is stateless. Issued temporary credentials are not retained and cannot be inspected."
            : null;
}
