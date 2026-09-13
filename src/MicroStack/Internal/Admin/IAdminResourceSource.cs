using MicroStack.Admin.Contracts;

namespace MicroStack.Internal.Admin;

internal interface IAdminResourceSource : IKnownAccountSource
{
    IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId);
    IEnumerable<AdminNode> GetAdminResources(string serviceId);
    string? GetAdminNotice(string serviceId) => null;
}
