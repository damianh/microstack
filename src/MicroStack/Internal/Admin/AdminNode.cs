using MicroStack.Admin.Contracts;

namespace MicroStack.Internal.Admin;

internal sealed record AdminNode(AdminResourceSummary Resource)
{
    public Func<IReadOnlyList<AdminField>>? ReadFields { get; init; }
    public Func<IEnumerable<AdminNode>>? ReadChildren { get; init; }
    public Func<IReadOnlyList<AdminConnection>>? ReadConnections { get; init; }
    public Func<AdminContent>? ReadContent { get; init; }
    public Func<string, AdminContent>? RevealField { get; init; }
    public IReadOnlyList<string> RevealableFields { get; init; } = [];
}
