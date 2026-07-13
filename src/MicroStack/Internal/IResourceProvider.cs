namespace MicroStack.Internal;

internal interface IResourceProvider
{
    ResourceSummary GetResources();
}

internal sealed record ResourceSummary(
    string Service,
    int Count,
    IReadOnlyList<ResourceItem> Items);

internal sealed record ResourceItem(
    string Name,
    string Arn,
    Dictionary<string, string>? Attributes);
