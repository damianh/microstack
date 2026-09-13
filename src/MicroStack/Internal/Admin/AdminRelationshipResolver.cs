using MicroStack.Admin.Contracts;

namespace MicroStack.Internal.Admin;

internal interface IAdminRelationshipSource
{
    AdminRelationshipSnapshot GetAdminRelationshipSnapshot();
}

internal sealed record AdminRelationshipResource(
    AdminKey[] Path, string Name, string? Status = null, string? Arn = null);

internal sealed record AdminConfiguredRelationship(
    string SourceServiceId, AdminKey[] SourcePath, string SourceName, AdminConnection Connection);

internal sealed record AdminRelationshipSnapshot(
    IReadOnlyList<AdminRelationshipResource> Resources,
    IReadOnlyList<AdminConfiguredRelationship> Relationships);

// Capture each provider independently. Only retained configuration is inspected;
// these snapshots are neither a delivery history nor an atomic cross-service view.
internal sealed class AdminRelationshipResolver(ServiceRegistry registry, string account)
{
    private readonly Dictionary<string, AdminRelationshipSnapshot> _snapshots = new(StringComparer.Ordinal);

    public IReadOnlyList<AdminConnection> Read(AdminNode node, string service, AdminKey[] path)
    {
        var relationships = new List<AdminConnection>();
        var configuredHierarchy = service switch
        {
            "sqs" => path[0].Kind == "queues" && path.Length == 1,
            "sns" => path[0].Kind == "topics",
            "events" => path[0].Kind == "event-buses",
            _ => false
        };
        if (configuredHierarchy)
        {
            foreach (var edge in Snapshot(service).Relationships)
            {
                if (!SameAccount(edge.SourcePath) || !IsPrefix(path, edge.SourcePath)
                    || edge.Connection.Relation == "belongs-to" && !SamePath(path, edge.SourcePath))
                    continue;
                relationships.Add(Resolve(edge.Connection with
                {
                    Label = SamePath(path, edge.SourcePath) ? edge.Connection.Label
                        : $"{edge.SourceName}: {edge.Connection.Label}",
                    SourceServiceId = edge.SourceServiceId,
                    SourcePath = edge.SourcePath
                }));
            }

            if (service == "sqs")
            {
                foreach (var producer in new[] { "sns", "events", "sqs" })
                foreach (var edge in Snapshot(producer).Relationships)
                {
                    if (!SameAccount(edge.SourcePath) || edge.Connection.TargetServiceId != "sqs"
                        || edge.Connection.TargetPath is not { } target || !SamePath(path, target))
                        continue;
                    relationships.Add(Resolve(new(edge.SourceName,
                        edge.Connection.Relation == "redrive" ? "redrive-source" : "configured-source",
                        producer, edge.SourcePath)
                    {
                        SourceServiceId = edge.SourceServiceId,
                        SourcePath = edge.SourcePath
                    }));
                }
            }
        }
        else
        {
            var direct = node.ReadConnections?.Invoke() ?? [];
            relationships.AddRange(service is "sqs" or "sns" or "events" ? direct.Select(Resolve) : direct);
        }

        return relationships.DistinctBy(Identity, StringComparer.Ordinal)
            .OrderBy(connection => connection.Label, StringComparer.Ordinal)
            .ThenBy(connection => connection.Relation, StringComparer.Ordinal)
            .ThenBy(Identity, StringComparer.Ordinal).ToArray();
    }

    private AdminRelationshipSnapshot Snapshot(string service)
    {
        if (_snapshots.TryGetValue(service, out var snapshot))
            return snapshot;
        var entry = AdminCatalog.Find(service);
        var handler = entry is null ? null : registry.Resolve(entry.CanonicalHandler);
        snapshot = handler is IAdminRelationshipSource source
            ? source.GetAdminRelationshipSnapshot() : new([], []);
        _snapshots.Add(service, snapshot);
        return snapshot;
    }

    private AdminConnection Resolve(AdminConnection connection)
    {
        if (connection.ExternalUri is not null)
            return connection with { State = "external" };
        if (connection.TargetServiceId is not { } targetService || connection.TargetPath is not { Length: > 0 } path)
            return connection with
            {
                State = connection.State == "external" || IsOtherAccount(connection.Label) ? "external" : "unavailable"
            };
        var entry = AdminCatalog.Find(targetService)
            ?? AdminCatalog.Entries.FirstOrDefault(item =>
                string.Equals(item.CanonicalHandler,
                    ServiceRegistry.Aliases.GetValueOrDefault(targetService, targetService), StringComparison.Ordinal));
        if (entry is null)
            return connection with { State = "unavailable" };
        connection = connection with { TargetServiceId = entry.Id };
        if (path.Any(key => IsOtherAccount(key.Id)))
            return connection with { State = "external" };
        var handler = registry.Resolve(entry.CanonicalHandler);
        if (handler is null)
            return connection with { State = "disabled" };
        if (handler is IAdminRelationshipSource)
        {
            var match = Snapshot(entry.Id).Resources.FirstOrDefault(resource => SamePath(path, resource.Path)
                || path.Length == 1 && resource.Path.Length == 1 && path[0].Kind == resource.Path[0].Kind
                    && path[0].Id == resource.Arn);
            var disabled = Snapshot(entry.Id).Resources.Any(resource =>
                IsPrefix(resource.Path, match?.Path ?? path) && IsDisabled(resource.Status));
            return connection with
            {
                TargetPath = match?.Path ?? path,
                State = match is null ? "missing" : disabled ? "disabled" : "configured"
            };
        }
        return connection with { State = "unavailable" };
    }

    private bool SameAccount(AdminKey[] path) => !path.Any(key => IsOtherAccount(key.Id));

    private bool IsOtherAccount(string identity)
    {
        var parts = identity.Split(':', 6);
        return parts.Length == 6 && parts[0] == "arn" && parts[4].Length != 0 && parts[4] != account;
    }

    private static bool IsDisabled(string? status) =>
        string.Equals(status, "disabled", StringComparison.OrdinalIgnoreCase)
        || string.Equals(status, "inactive", StringComparison.OrdinalIgnoreCase);

    private static bool SamePath(AdminKey[] left, AdminKey[] right) =>
        left.Length == right.Length && IsPrefix(left, right);

    private static bool IsPrefix(AdminKey[] prefix, AdminKey[] path) =>
        prefix.Length <= path.Length && prefix.Where((key, index) => key != path[index]).Any() == false;

    private static string PathIdentity(AdminKey[]? path) => path is null ? ""
        : string.Concat(path.Select(key => $"{key.Kind.Length}:{key.Kind}{key.Id.Length}:{key.Id}"));

    private static string Identity(AdminConnection connection) =>
        $"{connection.Relation.Length}:{connection.Relation}|{connection.TargetServiceId}|{PathIdentity(connection.TargetPath)}"
        + $"|{connection.ExternalUri}|{connection.SourceServiceId}|{PathIdentity(connection.SourcePath)}"
        + (connection.SourcePath is null ? $"|{connection.Label}" : "");
}
