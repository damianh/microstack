using System.Globalization;
using MicroStack.Admin.Contracts;

namespace MicroStack.UI.Client.Services;

public static class InspectionProfile
{
    public static IReadOnlyList<AdminResourceKind> RootKinds(AdminService? service) =>
        service?.Kinds.Where(kind => kind.IsRoot).ToArray() ?? [];

    public static string Description(string service) => service switch
    {
        "sqs" => "Inspect retained queues, non-consuming message snapshots, and configured connections.",
        "s3" => "Browse buckets and prefixes, inspect objects, and review retained configuration.",
        "dynamodb" => "Inspect table schemas and retained items with their DynamoDB attribute types.",
        "sns" => "Inspect subscriptions and configured destinations. Topics are not message inboxes.",
        "events" => "Browse event buses, rule patterns or schedules, and configured targets.",
        _ => "Browse retained resources, inspect their data, and follow configured connections."
    };

    public static string ContentTitle(string service, string kind) => (service, kind) switch
    {
        ("sqs", "messages") => "Message body",
        ("s3", "objects" or "versions") => "Object preview",
        ("dynamodb", "items") => "Item · DynamoDB JSON",
        ("events", "rules") => "Event pattern / schedule",
        _ => "Content preview"
    };

    public static string CollectionLabel(string? service, AdminResourceDetail? detail)
    {
        if (detail?.HasChildren != true) return "Content";
        if (service == "s3" && detail.Resource.Key.Kind is "buckets" or "prefixes") return "Objects";
        return detail.ChildKinds.Count == 1 ? detail.ChildKinds[0].Label : "Entries";
    }

    public static string? DisplayStatus(AdminResourceSummary resource) =>
        !string.IsNullOrWhiteSpace(resource.Type) && string.Equals(resource.Status, "available", StringComparison.OrdinalIgnoreCase)
            ? null : resource.Status;

    public static string ChildFilterLabel(string? service, AdminResourceDetail? detail, string? selectedKind)
    {
        var kind = detail?.ChildKinds.FirstOrDefault(candidate => candidate.Id == selectedKind) ??
            (detail?.ChildKinds.Count == 1 ? detail.ChildKinds[0] : null);
        if (service == "sqs" && kind?.Id == "messages") return "Filter message IDs";
        if (service == "s3" && detail?.Resource.Key.Kind is "buckets" or "prefixes") return "Filter object keys or prefix names";
        return kind is null ? "Filter entries by name or identifier" : $"Filter {kind.Label.ToLowerInvariant()} by name or identifier";
    }

    public static string FieldValue(AdminField field)
    {
        if (field.Sensitive) return "••••••••";
        if (field.Format is "timestamp" or "datetime" or "date-time" && DateTimeOffset.TryParse(field.Value,
            CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var timestamp))
            return ExplorerLocation.Utc(timestamp);
        if (field.Format == "bytes" && long.TryParse(field.Value, CultureInfo.InvariantCulture, out var bytes))
            return $"{bytes.ToString("N0", CultureInfo.InvariantCulture)} bytes";
        return field.Value ?? "Not set";
    }
}
