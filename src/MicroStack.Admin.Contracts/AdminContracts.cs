namespace MicroStack.Admin.Contracts;

public sealed record AdminContext(string DefaultAccount, string Region, int PreviewMaxBytes = 1_048_576);

public sealed record AdminService(
    string Id, string Name, string Label, string Category, string Icon,
    string CanonicalHandler, string Availability, string Scope, string? Notice = null)
{
    public IReadOnlyList<AdminResourceKind> Kinds { get; init => field = value ?? []; } = [];
}

public sealed record AdminResourceKind(string Id, string Label, bool IsRoot = true);

public sealed record AdminKey(string Kind, string Id);

public sealed record AdminResourceSummary(
    AdminKey Key, string Name, string? Arn = null, string? Status = null, string Scope = "account")
{
    public string? Type { get; init; }
    public IReadOnlyList<AdminField> Summary { get; init => field = value ?? []; } = [];
}

public sealed record AdminField(
    string Name, string? Value, bool Sensitive = false, bool CanReveal = false, string? Format = null)
{
    public bool Secondary { get; init; }
}

public sealed record AdminConnection(
    string Label, string Relation, string? TargetServiceId = null,
    AdminKey[]? TargetPath = null, string? ExternalUri = null, string State = "configured")
{
    public string? SourceServiceId { get; init; }
    public AdminKey[]? SourcePath { get; init; }
}

public sealed record AdminPage<T>
{
    public IReadOnlyList<T> Items { get; init; } = [];
    public string? NextCursor { get; init; }
    public long? KnownTotal { get; init; }
    public DateTimeOffset CapturedAt { get; init; }
}

public sealed record AdminResourceDetail(AdminResourceSummary Resource)
{
    public IReadOnlyList<AdminField> Fields { get; init => field = value ?? []; } = [];
    public bool HasChildren { get; init; }
    public bool HasContent { get; init; }
    public bool HasConnections { get; init; }
    public IReadOnlyList<AdminResourceKind> ChildKinds { get; init => field = value ?? []; } = [];
    public IReadOnlyList<AdminField> Summary { get; init => field = value ?? []; } = [];
    public long? ConnectionCount { get; init; }
    public IReadOnlyList<string> RevealableFields { get; init => field = value ?? []; } = [];
}

public sealed record AdminContent(
    string Kind, string ContentType, long? Length = null,
    string? Text = null, string? Reason = null, bool Sensitive = false);

public sealed record AdminError(string Code, string Message);

public sealed record AdminActivity(
    string Service, string Action, string AccountId,
    DateTimeOffset Timestamp, int StatusCode, long DurationMs);
