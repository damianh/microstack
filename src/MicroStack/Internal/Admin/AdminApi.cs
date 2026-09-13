using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MicroStack.Admin.Contracts;

namespace MicroStack.Internal.Admin;

internal static class AdminApi
{
    private const string Root = "/_microstack/admin/v1";

    internal static void MapAdminApi(
        this WebApplication app, ServiceRegistry registry, RequestLog requestLog,
        MicroStackOptions options, string corsPolicy)
    {
        app.MapGet(Root + "/context", () =>
            Ok(new AdminContext(options.DefaultAccountId, options.Region), AdminJsonContext.Default.AdminContext))
            .RequireCors(corsPolicy);

        app.MapGet(Root + "/accounts", (HttpContext context) =>
        {
            context.Response.Headers.CacheControl = "no-store";
            return Ok(registry.GetKnownAccountIds(options.DefaultAccountId), AdminJsonContext.Default.StringArray);
        }).RequireCors(corsPolicy);

        app.MapGet(Root + "/services", (HttpContext context) =>
            WithAccount(context, options, _ => Ok(
                AdminCatalog.Entries.Select(entry => AdminCatalog.Describe(entry, registry)).ToArray(),
                AdminJsonContext.Default.AdminServiceArray)))
            .RequireCors(corsPolicy);

        app.MapGet(Root + "/services/{service}/resources", (HttpContext context, string service) =>
            WithSource(context, service, registry, options, (entry, source, account) =>
            {
                if (!TryPage(context, Scope(entry.Id, account, [], "resources",
                        Query(context, "kind"), Query(context, "filter")), out var page, out var error))
                    return error;

                var kind = Query(context, "kind");
                var filter = Query(context, "filter");
                IEnumerable<AdminNode> resources = Order(source.GetAdminResources(entry.Id));
                if (!string.IsNullOrEmpty(kind))
                    resources = resources.Where(node =>
                        string.Equals(node.Resource.Key.Kind, kind, StringComparison.OrdinalIgnoreCase));
                if (!string.IsNullOrEmpty(filter))
                    resources = resources.Where(node => Matches(node.Resource, filter));
                return Page(resources.Select(node => node.Resource), page);
            }))
            .RequireCors(corsPolicy);

        app.MapGet(Root + "/services/{service}/resource", (HttpContext context, string service) =>
            WithNode(context, service, registry, options, (node, scope) =>
            {
                var connections = new AdminRelationshipResolver(registry, scope.Account)
                    .Read(node, scope.Service, scope.Path);
                var hasConnections = node.ReadConnections is not null || connections.Count > 0;
                return Ok(new AdminResourceDetail(node.Resource)
                {
                    Fields = node.ReadFields?.Invoke() ?? [],
                    Summary = node.ReadSummary?.Invoke() ?? [],
                    ChildKinds = node.ChildKinds,
                    HasChildren = node.ReadChildren is not null,
                    HasContent = node.ReadContent is not null,
                    HasConnections = hasConnections,
                    ConnectionCount = hasConnections ? connections.Count : null,
                    RevealableFields = node.RevealableFields
                }, AdminJsonContext.Default.AdminResourceDetail);
            }))
            .RequireCors(corsPolicy);

        app.MapGet(Root + "/services/{service}/children", (HttpContext context, string service) =>
            WithNode(context, service, registry, options, (node, scope) =>
            {
                if (node.ReadChildren is null)
                    return Error(409, "capability_unavailable", "This resource does not expose child resources.");
                if (!TryPage(context, Scope(scope.Service, scope.Account, scope.Path, "children",
                        Query(context, "kind"), Query(context, "filter"), Query(context, "prefix")),
                        out var page, out var error))
                    return error;

                var kind = Query(context, "kind");
                var filter = Query(context, "filter");
                var prefix = Query(context, "prefix");
                IEnumerable<AdminNode> children = Order(node.ReadChildren());
                if (!string.IsNullOrEmpty(kind))
                    children = children.Where(child =>
                        string.Equals(child.Resource.Key.Kind, kind, StringComparison.OrdinalIgnoreCase));
                if (!string.IsNullOrEmpty(filter))
                    children = children.Where(child => Matches(child.Resource, filter));
                if (!string.IsNullOrEmpty(prefix))
                    children = children.Where(child =>
                        child.Resource.Key.Id.StartsWith(prefix, StringComparison.Ordinal));
                return Page(children.Select(child => child.Resource), page);
            }))
            .RequireCors(corsPolicy);

        app.MapGet(Root + "/services/{service}/content", (HttpContext context, string service) =>
            WithNode(context, service, registry, options, (node, _) =>
            {
                if (node.ReadContent is null)
                    return Error(409, "capability_unavailable", "This resource does not expose content.");
                context.Response.Headers.CacheControl = "no-store";
                return Ok(node.ReadContent(), AdminJsonContext.Default.AdminContent);
            }))
            .RequireCors(corsPolicy);

        app.MapGet(Root + "/services/{service}/connections", (HttpContext context, string service) =>
            WithNode(context, service, registry, options, (node, scope) =>
            {
                if (!TryPage(context, Scope(scope.Service, scope.Account, scope.Path, "connections"),
                        out var page, out var error))
                    return error;
                var values = new AdminRelationshipResolver(registry, scope.Account)
                    .Read(node, scope.Service, scope.Path);
                if (node.ReadConnections is null && values.Count == 0)
                    return Error(409, "capability_unavailable", "This resource does not expose connections.");
                return ConnectionPage(values, page);
            }))
            .RequireCors(corsPolicy);

        app.MapPost(Root + "/services/{service}/reveal", (HttpContext context, string service) =>
            WithNode(context, service, registry, options, (node, _) =>
            {
                context.Response.Headers.CacheControl = "no-store";
                var field = Query(context, "field");
                if (string.IsNullOrWhiteSpace(field))
                    return Error(400, "invalid_field", "A field is required.");
                if (node.RevealField is null || !node.RevealableFields.Contains(field, StringComparer.Ordinal))
                    return Error(409, "capability_unavailable", "This field cannot be revealed.");
                return Ok(node.RevealField(field), AdminJsonContext.Default.AdminContent);
            }))
            .RequireCors(corsPolicy);

        app.MapGet(Root + "/services/{service}/activity", (HttpContext context, string service) =>
            WithAccount(context, options, account =>
            {
                var entry = AdminCatalog.Find(service);
                if (entry is null)
                    return Error(404, "service_not_found", "The requested service is not in the admin catalog.");
                if (registry.Resolve(entry.CanonicalHandler) is null)
                    return Error(409, "service_disabled", "The requested service is disabled.");
                if (!TryPage(context, Scope(entry.Id, account, [], "activity"), out var page, out var error))
                    return error;

                // Read the bounded log, then filter service/account before applying the page limit.
                var entries = requestLog.GetEntries()
                    .Where(item => string.Equals(item.Service, entry.CanonicalHandler,
                        StringComparison.OrdinalIgnoreCase)
                        && string.Equals(item.AccountId, account, StringComparison.Ordinal))
                    .Skip(page.Offset).Take(page.Size + 1)
                    .Select(item => new AdminActivity(item.Service, item.Action, item.AccountId,
                        item.Timestamp, item.StatusCode, item.DurationMs))
                    .ToArray();
                return ActivityPage(entries, page);
            }))
            .RequireCors(corsPolicy);
    }

    private static IResult WithSource(
        HttpContext context, string service, ServiceRegistry registry, MicroStackOptions options,
        Func<AdminCatalogEntry, IAdminResourceSource, string, IResult> action) =>
        WithAccount(context, options, account =>
        {
            var entry = AdminCatalog.Find(service);
            if (entry is null)
                return Error(404, "service_not_found", "The requested service is not in the admin catalog.");
            var handler = registry.Resolve(entry.CanonicalHandler);
            if (handler is null)
                return Error(409, "service_disabled", "The requested service is disabled.");
            if (handler is not IAdminResourceSource source)
                return Error(409, "capability_unavailable", "Inspection is unavailable for this service.");
            return action(entry, source, account);
        });

    private static IResult WithNode(
        HttpContext context, string service, ServiceRegistry registry, MicroStackOptions options,
        Func<AdminNode, NodeScope, IResult> action) =>
        WithSource(context, service, registry, options, (entry, source, account) =>
        {
            if (!TryPath(context, out var path, out var error))
                return error;
            var node = Resolve(source, entry.Id, path);
            return node is null
                ? Error(404, "resource_not_found", "The requested resource was not found.")
                : action(node, new(entry.Id, account, path));
        });

    private static IResult WithAccount(
        HttpContext context, MicroStackOptions options, Func<string, IResult> action)
    {
        var account = context.Request.Query.ContainsKey("accountId")
            ? Query(context, "accountId")
            : options.DefaultAccountId;
        if (account.Length != 12 || account.Any(character => character is < '0' or > '9'))
            return Error(400, "invalid_account", "Account IDs must contain exactly 12 digits.");
        using var scope = AccountContext.BeginScope(account);
        return action(account);
    }

    private static AdminNode? Resolve(IAdminResourceSource source, string service, AdminKey[] path)
    {
        IEnumerable<AdminNode> level = source.GetAdminResources(service);
        AdminNode? current = null;
        for (var index = 0; index < path.Length; index++)
        {
            var key = path[index];
            current = level.FirstOrDefault(node =>
                string.Equals(node.Resource.Key.Kind, key.Kind, StringComparison.Ordinal)
                && string.Equals(node.Resource.Key.Id, key.Id, StringComparison.Ordinal));
            if (current is null)
                return null;
            if (index < path.Length - 1)
                level = current.ReadChildren?.Invoke() ?? [];
        }
        return current;
    }

    private static bool TryPath(HttpContext context, out AdminKey[] path, out IResult error)
    {
        path = [];
        error = null!;
        var encoded = Query(context, "path");
        if (string.IsNullOrEmpty(encoded))
        {
            error = Error(400, "invalid_path", "A non-empty resource path is required.");
            return false;
        }
        try
        {
            path = JsonSerializer.Deserialize(encoded, AdminJsonContext.Default.AdminKeyArray) ?? [];
        }
        catch (JsonException)
        {
            error = Error(400, "invalid_path", "The resource path is invalid.");
            return false;
        }
        if (path.Length is 0 or > 32 || path.Any(key =>
                string.IsNullOrEmpty(key.Kind) || string.IsNullOrEmpty(key.Id)
                || key.Kind.Length > 256 || key.Id.Length > 16_384))
        {
            error = Error(400, "invalid_path", "The resource path is invalid.");
            return false;
        }
        return true;
    }

    private static bool TryPage(HttpContext context, string scope, out PageRequest page, out IResult error)
    {
        page = default;
        error = null!;
        var sizeText = Query(context, "pageSize");
        if (!string.IsNullOrEmpty(sizeText)
            && (!int.TryParse(sizeText, out var parsed) || parsed is < 1 or > 200))
        {
            error = Error(400, "invalid_limit", "pageSize must be between 1 and 200.");
            return false;
        }
        var size = string.IsNullOrEmpty(sizeText) ? 50 : int.Parse(sizeText);
        var cursor = Query(context, "cursor");
        var offset = 0;
        if (!string.IsNullOrEmpty(cursor) && !Cursor.TryDecode(cursor, scope, out offset))
        {
            error = Error(400, "invalid_cursor", "The cursor is invalid for this request.");
            return false;
        }
        page = new(offset, size, scope);
        return true;
    }

    private static IResult Page(IEnumerable<AdminResourceSummary> values, PageRequest page)
    {
        var items = values.Skip(page.Offset).Take(page.Size + 1).ToArray();
        var hasMore = items.Length > page.Size;
        return Ok(new AdminPage<AdminResourceSummary>
        {
            Items = hasMore ? items[..page.Size] : items,
            NextCursor = hasMore ? Cursor.Encode(page.Offset + page.Size, page.Scope) : null,
            CapturedAt = DateTimeOffset.UtcNow
        }, AdminJsonContext.Default.AdminPageAdminResourceSummary);
    }

    private static IResult ConnectionPage(IReadOnlyList<AdminConnection> values, PageRequest page)
    {
        var items = values.Skip(page.Offset).Take(page.Size + 1).ToArray();
        var hasMore = items.Length > page.Size;
        return Ok(new AdminPage<AdminConnection>
        {
            Items = hasMore ? items[..page.Size] : items,
            NextCursor = hasMore ? Cursor.Encode(page.Offset + page.Size, page.Scope) : null,
            KnownTotal = values.Count,
            CapturedAt = DateTimeOffset.UtcNow
        }, AdminJsonContext.Default.AdminPageAdminConnection);
    }

    private static IResult ActivityPage(AdminActivity[] items, PageRequest page)
    {
        var hasMore = items.Length > page.Size;
        return Ok(new AdminPage<AdminActivity>
        {
            Items = hasMore ? items[..page.Size] : items,
            NextCursor = hasMore ? Cursor.Encode(page.Offset + page.Size, page.Scope) : null,
            CapturedAt = DateTimeOffset.UtcNow
        }, AdminJsonContext.Default.AdminPageAdminActivity);
    }

    private static IOrderedEnumerable<AdminNode> Order(IEnumerable<AdminNode> nodes) =>
        nodes.OrderBy(node => node.Resource.Key.Kind, StringComparer.Ordinal)
            .ThenBy(node => node.Resource.Key.Id, StringComparer.Ordinal);

    private static bool Matches(AdminResourceSummary resource, string filter) =>
        resource.Name.Contains(filter, StringComparison.OrdinalIgnoreCase)
        || resource.Key.Id.Contains(filter, StringComparison.OrdinalIgnoreCase)
        || (resource.Arn?.Contains(filter, StringComparison.OrdinalIgnoreCase) ?? false);

    private static string Scope(string service, string account, AdminKey[] path, string operation,
        params string[] qualifiers)
    {
        var builder = new StringBuilder().Append(service).Append('\0').Append(account).Append('\0')
            .Append(operation);
        foreach (var key in path)
            builder.Append('\0').Append(key.Kind.Length).Append(':').Append(key.Kind)
                .Append(key.Id.Length).Append(':').Append(key.Id);
        foreach (var qualifier in qualifiers)
            builder.Append('\0').Append(qualifier);
        return builder.ToString();
    }

    private static string Query(HttpContext context, string name) => context.Request.Query[name].ToString();

    private static IResult Ok<T>(T value, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> type) =>
        Results.Json(value, type);

    private static IResult Error(int status, string code, string message) =>
        Results.Json(new AdminError(code, message), AdminJsonContext.Default.AdminError, statusCode: status);

    private readonly record struct NodeScope(string Service, string Account, AdminKey[] Path);
    private readonly record struct PageRequest(int Offset, int Size, string Scope);

    private static class Cursor
    {
        internal static string Encode(int offset, string scope)
        {
            Span<byte> bytes = stackalloc byte[20];
            BinaryPrimitives.WriteInt32BigEndian(bytes, offset);
            Span<byte> hash = stackalloc byte[32];
            SHA256.HashData(Encoding.UTF8.GetBytes(scope), hash);
            hash[..16].CopyTo(bytes[4..]);
            return Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        }

        internal static bool TryDecode(string value, string scope, out int offset)
        {
            offset = 0;
            try
            {
                var normalized = value.Replace('-', '+').Replace('_', '/');
                normalized += new string('=', (4 - normalized.Length % 4) % 4);
                var bytes = Convert.FromBase64String(normalized);
                if (bytes.Length != 20)
                    return false;
                Span<byte> expected = stackalloc byte[32];
                SHA256.HashData(Encoding.UTF8.GetBytes(scope), expected);
                if (!CryptographicOperations.FixedTimeEquals(bytes.AsSpan(4), expected[..16]))
                    return false;
                offset = BinaryPrimitives.ReadInt32BigEndian(bytes);
                return offset >= 0;
            }
            catch (FormatException)
            {
                return false;
            }
        }
    }
}
