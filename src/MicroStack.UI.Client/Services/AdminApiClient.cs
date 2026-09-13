using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using MicroStack.Admin.Contracts;

namespace MicroStack.UI.Client.Services;

public sealed class AdminApiException(HttpStatusCode status, string code, string message) : Exception(message)
{
    public HttpStatusCode Status { get; } = status;
    public string Code { get; } = code;
}

public sealed class AdminApiClient(HttpClient http)
{
    private const string Root = "/_microstack/admin/v1";
    public Task<AdminContext> ContextAsync(CancellationToken ct) => GetAsync(Root + "/context", AdminJsonContext.Default.AdminContext, ct);
    public Task<string[]> AccountsAsync(CancellationToken ct) => GetAsync(Root + "/accounts", AdminJsonContext.Default.StringArray, ct);
    public Task<AdminService[]> ServicesAsync(string account, CancellationToken ct) =>
        GetAsync(Url("/services", account), AdminJsonContext.Default.AdminServiceArray, ct);
    public Task<AdminPage<AdminResourceSummary>> ResourcesAsync(string service, string account, string? kind, string? filter, CancellationToken ct) =>
        GetAllAsync(cursor => Url($"/services/{Uri.EscapeDataString(service)}/resources", account, kind: kind, filter: filter, cursor: cursor),
            AdminJsonContext.Default.AdminPageAdminResourceSummary, ct, item => item.Key);
    public Task<AdminResourceDetail> DetailAsync(string service, string account, AdminKey[] path, CancellationToken ct) =>
        GetAsync(Url($"/services/{Uri.EscapeDataString(service)}/resource", account, path), AdminJsonContext.Default.AdminResourceDetail, ct);
    public Task<AdminPage<AdminResourceSummary>> ChildrenAsync(string service, string account, AdminKey[] path, string? kind, string? filter, string? prefix, CancellationToken ct) =>
        GetAllAsync(cursor => Url($"/services/{Uri.EscapeDataString(service)}/children", account, path, kind, filter, cursor, prefix),
            AdminJsonContext.Default.AdminPageAdminResourceSummary, ct, item => item.Key);
    public Task<AdminContent> ContentAsync(string service, string account, AdminKey[] path, CancellationToken ct) =>
        GetAsync(Url($"/services/{Uri.EscapeDataString(service)}/content", account, path), AdminJsonContext.Default.AdminContent, ct);
    public Task<AdminPage<AdminConnection>> ConnectionsAsync(string service, string account, AdminKey[] path, CancellationToken ct) =>
        GetAllAsync(cursor => Url($"/services/{Uri.EscapeDataString(service)}/connections", account, path, cursor: cursor),
            AdminJsonContext.Default.AdminPageAdminConnection, ct);
    public Task<AdminPage<AdminActivity>> ActivityAsync(string service, string account, CancellationToken ct) =>
        GetAllAsync(cursor => Url($"/services/{Uri.EscapeDataString(service)}/activity", account, cursor: cursor), AdminJsonContext.Default.AdminPageAdminActivity, ct);
    public async Task<AdminContent> RevealAsync(string service, string account, AdminKey[] path, string field, CancellationToken ct)
    {
        var url = Url($"/services/{Uri.EscapeDataString(service)}/reveal", account, path) + "&field=" + Uri.EscapeDataString(field);
        using var response = await http.PostAsync(url, null, ct);
        return await ReadAsync(response, AdminJsonContext.Default.AdminContent, ct);
    }

    private async Task<T> GetAsync<T>(string url, JsonTypeInfo<T> type, CancellationToken ct)
    {
        using var response = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct);
        return await ReadAsync(response, type, ct);
    }

    private async Task<AdminPage<T>> GetAllAsync<T>(Func<string?, string> url, JsonTypeInfo<AdminPage<T>> type,
        CancellationToken ct, Func<T, AdminKey>? keySelector = null)
    {
        List<T> items = [];
        HashSet<string> cursors = new(StringComparer.Ordinal);
        HashSet<AdminKey> keys = [];
        string? cursor = null;
        DateTimeOffset captured;
        do
        {
            ct.ThrowIfCancellationRequested();
            var page = await GetAsync(url(cursor), type, ct);
            ct.ThrowIfCancellationRequested();
            // A moving collection may repeat a resource at a page boundary.
            foreach (var item in page.Items)
                if (keySelector is null || keys.Add(keySelector(item))) items.Add(item);
            captured = page.CapturedAt;
            cursor = page.NextCursor;
            if (!string.IsNullOrEmpty(cursor) && !cursors.Add(cursor))
                throw new JsonException("The API repeated a continuation cursor. The complete list could not be loaded.");
        } while (!string.IsNullOrEmpty(cursor));
        return new AdminPage<T> { Items = items, KnownTotal = items.Count, CapturedAt = captured };
    }

    private static async Task<T> ReadAsync<T>(HttpResponseMessage response, JsonTypeInfo<T> type, CancellationToken ct)
    {
        if (!response.IsSuccessStatusCode)
        {
            AdminError? error = null;
            try { error = await response.Content.ReadFromJsonAsync(AdminJsonContext.Default.AdminError, ct); }
            catch (JsonException) { }
            throw new AdminApiException(response.StatusCode, error?.Code ?? "request_failed",
                error?.Message ?? $"The API returned HTTP {(int)response.StatusCode}. Refresh to try again.");
        }
        return await response.Content.ReadFromJsonAsync(type, ct)
            ?? throw new JsonException("The API returned no response data.");
    }

    private static string Url(string route, string account, AdminKey[]? path = null, string? kind = null, string? filter = null, string? cursor = null, string? prefix = null)
    {
        var values = new Dictionary<string, string?>
        {
            ["accountId"] = account, ["path"] = path is null ? null : ExplorerLocation.EncodePath(path),
            ["kind"] = kind, ["filter"] = filter, ["cursor"] = cursor, ["prefix"] = prefix, ["pageSize"] = "200"
        };
        return Root + route + "?" + string.Join("&", values.Where(pair => !string.IsNullOrEmpty(pair.Value))
            .Select(pair => pair.Key + "=" + Uri.EscapeDataString(pair.Value!)));
    }
}
