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
    public Task<AdminPage<AdminResourceSummary>> ResourcesAsync(string service, string account, string? kind, string? filter, string? cursor, CancellationToken ct) =>
        GetAsync(Url($"/services/{Uri.EscapeDataString(service)}/resources", account, kind: kind, filter: filter, cursor: cursor),
            AdminJsonContext.Default.AdminPageAdminResourceSummary, ct);
    public Task<AdminResourceDetail> DetailAsync(string service, string account, AdminKey[] path, CancellationToken ct) =>
        GetAsync(Url($"/services/{Uri.EscapeDataString(service)}/resource", account, path), AdminJsonContext.Default.AdminResourceDetail, ct);
    public Task<AdminPage<AdminResourceSummary>> ChildrenAsync(string service, string account, AdminKey[] path, string? kind, string? filter, string? prefix, string? cursor, CancellationToken ct) =>
        GetAsync(Url($"/services/{Uri.EscapeDataString(service)}/children", account, path, kind, filter, cursor, prefix),
            AdminJsonContext.Default.AdminPageAdminResourceSummary, ct);
    public Task<AdminContent> ContentAsync(string service, string account, AdminKey[] path, CancellationToken ct) =>
        GetAsync(Url($"/services/{Uri.EscapeDataString(service)}/content", account, path), AdminJsonContext.Default.AdminContent, ct);
    public Task<AdminPage<AdminConnection>> ConnectionsAsync(string service, string account, AdminKey[] path, string? cursor, CancellationToken ct) =>
        GetAsync(Url($"/services/{Uri.EscapeDataString(service)}/connections", account, path, cursor: cursor),
            AdminJsonContext.Default.AdminPageAdminConnection, ct);
    public Task<AdminPage<AdminActivity>> ActivityAsync(string service, string account, CancellationToken ct) =>
        GetAsync(Url($"/services/{Uri.EscapeDataString(service)}/activity", account), AdminJsonContext.Default.AdminPageAdminActivity, ct);
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
            ["kind"] = kind, ["filter"] = filter, ["cursor"] = cursor, ["prefix"] = prefix, ["pageSize"] = "50"
        };
        return Root + route + "?" + string.Join("&", values.Where(pair => !string.IsNullOrEmpty(pair.Value))
            .Select(pair => pair.Key + "=" + Uri.EscapeDataString(pair.Value!)));
    }
}
