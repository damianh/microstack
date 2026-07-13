using System.Net.Http.Json;

namespace MicroStack.UI.Client.Services;

internal sealed class MicroStackApiService(HttpClient httpClient)
{
    public async Task<HealthResponse?> GetHealthAsync(CancellationToken cancellationToken = default)
    {
        return await httpClient.GetFromJsonAsync<HealthResponse>("/_microstack/health", cancellationToken);
    }

    public async Task<IReadOnlyList<RequestLogEntry>> GetRequestsAsync(int limit = 1000, CancellationToken cancellationToken = default)
    {
        var requests = await httpClient.GetFromJsonAsync<List<RequestLogEntry>>($"/_microstack/requests?limit={limit}", cancellationToken);
        return requests ?? [];
    }

    public async Task DeleteRequestsAsync(CancellationToken cancellationToken = default)
    {
        using var response = await httpClient.DeleteAsync("/_microstack/requests", cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task<IReadOnlyList<ResourceSummary>> GetResourcesAsync(CancellationToken cancellationToken = default)
    {
        var resources = await httpClient.GetFromJsonAsync<List<ResourceSummary>>("/_microstack/resources", cancellationToken);
        return resources ?? [];
    }

    public async Task ResetAsync(CancellationToken cancellationToken = default)
    {
        using var response = await httpClient.PostAsync("/_microstack/reset", content: null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }
}

internal sealed record HealthResponse(Dictionary<string, string> Services, string Edition, string Version);
internal sealed record RequestLogEntry(string Service, string Action, string AccountId, DateTimeOffset Timestamp, int StatusCode, long DurationMs);
internal sealed record ResourceSummary(string Service, int Count, IReadOnlyList<ResourceItem> Items);
internal sealed record ResourceItem(string Name, string Arn, Dictionary<string, string>? Attributes);
