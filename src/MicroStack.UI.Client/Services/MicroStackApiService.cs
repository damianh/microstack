using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MicroStack.UI.Client.Services;

internal sealed class MicroStackApiService(HttpClient httpClient)
{
    public async Task<HealthResponse?> GetHealthAsync(CancellationToken cancellationToken = default)
    {
        return await httpClient.GetFromJsonAsync("/_microstack/health", LegacyJsonContext.Default.HealthResponse, cancellationToken)
            ?? throw new JsonException("The API returned no health data.");
    }

    public async Task<IReadOnlyList<RequestLogEntry>> GetRequestsAsync(int limit = 1000, CancellationToken cancellationToken = default)
    {
        return await httpClient.GetFromJsonAsync($"/_microstack/requests?limit={limit}", LegacyJsonContext.Default.RequestLogEntryArray, cancellationToken)
            ?? throw new JsonException("The API returned no request data.");
    }

    public async Task DeleteRequestsAsync(CancellationToken cancellationToken = default)
    {
        using var response = await httpClient.DeleteAsync("/_microstack/requests", cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task<IReadOnlyList<ResourceSummary>> GetResourcesAsync(CancellationToken cancellationToken = default)
    {
        return await httpClient.GetFromJsonAsync("/_microstack/resources", LegacyJsonContext.Default.ResourceSummaryArray, cancellationToken)
            ?? throw new JsonException("The API returned no resource data.");
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

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(HealthResponse))]
[JsonSerializable(typeof(RequestLogEntry[]))]
[JsonSerializable(typeof(ResourceSummary[]))]
internal partial class LegacyJsonContext : JsonSerializerContext;
