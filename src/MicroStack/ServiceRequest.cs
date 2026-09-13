namespace MicroStack;

/// <summary>
/// Represents an incoming AWS API request after decoding from HTTP.
/// </summary>
public sealed record ServiceRequest(
    string Method,
    string Path,
    IReadOnlyDictionary<string, string> Headers,
    byte[] Body,
    IReadOnlyDictionary<string, string[]> QueryParams)
{
    /// <summary>
    /// Gets the externally visible origin of the HTTP request, when the request
    /// originated from the gateway.
    /// </summary>
    public string? Origin { get; init; }

    /// <summary>Gets a header value case-insensitively, or null if not present.</summary>
    public string? GetHeader(string name)
    {
        foreach (var kv in Headers)
        {
            if (string.Equals(kv.Key, name, StringComparison.OrdinalIgnoreCase))
                return kv.Value;
        }
        return null;
    }

    /// <summary>Gets the first value of a query parameter, or null if not present.</summary>
    public string? GetQueryParam(string name)
    {
        if (QueryParams.TryGetValue(name, out var values) && values.Length > 0)
            return values[0];
        return null;
    }
}
