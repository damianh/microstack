namespace MicroStack;

/// <summary>
/// Represents a response to be sent back to the AWS SDK client.
/// </summary>
public sealed record ServiceResponse(
    int StatusCode,
    IReadOnlyDictionary<string, string> Headers,
    byte[] Body)
{
    public static ServiceResponse Empty(int statusCode) =>
        new(statusCode, EmptyHeaders, []);

    public static ServiceResponse Text(int statusCode, string body) =>
        new(statusCode, PlainTextHeaders, System.Text.Encoding.UTF8.GetBytes(body));

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();

    private static readonly IReadOnlyDictionary<string, string> PlainTextHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "text/plain" };
}
