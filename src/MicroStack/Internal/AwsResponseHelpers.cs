using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Xml.Linq;

namespace MicroStack.Internal;

/// <summary>
/// Builds AWS-compatible XML and JSON service responses.
/// Port of ministack/core/responses.py (xml_response, json_response, error helpers).
/// </summary>
internal static class AwsResponseHelpers
{
    private static readonly Dictionary<string, string> XmlHeaders = new()
        { ["Content-Type"] = "application/xml" };

    private static readonly Dictionary<string, string> JsonHeaders = new()
        { ["Content-Type"] = "application/x-amz-json-1.0" };

    // ── XML responses ──────────────────────────────────────────────────────────

    /// <summary>
    /// Build an AWS-style XML response with RequestId in ResponseMetadata.
    /// </summary>
    internal static ServiceResponse XmlResponse(
        string rootTag,
        string xmlNamespace,
        IReadOnlyDictionary<string, object?> children,
        int statusCode = 200)
    {
        var root = new XElement(rootTag, new XAttribute("xmlns", xmlNamespace));
        DictToXml(root, children);

        var metadata  = new XElement("ResponseMetadata");
        metadata.Add(new XElement("RequestId", Guid.NewGuid().ToString()));
        root.Add(metadata);

        var body = SerializeXml(root);
        return new ServiceResponse(statusCode, XmlHeaders, body);
    }

    /// <summary>Build an AWS-style XML error response.</summary>
    internal static ServiceResponse ErrorResponseXml(
        string code,
        string message,
        int statusCode,
        string xmlNamespace = "http://s3.amazonaws.com/doc/2006-03-01/")
    {
        var root  = new XElement("ErrorResponse", new XAttribute("xmlns", xmlNamespace));
        var error = new XElement("Error");
        error.Add(new XElement("Type",    statusCode < 500 ? "Sender" : "Receiver"));
        error.Add(new XElement("Code",    code));
        error.Add(new XElement("Message", message));
        root.Add(error);
        root.Add(new XElement("RequestId", Guid.NewGuid().ToString()));

        var body = SerializeXml(root);
        return new ServiceResponse(statusCode, XmlHeaders, body);
    }

    // ── JSON responses ─────────────────────────────────────────────────────────

    /// <summary>Build an AWS-style JSON response from a dictionary.</summary>
    internal static ServiceResponse JsonResponse(Dictionary<string, object?> data, int statusCode = 200)
    {
        using var ms     = new System.IO.MemoryStream();
        using var writer = new Utf8JsonWriter(ms);
        DictionaryObjectJsonConverter.WriteObject(writer, data);
        writer.Flush();
        return new ServiceResponse(statusCode, JsonHeaders, ms.ToArray());
    }

    /// <summary>Build an AWS-style JSON error response.</summary>
    internal static ServiceResponse ErrorResponseJson(string code, string message, int statusCode = 400)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(new AwsJsonError(code, message), MicroStackJsonContext.Default.AwsJsonError);
        return new ServiceResponse(statusCode, JsonHeaders, json);
    }

    // ── Recursive XML builder ──────────────────────────────────────────────────

    private static void DictToXml(XElement parent, IReadOnlyDictionary<string, object?> dict)
    {
        foreach (var (key, value) in dict)
            AppendValue(parent, key, value);
    }

    private static void AppendValue(XElement parent, string tag, object? value)
    {
        switch (value)
        {
            case null:
                parent.Add(new XElement(tag, ""));
                break;

            case IReadOnlyDictionary<string, object?> nested:
                var child = new XElement(tag);
                DictToXml(child, nested);
                parent.Add(child);
                break;

            case IEnumerable<object?> list:
                foreach (var item in list)
                {
                    var listChild = new XElement(tag);
                    if (item is IReadOnlyDictionary<string, object?> itemDict)
                        DictToXml(listChild, itemDict);
                    else
                        listChild.Value = item?.ToString() ?? "";
                    parent.Add(listChild);
                }
                break;

            default:
                parent.Add(new XElement(tag, value.ToString() ?? ""));
                break;
        }
    }

    private static byte[] SerializeXml(XElement root)
    {
        var declaration = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"u8.ToArray();
        var element     = Encoding.UTF8.GetBytes(root.ToString(SaveOptions.DisableFormatting));
        return [.. declaration, .. element];
    }
}

/// <summary>JSON body for AWS-style error responses.</summary>
internal sealed record AwsJsonError(
    [property: JsonPropertyName("__type")] string Type,
    [property: JsonPropertyName("message")] string Message);
