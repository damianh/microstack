using System.Formats.Cbor;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace MicroStack.Internal;

/// <summary>
/// Adapts Smithy RPC v2 CBOR requests (<c>POST /service/{Service}/operation/{Operation}</c>)
/// to an existing AWS JSON protocol handler by transcoding the request body to JSON,
/// synthesising the <c>X-Amz-Target</c> header, and transcoding the JSON response back to CBOR.
/// </summary>
internal static partial class SmithyRpcV2Cbor
{
    private static readonly IReadOnlyDictionary<string, string> ResponseHeaders = new Dictionary<string, string>
    {
        ["Content-Type"] = "application/cbor",
        ["smithy-protocol"] = "rpc-v2-cbor",
    };

    internal static bool IsCborRequest(ServiceRequest request) =>
        (request.GetHeader("smithy-protocol") ?? "").Contains("rpc-v2-cbor", StringComparison.OrdinalIgnoreCase)
        || (request.GetHeader("content-type") ?? "").Contains("cbor", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Invokes <paramref name="jsonHandler"/> with a JSON-protocol equivalent of the CBOR request.
    /// Numeric values of <paramref name="timestampFields"/> in the response are emitted as CBOR
    /// epoch-seconds timestamps (tag 1), as required by the AWS SDK unmarshallers.
    /// </summary>
    internal static async Task<ServiceResponse> InvokeAsJsonAsync(
        ServiceRequest request,
        string targetPrefix,
        Func<ServiceRequest, Task<ServiceResponse>> jsonHandler,
        IReadOnlySet<string> timestampFields)
    {
        var match = OperationPathRegex().Match(request.Path);
        if (!match.Success)
            return Error("UnknownOperationException", "The request path does not identify an operation.", 404);

        byte[] jsonBody;
        try
        {
            jsonBody = request.Body.Length == 0 ? "{}"u8.ToArray() : ToJson(request.Body);
        }
        catch (Exception ex) when (ex is CborContentException or InvalidOperationException or FormatException)
        {
            return Error("SerializationException", "Invalid CBOR request body.", 400);
        }

        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in request.Headers)
            headers[key] = value;
        headers["x-amz-target"] = $"{targetPrefix}.{Uri.UnescapeDataString(match.Groups[1].Value)}";
        headers["content-type"] = "application/x-amz-json-1.1";

        var response = await jsonHandler(request with { Headers = headers, Body = jsonBody });
        return ToCborResponse(response, timestampFields);
    }

    internal static byte[] ToJson(byte[] cbor)
    {
        var reader = new CborReader(cbor, CborConformanceMode.Lax, allowMultipleRootLevelValues: false);
        using var ms = new MemoryStream();
        using (var writer = new Utf8JsonWriter(ms))
        {
            WriteJson(reader, writer);
        }

        if (reader.BytesRemaining != 0)
            throw new CborContentException("Unexpected trailing data after the CBOR root value.");
        return ms.ToArray();
    }

    internal static byte[] ToCbor(ReadOnlyMemory<byte> json, IReadOnlySet<string> timestampFields)
    {
        using var doc = JsonDocument.Parse(json);
        var writer = new CborWriter(CborConformanceMode.Lax);
        WriteCbor(doc.RootElement, writer, timestampFields, isTimestamp: false);
        return writer.Encode();
    }

    private static ServiceResponse ToCborResponse(ServiceResponse response, IReadOnlySet<string> timestampFields)
    {
        var headers = new Dictionary<string, string>(ResponseHeaders);
        if (response.Body.Length == 0)
            return new ServiceResponse(response.StatusCode, headers, []);

        byte[] body;
        try
        {
            body = ToCbor(response.Body, timestampFields);
        }
        catch (JsonException)
        {
            return Error("InternalFailure", "The service returned a response that could not be encoded as CBOR.", 500);
        }

        if (response.StatusCode >= 400 && TryGetErrorType(response.Body) is { } errorType)
            headers["x-amzn-errortype"] = errorType;
        return new ServiceResponse(response.StatusCode, headers, body);
    }

    private static ServiceResponse Error(string code, string message, int statusCode)
    {
        var writer = new CborWriter(CborConformanceMode.Lax);
        writer.WriteStartMap(2);
        writer.WriteTextString("__type");
        writer.WriteTextString(code);
        writer.WriteTextString("message");
        writer.WriteTextString(message);
        writer.WriteEndMap();
        var headers = new Dictionary<string, string>(ResponseHeaders) { ["x-amzn-errortype"] = code };
        return new ServiceResponse(statusCode, headers, writer.Encode());
    }

    private static string? TryGetErrorType(byte[] json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            return doc.RootElement.ValueKind == JsonValueKind.Object
                   && doc.RootElement.TryGetProperty("__type", out var type)
                   && type.ValueKind == JsonValueKind.String
                ? type.GetString()
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static void WriteJson(CborReader reader, Utf8JsonWriter writer)
    {
        switch (reader.PeekState())
        {
            case CborReaderState.UnsignedInteger:
                writer.WriteNumberValue(reader.ReadUInt64());
                break;
            case CborReaderState.NegativeInteger:
                writer.WriteNumberValue(reader.ReadInt64());
                break;
            case CborReaderState.HalfPrecisionFloat:
            case CborReaderState.SinglePrecisionFloat:
            case CborReaderState.DoublePrecisionFloat:
                var number = reader.ReadDouble();
                if (double.IsNaN(number)) writer.WriteStringValue("NaN");
                else if (double.IsPositiveInfinity(number)) writer.WriteStringValue("Infinity");
                else if (double.IsNegativeInfinity(number)) writer.WriteStringValue("-Infinity");
                else writer.WriteNumberValue(number);
                break;
            case CborReaderState.TextString:
            case CborReaderState.StartIndefiniteLengthTextString:
                writer.WriteStringValue(reader.ReadTextString());
                break;
            case CborReaderState.ByteString:
            case CborReaderState.StartIndefiniteLengthByteString:
                writer.WriteBase64StringValue(reader.ReadByteString());
                break;
            case CborReaderState.Boolean:
                writer.WriteBooleanValue(reader.ReadBoolean());
                break;
            case CborReaderState.Null:
                reader.ReadNull();
                writer.WriteNullValue();
                break;
            case CborReaderState.SimpleValue:
                reader.ReadSimpleValue();
                writer.WriteNullValue();
                break;
            case CborReaderState.Tag:
                // Timestamps (tag 1) become epoch-second numbers, matching the AWS JSON protocol.
                reader.ReadTag();
                WriteJson(reader, writer);
                break;
            case CborReaderState.StartArray:
                reader.ReadStartArray();
                writer.WriteStartArray();
                while (reader.PeekState() != CborReaderState.EndArray)
                    WriteJson(reader, writer);
                reader.ReadEndArray();
                writer.WriteEndArray();
                break;
            case CborReaderState.StartMap:
                reader.ReadStartMap();
                writer.WriteStartObject();
                while (reader.PeekState() != CborReaderState.EndMap)
                {
                    writer.WritePropertyName(reader.ReadTextString());
                    WriteJson(reader, writer);
                }
                reader.ReadEndMap();
                writer.WriteEndObject();
                break;
            default:
                throw new CborContentException($"Unsupported CBOR data item: {reader.PeekState()}.");
        }
    }

    private static void WriteCbor(JsonElement element, CborWriter writer, IReadOnlySet<string> timestampFields, bool isTimestamp)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var properties = element.EnumerateObject().ToList();
                writer.WriteStartMap(properties.Count);
                foreach (var property in properties)
                {
                    writer.WriteTextString(property.Name);
                    WriteCbor(property.Value, writer, timestampFields, timestampFields.Contains(property.Name));
                }
                writer.WriteEndMap();
                break;
            case JsonValueKind.Array:
                writer.WriteStartArray(element.GetArrayLength());
                foreach (var item in element.EnumerateArray())
                    WriteCbor(item, writer, timestampFields, isTimestamp: false);
                writer.WriteEndArray();
                break;
            case JsonValueKind.Number when isTimestamp:
                writer.WriteTag(CborTag.UnixTimeSeconds);
                writer.WriteDouble(element.GetDouble());
                break;
            case JsonValueKind.Number:
                if (element.TryGetInt64(out var integer)) writer.WriteInt64(integer);
                else writer.WriteDouble(element.GetDouble());
                break;
            case JsonValueKind.String:
                writer.WriteTextString(element.GetString()!);
                break;
            case JsonValueKind.True:
                writer.WriteBoolean(true);
                break;
            case JsonValueKind.False:
                writer.WriteBoolean(false);
                break;
            default:
                writer.WriteNull();
                break;
        }
    }

    [GeneratedRegex(@"/service/[^/]+/operation/([^/?]+)")]
    private static partial Regex OperationPathRegex();
}
