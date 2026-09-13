using System.Buffers;
using System.Collections;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using MicroStack.Admin.Contracts;

namespace MicroStack.Internal.Admin;

internal static class AdminData
{
    public const int PreviewMaxBytes = 1_048_576;
    public const string MaskedValue = "••••••••";
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    public static string IsoUtc(DateTimeOffset value) =>
        value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.FFFFFFF'Z'", CultureInfo.InvariantCulture);

    public static string IsoUtc(DateTime value) =>
        value.Kind == DateTimeKind.Unspecified
            ? throw new ArgumentException("An explicit UTC or local timestamp is required.", nameof(value))
            : IsoUtc(new DateTimeOffset(value));

    public static AdminField Field(
        string name, string? value, bool sensitive = false, bool canReveal = false, string? format = null) =>
        new(name, sensitive ? MaskedValue : value, sensitive, canReveal, format);

    public static AdminNode Node(
        string kind, string id, string name, string? arn = null, string? status = null, string scope = "account") =>
        new(new(new(kind, id), name, arn, status, scope));

    public static AdminContent Text(string text, string contentType = "text/plain", bool sensitive = false)
    {
        var length = StrictUtf8.GetByteCount(text);
        return length > PreviewMaxBytes
            ? Oversized(contentType, length, sensitive)
            : new("text", contentType, length, text, Sensitive: sensitive);
    }

    public static AdminContent Text(
        ReadOnlySpan<byte> utf8, string contentType = "text/plain", bool sensitive = false) =>
        utf8.Length > PreviewMaxBytes
            ? Oversized(contentType, utf8.Length, sensitive)
            : new("text", contentType, utf8.Length, StrictUtf8.GetString(utf8), Sensitive: sensitive);

    public static AdminContent Binary(
        long? length, string contentType = "application/octet-stream", bool sensitive = false) =>
        new("binary", contentType, length, Reason: "Binary content is metadata-only.", Sensitive: sensitive);

    public static AdminContent Unavailable(
        string reason, string contentType = "application/octet-stream", long? length = null, bool sensitive = false) =>
        new("unavailable", contentType, length, Reason: reason, Sensitive: sensitive);

    public static AdminContent Oversized(
        string contentType, long? length = null, bool sensitive = false) =>
        new("oversized", contentType, length, Reason: "Content exceeds the 1 MiB preview limit.", Sensitive: sensitive);

    public static AdminContent Json(object? value, bool sensitive = false) =>
        Json(writer => WriteValue(writer, value, 0), sensitive);

    public static AdminContent Json<T>(T value, JsonTypeInfo<T> typeInfo, bool sensitive = false) =>
        Json(writer => JsonSerializer.Serialize(writer, value, typeInfo), sensitive);

    public static AdminContent Json(Action<Utf8JsonWriter> write, bool sensitive = false)
    {
        var buffer = new PreviewBuffer();
        try
        {
            using (var writer = new Utf8JsonWriter(buffer))
            {
                write(writer);
                writer.Flush();
            }
            // Also rejects incomplete output from a custom projection callback.
            using var document = JsonDocument.Parse(buffer.WrittenMemory);
            return new("json", "application/json", buffer.WrittenMemory.Length,
                StrictUtf8.GetString(buffer.WrittenMemory.Span), Sensitive: sensitive);
        }
        catch (PreviewLimitException)
        {
            return Oversized("application/json", sensitive: sensitive);
        }
    }

    public static AdminContent JsonText(string json, bool sensitive = false)
    {
        var length = StrictUtf8.GetByteCount(json);
        if (length > PreviewMaxBytes)
            return Oversized("application/json", length, sensitive);
        using var document = JsonDocument.Parse(json);
        return new("json", "application/json", length, json, Sensitive: sensitive);
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value, int depth)
    {
        if (depth > 64)
            throw new JsonException("The explicit admin projection exceeds the maximum depth.");
        switch (value)
        {
            case null: writer.WriteNullValue(); break;
            case JsonElement element: element.WriteTo(writer); break;
            case string text: writer.WriteStringValue(text); break;
            case bool boolean: writer.WriteBooleanValue(boolean); break;
            case byte number: writer.WriteNumberValue(number); break;
            case sbyte number: writer.WriteNumberValue(number); break;
            case short number: writer.WriteNumberValue(number); break;
            case ushort number: writer.WriteNumberValue(number); break;
            case int number: writer.WriteNumberValue(number); break;
            case uint number: writer.WriteNumberValue(number); break;
            case long number: writer.WriteNumberValue(number); break;
            case ulong number: writer.WriteNumberValue(number); break;
            case float number: writer.WriteNumberValue(number); break;
            case double number: writer.WriteNumberValue(number); break;
            case decimal number: writer.WriteNumberValue(number); break;
            case IReadOnlyDictionary<string, object?> dictionary:
                writer.WriteStartObject();
                foreach (var (key, item) in dictionary)
                {
                    writer.WritePropertyName(key);
                    WriteValue(writer, item, depth + 1);
                }
                writer.WriteEndObject();
                break;
            case IReadOnlyDictionary<string, string?> dictionary:
                writer.WriteStartObject();
                foreach (var (key, item) in dictionary)
                    writer.WriteString(key, item);
                writer.WriteEndObject();
                break;
            case IDictionary dictionary:
                writer.WriteStartObject();
                foreach (DictionaryEntry entry in dictionary)
                {
                    if (entry.Key is not string key)
                        throw new NotSupportedException("Admin JSON dictionary keys must be strings.");
                    writer.WritePropertyName(key);
                    WriteValue(writer, entry.Value, depth + 1);
                }
                writer.WriteEndObject();
                break;
            case Array array:
                writer.WriteStartArray();
                foreach (var item in array)
                    WriteValue(writer, item, depth + 1);
                writer.WriteEndArray();
                break;
            default:
                throw new NotSupportedException(
                    "Admin JSON requires explicit primitives, string-keyed dictionaries, arrays, JsonElement, or JsonTypeInfo.");
        }
    }

    private sealed class PreviewLimitException : Exception;

    private sealed class PreviewBuffer : IBufferWriter<byte>
    {
        // Utf8JsonWriter reserves up to six bytes per UTF-16 character before
        // escaping. Permit that bounded reservation; enforce the exact cap on Advance.
        private const int MaxReservation = (PreviewMaxBytes + 1) * 6 + 4096;
        private byte[] _buffer = new byte[4096];
        private int _written;
        public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _written);

        public void Advance(int count)
        {
            if (count < 0 || count > _buffer.Length - _written)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (count > PreviewMaxBytes - _written)
                throw new PreviewLimitException();
            _written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            if (sizeHint < 0)
                throw new ArgumentOutOfRangeException(nameof(sizeHint));
            sizeHint = Math.Max(1, sizeHint);
            if (sizeHint > MaxReservation - _written)
                throw new PreviewLimitException();
            var required = _written + sizeHint;
            if (required > _buffer.Length)
                Array.Resize(ref _buffer, Math.Min(MaxReservation, Math.Max(required, _buffer.Length * 2)));
            return _buffer.AsMemory(_written);
        }

        public Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;
    }
}
