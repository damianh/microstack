using System.Collections;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace MicroStack.Internal;

/// <summary>
/// AOT-safe converter for <see cref="Dictionary{TKey,TValue}">Dictionary&lt;string, object?&gt;</see>.
/// Handles the known primitive value types used throughout MicroStack:
/// string, long, int, double, bool, null, nested Dictionary&lt;string, object?&gt;, List&lt;object?&gt;.
/// </summary>
internal sealed class DictionaryObjectJsonConverter : JsonConverter<Dictionary<string, object?>>
{
    public override Dictionary<string, object?> Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected StartObject");

        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return dict;

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("Expected PropertyName");

            var key = reader.GetString()!;
            reader.Read();
            dict[key] = ReadValue(ref reader, options);
        }

        throw new JsonException("Unexpected end of JSON");
    }

    private static object? ReadValue(ref Utf8JsonReader reader, JsonSerializerOptions options)
    {
        return reader.TokenType switch
        {
            JsonTokenType.Null => null,
            JsonTokenType.True => true,
            JsonTokenType.False => false,
            JsonTokenType.String => reader.GetString(),
            JsonTokenType.Number => reader.TryGetInt64(out var l) ? l : reader.GetDouble(),
            JsonTokenType.StartObject => ReadDict(ref reader, options),
            JsonTokenType.StartArray => ReadList(ref reader, options),
            _ => throw new JsonException($"Unsupported token: {reader.TokenType}"),
        };
    }

    private static Dictionary<string, object?> ReadDict(ref Utf8JsonReader reader, JsonSerializerOptions options)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return dict;

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException("Expected PropertyName");

            var key = reader.GetString()!;
            reader.Read();
            dict[key] = ReadValue(ref reader, options);
        }

        throw new JsonException("Unexpected end of JSON object");
    }

    private static List<object?> ReadList(ref Utf8JsonReader reader, JsonSerializerOptions options)
    {
        var list = new List<object?>();
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                return list;
            list.Add(ReadValue(ref reader, options));
        }

        throw new JsonException("Unexpected end of JSON array");
    }

    public override void Write(
        Utf8JsonWriter writer,
        Dictionary<string, object?> value,
        JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        foreach (var (k, v) in value)
        {
            writer.WritePropertyName(k);
            WriteValue(writer, v);
        }
        writer.WriteEndObject();
    }

    /// <summary>
    /// Deserializes a UTF-8 JSON byte span to a <see cref="Dictionary{TKey,TValue}">Dictionary&lt;string, object?&gt;</see>.
    /// Top-level values are converted to primitives (string, long, double, bool, null).
    /// Nested objects and arrays are preserved as <see cref="JsonElement"/> values, matching the behaviour of
    /// <c>JsonSerializer.Deserialize&lt;Dictionary&lt;string, object?&gt;&gt;()</c> with default options.
    /// </summary>
    internal static Dictionary<string, object?> DeserializeObject(ReadOnlyMemory<byte> utf8Json)
    {
        using var doc = JsonDocument.Parse(utf8Json);
        return ParseTopLevel(doc.RootElement);
    }

    private static Dictionary<string, object?> ParseTopLevel(JsonElement root)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (root.ValueKind != JsonValueKind.Object)
            return dict;
        foreach (var prop in root.EnumerateObject())
        {
            dict[prop.Name] = ParseTopLevelValue(prop.Value);
        }
        return dict;
    }

    private static object? ParseTopLevelValue(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.Null or JsonValueKind.Undefined => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? (object?)l : el.GetDouble(),
            // Nested objects and arrays stay as JsonElement (matches default STJ behaviour)
            _ => el.Clone(),
        };
    }

    /// <summary>Deserializes a JSON string to a <see cref="Dictionary{TKey,TValue}">Dictionary&lt;string, object?&gt;</see>.</summary>
    internal static Dictionary<string, object?> DeserializeObject(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return ParseTopLevel(doc.RootElement);
    }

    /// <summary>
    /// Deeply converts a <see cref="JsonElement"/> to a <c>Dictionary&lt;string, object?&gt;</c>.
    /// Unlike <see cref="DeserializeObject(string)"/> which preserves nested elements as <see cref="JsonElement"/>,
    /// this method recursively converts nested objects to dictionaries and arrays to lists.
    /// </summary>
    internal static Dictionary<string, object?> DeserializeElementDeep(JsonElement element)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (element.ValueKind != JsonValueKind.Object)
            return dict;
        foreach (var prop in element.EnumerateObject())
        {
            dict[prop.Name] = ConvertElementDeep(prop.Value);
        }
        return dict;
    }

    private static object? ConvertElementDeep(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Object => DeserializeElementDeep(element),
            JsonValueKind.Array => element.EnumerateArray().Select(ConvertElementDeep).ToList(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var l) ? (object?)l : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    /// <summary>Serializes an arbitrary object (must be a known primitive type or nested dict/list) to a JSON string.</summary>
    internal static string SerializeValue(object? value)
    {
        using var ms     = new System.IO.MemoryStream();
        using var writer = new Utf8JsonWriter(ms);
        WriteValue(writer, value);
        writer.Flush();
        return System.Text.Encoding.UTF8.GetString(ms.ToArray());
    }

    /// <summary>Serializes a <see cref="Dictionary{TKey,TValue}">Dictionary&lt;string, object?&gt;</see> to a UTF-8 JSON byte array.</summary>
    internal static byte[] SerializeObject(Dictionary<string, object?> value)
    {
        using var ms     = new System.IO.MemoryStream();
        using var writer = new Utf8JsonWriter(ms);
        WriteObject(writer, value);
        writer.Flush();
        return ms.ToArray();
    }

    /// <summary>Writes a <see cref="Dictionary{TKey,TValue}">Dictionary&lt;string, object?&gt;</see> as a JSON object to the writer.</summary>
    internal static void WriteObject(Utf8JsonWriter writer, Dictionary<string, object?> value)
    {
        writer.WriteStartObject();
        foreach (var (k, v) in value)
        {
            writer.WritePropertyName(k);
            WriteValue(writer, v);
        }
        writer.WriteEndObject();
    }

    internal static void WriteValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case bool b:
                writer.WriteBooleanValue(b);
                break;
            case string s:
                writer.WriteStringValue(s);
                break;
            case int i:
                writer.WriteNumberValue(i);
                break;
            case long l:
                writer.WriteNumberValue(l);
                break;
            case double d:
                writer.WriteNumberValue(d);
                break;
            case float f:
                writer.WriteNumberValue(f);
                break;
            case ulong u:
                writer.WriteNumberValue(u);
                break;
            case uint ui:
                writer.WriteNumberValue(ui);
                break;
            case short s2:
                writer.WriteNumberValue(s2);
                break;
            case byte b2:
                writer.WriteNumberValue(b2);
                break;
            case Dictionary<string, object?> dict:
                writer.WriteStartObject();
                foreach (var (k, v) in dict)
                {
                    writer.WritePropertyName(k);
                    WriteValue(writer, v);
                }
                writer.WriteEndObject();
                break;
            case Dictionary<string, string> sdict:
                writer.WriteStartObject();
                foreach (var (k, v) in sdict)
                {
                    writer.WritePropertyName(k);
                    writer.WriteStringValue(v);
                }
                writer.WriteEndObject();
                break;
            case Dictionary<string, List<string>> slsdict:
                writer.WriteStartObject();
                foreach (var (k, v) in slsdict)
                {
                    writer.WritePropertyName(k);
                    writer.WriteStartArray();
                    foreach (var s in v) writer.WriteStringValue(s);
                    writer.WriteEndArray();
                }
                writer.WriteEndObject();
                break;
            case List<string> strs:
                writer.WriteStartArray();
                foreach (var s in strs) writer.WriteStringValue(s);
                writer.WriteEndArray();
                break;
            case List<object?> list:
                writer.WriteStartArray();
                foreach (var item in list)
                    WriteValue(writer, item);
                writer.WriteEndArray();
                break;
            // IEnumerable<object?> catches typed collections (e.g. List<Dictionary<string,object?>>)
            // that aren't matched by List<object?> above. Must precede IDictionary to avoid
            // treating non-generic dictionaries as sequences.
            case IEnumerable<object?> seq:
                writer.WriteStartArray();
                foreach (var item in seq)
                    WriteValue(writer, item);
                writer.WriteEndArray();
                break;
            case IDictionary idict:
                writer.WriteStartObject();
                foreach (DictionaryEntry entry in idict)
                {
                    writer.WritePropertyName(entry.Key.ToString()!);
                    WriteValue(writer, entry.Value);
                }
                writer.WriteEndObject();
                break;
            // Non-generic IEnumerable fallback (e.g. arrays of unknown element type).
            case IEnumerable seq:
                writer.WriteStartArray();
                foreach (var item in seq)
                    WriteValue(writer, item);
                writer.WriteEndArray();
                break;
            case JsonElement el:
                el.WriteTo(writer);
                break;
            case JsonNode node:
                node.WriteTo(writer);
                break;
            default:
                // Fallback: serialize as string for unknown primitive types
                writer.WriteStringValue(value.ToString());
                break;
        }
    }
}
