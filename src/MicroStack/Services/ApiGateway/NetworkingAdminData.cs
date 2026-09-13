using System.Collections;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.ApiGateway;

internal static class NetworkingAdminData
{
    internal static string Text(object? value) => value switch
    {
        null => "",
        bool b => b ? "true" : "false",
        string s => s,
        DateTime dt => AdminData.IsoUtc(dt),
        DateTimeOffset dto => AdminData.IsoUtc(dto),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    internal static IReadOnlyList<AdminField> Fields(
        IEnumerable<KeyValuePair<string, object?>> values,
        params string[] sensitiveNames)
    {
        var sensitive = new HashSet<string>(sensitiveNames, StringComparer.OrdinalIgnoreCase);
        return values
            .Where(kv => kv.Value is null or string or bool or byte or sbyte or short or ushort
                or int or uint or long or ulong or float or double or decimal or DateTime or DateTimeOffset)
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => AdminData.Field(kv.Key, Text(kv.Value), sensitive.Contains(kv.Key)))
            .ToArray();
    }

    internal static AdminContent Json(object? value, params string[] sensitiveNames)
    {
        var sensitive = new HashSet<string>(sensitiveNames, StringComparer.OrdinalIgnoreCase);
        return AdminData.Json(writer => Write(writer, value, sensitive, null));
    }

    private static void Write(
        Utf8JsonWriter writer, object? value, HashSet<string> sensitive, string? propertyName)
    {
        if (propertyName is not null && sensitive.Contains(propertyName))
        {
            writer.WriteStringValue(AdminData.MaskedValue);
            return;
        }

        switch (value)
        {
            case null: writer.WriteNullValue(); return;
            case string text: writer.WriteStringValue(text); return;
            case bool boolean: writer.WriteBooleanValue(boolean); return;
            case byte number: writer.WriteNumberValue(number); return;
            case sbyte number: writer.WriteNumberValue(number); return;
            case short number: writer.WriteNumberValue(number); return;
            case ushort number: writer.WriteNumberValue(number); return;
            case int number: writer.WriteNumberValue(number); return;
            case uint number: writer.WriteNumberValue(number); return;
            case long number: writer.WriteNumberValue(number); return;
            case ulong number: writer.WriteNumberValue(number); return;
            case float number: writer.WriteNumberValue(number); return;
            case double number: writer.WriteNumberValue(number); return;
            case decimal number: writer.WriteNumberValue(number); return;
            case DateTime date: writer.WriteStringValue(AdminData.IsoUtc(date)); return;
            case DateTimeOffset date: writer.WriteStringValue(AdminData.IsoUtc(date)); return;
            case IDictionary dictionary:
                writer.WriteStartObject();
                foreach (DictionaryEntry entry in dictionary)
                {
                    if (entry.Key is not string key)
                        continue;
                    writer.WritePropertyName(key);
                    Write(writer, entry.Value, sensitive, key);
                }
                writer.WriteEndObject();
                return;
            case IEnumerable enumerable:
                writer.WriteStartArray();
                foreach (var item in enumerable)
                    Write(writer, item, sensitive, null);
                writer.WriteEndArray();
                return;
            default:
                writer.WriteStringValue(Text(value));
                return;
        }
    }

    internal static string Get<T>(
        IReadOnlyDictionary<string, T> value, string key, string fallback = "") =>
        value.TryGetValue(key, out var item) ? Text(item) : fallback;

    internal static string Composite(params string[] values) =>
        string.Join("/", values.Select(Uri.EscapeDataString));

    internal static string SecretId(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)))[..16].ToLowerInvariant();
}
