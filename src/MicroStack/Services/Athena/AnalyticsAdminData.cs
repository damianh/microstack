using System.Globalization;
using System.Text;
using System.Text.Json;
using MicroStack.Admin.Contracts;

namespace MicroStack.Internal.Admin;

internal static class AnalyticsAdminData
{
    internal static string? String(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) ? Convert.ToString(item, CultureInfo.InvariantCulture) : null;

    internal static IReadOnlyDictionary<string, object?> Dict(
        IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) && item is IReadOnlyDictionary<string, object?> dictionary
            ? dictionary
            : new Dictionary<string, object?>();

    internal static IEnumerable<IReadOnlyDictionary<string, object?>> Dicts(object? value) =>
        value is IEnumerable<Dictionary<string, object?>> dictionaries
            ? dictionaries
            : value is IEnumerable<object?> objects
                ? objects.OfType<IReadOnlyDictionary<string, object?>>()
                : [];

    internal static string? Epoch(object? value)
    {
        if (value is null || !double.TryParse(
                Convert.ToString(value, CultureInfo.InvariantCulture),
                NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds))
            return null;
        return AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds((long)(seconds * 1000)));
    }

    internal static IReadOnlyDictionary<string, object?> Project(params (string Name, object? Value)[] values) =>
        values.Where(x => x.Value is not null)
            .ToDictionary(x => x.Name, x => Masked(x.Name) ? AdminData.MaskedValue : x.Value,
                StringComparer.Ordinal);

    internal static AdminContent Json(IReadOnlyDictionary<string, object?> value) =>
        AdminData.Json(writer => Write(writer, value));

    internal static AdminContent DecodedBase64(string data)
    {
        try
        {
            var bytes = Convert.FromBase64String(data);
            try
            {
                return AdminData.Text(bytes);
            }
            catch (DecoderFallbackException)
            {
                return AdminData.Binary(bytes.Length);
            }
        }
        catch (FormatException)
        {
            return AdminData.Text(data);
        }
    }

    internal static AdminConnection? S3Connection(string label, string? uri)
    {
        if (string.IsNullOrWhiteSpace(uri))
            return null;
        if (!Uri.TryCreate(uri, UriKind.Absolute, out var parsed) ||
            !string.Equals(parsed.Scheme, "s3", StringComparison.OrdinalIgnoreCase))
            return new(label, "configured-destination", ExternalUri: uri);
        var bucket = parsed.Host;
        return string.IsNullOrEmpty(bucket)
            ? null
            : new(label, "configured-destination", "s3",
                [new AdminKey("bucket", bucket)]);
    }

    private static bool Masked(string name) =>
        name.Contains("password", StringComparison.OrdinalIgnoreCase) ||
        name.Contains("secret", StringComparison.OrdinalIgnoreCase) ||
        name.Contains("credential", StringComparison.OrdinalIgnoreCase) ||
        name.Contains("privatekey", StringComparison.OrdinalIgnoreCase) ||
        name.Contains("accesskey", StringComparison.OrdinalIgnoreCase) ||
        name.Contains("apikey", StringComparison.OrdinalIgnoreCase) ||
        name.Contains("token", StringComparison.OrdinalIgnoreCase) ||
        name.Contains("authorization", StringComparison.OrdinalIgnoreCase);

    private static void Write(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case IReadOnlyDictionary<string, object?> dictionary:
                writer.WriteStartObject();
                foreach (var (key, item) in dictionary.OrderBy(x => x.Key, StringComparer.Ordinal))
                {
                    writer.WritePropertyName(key);
                    if (Masked(key))
                        writer.WriteStringValue(AdminData.MaskedValue);
                    else
                        Write(writer, item);
                }
                writer.WriteEndObject();
                break;
            case IEnumerable<object?> items when value is not string:
                writer.WriteStartArray();
                foreach (var item in items)
                    Write(writer, item);
                writer.WriteEndArray();
                break;
            case string text:
                writer.WriteStringValue(text);
                break;
            case bool boolean:
                writer.WriteBooleanValue(boolean);
                break;
            case byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal:
                writer.WriteRawValue(Convert.ToString(value, CultureInfo.InvariantCulture)!);
                break;
            default:
                writer.WriteStringValue(Convert.ToString(value, CultureInfo.InvariantCulture));
                break;
        }
    }
}
