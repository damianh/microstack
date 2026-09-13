using System.Collections;
using System.Globalization;
using MicroStack.Admin.Contracts;

namespace MicroStack.Internal.Admin;

internal static class AdminProjection
{
    internal static IReadOnlyDictionary<string, object?> Snapshot(
        IDictionary source, Func<string, bool>? redact = null, Func<string, bool>? omit = null)
    {
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (DictionaryEntry entry in source)
        {
            if (entry.Key is not string key || omit?.Invoke(key) == true)
                continue;
            result[key] = redact?.Invoke(key) == true
                ? AdminData.MaskedValue
                : SnapshotValue(entry.Value, redact, omit);
        }
        return result;
    }

    internal static AdminContent Content(IReadOnlyDictionary<string, object?> snapshot) =>
        AdminData.Json(snapshot);

    internal static IReadOnlyList<AdminField> Fields(
        IReadOnlyDictionary<string, object?> snapshot, params string[] names) =>
        names.Where(snapshot.ContainsKey)
            .Select(name => AdminData.Field(name, Scalar(snapshot[name])))
            .ToArray();

    internal static string? Scalar(object? value) => value switch
    {
        null => null,
        string text => text,
        bool boolean => boolean ? "true" : "false",
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => null,
    };

    private static object? SnapshotValue(object? value, Func<string, bool>? redact, Func<string, bool>? omit) =>
        value switch
        {
            null or string or bool or byte or sbyte or short or ushort or int or uint or long or ulong
                or float or double or decimal => value,
            DateTime date => AdminData.IsoUtc(date),
            DateTimeOffset date => AdminData.IsoUtc(date),
            IDictionary dictionary => Snapshot(dictionary, redact, omit),
            IEnumerable sequence => sequence.Cast<object?>()
                .Select(item => SnapshotValue(item, redact, omit)).ToArray(),
            _ => value.ToString(),
        };
}
