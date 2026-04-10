namespace MicroStack.Internal;

/// <summary>
/// Time and date formatting utilities for AWS responses.
/// Port of ministack/core/responses.py time utilities.
/// </summary>
internal static class TimeHelpers
{
    /// <summary>Current UTC time in AWS ISO 8601 format: 2024-01-15T12:34:56.789Z</summary>
    internal static string NowIso() =>
        DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z";

    /// <summary>Current UTC time in RFC 7231 format for HTTP headers: Mon, 15 Jan 2024 12:34:56 GMT</summary>
    internal static string NowRfc7231() =>
        DateTime.UtcNow.ToString("ddd, dd MMM yyyy HH:mm:ss") + " GMT";

    /// <summary>Convert an ISO 8601 timestamp to RFC 7231 format.</summary>
    internal static string IsoToRfc7231(string isoStr)
    {
        if (DateTimeOffset.TryParse(isoStr, out var dt))
            return dt.UtcDateTime.ToString("ddd, dd MMM yyyy HH:mm:ss") + " GMT";
        return isoStr;
    }

    /// <summary>Current UTC time as Unix epoch seconds.</summary>
    internal static double NowEpoch() =>
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;

    /// <summary>Current UTC time as Unix epoch milliseconds (long).</summary>
    internal static long NowEpochMs() =>
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
