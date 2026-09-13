using System.Text.RegularExpressions;

namespace MicroStack.Internal;

/// <summary>
/// Manages the per-request AWS account ID, scoped via AsyncLocal
/// (equivalent of Python's contextvars.ContextVar).
/// </summary>
internal static partial class AccountContext
{
    private static readonly AsyncLocal<string?> _accountId = new();

    private static string _defaultAccountId = "000000000000";

    /// <summary>
    /// Configures the default account ID from options. Called once at startup.
    /// </summary>
    internal static void Configure(MicroStackOptions options)
    {
        _defaultAccountId = options.DefaultAccountId;
    }

    [GeneratedRegex(@"^\d{12}$")]
    private static partial Regex TwelveDigitRegex();

    /// <summary>
    /// Set the per-request account ID from the AWS access key.
    /// If the access key is a 12-digit number it is used directly as the account ID;
    /// otherwise falls back to MICROSTACK_ACCOUNT_ID env var or "000000000000".
    /// </summary>
    internal static void SetFromAccessKey(string? accessKeyId)
    {
        if (!string.IsNullOrEmpty(accessKeyId) && TwelveDigitRegex().IsMatch(accessKeyId))
            _accountId.Value = accessKeyId;
        else
            _accountId.Value = _defaultAccountId;
    }

    /// <summary>Returns the account ID for the current request.</summary>
    internal static string GetAccountId() =>
        _accountId.Value ?? _defaultAccountId;

    internal static IDisposable BeginScope(string accountId)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        if (!TwelveDigitRegex().IsMatch(accountId))
            throw new ArgumentException("Account IDs must contain exactly 12 digits.", nameof(accountId));

        var previous = _accountId.Value;
        _accountId.Value = accountId;
        return new Scope(previous);
    }

    /// <summary>Resets to default (used in tests).</summary>
    internal static void Reset() => _accountId.Value = null;

    private sealed class Scope(string? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
                return;
            _accountId.Value = previous;
            _disposed = true;
        }
    }
}
