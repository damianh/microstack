using System.Globalization;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using Microsoft.AspNetCore.Components;

namespace MicroStack.UI.Client.Services;

public static class ExplorerLocation
{
    public static bool ValidAccount(string? value) =>
        value is { Length: 12 } && value.All(c => c is >= '0' and <= '9');

    public static string Utc(DateTimeOffset value) =>
        value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.FFFFFFF'Z'", CultureInfo.InvariantCulture);

    public static string EncodePath(AdminKey[] path) =>
        JsonSerializer.Serialize(path, AdminJsonContext.Default.AdminKeyArray);

    public static AdminKey[] DecodePath(string? value)
    {
        if (string.IsNullOrEmpty(value)) return [];
        var keys = JsonSerializer.Deserialize(value, AdminJsonContext.Default.AdminKeyArray)
            ?? throw new FormatException("The resource path is invalid.");
        if (keys.Length > 32 || keys.Any(key => key is null || string.IsNullOrEmpty(key.Kind) || string.IsNullOrEmpty(key.Id)))
            throw new FormatException("The resource path is invalid.");
        return keys;
    }

    public static string DirectoryUrl(NavigationManager navigation, string account) =>
        navigation.BaseUri + "accounts/" + Uri.EscapeDataString(account) + "/services";

    public static string ServiceUrl(NavigationManager navigation, string serviceId, string account, AdminKey[]? path = null, string? returnTo = null) =>
        navigation.GetUriWithQueryParameters(DirectoryUrl(navigation, account) + "/" + Uri.EscapeDataString(serviceId),
            new Dictionary<string, object?> { ["path"] = path is { Length: > 0 } ? EncodePath(path) : null, ["returnTo"] = SafeReturn(returnTo) });

    public static string? RouteAccount(NavigationManager navigation)
    {
        var segments = navigation.ToBaseRelativePath(navigation.Uri).Split('?')[0].Split('/');
        return segments.Length >= 3 && segments[0] == "accounts" && segments[2] == "services" && ValidAccount(segments[1])
            ? segments[1] : null;
    }

    public static string? SafeReturn(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 16_384 || !value.StartsWith('/') || value.StartsWith("//") || value.Contains('\\'))
            return null;
        var route = value.Split('?')[0];
        var segments = route.Split('/');
        var accountRoute = segments.Length is 4 or 5 && segments[1] == "accounts" &&
            ValidAccount(segments[2]) && segments[3] == "services" &&
            (segments.Length == 4 || segments[4].Length > 0 && segments[4] is not "." and not ".." && !segments[4].Contains('%'));
        return route is "/" or "/resources" || route.StartsWith("/services/", StringComparison.Ordinal) || accountRoute ? value : null;
    }

    public static string? ResolveReturn(NavigationManager navigation, string? value) =>
        SafeReturn(value) is { } safe
            ? new Uri(new Uri(navigation.BaseUri), safe.TrimStart('/')).AbsoluteUri
            : null;
}
