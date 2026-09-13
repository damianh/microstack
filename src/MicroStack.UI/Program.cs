var builder = WebApplication.CreateBuilder(args);

var port = ReadPort(Environment.GetEnvironmentVariable("MICROSTACK_UI_PORT"), 4567, "MICROSTACK_UI_PORT");
var gatewayPort = ReadPort(Environment.GetEnvironmentVariable("GATEWAY_PORT")
    ?? Environment.GetEnvironmentVariable("EDGE_PORT"), 4566, "GATEWAY_PORT");
var configuredApiUrl = Environment.GetEnvironmentVariable("MICROSTACK_API_URL")
    ?? builder.Configuration["ApiBaseUrl"];
if (configuredApiUrl is not null
    && (!Uri.TryCreate(configuredApiUrl, UriKind.Absolute, out var configuredUri)
        || configuredUri.Scheme is not ("http" or "https")
        || !string.IsNullOrEmpty(configuredUri.UserInfo)))
{
    throw new InvalidOperationException("MICROSTACK_API_URL / ApiBaseUrl must be an absolute HTTP(S) URL without credentials.");
}
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");

var app = builder.Build();

app.MapGet("/_microstack/ui-config", (HttpContext context) =>
{
    context.Response.Headers.CacheControl = "no-store";
    var apiBaseUrl = configuredApiUrl
        ?? new UriBuilder(context.Request.Scheme, context.Request.Host.Host, gatewayPort).Uri.AbsoluteUri;
    return Results.Ok(new { apiBaseUrl });
});

app.UseBlazorFrameworkFiles();
app.UseStaticFiles();
app.MapFallbackToFile("index.html");

app.Logger.LogInformation("MicroStack UI listening on http://0.0.0.0:{Port}", port);

app.Run();

static int ReadPort(string? value, int fallback, string name)
{
    if (value is null)
        return fallback;
    if (int.TryParse(value, out var port) && port is >= 1 and <= 65535)
        return port;
    throw new InvalidOperationException($"{name} must be a port between 1 and 65535.");
}
