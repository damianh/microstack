using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using MicroStack.UI.Client;
using MicroStack.UI.Client.Services;
using System.Net.Http.Json;
using System.Text.Json.Serialization;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

var apiBaseUrl = builder.Configuration["ApiBaseUrl"];
if (string.IsNullOrWhiteSpace(apiBaseUrl))
{
    using var hostClient = new HttpClient { BaseAddress = new Uri(builder.HostEnvironment.BaseAddress) };
    var hostConfiguration = await hostClient.GetFromJsonAsync(
        "_microstack/ui-config", UiHostJsonContext.Default.UiHostConfiguration);
    apiBaseUrl = hostConfiguration?.ApiBaseUrl
        ?? throw new InvalidOperationException("The UI host returned no API connection settings.");
}
if (!Uri.TryCreate(apiBaseUrl, UriKind.Absolute, out var apiBaseUri)
    || apiBaseUri.Scheme is not ("http" or "https")
    || !string.IsNullOrEmpty(apiBaseUri.UserInfo))
{
    throw new InvalidOperationException("ApiBaseUrl must be an absolute HTTP(S) URL without credentials.");
}
builder.Services.AddScoped(_ => new HttpClient { BaseAddress = apiBaseUri });
builder.Services.AddScoped<MicroStackApiService>();
builder.Services.AddScoped<AdminApiClient>();

await builder.Build().RunAsync();

internal sealed record UiHostConfiguration(string ApiBaseUrl);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(UiHostConfiguration))]
internal partial class UiHostJsonContext : JsonSerializerContext;
