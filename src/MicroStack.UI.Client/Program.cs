using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using MicroStack.UI.Client;
using MicroStack.UI.Client.Services;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

var applicationBaseUri = new Uri(builder.HostEnvironment.BaseAddress);
var apiBaseUri = new Uri(applicationBaseUri.GetLeftPart(UriPartial.Authority) + "/");
builder.Services.AddScoped(_ => new HttpClient { BaseAddress = apiBaseUri });
builder.Services.AddScoped<MicroStackApiService>();
builder.Services.AddScoped<AdminApiClient>();
builder.Services.AddScoped<ExplorerAccountState>();

await builder.Build().RunAsync();
