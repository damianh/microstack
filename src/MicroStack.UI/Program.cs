var builder = WebApplication.CreateBuilder(args);

var portText = Environment.GetEnvironmentVariable("MICROSTACK_UI_PORT");
var port = int.TryParse(portText, out var parsedPort) ? parsedPort : 4567;
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");

var app = builder.Build();

app.UseBlazorFrameworkFiles();
app.UseStaticFiles();
app.MapFallbackToFile("index.html");

app.Logger.LogInformation("MicroStack UI listening on http://0.0.0.0:{Port}", port);

app.Run();
