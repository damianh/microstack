using System.Net;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Hosting;

namespace MicroStack.Tests;

/// <summary>
/// Shared test fixture that starts MicroStack on an ephemeral loopback port.
/// </summary>
public sealed class MicroStackFixture : IDisposable
{
    public WebApplicationFactory<Program> Factory { get; }
    public HttpClient HttpClient { get; }
    public Uri ServerAddress { get; }

    public MicroStackFixture()
    {
        Factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
            builder.UseEnvironment("Testing"));
        Factory.UseKestrel(options => options.Listen(IPAddress.Loopback, 0));
        HttpClient = Factory.CreateClient();
        ServerAddress = HttpClient.BaseAddress
            ?? throw new InvalidOperationException("The test server returned no address.");
    }

    public HttpClient CreateClient(bool allowAutoRedirect = true) =>
        new(new SocketsHttpHandler
        {
            AllowAutoRedirect = allowAutoRedirect,
            UseProxy = false
        })
        {
            BaseAddress = ServerAddress
        };

    public HttpMessageHandler CreateHandler() => new LoopbackHttpHandler(ServerAddress);

    public void Dispose()
    {
        HttpClient.Dispose();
        Factory.Dispose();
    }

    private sealed class LoopbackHttpHandler(Uri serverAddress)
        : DelegatingHandler(new SocketsHttpHandler { UseProxy = false })
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var original = request.RequestUri
                ?? throw new InvalidOperationException("The test request has no URI.");
            request.Headers.Host ??= original.Authority;
            request.RequestUri = new UriBuilder(serverAddress)
            {
                Path = original.AbsolutePath,
                Query = original.Query.TrimStart('?')
            }.Uri;
            return base.SendAsync(request, cancellationToken);
        }
    }
}
