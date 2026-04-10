using Microsoft.AspNetCore.Mvc.Testing;

namespace MicroStack.Tests;

/// <summary>
/// Shared test fixture that starts the MicroStack server in-process using
/// WebApplicationFactory and provides an HttpClient pointed at it.
/// </summary>
public sealed class MicroStackFixture : IDisposable
{
    public WebApplicationFactory<Program> Factory { get; }
    public HttpClient HttpClient { get; }

    public MicroStackFixture()
    {
        Factory    = new WebApplicationFactory<Program>();
        HttpClient = Factory.CreateClient();
    }

    public void Dispose()
    {
        HttpClient.Dispose();
        Factory.Dispose();
    }
}
