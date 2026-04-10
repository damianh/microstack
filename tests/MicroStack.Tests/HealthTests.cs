using Microsoft.AspNetCore.Mvc.Testing;

namespace MicroStack.Tests;

public sealed class HealthTests : IClassFixture<MicroStackFixture>
{
    private readonly HttpClient _client;

    public HealthTests(MicroStackFixture fixture)
    {
        _client = fixture.HttpClient;
    }

    [Fact]
    public async Task HealthEndpointReturns200()
    {
        var response = await _client.GetAsync("/_ministack/health");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task HealthEndpointReturnsEditionAndVersion()
    {
        var response = await _client.GetAsync("/_ministack/health");
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();

        Assert.Equal("light",   body.GetProperty("edition").GetString());
        Assert.Equal("0.1.0",   body.GetProperty("version").GetString());
    }

    [Fact]
    public async Task HealthEndpointReturnsServicesDict()
    {
        var response = await _client.GetAsync("/_ministack/health");
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();

        Assert.Equal(JsonValueKind.Object, body.GetProperty("services").ValueKind);
    }

    [Fact]
    public async Task LocalStackHealthAliasReturns200()
    {
        var response = await _client.GetAsync("/_localstack/health");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task ShortHealthAliasReturns200()
    {
        var response = await _client.GetAsync("/health");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task ResetEndpointReturnsOk()
    {
        var response = await _client.PostAsync("/_ministack/reset", null);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("ok", body.GetProperty("reset").GetString());
    }

    [Fact]
    public async Task OptionsRequestReturnsCorsHeaders()
    {
        var request = new HttpRequestMessage(HttpMethod.Options, "/_ministack/health");
        var response = await _client.SendAsync(request);

        Assert.True(
            response.Headers.Contains("Access-Control-Allow-Origin"),
            "Expected Access-Control-Allow-Origin CORS header on OPTIONS response");
    }
}
