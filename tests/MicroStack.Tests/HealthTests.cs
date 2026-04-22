namespace MicroStack.Tests;

public sealed class HealthTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>
{
    private readonly HttpClient _client = fixture.HttpClient;

    [Fact]
    public async Task HealthEndpointReturns200()
    {
        var response = await _client.GetAsync("/_microstack/health");
        response.StatusCode.ShouldBe(HttpStatusCode.OK);
    }

    [Fact]
    public async Task HealthEndpointReturnsEditionAndVersion()
    {
        var response = await _client.GetAsync("/_microstack/health");
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();

        body.GetProperty("edition").GetString().ShouldBe("light");
        body.GetProperty("version").GetString().ShouldBe("0.1.0");
    }

    [Fact]
    public async Task HealthEndpointReturnsServicesDict()
    {
        var response = await _client.GetAsync("/_microstack/health");
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();

        body.GetProperty("services").ValueKind.ShouldBe(JsonValueKind.Object);
    }

    [Fact]
    public async Task LocalStackHealthAliasReturns200()
    {
        var response = await _client.GetAsync("/_localstack/health");
        response.StatusCode.ShouldBe(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ShortHealthAliasReturns200()
    {
        var response = await _client.GetAsync("/health");
        response.StatusCode.ShouldBe(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ResetEndpointReturnsOk()
    {
        var response = await _client.PostAsync("/_microstack/reset", null);
        response.StatusCode.ShouldBe(HttpStatusCode.OK);

        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        body.GetProperty("reset").GetString().ShouldBe("ok");
    }

    [Fact]
    public async Task OptionsRequestReturnsCorsHeaders()
    {
        var request = new HttpRequestMessage(HttpMethod.Options, "/_microstack/health");
        var response = await _client.SendAsync(request);

        response.Headers.Contains("Access-Control-Allow-Origin").ShouldBe(true, "Expected Access-Control-Allow-Origin CORS header on OPTIONS response");
    }
}
