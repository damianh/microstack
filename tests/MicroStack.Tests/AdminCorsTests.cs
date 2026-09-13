using MicroStack.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace MicroStack.Tests;

public sealed class AdminCorsTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>
{
    private string UiOrigin
    {
        get
        {
            var options = fixture.Factory.Services.GetRequiredService<MicroStackOptions>();
            return new UriBuilder("http", options.Host, options.UiPort).Uri.GetLeftPart(UriPartial.Authority);
        }
    }

    [Theory]
    [InlineData("GET", "/_microstack/health")]
    [InlineData("GET", "/health")]
    [InlineData("GET", "/_localstack/health")]
    [InlineData("GET", "/_microstack/resources")]
    [InlineData("GET", "/_microstack/requests")]
    [InlineData("DELETE", "/_microstack/requests")]
    [InlineData("POST", "/_microstack/reset")]
    [InlineData("POST", "/_microstack/config")]
    [InlineData("GET", "/_microstack/admin/v1/context")]
    [InlineData("GET", "/_microstack/admin/v1/services")]
    public async Task AdminResponseAllowsUiOrigin(string method, string path)
    {
        using var request = new HttpRequestMessage(new HttpMethod(method), path);
        request.Headers.Add("Origin", UiOrigin);

        using var response = await fixture.HttpClient.SendAsync(request);

        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        response.Headers.GetValues("Access-Control-Allow-Origin").ShouldBe([UiOrigin]);
        response.Headers.Contains("Access-Control-Allow-Credentials").ShouldBeFalse();
    }

    [Theory]
    [InlineData("GET", "/_microstack/health")]
    [InlineData("GET", "/_microstack/resources")]
    [InlineData("GET", "/_microstack/requests")]
    [InlineData("DELETE", "/_microstack/requests")]
    [InlineData("POST", "/_microstack/reset")]
    [InlineData("POST", "/_microstack/config")]
    [InlineData("GET", "/_microstack/admin/v1/context")]
    [InlineData("GET", "/_microstack/admin/v1/services")]
    public async Task AdminPreflightAllowsUiMethodAndHeaders(string method, string path)
    {
        using var request = new HttpRequestMessage(HttpMethod.Options, path);
        request.Headers.Add("Origin", UiOrigin);
        request.Headers.Add("Access-Control-Request-Method", method);
        request.Headers.Add("Access-Control-Request-Headers", "content-type");

        using var response = await fixture.HttpClient.SendAsync(request);

        response.StatusCode.ShouldBe(HttpStatusCode.NoContent);
        response.Headers.GetValues("Access-Control-Allow-Origin").ShouldBe([UiOrigin]);
        string.Join(",", response.Headers.GetValues("Access-Control-Allow-Methods")).ShouldContain(method);
        string.Join(",", response.Headers.GetValues("Access-Control-Allow-Headers")).ShouldContain("content-type");
    }

    [Theory]
    [InlineData("http://untrusted.example")]
    [InlineData("http://localhost:9999")]
    [InlineData("null")]
    public async Task AdminRequestsDoNotAllowUnconfiguredOrigins(string origin)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, "/_microstack/health");
        request.Headers.Add("Origin", origin);
        using var response = await fixture.HttpClient.SendAsync(request);
        response.Headers.Contains("Access-Control-Allow-Origin").ShouldBeFalse();

        using var preflight = new HttpRequestMessage(HttpMethod.Options, "/_microstack/requests");
        preflight.Headers.Add("Origin", origin);
        preflight.Headers.Add("Access-Control-Request-Method", "DELETE");
        using var preflightResponse = await fixture.HttpClient.SendAsync(preflight);
        preflightResponse.StatusCode.ShouldBe(HttpStatusCode.NoContent);
        preflightResponse.Headers.Contains("Access-Control-Allow-Origin").ShouldBeFalse();
    }

    [Fact]
    public async Task AwsRequestsKeepExistingCorsBehavior()
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, "/");
        request.Headers.Add("Origin", "http://sdk-client.example");
        using var response = await fixture.HttpClient.SendAsync(request);
        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        response.Headers.GetValues("Access-Control-Allow-Origin").ShouldBe(["*"]);

        using var preflight = new HttpRequestMessage(HttpMethod.Options, "/");
        preflight.Headers.Add("Origin", "http://sdk-client.example");
        preflight.Headers.Add("Access-Control-Request-Method", "PUT");
        using var preflightResponse = await fixture.HttpClient.SendAsync(preflight);
        preflightResponse.StatusCode.ShouldBe(HttpStatusCode.NoContent);
        preflightResponse.Headers.GetValues("Access-Control-Allow-Origin").ShouldBe(["*"]);
    }
}
