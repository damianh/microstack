using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.UI.Client.Services;
using Xunit;

namespace MicroStack.UI.Tests;

public sealed class AdminCollectionTests
{
    private const string Account = "123456789012";
    private static readonly AdminKey[] Path = [new("buckets", "folder/bucket")];

    [Theory]
    [InlineData("resources")]
    [InlineData("children")]
    [InlineData("connections")]
    [InlineData("activity")]
    public async Task Collections_load_beyond_api_page_size_with_scope_preserved(string endpoint)
    {
        var requests = new List<Dictionary<string, string>>();
        using var handler = new Handler((request, _) =>
        {
            Assert.EndsWith("/" + endpoint, request.RequestUri!.AbsolutePath, StringComparison.Ordinal);
            var query = Query(request.RequestUri);
            requests.Add(query);
            var second = query.ContainsKey("cursor");
            var start = second ? 200 : 0;
            var count = second ? 5 : 200;
            var next = second ? null : "opaque/page+token?";
            HttpContent content = endpoint switch
            {
                "resources" or "children" => JsonContent.Create(new AdminPage<AdminResourceSummary>
                {
                    Items = Enumerable.Range(start, count).Select(i => new AdminResourceSummary(new("objects", i.ToString()), $"Object {i}")).ToArray(),
                    NextCursor = next
                }, AdminJsonContext.Default.AdminPageAdminResourceSummary),
                "connections" => JsonContent.Create(new AdminPage<AdminConnection>
                {
                    Items = Enumerable.Range(start, count).Select(i => new AdminConnection($"Connection {i}", "configured")).ToArray(),
                    NextCursor = next
                }, AdminJsonContext.Default.AdminPageAdminConnection),
                _ => JsonContent.Create(new AdminPage<AdminActivity>
                {
                    Items = Enumerable.Range(start, count).Select(_ => new AdminActivity("sqs", "SameAction", Account, DateTimeOffset.UnixEpoch, 200, 1)).ToArray(),
                    NextCursor = next
                }, AdminJsonContext.Default.AdminPageAdminActivity)
            };
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content });
        });
        var api = new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") });
        var count = endpoint switch
        {
            "resources" => (await api.ResourcesAsync("s3", Account, "objects", "sample", CancellationToken.None)).Items.Count,
            "children" => (await api.ChildrenAsync("s3", Account, Path, "objects", "sample", "folder/", CancellationToken.None)).Items.Count,
            "connections" => (await api.ConnectionsAsync("s3", Account, Path, CancellationToken.None)).Items.Count,
            _ => (await api.ActivityAsync("s3", Account, CancellationToken.None)).Items.Count
        };
        Assert.Equal(205, count);
        Assert.Equal(2, requests.Count);
        Assert.False(requests[0].ContainsKey("cursor"));
        Assert.Equal("opaque/page+token?", requests[1]["cursor"]);
        Assert.All(requests, query =>
        {
            Assert.Equal(Account, query["accountId"]);
            Assert.Equal("200", query["pageSize"]);
            if (endpoint is "resources" or "children")
            {
                Assert.Equal("objects", query["kind"]);
                Assert.Equal("sample", query["filter"]);
            }
            if (endpoint is "children" or "connections") Assert.Equal(Path, ExplorerLocation.DecodePath(query["path"]));
            if (endpoint == "children") Assert.Equal("folder/", query["prefix"]);
        });
    }

    [Fact]
    public async Task Moving_page_boundary_does_not_duplicate_resource_keys()
    {
        var calls = 0;
        using var handler = new Handler((_, _) => Task.FromResult(Page(
            ++calls == 1 ? ["a", "b"] : ["b", "c"], calls == 1 ? "next" : null)));
        var api = new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") });
        var result = await api.ResourcesAsync("s3", Account, "objects", null, CancellationToken.None);
        Assert.Equal(["a", "b", "c"], result.Items.Select(item => item.Key.Id));
        Assert.Equal(3, result.KnownTotal);
        Assert.Null(result.NextCursor);
    }

    [Fact]
    public async Task Later_page_failure_is_not_a_partial_success()
    {
        var calls = 0;
        using var handler = new Handler((_, _) => Task.FromResult(
            ++calls == 1 ? Page(["a"], "next") : new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        var api = new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") });
        await Assert.ThrowsAsync<AdminApiException>(() => api.ResourcesAsync("s3", Account, "objects", null, CancellationToken.None));
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task Repeated_cursor_is_reported_instead_of_looping()
    {
        var calls = 0;
        using var handler = new Handler((_, _) =>
        {
            calls++;
            return Task.FromResult(Page(["a"], "same"));
        });
        var api = new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") });
        await Assert.ThrowsAsync<JsonException>(() => api.ResourcesAsync("s3", Account, "objects", null, CancellationToken.None));
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task Cancellation_stops_loading_remaining_pages()
    {
        using var cancellation = new CancellationTokenSource();
        var calls = 0;
        using var handler = new Handler((_, _) =>
        {
            calls++;
            cancellation.Cancel();
            return Task.FromResult(Page(["a"], "next"));
        });
        var api = new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => api.ResourcesAsync("s3", Account, "objects", null, cancellation.Token));
        Assert.Equal(1, calls);
    }

    private static HttpResponseMessage Page(string[] keys, string? next) => new(HttpStatusCode.OK)
    {
        Content = JsonContent.Create(new AdminPage<AdminResourceSummary>
        {
            Items = keys.Select(key => new AdminResourceSummary(new("objects", key), key)).ToArray(),
            NextCursor = next
        }, AdminJsonContext.Default.AdminPageAdminResourceSummary)
    };

    private static Dictionary<string, string> Query(Uri uri) => uri.Query.TrimStart('?')
        .Split('&', StringSplitOptions.RemoveEmptyEntries).Select(part => part.Split('=', 2))
        .ToDictionary(part => part[0], part => Uri.UnescapeDataString(part[1]));

    private sealed class Handler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            send(request, cancellationToken);
    }
}
