using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Serialization.Metadata;
using Bunit;
using MicroStack.Admin.Contracts;
using MicroStack.UI.Client.Components;
using MicroStack.UI.Client.Pages;
using MicroStack.UI.Client.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MicroStack.UI.Tests;

public sealed class AdminClientTests
{
    [Fact]
    public async Task Api_encodes_opaque_nested_keys_and_account_without_path_segments()
    {
        Uri? uri = null;
        using var handler = new Handler((request, _) =>
        {
            uri = request.RequestUri;
            return Task.FromResult(Json(new AdminResourceDetail(new(new("objects", "a/b"), "object")), AdminJsonContext.Default.AdminResourceDetail));
        });
        var api = new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") });
        AdminKey[] path = [new("buckets", "a"), new("objects", "a/b?#文")];
        await api.DetailAsync("s3", "123456789012", path, CancellationToken.None);
        Assert.NotNull(uri);
        Assert.Equal("/_microstack/admin/v1/services/s3/resource", uri.AbsolutePath);
        var query = uri.Query.TrimStart('?').Split('&').Select(part => part.Split('=', 2))
            .ToDictionary(part => part[0], part => Uri.UnescapeDataString(part[1]));
        Assert.Equal("123456789012", query["accountId"]);
        Assert.Equal(path, ExplorerLocation.DecodePath(query["path"]));
    }

    [Fact]
    public async Task Failed_api_read_is_not_an_empty_success()
    {
        using var handler = new Handler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        { Content = JsonContent.Create(new AdminError("unavailable", "Service unavailable."), AdminJsonContext.Default.AdminError) }));
        var api = new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") });
        var exception = await Assert.ThrowsAsync<AdminApiException>(() => api.ResourcesAsync("s3", "000000000000", "buckets", null, CancellationToken.None));
        Assert.Equal("unavailable", exception.Code);
    }

    [Fact]
    public void Directory_renders_every_live_catalog_entry_without_frontend_inventory()
    {
        using var context = new BunitContext();
        LiveTestServices.AddPaused(context);
        context.JSInterop.Mode = JSRuntimeMode.Loose;
        context.Services.AddScoped<ExplorerAccountState>();
        var services = Enumerable.Range(1, 40).Select(index =>
            new AdminService($"service-{index}", $"Live service {index}", $"Service {index}", "Live category", "s3", $"handler-{index}", "enabled", "account")
            { Kinds = [new("resources", "Resources")] }).ToArray();
        using var handler = new Handler((request, _) => Task.FromResult(request.RequestUri!.AbsolutePath.Split('/').Last() switch
        {
            "context" => Json(new AdminContext("000000000000", "eu-west-1"), AdminJsonContext.Default.AdminContext),
            "accounts" => Json(new[] { "000000000000" }, AdminJsonContext.Default.StringArray),
            _ => Json(services, AdminJsonContext.Default.AdminServiceArray)
        }));
        context.Services.AddSingleton(new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") }));
        var view = context.Render<MicroStack.UI.Client.App>();
        view.WaitForAssertion(() => Assert.Equal(40, view.FindAll(".directory-service").Count));
        Assert.DoesNotContain("eu-west-1", view.Markup);
        Assert.Contains("Live category", view.Markup);
        Assert.DoesNotContain("SQS", view.Markup);
        Assert.Equal("http://localhost/accounts/000000000000/services",
            context.Services.GetRequiredService<NavigationManager>().Uri);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Overview_reads_instance_region_and_reports_context_failure(bool failContext)
    {
        using var context = new BunitContext();
        LiveTestServices.AddPaused(context);
        using var handler = new Handler((request, _) => Task.FromResult(request.RequestUri!.AbsolutePath switch
        {
            "/_microstack/health" => new HttpResponseMessage(HttpStatusCode.OK)
            { Content = new StringContent("""{"services":{},"edition":"test","version":"1"}""") },
            "/_microstack/resources" => new HttpResponseMessage(HttpStatusCode.OK)
            { Content = new StringContent("[]") },
            "/_microstack/admin/v1/context" when failContext => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable),
            "/_microstack/admin/v1/context" => Json(new AdminContext("000000000000", "eu-west-1"), AdminJsonContext.Default.AdminContext),
            _ => throw new InvalidOperationException(request.RequestUri.AbsolutePath)
        }));
        using var http = new HttpClient(handler) { BaseAddress = new("http://localhost:4566") };
        context.Services.AddSingleton(new MicroStackApiService(http));
        context.Services.AddSingleton(new AdminApiClient(http));
        var view = context.Render<Overview>();
        if (failContext)
        {
            view.WaitForAssertion(() => Assert.Contains("Instance status could not be loaded", view.Find("[role=alert]").TextContent));
            Assert.Empty(view.FindAll(".overview-facts"));
        }
        else
        {
            view.WaitForAssertion(() => Assert.Contains("eu-west-1", view.Find(".overview-facts").TextContent));
            Assert.Contains(view.FindAll(".overview-facts dt"), label => label.TextContent == "Region");
            Assert.Empty(view.FindAll("#account-id"));
        }
    }

    [Fact]
    public async Task Reveal_requires_a_click_and_hides_when_account_changes()
    {
        using var context = new BunitContext();
        var calls = 0;
        using var handler = new Handler((request, _) =>
        {
            calls++;
            Assert.Equal(HttpMethod.Post, request.Method);
            return Task.FromResult(Json(new AdminContent("text", "text/plain", Text: "revealed-value", Sensitive: true), AdminJsonContext.Default.AdminContent));
        });
        context.Services.AddSingleton(new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") }));
        var view = context.Render<FieldList>(parameters => parameters
            .Add(component => component.Fields, [new AdminField("Password", null, true, true)])
            .Add(component => component.Service, "rds")
            .Add(component => component.Account, "000000000000")
            .Add(component => component.Path, [new("databases", "db")]));
        Assert.Equal(0, calls);
        await view.Find("button").ClickAsync(new MouseEventArgs());
        Assert.Equal(1, calls);
        Assert.Contains("revealed-value", view.Markup);
        view.Render(parameters => parameters.Add(component => component.Account, "111111111111"));
        Assert.DoesNotContain("revealed-value", view.Markup);
        Assert.Equal(1, calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Live_revision_clears_reveal_and_cancels_late_result_without_fetching_again(bool pending)
    {
        using var context = new BunitContext();
        context.JSInterop.Mode = JSRuntimeMode.Loose;
        var response = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        using var handler = new Handler((_, _) =>
        {
            calls++;
            return response.Task;
        });
        context.Services.AddSingleton(new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") }));
        var view = context.Render<FieldList>(parameters => parameters
            .Add(component => component.Fields, [new AdminField("Password", null, true, true)])
            .Add(component => component.Service, "rds")
            .Add(component => component.Account, "000000000000")
            .Add(component => component.Path, [new("databases", "db")]));
        var click = view.Find("button").ClickAsync(new MouseEventArgs());
        if (!pending)
        {
            response.SetResult(Json(new AdminContent("text", "text/plain", Text: "private-value", Sensitive: true), AdminJsonContext.Default.AdminContent));
            await click;
            Assert.Contains("private-value", view.Markup);
        }
        view.Render(parameters => parameters.Add(component => component.Revision, 1));
        if (pending)
        {
            response.SetResult(Json(new AdminContent("text", "text/plain", Text: "private-value", Sensitive: true), AdminJsonContext.Default.AdminContent));
            await click;
        }
        Assert.DoesNotContain("private-value", view.Markup);
        Assert.Equal(1, calls);
        Assert.Equal("Reveal Password", view.Find("button").TextContent);
    }

    [Fact]
    public void Deep_link_loads_selected_nested_content_in_requested_account()
    {
        using var context = new BunitContext();
        LiveTestServices.AddPaused(context);
        context.JSInterop.Mode = JSRuntimeMode.Loose;
        context.Services.AddScoped<ExplorerAccountState>();
        var account = "111111111111";
        var bucket = new AdminResourceSummary(new("buckets", "a"), "Bucket A");
        var child = new AdminResourceSummary(new("objects", "folder/a.json"), "folder/a.json");
        var observedAccounts = new List<string>();
        var service = new AdminService("s3", "Simple Storage Service", "S3", "Storage", "s3", "s3", "enabled", "account")
        { Kinds = [new("buckets", "Buckets")] };
        using var handler = new Handler((request, _) =>
        {
            var uri = request.RequestUri!;
            var query = uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries).Select(part => part.Split('=', 2))
                .ToDictionary(part => part[0], part => Uri.UnescapeDataString(part[1]));
            if (query.TryGetValue("accountId", out var requestedAccount)) observedAccounts.Add(requestedAccount);
            var endpoint = uri.AbsolutePath.Split('/').Last();
            return Task.FromResult(endpoint switch
            {
                "context" => Json(new AdminContext("000000000000", "eu-west-1"), AdminJsonContext.Default.AdminContext),
                "accounts" => Json(new[] { "000000000000", account }, AdminJsonContext.Default.StringArray),
                "services" => Json(new[] { service }, AdminJsonContext.Default.AdminServiceArray),
                "resources" => Json(new AdminPage<AdminResourceSummary> { Items = [bucket] }, AdminJsonContext.Default.AdminPageAdminResourceSummary),
                "children" => Json(new AdminPage<AdminResourceSummary> { Items = [child] }, AdminJsonContext.Default.AdminPageAdminResourceSummary),
                "resource" => Json(ExplorerLocation.DecodePath(query["path"]).Length == 1
                    ? new AdminResourceDetail(bucket) { HasChildren = true }
                    : new AdminResourceDetail(child) { HasContent = true }, AdminJsonContext.Default.AdminResourceDetail),
                "content" => Json(new AdminContent("json", "application/json", Text: """{"live":true}"""), AdminJsonContext.Default.AdminContent),
                _ => throw new InvalidOperationException(endpoint)
            });
        });
        context.Services.AddSingleton(new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") }));
        var navigation = context.Services.GetRequiredService<NavigationManager>();
        navigation.NavigateTo($"/services/s3?account={account}&path={Uri.EscapeDataString(ExplorerLocation.EncodePath([bucket.Key]))}&item={Uri.EscapeDataString(ExplorerLocation.EncodePath([child.Key]))}");
        var view = context.Render<MicroStack.UI.Client.App>();
        view.WaitForAssertion(() => Assert.Contains("\"live\": true", view.Find("pre").TextContent));
        Assert.NotEmpty(observedAccounts);
        Assert.All(observedAccounts, actual => Assert.Equal(account, actual));
        Assert.Contains("folder/a.json", view.Markup);
        Assert.StartsWith("http://localhost/accounts/111111111111/services/s3?", navigation.Uri, StringComparison.Ordinal);
        Assert.DoesNotContain("account=", navigation.Uri, StringComparison.Ordinal);
        Assert.Contains("item=", navigation.Uri, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Late_reveal_cannot_cross_account_scope()
    {
        using var context = new BunitContext();
        var completion = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken readToken = default;
        using var handler = new Handler((_, token) => { readToken = token; return completion.Task; });
        context.Services.AddSingleton(new AdminApiClient(new HttpClient(handler) { BaseAddress = new("http://localhost:4566") }));
        var view = context.Render<FieldList>(parameters => parameters
            .Add(component => component.Fields, [new AdminField("Password", null, true, true)])
            .Add(component => component.Service, "rds")
            .Add(component => component.Account, "000000000000")
            .Add(component => component.Path, [new("databases", "db")]));
        var click = view.Find("button").ClickAsync(new MouseEventArgs());
        view.WaitForAssertion(() => Assert.True(readToken.CanBeCanceled));
        view.Render(parameters => parameters.Add(component => component.Account, "111111111111"));
        Assert.True(readToken.IsCancellationRequested);
        completion.SetResult(Json(new AdminContent("text", "text/plain", Text: "previous-account-secret", Sensitive: true), AdminJsonContext.Default.AdminContent));
        await click;
        Assert.DoesNotContain("previous-account-secret", view.Markup);
    }

    private static HttpResponseMessage Json<T>(T value, JsonTypeInfo<T> type) => new(HttpStatusCode.OK) { Content = JsonContent.Create(value, type) };
    private sealed class Handler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => send(request, cancellationToken);
    }
}
