using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Serialization.Metadata;
using Bunit;
using MicroStack.Admin.Contracts;
using MicroStack.UI.Client.Components;
using MicroStack.UI.Client.Pages;
using MicroStack.UI.Client.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using Xunit;

namespace MicroStack.UI.Tests;

public sealed class LiveGlobalTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Mutation_success_immediately_clears_rows_and_cancels_reads_started_during_mutation(bool reset)
    {
        await using var context = new BunitContext();
        var mutationEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseMutation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var readEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var mutated = false;
        CancellationToken staleToken = default;
        using var handler = new Handler(async (request, token) =>
        {
            if (request.Method == HttpMethod.Post || request.Method == HttpMethod.Delete)
            {
                mutationEntered.TrySetResult();
                await releaseMutation.Task;
                mutated = true;
                return new(HttpStatusCode.NoContent);
            }
            var old = !mutated;
            var response = reset
                ? OverviewResponse(request, old ? 3 : 0)
                : Requests(old ? [Entry("Obsolete request", "111111111111")] : []);
            var displayedEndpoint = reset ? "/_microstack/resources" : "/_microstack/requests";
            if (mutationEntered.Task.IsCompleted && old && request.RequestUri!.AbsolutePath == displayedEndpoint)
            {
                staleToken = token;
                readEntered.TrySetResult();
                await releaseRead.Task;
            }
            return response;
        });
        RegisterApis(context, handler);
        var live = new LiveUpdateCoordinator(new Browser());
        context.Services.AddSingleton(live);
        await live.PauseAsync();
        IRenderedComponent<Microsoft.AspNetCore.Components.IComponent> view = reset
            ? context.Render<Overview>()
            : context.Render<RequestActivity>(parameters => parameters.Add(component => component.Global, true));
        var obsolete = reset ? "3 resources" : "Obsolete request";
        view.WaitForAssertion(() => Assert.Contains(obsolete, view.Markup));
        await Button(view, reset ? "Reset all state" : "Clear global log").ClickAsync(new());
        var mutation = Button(view, reset ? "Confirm reset all state" : "Confirm clear global log").ClickAsync(new());
        await mutationEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var refresh = live.RefreshOnceAsync();
        await readEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        releaseMutation.TrySetResult();
        view.WaitForAssertion(() => Assert.DoesNotContain(obsolete, view.Markup));
        Assert.True(staleToken.IsCancellationRequested);
        Assert.False(mutation.IsCompleted);
        releaseRead.TrySetResult();
        await Task.WhenAll(mutation, refresh).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.DoesNotContain(obsolete, view.Markup);
        Assert.True(live.Paused);
    }

    [Fact]
    public async Task Pausing_during_initial_navigation_read_does_not_cancel_required_initial_data()
    {
        await using var context = new BunitContext();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken initialToken = default;
        using var handler = new Handler(async (_, token) =>
        {
            initialToken = token;
            entered.TrySetResult();
            await release.Task;
            return Requests([Entry("Initial navigation", "111111111111")]);
        });
        RegisterApis(context, handler);
        var live = new LiveUpdateCoordinator(new Browser());
        context.Services.AddSingleton(live);
        var view = context.Render<RequestActivity>(parameters => parameters.Add(component => component.Global, true));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await live.PauseAsync();
        Assert.False(initialToken.IsCancellationRequested);
        release.TrySetResult();
        view.WaitForAssertion(() => Assert.Contains("Initial navigation", view.Markup));
        Assert.True(live.Paused);
    }

    [Fact]
    public async Task Paused_clear_removes_obsolete_rows_even_when_followup_read_fails()
    {
        await using var context = new BunitContext();
        var cleared = false;
        using var handler = new Handler((request, _) =>
        {
            if (request.Method == HttpMethod.Delete)
            {
                cleared = true;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent));
            }
            return Task.FromResult(cleared
                ? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                : Requests([Entry("Obsolete request", "111111111111")]));
        });
        RegisterApis(context, handler);
        var live = new LiveUpdateCoordinator(new Browser());
        context.Services.AddSingleton(live);
        await live.PauseAsync();
        var view = context.Render<RequestActivity>(parameters => parameters.Add(component => component.Global, true));
        view.WaitForAssertion(() => Assert.Contains("Obsolete request", view.Markup));
        await Button(view, "Clear global log").ClickAsync(new());
        await Button(view, "Confirm clear global log").ClickAsync(new());
        Assert.DoesNotContain("Obsolete request", view.Markup);
        Assert.Contains("Request metadata could not be refreshed", view.Find("[role=alert]").TextContent);
        Assert.True(live.Paused);
        Assert.NotNull(live.Error);
    }

    [Fact]
    public async Task Shared_status_distinguishes_connection_and_snapshot_failure_without_announcing_timestamps()
    {
        await using var context = new BunitContext();
        var browser = new Browser();
        var live = new LiveUpdateCoordinator(browser);
        context.Services.AddSingleton(live);
        var controls = context.Render<LiveControls>();
        Assert.Equal("Connecting", controls.Find("[role=status]").TextContent);
        var fail = false;
        await using var registration = await live.RegisterAsync(_ =>
            fail ? Task.FromException(new HttpRequestException("Unavailable")) : Task.CompletedTask);
        await live.PendingRefresh;
        await live.OnConnectionChanged(browser.Generation, true);
        controls.WaitForAssertion(() => Assert.Equal("Live", controls.Find("[role=status]").TextContent));
        await live.PendingRefresh;
        var lastSuccess = live.LastSuccess;
        await live.OnConnectionChanged(browser.Generation, false);
        controls.WaitForAssertion(() => Assert.Equal("Reconnecting", controls.Find("[role=status]").TextContent));
        Assert.Equal(lastSuccess, live.LastSuccess);
        fail = true;
        await controls.InvokeAsync(live.RefreshOnceAsync);
        controls.WaitForAssertion(() => Assert.Equal("Stale", controls.Find("[role=status]").TextContent));
        Assert.Equal(lastSuccess, live.LastSuccess);
        Assert.Null(controls.Find("time").Closest("[aria-live]"));
        Assert.Equal("true", controls.Find("button[aria-label='Live updates']").GetAttribute("aria-pressed"));
        await controls.Find("button[aria-label='Live updates']").ClickAsync(new());
        Assert.Equal("Paused", controls.Find("[role=status]").TextContent);
        Assert.Equal("false", controls.Find("button[aria-label='Live updates']").GetAttribute("aria-pressed"));
        Assert.Contains("Resume live updates", controls.Find("button[aria-label='Live updates']").GetAttribute("title"));
        Assert.NotNull(Button(controls, "Retry"));
    }

    [Fact]
    public async Task Disposing_activity_cancels_pending_read_and_unregisters_it()
    {
        await using var context = new BunitContext();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reads = 0;
        CancellationToken pendingToken = default;
        using var handler = new Handler(async (_, token) =>
        {
            if (++reads > 1)
            {
                pendingToken = token;
                entered.TrySetResult();
                await release.Task;
            }
            return Requests([Entry("SendMessage", "111111111111")]);
        });
        RegisterApis(context, handler);
        var live = new LiveUpdateCoordinator(new Browser());
        context.Services.AddSingleton(live);
        await live.PauseAsync();
        var view = context.Render<RequestActivity>(parameters => parameters.Add(component => component.Global, true));
        view.WaitForAssertion(() => Assert.Contains("SendMessage", view.Markup));
        var refresh = live.RefreshOnceAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await view.InvokeAsync(() => view.Instance.DisposeAsync().AsTask());
        Assert.True(pendingToken.IsCancellationRequested);
        release.TrySetResult();
        await refresh.WaitAsync(TimeSpan.FromSeconds(5));
        await live.RefreshOnceAsync();
        Assert.Equal(2, reads);
    }

    [Fact]
    public async Task Paused_initial_load_manual_refresh_and_retry_use_shared_controls()
    {
        await using var context = new BunitContext();
        var fail = false;
        var action = "CreateQueue";
        var reads = 0;
        using var handler = new Handler((_, _) =>
        {
            reads++;
            return Task.FromResult(fail ? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) : Requests([Entry(action, "111111111111")]));
        });
        RegisterApis(context, handler);
        var live = new LiveUpdateCoordinator(new Browser());
        context.Services.AddSingleton(live);
        await live.PauseAsync();
        var activity = context.Render<RequestActivity>(parameters => parameters.Add(component => component.Global, true));
        var controls = context.Render<LiveControls>();
        activity.WaitForAssertion(() => Assert.Contains("CreateQueue", activity.Markup));
        Assert.Equal(1, reads);
        Assert.Equal("Paused", controls.Find("[role=status]").TextContent);
        action = "SendMessage";
        await controls.Find("[aria-label=Refresh]").ClickAsync(new());
        Assert.Contains("SendMessage", activity.Markup);
        Assert.True(live.Paused);
        Assert.NotNull(live.LastSuccess);
        Assert.Single(controls.FindAll("time"));
        Assert.Null(controls.Find("time").Closest("[aria-live]"));
        Assert.Contains(ExplorerLocation.Utc(live.LastSuccess.Value), controls.Find("[aria-label=Refresh]").GetAttribute("title"));
        Assert.Equal("sr-only", controls.Find("#live-refresh-time").GetAttribute("class"));
        Assert.All(controls.FindAll(".icon-button"), button => Assert.Equal("", button.TextContent.Trim()));

        fail = true;
        await controls.Find("[aria-label=Refresh]").ClickAsync(new());
        Assert.NotNull(live.Error);
        Assert.Contains("SendMessage", activity.Markup);
        Assert.Contains("last successful snapshot", activity.Find("[role=alert]").TextContent);
        fail = false;
        await Button(controls, "Retry").ClickAsync(new());
        Assert.Null(live.Error);
        Assert.True(live.Paused);
        Assert.DoesNotContain(controls.FindAll("button"), button => button.TextContent == "Retry");
    }

    [Fact]
    public async Task Paused_overview_refresh_keeps_confirmation_and_reset_updates_local_state()
    {
        await using var context = new BunitContext();
        var count = 3;
        var contexts = 0;
        using var handler = new Handler((request, _) =>
        {
            if (request.RequestUri!.AbsolutePath == "/_microstack/reset")
            {
                count = 0;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent));
            }
            if (request.RequestUri.AbsolutePath.EndsWith("/context")) contexts++;
            return Task.FromResult(OverviewResponse(request, count));
        });
        RegisterApis(context, handler);
        var live = new LiveUpdateCoordinator(new Browser());
        context.Services.AddSingleton(live);
        await live.PauseAsync();
        var propagatedRefreshes = 0;
        await using var observer = await live.RegisterAsync(_ =>
        {
            propagatedRefreshes++;
            return Task.CompletedTask;
        });
        var view = context.Render<Overview>();
        view.WaitForAssertion(() => Assert.Contains("3 resources", view.Markup));
        await Button(view, "Reset all state").ClickAsync(new());
        count = 5;
        await view.InvokeAsync(live.RefreshOnceAsync);
        Assert.Contains("5 resources", view.Markup);
        Assert.Equal(1, contexts);
        Assert.Contains("Confirm reset all state", view.Find("[role=alert]").TextContent);
        await Button(view, "Confirm reset all state").ClickAsync(new());
        Assert.Contains("0 resources", view.Markup);
        Assert.True(live.Paused);
        Assert.Equal(2, contexts);
        Assert.Equal(2, propagatedRefreshes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Pause_or_hide_invalidates_late_background_log_completion(bool hidden)
    {
        await using var context = new BunitContext();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reads = 0;
        CancellationToken backgroundToken = default;
        using var handler = new Handler(async (_, token) =>
        {
            if (Interlocked.Increment(ref reads) == 1) return Requests([Entry("Original", "111111111111")]);
            backgroundToken = token;
            entered.TrySetResult();
            await release.Task;
            return Requests([Entry("Too late", "111111111111")]);
        });
        RegisterApis(context, handler);
        var live = new LiveUpdateCoordinator(new Browser());
        context.Services.AddSingleton(live);
        await live.PauseAsync();
        var view = context.Render<RequestActivity>(parameters => parameters.Add(component => component.Global, true));
        view.WaitForAssertion(() => Assert.Contains("Original", view.Markup));
        await live.ResumeAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        if (hidden) await live.OnVisibilityChanged(false);
        else await live.PauseAsync();
        Assert.True(backgroundToken.IsCancellationRequested);
        release.TrySetResult();
        await live.PendingRefresh.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Contains("Original", view.Markup);
        Assert.DoesNotContain("Too late", view.Markup);
    }

    [Fact]
    public async Task Changing_scoped_account_discards_late_previous_account_rows()
    {
        await using var context = new BunitContext();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken previousToken = default;
        using var handler = new Handler(async (request, token) =>
        {
            var old = request.RequestUri!.Query.Contains("111111111111");
            if (old)
            {
                previousToken = token;
                entered.TrySetResult();
                await release.Task;
            }
            return Json(new AdminPage<AdminActivity>
            {
                Items = [new("sqs", old ? "Old account" : "Current account", old ? "111111111111" : "222222222222", DateTimeOffset.UtcNow, 200, 1)],
                CapturedAt = DateTimeOffset.UtcNow
            }, AdminJsonContext.Default.AdminPageAdminActivity);
        });
        RegisterApis(context, handler);
        var live = new LiveUpdateCoordinator(new Browser());
        context.Services.AddSingleton(live);
        await live.PauseAsync();
        var view = context.Render<RequestActivity>(parameters => parameters
            .Add(component => component.Service, "sqs").Add(component => component.Account, "111111111111"));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        view.Render(parameters => parameters.Add(component => component.Account, "222222222222"));
        view.WaitForAssertion(() => Assert.Contains("Current account", view.Markup));
        Assert.True(previousToken.IsCancellationRequested);
        release.TrySetResult();
        await view.InvokeAsync(() => Task.CompletedTask);
        Assert.DoesNotContain("Old account", view.Markup);
        Assert.DoesNotContain("111111111111", view.Markup);
    }

    [Fact]
    public async Task Global_log_preserves_rows_and_confirmation_without_local_live_controls()
    {
        using var context = new BunitContext();
        var live = LiveTestServices.AddPaused(context);
        var reads = new List<Uri>();
        var deleted = false;
        using var handler = new Handler((request, _) =>
        {
            Assert.Equal("/_microstack/requests", request.RequestUri!.AbsolutePath);
            if (request.Method == HttpMethod.Delete)
            {
                deleted = true;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent));
            }
            reads.Add(request.RequestUri);
            return Task.FromResult(Requests(deleted ? [] : [Entry("CreateQueue", "111111111111")]));
        });
        RegisterApis(context, handler);
        var view = context.Render<RequestActivity>(parameters => parameters
            .Add(component => component.Global, true)
            .Add(component => component.Account, "222222222222"));
        view.WaitForAssertion(() => Assert.Contains("111111111111", view.Markup));
        Assert.DoesNotContain("accountId", reads[0].Query);
        Assert.DoesNotContain(view.FindAll("button"), button => button.TextContent is "Refresh" or "Pause" or "Resume");
        Assert.Empty(view.FindAll("select"));
        Assert.Contains("limit=1000", reads[^1].Query);
        await Button(view, "Clear global log").ClickAsync(new());
        Assert.False(deleted);
        await view.InvokeAsync(live.RefreshOnceAsync);
        Assert.Contains("Clear recorded requests for every account", view.Find("[role=alert]").TextContent);
        await Button(view, "Confirm clear global log").ClickAsync(new());
        Assert.True(deleted);
        Assert.Contains("No recorded requests", view.Markup);
        Assert.Empty(view.FindAll("[role=alert]"));
    }

    [Fact]
    public async Task Failed_log_refresh_retains_the_last_successful_rows()
    {
        using var context = new BunitContext();
        var live = LiveTestServices.AddPaused(context);
        var fail = false;
        using var handler = new Handler((_, _) => Task.FromResult(fail
            ? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
            : Requests([Entry("SendMessage", "111111111111")])));
        RegisterApis(context, handler);
        var view = context.Render<RequestActivity>(parameters => parameters.Add(component => component.Global, true));
        view.WaitForAssertion(() => Assert.Contains("SendMessage", view.Markup));
        fail = true;
        await view.InvokeAsync(live.RefreshOnceAsync);
        Assert.Contains("SendMessage", view.Markup);
        Assert.Contains("last successful snapshot", view.Find("[role=alert]").TextContent);
    }

    [Fact]
    public void Embedded_activity_keeps_service_and_account_scope_and_no_mutation_controls()
    {
        using var context = new BunitContext();
        LiveTestServices.AddPaused(context);
        using var handler = new Handler((request, _) =>
        {
            Assert.Equal("/_microstack/admin/v1/services/sqs/activity", request.RequestUri!.AbsolutePath);
            Assert.Contains("accountId=111111111111", request.RequestUri.Query);
            return Task.FromResult(Json(new AdminPage<AdminActivity>
            {
                Items = [new("sqs", "SendMessage", "111111111111", DateTimeOffset.UtcNow, 200, 1)],
                CapturedAt = DateTimeOffset.UtcNow
            }, AdminJsonContext.Default.AdminPageAdminActivity));
        });
        RegisterApis(context, handler);
        var view = context.Render<RequestActivity>(parameters => parameters
            .Add(component => component.Service, "sqs").Add(component => component.Account, "111111111111"));
        view.WaitForAssertion(() => Assert.Contains("SendMessage", view.Markup));
        Assert.Empty(view.FindAll("button,select"));
        Assert.Contains("not a resource-specific delivery trace", view.Markup);
    }

    [Fact]
    public async Task Overview_preserves_global_reset_confirmation_and_labels_legacy_default_scope()
    {
        using var context = new BunitContext();
        LiveTestServices.AddPaused(context);
        var reset = false;
        using var handler = new Handler((request, _) =>
        {
            Assert.DoesNotContain("accountId", request.RequestUri!.Query);
            if (request.RequestUri.AbsolutePath == "/_microstack/reset")
            {
                reset = true;
                Assert.Equal(HttpMethod.Post, request.Method);
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent));
            }
            return Task.FromResult(OverviewResponse(request, reset ? 0 : 3));
        });
        RegisterApis(context, handler);
        var view = context.Render<Overview>();
        view.WaitForAssertion(() => Assert.Contains("3 resources", view.Markup));
        Assert.Contains("Legacy resource summary (default account)", view.Markup);
        Assert.Contains("configured default account", view.Markup);
        Assert.Empty(view.FindAll("select"));
        Assert.DoesNotContain(view.FindAll("button"), button => button.TextContent == "Refresh");
        await Button(view, "Reset all state").ClickAsync(new());
        Assert.False(reset);
        await Button(view, "Confirm reset all state").ClickAsync(new());
        Assert.True(reset);
        Assert.Contains("0 resources", view.Markup);
        Assert.Contains("All in-memory state was reset", view.Markup);
    }

    private static AngleSharp.Dom.IElement Button<T>(IRenderedComponent<T> view, string text) where T : Microsoft.AspNetCore.Components.IComponent =>
        view.FindAll("button").Single(button => button.TextContent.Trim() == text);

    private static RequestLogEntry Entry(string action, string account) =>
        new("sqs", action, account, DateTimeOffset.UtcNow, 200, 1);

    private static HttpResponseMessage Requests(RequestLogEntry[] entries) =>
        Json(entries, LegacyJsonContext.Default.RequestLogEntryArray);

    private static HttpResponseMessage OverviewResponse(HttpRequestMessage request, int count) => request.RequestUri!.AbsolutePath switch
    {
        "/_microstack/health" => Json(new HealthResponse(new() { ["sqs"] = "enabled" }, "test", "1"), LegacyJsonContext.Default.HealthResponse),
        "/_microstack/resources" => Json(new[] { new ResourceSummary("sqs", count, []) }, LegacyJsonContext.Default.ResourceSummaryArray),
        "/_microstack/admin/v1/context" => Json(new AdminContext("000000000000", "eu-west-1"), AdminJsonContext.Default.AdminContext),
        _ => throw new InvalidOperationException(request.RequestUri.AbsolutePath)
    };

    private static HttpResponseMessage Json<T>(T value, JsonTypeInfo<T> type) =>
        new(HttpStatusCode.OK) { Content = JsonContent.Create(value, type) };

    private static void RegisterApis(BunitContext context, HttpMessageHandler handler)
    {
        var http = new HttpClient(handler) { BaseAddress = new("http://localhost:4566") };
        context.Services.AddSingleton(new AdminApiClient(http));
        context.Services.AddSingleton(new MicroStackApiService(http));
    }

    private sealed class Handler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> response) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            response(request, cancellationToken);
    }

    private sealed class Browser : IJSRuntime, IJSObjectReference
    {
        public long Generation { get; private set; }

        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) =>
            InvokeAsync<TValue>(identifier, CancellationToken.None, args);

        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args)
        {
            if (identifier == "start") Generation = (long)args![1]!;
            return ValueTask.FromResult(identifier switch
            {
                "import" or "create" => (TValue)(object)this,
                "isVisible" => (TValue)(object)true,
                _ => default!
            });
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
