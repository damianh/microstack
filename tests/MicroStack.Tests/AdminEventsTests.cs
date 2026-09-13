using System.Net;
using System.Text;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using Microsoft.Extensions.DependencyInjection;

namespace MicroStack.Tests;

public sealed class AdminEventsTests
{
    private const string First = "111111111111";
    private const string Second = "222222222222";
    private const string Events = "/_microstack/admin/v1/events";

    [Fact]
    public void Hub_BoundsSubscribersAndMergesIndependentDirtyScopes()
    {
        using var hub = new AdminChangeHub();
        using var first = hub.Subscribe(First)!;
        using var second = hub.Subscribe(Second)!;
        using var global = hub.Subscribe(null)!;
        first.Take(true).Resync.ShouldBeTrue();
        for (var i = 0; i < 10_000; i++)
            hub.DispatchCompleted("sqs", First);
        hub.Publish(AdminDirty.Activity);
        first.Wakeup.Reader.Count.ShouldBe(1);
        var change = first.Take();
        change.Resources.ShouldBeTrue();
        change.Accounts.ShouldBeTrue();
        change.Activity.ShouldBeTrue();
        change.Instance.ShouldBeFalse();
        change.Sequence.ShouldBe(10_001);
        second.Take().Resources.ShouldBeFalse();
        global.Take().Resources.ShouldBeTrue();
        first.Wakeup.Reader.Count.ShouldBe(0);
        hub.Publish(AdminDirty.Resources | AdminDirty.Instance);
        second.Take().Resources.ShouldBeTrue();
        global.Take().Instance.ShouldBeTrue();
        var rest = Enumerable.Range(0, 61).Select(_ => hub.Subscribe(null)!).ToArray();
        hub.Subscribe(null).ShouldBeNull();
        rest[0].Dispose();
        using var replacement = hub.Subscribe(null);
        replacement.ShouldNotBeNull();
        foreach (var item in rest)
            item.Dispose();
    }

    [Theory]
    [InlineData("athena")]
    [InlineData("events")]
    [InlineData("firehose")]
    [InlineData("unknown-service")]
    public void SharedOrUnknownDispatch_BroadensResourceScope(string service)
    {
        using var hub = new AdminChangeHub();
        using var other = hub.Subscribe(Second)!;
        hub.DispatchCompleted(service, First);
        other.Take().Resources.ShouldBeTrue();
    }

    [Fact]
    public async Task Stream_ResyncsFiltersCoalescesAndNeverLogsItsOwnReads()
    {
        using var fixture = new MicroStackFixture();
        using var first = await Open(fixture, First);
        using var second = await Open(fixture, Second);
        var initial = await first.Read();
        initial.ShouldBe(initial with { Version = 1, Resync = true, Resources = true,
            Accounts = true, Instance = true, Activity = true });
        (await second.Read()).Epoch.ShouldBe(initial.Epoch);
        fixture.Factory.Services.GetRequiredService<RequestLog>().GetEntries().ShouldBeEmpty();

        using var observer = fixture.Factory.Services.GetRequiredService<AdminChangeHub>().Subscribe(First)!;
        using var resources = await fixture.HttpClient.GetAsync(
            "/_microstack/admin/v1/services/sqs/resources?accountId=" + First);
        resources.EnsureSuccessStatusCode();
        observer.Wakeup.Reader.Count.ShouldBe(0);
        using var created = await Aws(fixture, First, "sqs", "AmazonSQS.CreateQueue",
            """{"QueueName":"live-queue"}""");
        created.EnsureSuccessStatusCode();
        var changed = await first.Read();
        changed.Resync.ShouldBeFalse();
        changed.Resources.ShouldBeTrue();
        changed.Accounts.ShouldBeTrue();
        changed.Activity.ShouldBeTrue();
        changed.Instance.ShouldBeFalse();
        changed.Sequence.ShouldBeGreaterThan(initial.Sequence);
        var other = await second.Read();
        other.Resources.ShouldBeFalse();
        other.Accounts.ShouldBeTrue();
        other.Activity.ShouldBeTrue();
        fixture.Factory.Services.GetRequiredService<RequestLog>().GetEntries().Count.ShouldBe(1);

        using var cleared = await fixture.HttpClient.DeleteAsync("/_microstack/requests");
        cleared.EnsureSuccessStatusCode();
        var clear = await first.Read();
        clear.Activity.ShouldBeTrue();
        clear.Resources.ShouldBeFalse();
        clear.Accounts.ShouldBeFalse();
        fixture.Factory.Services.GetRequiredService<RequestLog>().GetEntries().ShouldBeEmpty();

        using var reconnected = await Open(fixture, First, initial.Epoch + ":0");
        var resync = await reconnected.Read();
        resync.Resync.ShouldBeTrue();
        resync.Epoch.ShouldBe(initial.Epoch);
        resync.Sequence.ShouldBeGreaterThan(changed.Sequence);
    }

    [Fact]
    public async Task UnscopedStreamReceivesResourceOnlyHintsFromEveryAccount()
    {
        using var fixture = new MicroStackFixture();
        using var global = await Open(fixture, null);
        using var first = await Open(fixture, First);
        await global.Read();
        await first.Read();
        var hub = fixture.Factory.Services.GetRequiredService<AdminChangeHub>();
        using var observer = hub.Subscribe(First)!;
        hub.Publish(AdminDirty.Resources, Second);
        var otherAccount = await global.Read();
        otherAccount.Resources.ShouldBeTrue();
        otherAccount.Accounts.ShouldBeFalse();
        otherAccount.Activity.ShouldBeFalse();
        observer.Wakeup.Reader.Count.ShouldBe(0);
        hub.Publish(AdminDirty.Resources, First);
        (await first.Read()).Resources.ShouldBeTrue();
        (await global.Read()).Resources.ShouldBeTrue();
    }

    [Fact]
    public async Task StreamCoalescesBurstDuringHalfSecondWindow()
    {
        using var fixture = new MicroStackFixture();
        using var stream = await Open(fixture, First);
        var initial = await stream.Read();
        var hub = fixture.Factory.Services.GetRequiredService<AdminChangeHub>();
        var started = System.Diagnostics.Stopwatch.StartNew();
        for (var i = 0; i < 1_000; i++)
            hub.Publish(AdminDirty.Resources, First);
        hub.Publish(AdminDirty.Activity);
        hub.Publish(AdminDirty.Instance);
        var change = await stream.Read();
        started.Elapsed.ShouldBeGreaterThan(TimeSpan.FromMilliseconds(400));
        change.Resources.ShouldBeTrue();
        change.Activity.ShouldBeTrue();
        change.Instance.ShouldBeTrue();
        change.Accounts.ShouldBeFalse();
        change.Sequence.ShouldBe(initial.Sequence + 1_002);
    }

    [Fact]
    public async Task AdministrativeMutations_SignalConfigResetAndPartialReset()
    {
        using var fixture = new MicroStackFixture();
        using var stream = await Open(fixture, First);
        await stream.Read();
        using var config = await fixture.HttpClient.PostAsync("/_microstack/config",
            new StringContent("""{"stepfunctions":{"_sfn_mock_config":{}}}""", Encoding.UTF8, "application/json"));
        config.EnsureSuccessStatusCode();
        var changed = await stream.Read();
        changed.Instance.ShouldBeTrue();
        changed.Resources.ShouldBeTrue();
        changed.Activity.ShouldBeFalse();
        using var reset = await fixture.HttpClient.PostAsync("/_microstack/reset", null);
        reset.EnsureSuccessStatusCode();
        (await stream.Read()).ShouldBe(changed with
        {
            Sequence = changed.Sequence + 1, Accounts = true, Activity = true
        });
        fixture.Factory.Services.GetRequiredService<ServiceRegistry>().Register(new FailingHandler("reset-test"));
        using var failedReset = await fixture.HttpClient.PostAsync("/_microstack/reset", null);
        failedReset.StatusCode.ShouldBe(HttpStatusCode.InternalServerError);
        (await stream.Read()).Instance.ShouldBeTrue();
    }

    [Fact]
    public async Task CompletedFailedDispatch_UsesCapturedAccountAndCanonicalSpecialRoute()
    {
        using var fixture = new MicroStackFixture();
        fixture.Factory.Services.GetRequiredService<ServiceRegistry>().Register(new FailingHandler("s3"));
        using var first = await Open(fixture, First);
        using var second = await Open(fixture, Second);
        await first.Read();
        await second.Read();
        using var request = new HttpRequestMessage(HttpMethod.Get, "/v20180820/example");
        request.Headers.TryAddWithoutValidation("Authorization", Auth(First, "s3"));
        using var response = await fixture.HttpClient.SendAsync(request);
        response.StatusCode.ShouldBe(HttpStatusCode.InternalServerError);
        (await first.Read()).Resources.ShouldBeTrue();
        (await second.Read()).Resources.ShouldBeFalse();
    }

    [Fact]
    public async Task Stream_ValidatesAccountAndCapacityAndReleasesAbortedSubscription()
    {
        using var fixture = new MicroStackFixture();
        foreach (var query in new[] { "?accountId=", "?accountId=123", "?accountId=111111111111&accountId=222222222222" })
        {
            using var invalid = await fixture.HttpClient.GetAsync(Events + query);
            invalid.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
        }
        var hub = fixture.Factory.Services.GetRequiredService<AdminChangeHub>();
        var subscribers = Enumerable.Range(0, 63).Select(_ => hub.Subscribe(null)!).ToArray();
        using (var stream = await Open(fixture, null))
        {
            await stream.Read();
            using var full = await fixture.HttpClient.GetAsync(Events);
            full.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
            full.Headers.RetryAfter!.Delta.ShouldBe(TimeSpan.FromSeconds(5));
        }
        AdminChangeHub.Subscription? replacement = null;
        for (var attempt = 0; attempt < 100 && replacement is null; attempt++)
        {
            await Task.Delay(20);
            replacement = hub.Subscribe(null);
        }
        replacement.ShouldNotBeNull();
        replacement.Dispose();
        foreach (var subscription in subscribers)
            subscription.Dispose();
    }

    [Fact]
    public async Task BackgroundEsmProcessingSignalsCapturedAccountWithoutRequestLogActivity()
    {
        using var fixture = new MicroStackFixture();
        var registry = fixture.Factory.Services.GetRequiredService<ServiceRegistry>();
        var sqs = registry.Resolve("sqs")!;
        var lambda = registry.Resolve("lambda")!;
        using var first = await Open(fixture, First);
        using var second = await Open(fixture, Second);
        await first.Read();
        await second.Read();
        try
        {
            using (AccountContext.BeginScope(First))
            {
                var created = await Direct(sqs, "/", "AmazonSQS.CreateQueue",
                    """{"QueueName":"live-background"}""");
                created.StatusCode.ShouldBe(200);
                using var json = JsonDocument.Parse(created.Body);
                var url = json.RootElement.GetProperty("QueueUrl").GetString()!;
                (await Direct(sqs, "/", "AmazonSQS.SendMessage",
                    $$"""{"QueueUrl":"{{url}}","MessageBody":"background"}""")).StatusCode.ShouldBe(200);
                (await Direct(lambda, "/2015-03-31/event-source-mappings/", null,
                    $$"""{"FunctionName":"missing-function","EventSourceArn":"arn:aws:sqs:us-east-1:{{First}}:live-background"}"""))
                    .StatusCode.ShouldBe(202);
            }
            var change = await first.Read();
            change.Resources.ShouldBeTrue();
            change.Accounts.ShouldBeTrue();
            change.Activity.ShouldBeFalse();
            (await second.Read()).Resources.ShouldBeFalse();
            fixture.Factory.Services.GetRequiredService<RequestLog>().GetEntries().ShouldBeEmpty();
        }
        finally
        {
            lambda.Reset();
        }

        static Task<ServiceResponse> Direct(IServiceHandler handler, string path, string? target, string body)
        {
            var headers = new Dictionary<string, string> { ["content-type"] = "application/x-amz-json-1.0" };
            if (target is not null)
                headers["x-amz-target"] = target;
            return handler.HandleAsync(new("POST", path, headers, Encoding.UTF8.GetBytes(body), new Dictionary<string, string[]>()));
        }
    }

    [Fact]
    public async Task StreamRetainsAdminCorsPolicy()
    {
        using var fixture = new MicroStackFixture();
        using var request = new HttpRequestMessage(HttpMethod.Get, Events);
        request.Headers.Add("Origin", "http://untrusted.example");
        using var response = await fixture.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        response.StatusCode.ShouldBe(HttpStatusCode.OK);
        response.Headers.Contains("Access-Control-Allow-Origin").ShouldBeFalse();
        using var preflight = new HttpRequestMessage(HttpMethod.Options, Events);
        preflight.Headers.Add("Origin", "http://untrusted.example");
        preflight.Headers.Add("Access-Control-Request-Method", "GET");
        using var preflightResponse = await fixture.HttpClient.SendAsync(preflight);
        preflightResponse.StatusCode.ShouldBe(HttpStatusCode.NoContent);
        preflightResponse.Headers.Contains("Access-Control-Allow-Origin").ShouldBeFalse();
    }

    [Fact]
    public async Task Stream_HeartbeatIsCommentAndShutdownCompletesSubscriptions()
    {
        using var fixture = new MicroStackFixture();
        using var stream = await Open(fixture, null);
        await stream.Read();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        (await stream.Reader.ReadLineAsync(timeout.Token)).ShouldBe(": heartbeat");
        using var hub = new AdminChangeHub();
        using var subscription = hub.Subscribe(null)!;
        hub.Dispose();
        (await subscription.Wakeup.Reader.WaitToReadAsync()).ShouldBeFalse();
        hub.Subscribe(null).ShouldBeNull();
    }

    private static string Auth(string account, string service) =>
        $"AWS4-HMAC-SHA256 Credential={account}/20260913/us-east-1/{service}/aws4_request, SignedHeaders=host, Signature=test";

    private static async Task<HttpResponseMessage> Aws(MicroStackFixture fixture, string account,
        string service, string target, string body)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/");
        request.Headers.TryAddWithoutValidation("Authorization", Auth(account, service));
        request.Headers.TryAddWithoutValidation("X-Amz-Target", target);
        request.Content = new StringContent(body, Encoding.UTF8, "application/x-amz-json-1.0");
        return await fixture.HttpClient.SendAsync(request);
    }

    private static async Task<EventStream> Open(MicroStackFixture fixture, string? account, string? lastId = null)
    {
        var client = fixture.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Get,
            Events + (account is null ? "" : "?accountId=" + account));
        if (lastId is not null)
            request.Headers.TryAddWithoutValidation("Last-Event-ID", lastId);
        var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        response.EnsureSuccessStatusCode();
        response.Content.Headers.ContentType!.MediaType.ShouldBe("text/event-stream");
        response.Headers.CacheControl!.NoStore.ShouldBeTrue();
        return new(client, response, new StreamReader(await response.Content.ReadAsStreamAsync()));
    }

    private sealed class EventStream(HttpClient client, HttpResponseMessage response, StreamReader reader) : IDisposable
    {
        internal StreamReader Reader => reader;
        internal async Task<AdminChangeEvent> Read()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            string? json = null;
            while (true)
            {
                var line = await reader.ReadLineAsync(timeout.Token);
                if (line is null)
                    throw new EndOfStreamException();
                if (line.StartsWith("data: ", StringComparison.Ordinal))
                    json = line[6..];
                if (line.Length == 0 && json is not null)
                    return JsonSerializer.Deserialize(json, AdminJsonContext.Default.AdminChangeEvent)!;
            }
        }
        public void Dispose()
        {
            reader.Dispose();
            response.Dispose();
            client.Dispose();
        }
    }

    private sealed class FailingHandler(string service) : IServiceHandler
    {
        public string ServiceName => service;
        public Task<ServiceResponse> HandleAsync(ServiceRequest request)
        {
            AccountContext.SetFromAccessKey(Second);
            throw new InvalidOperationException("Partial dispatch failure.");
        }
        public void Reset() => throw new InvalidOperationException("Partial reset failure.");
        public JsonElement? GetState() => null;
        public void RestoreState(JsonElement state) { }
    }
}
