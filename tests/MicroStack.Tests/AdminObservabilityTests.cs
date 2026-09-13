using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon;
using Amazon.CloudFormation;
using Amazon.CloudFormation.Model;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.CloudWatchLogs;
using Amazon.CloudWatchLogs.Model;
using Amazon.Runtime;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;

namespace MicroStack.Tests;

public sealed class AdminObservabilityTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly AmazonCloudFormationClient _cloudFormation = Client<AmazonCloudFormationClient, AmazonCloudFormationConfig>(
        fixture, config => new(new BasicAWSCredentials("test", "test"), config));
    private readonly AmazonCloudWatchClient _cloudWatch = Client<AmazonCloudWatchClient, AmazonCloudWatchConfig>(
        fixture, config => new(new BasicAWSCredentials("test", "test"), config));
    private readonly AmazonCloudWatchLogsClient _logs = Client<AmazonCloudWatchLogsClient, AmazonCloudWatchLogsConfig>(
        fixture, config => new(new BasicAWSCredentials("test", "test"), config));
    private readonly AmazonSimpleSystemsManagementClient _ssm =
        Client<AmazonSimpleSystemsManagementClient, AmazonSimpleSystemsManagementConfig>(
            fixture, config => new(new BasicAWSCredentials("test", "test"), config));

    public async ValueTask InitializeAsync() => await fixture.HttpClient.PostAsync("/_microstack/reset", null);

    public ValueTask DisposeAsync()
    {
        _cloudFormation.Dispose();
        _cloudWatch.Dispose();
        _logs.Dispose();
        _ssm.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task ExposesEveryObservabilityRootAndCloudFormationChildKind()
    {
        const string template = """
            {"Resources":{"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"admin-cfn-queue"}}},
             "Outputs":{"QueueName":{"Value":"admin-cfn-queue","Description":"created queue"}}}
            """;
        await _cloudFormation.CreateStackAsync(new CreateStackRequest { StackName = "admin-stack", TemplateBody = template });
        await _cloudWatch.PutMetricDataAsync(new PutMetricDataRequest
        {
            Namespace = "Admin/Test",
            MetricData =
            [
                new MetricDatum
                {
                    MetricName = "Latency", Value = 2.5, Unit = Amazon.CloudWatch.StandardUnit.Milliseconds,
                },
            ],
        });
        await _cloudWatch.PutMetricAlarmAsync(new PutMetricAlarmRequest
        {
            AlarmName = "admin-alarm", Namespace = "Admin/Test", MetricName = "Latency",
            ComparisonOperator = ComparisonOperator.GreaterThanThreshold, EvaluationPeriods = 1,
            Period = 60, Statistic = Statistic.Average, Threshold = 1,
        });
        await _cloudWatch.PutDashboardAsync(new PutDashboardRequest
        {
            DashboardName = "admin-dashboard", DashboardBody = """{"widgets":[]}""",
        });

        var cfn = await Resources("cloudformation");
        Kinds(cfn).ShouldBe(["stack"]);
        var stackChildren = await Children("cloudformation", new PathKey("stack", "admin-stack"));
        Kinds(stackChildren).ShouldContain("resource");
        Kinds(stackChildren).ShouldContain("event");
        Kinds(stackChildren).ShouldContain("output");
        Kinds(stackChildren).ShouldContain("template");

        var cloudWatch = await Resources("cloudwatch");
        Kinds(cloudWatch).ShouldContain("metric");
        Kinds(cloudWatch).ShouldContain("alarm");
        Kinds(cloudWatch).ShouldContain("dashboard");
        var metric = cloudWatch.GetProperty("items").EnumerateArray().Single(x => Kind(x) == "metric");
        var points = await Children("cloudwatch", Key(metric));
        Kinds(points).ShouldBe(["datapoint"]);
        var point = points.GetProperty("items")[0];
        (await Content("cloudwatch", Key(metric), Key(point))).GetProperty("kind").GetString().ShouldBe("json");
    }

    [Fact]
    public async Task LogHierarchyPaginatesWithoutConsumingEventsAndIncludesFilters()
    {
        await _logs.CreateLogGroupAsync(new CreateLogGroupRequest
        {
            LogGroupName = "/admin/logs", Tags = new Dictionary<string, string> { ["environment"] = "test" },
        });
        await _logs.CreateLogStreamAsync(new CreateLogStreamRequest
        {
            LogGroupName = "/admin/logs", LogStreamName = "stream",
        });
        await _logs.PutLogEventsAsync(new PutLogEventsRequest
        {
            LogGroupName = "/admin/logs", LogStreamName = "stream",
            LogEvents =
            [
                new() { Timestamp = DateTime.UtcNow, Message = "first" },
                new() { Timestamp = DateTime.UtcNow.AddMilliseconds(1), Message = "second" },
            ],
        });
        await _logs.PutSubscriptionFilterAsync(new PutSubscriptionFilterRequest
        {
            LogGroupName = "/admin/logs", FilterName = "subscription", FilterPattern = "ERROR",
            DestinationArn = "arn:aws:lambda:us-east-1:000000000000:function:sink",
        });
        await _logs.PutMetricFilterAsync(new PutMetricFilterRequest
        {
            LogGroupName = "/admin/logs", FilterName = "metric", FilterPattern = "ERROR",
            MetricTransformations =
            [
                new() { MetricName = "Errors", MetricNamespace = "Admin/Test", MetricValue = "1" },
            ],
        });

        var groups = await Resources("logs");
        Kinds(groups).ShouldBe(["log-group"]);
        var groupChildren = await Children("logs", new PathKey("log-group", "/admin/logs"));
        Kinds(groupChildren).ShouldContain("log-stream");
        Kinds(groupChildren).ShouldContain("subscription-filter");
        Kinds(groupChildren).ShouldContain("metric-filter");
        Kinds(groupChildren).ShouldContain("tag");

        var firstPage = await Children("logs", new("log-group", "/admin/logs"),
            new("log-stream", "stream"), pageSize: 1);
        firstPage.GetProperty("items").GetArrayLength().ShouldBe(1);
        var cursor = firstPage.GetProperty("nextCursor").GetString();
        cursor.ShouldNotBeNull();
        var secondPage = await Children("logs", [new("log-group", "/admin/logs"), new("log-stream", "stream")],
            pageSize: 1, cursor: cursor);
        secondPage.GetProperty("items").GetArrayLength().ShouldBe(1);

        var eventsAfterInspection = await _logs.GetLogEventsAsync(new GetLogEventsRequest
        {
            LogGroupName = "/admin/logs", LogStreamName = "stream",
        });
        eventsAfterInspection.Events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task SecureParametersStayRedactedUntilExplicitRevealAndInspectionDoesNotMutateOrCrossAccounts()
    {
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/admin/plain", Type = ParameterType.String, Value = "ordinary",
        });
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/admin/secret", Type = ParameterType.SecureString, Value = "swordfish",
        });
        await _ssm.PutParameterAsync(new PutParameterRequest
        {
            Name = "/admin/secret", Type = ParameterType.SecureString, Value = "new-swordfish", Overwrite = true,
        });
        await _ssm.AddTagsToResourceAsync(new AddTagsToResourceRequest
        {
            ResourceId = "/admin/secret", ResourceType = ResourceTypeForTagging.Parameter,
            Tags = [new() { Key = "owner", Value = "admin-test" }],
        });

        var parameters = await Resources("ssm");
        Kinds(parameters).ShouldBe(["parameter", "parameter"]);
        (await Content("ssm", new PathKey("parameter", "/admin/plain"))).GetProperty("text").GetString()
            .ShouldBe("ordinary");
        var secretPath = new PathKey("parameter", "/admin/secret");
        var detail = await Detail("ssm", secretPath);
        detail.GetRawText().ShouldNotContain("swordfish");
        detail.GetProperty("fields").EnumerateArray().Single(x => x.GetProperty("name").GetString() == "Value")
            .GetProperty("value").GetString().ShouldBe("••••••••");
        (await Content("ssm", secretPath)).GetRawText().ShouldNotContain("swordfish");

        var children = await Children("ssm", secretPath);
        Kinds(children).Count(x => x == "parameter-version").ShouldBe(2);
        Kinds(children).ShouldContain("tag");
        children.GetRawText().ShouldNotContain("swordfish");

        using var revealResponse = await fixture.HttpClient.PostAsync(
            AdminUrl("ssm", "reveal", [secretPath]) + "&field=Value", null);
        revealResponse.Headers.CacheControl?.NoStore.ShouldBeTrue();
        var reveal = await ReadJson(revealResponse);
        reveal.GetProperty("text").GetString().ShouldBe("new-swordfish");
        reveal.GetProperty("sensitive").GetBoolean().ShouldBeTrue();

        var after = await _ssm.GetParameterAsync(new GetParameterRequest { Name = "/admin/secret", WithDecryption = true });
        after.Parameter.Value.ShouldBe("new-swordfish");
        (await Resources("ssm", "111111111111")).GetProperty("items").GetArrayLength().ShouldBe(0);
        using var ordinaryReveal = await fixture.HttpClient.PostAsync(
            AdminUrl("ssm", "reveal", [new PathKey("parameter", "/admin/plain")]) + "&field=Value", null);
        ordinaryReveal.StatusCode.ShouldBe(HttpStatusCode.Conflict);
    }

    private async Task<JsonElement> Resources(string service, string? account = null)
    {
        var url = $"/_microstack/admin/v1/services/{service}/resources";
        if (account is not null) url += $"?accountId={account}";
        return await ReadJson(await fixture.HttpClient.GetAsync(url));
    }

    private async Task<JsonElement> Detail(string service, params PathKey[] path) =>
        await ReadJson(await fixture.HttpClient.GetAsync(AdminUrl(service, "resource", path)));

    private async Task<JsonElement> Content(string service, params PathKey[] path) =>
        await ReadJson(await fixture.HttpClient.GetAsync(AdminUrl(service, "content", path)));

    private Task<JsonElement> Children(string service, PathKey key, int pageSize = 50) =>
        Children(service, [key], pageSize);

    private Task<JsonElement> Children(string service, PathKey first, PathKey second, int pageSize = 50) =>
        Children(service, [first, second], pageSize);

    private async Task<JsonElement> Children(
        string service, PathKey[] path, int pageSize = 50, string? cursor = null)
    {
        var url = AdminUrl(service, "children", path) + $"&pageSize={pageSize}";
        if (cursor is not null) url += $"&cursor={Uri.EscapeDataString(cursor)}";
        return await ReadJson(await fixture.HttpClient.GetAsync(url));
    }

    private static string AdminUrl(string service, string operation, PathKey[] path) =>
        $"/_microstack/admin/v1/services/{service}/{operation}?path=" +
        Uri.EscapeDataString(JsonSerializer.Serialize(path));

    private static async Task<JsonElement> ReadJson(HttpResponseMessage response)
    {
        response.EnsureSuccessStatusCode();
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.Clone();
    }

    private static PathKey Key(JsonElement resource)
    {
        var key = resource.GetProperty("key");
        return new(key.GetProperty("kind").GetString()!, key.GetProperty("id").GetString()!);
    }

    private static string Kind(JsonElement resource) => resource.GetProperty("key").GetProperty("kind").GetString()!;

    private static string[] Kinds(JsonElement page) =>
        page.GetProperty("items").EnumerateArray().Select(Kind).ToArray();

    private static TClient Client<TClient, TConfig>(
        MicroStackFixture fixture, Func<TConfig, TClient> create)
        where TClient : AmazonServiceClient
        where TConfig : ClientConfig, new()
    {
        var httpClient = new HttpClient(new CanonicalizeUriHandler(fixture.CreateHandler()))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        var config = new TConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };
        return create(config);
    }

    private sealed record PathKey(
        [property: JsonPropertyName("kind")] string Kind,
        [property: JsonPropertyName("id")] string Id);
}
