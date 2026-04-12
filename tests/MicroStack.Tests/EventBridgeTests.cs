using System.Text.Json;
using Amazon;
using Amazon.EventBridge;
using Amazon.EventBridge.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the EventBridge service handler.
/// Uses the AWS SDK for .NET v4 pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_eventbridge.py.
/// </summary>
public sealed class EventBridgeTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonEventBridgeClient _eb;

    public EventBridgeTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _eb = CreateEventBridgeClient(fixture);
    }

    private static AmazonEventBridgeClient CreateEventBridgeClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonEventBridgeConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonEventBridgeClient(new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _eb.Dispose();
        return Task.CompletedTask;
    }

    // ── Event bus lifecycle ─────────────────────────────────────────────────────

    [Fact]
    public async Task CreateEventBusAndDescribe()
    {
        var resp = await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "eb-bus-v2" });
        Assert.Contains("eb-bus-v2", resp.EventBusArn);

        var buses = await _eb.ListEventBusesAsync(new ListEventBusesRequest());
        Assert.Contains(buses.EventBuses, b => b.Name == "eb-bus-v2");

        var desc = await _eb.DescribeEventBusAsync(new DescribeEventBusRequest { Name = "eb-bus-v2" });
        Assert.Equal("eb-bus-v2", desc.Name);
    }

    [Fact]
    public async Task UpdateEventBusDescription()
    {
        await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "eb-upd-bus" });

        await _eb.UpdateEventBusAsync(new UpdateEventBusRequest
        {
            Name = "eb-upd-bus",
            Description = "updated description",
        });

        var desc = await _eb.DescribeEventBusAsync(new DescribeEventBusRequest { Name = "eb-upd-bus" });
        Assert.Equal("updated description", desc.Description);
    }

    [Fact]
    public async Task DeleteEventBus()
    {
        await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "eb-del-bus" });
        await _eb.DeleteEventBusAsync(new DeleteEventBusRequest { Name = "eb-del-bus" });

        var buses = await _eb.ListEventBusesAsync(new ListEventBusesRequest());
        Assert.DoesNotContain(buses.EventBuses, b => b.Name == "eb-del-bus");
    }

    [Fact]
    public async Task DeleteDefaultEventBusFails()
    {
        var ex = await Assert.ThrowsAsync<AmazonEventBridgeException>(
            () => _eb.DeleteEventBusAsync(new DeleteEventBusRequest { Name = "default" }));
        Assert.Equal("ValidationException", ex.ErrorCode);
    }

    // ── Rules ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PutRuleAndListRules()
    {
        await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "eb-rule-bus" });

        var resp = await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "eb-rule-v2",
            EventBusName = "eb-rule-bus",
            EventPattern = JsonSerializer.Serialize(new { source = new[] { "my.app" } }),
            State = RuleState.ENABLED,
        });
        Assert.NotEmpty(resp.RuleArn);

        var rules = await _eb.ListRulesAsync(new ListRulesRequest { EventBusName = "eb-rule-bus" });
        Assert.Contains(rules.Rules, r => r.Name == "eb-rule-v2");

        var described = await _eb.DescribeRuleAsync(new DescribeRuleRequest
        {
            Name = "eb-rule-v2",
            EventBusName = "eb-rule-bus",
        });
        Assert.Equal("eb-rule-v2", described.Name);
        Assert.Equal(RuleState.ENABLED, described.State);
    }

    [Fact]
    public async Task PutRuleWithScheduleExpression()
    {
        await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "test-bus" });
        await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "test-rule",
            EventBusName = "test-bus",
            ScheduleExpression = "rate(5 minutes)",
            State = RuleState.ENABLED,
        });

        var rules = await _eb.ListRulesAsync(new ListRulesRequest { EventBusName = "test-bus" });
        Assert.Contains(rules.Rules, r => r.Name == "test-rule");
    }

    [Fact]
    public async Task DeleteRule()
    {
        await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "eb-del-v2",
            ScheduleExpression = "rate(1 day)",
            State = RuleState.ENABLED,
        });

        await _eb.DeleteRuleAsync(new DeleteRuleRequest { Name = "eb-del-v2" });

        var ex = await Assert.ThrowsAsync<ResourceNotFoundException>(
            () => _eb.DescribeRuleAsync(new DescribeRuleRequest { Name = "eb-del-v2" }));
        Assert.Equal("ResourceNotFoundException", ex.ErrorCode);
    }

    [Fact]
    public async Task EnableAndDisableRule()
    {
        await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "toggle-rule",
            ScheduleExpression = "rate(1 hour)",
            State = RuleState.ENABLED,
        });

        await _eb.DisableRuleAsync(new DisableRuleRequest { Name = "toggle-rule" });
        var desc = await _eb.DescribeRuleAsync(new DescribeRuleRequest { Name = "toggle-rule" });
        Assert.Equal(RuleState.DISABLED, desc.State);

        await _eb.EnableRuleAsync(new EnableRuleRequest { Name = "toggle-rule" });
        desc = await _eb.DescribeRuleAsync(new DescribeRuleRequest { Name = "toggle-rule" });
        Assert.Equal(RuleState.ENABLED, desc.State);
    }

    // ── Targets ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PutTargetsAndListTargets()
    {
        await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "eb-tgt-v2",
            ScheduleExpression = "rate(10 minutes)",
            State = RuleState.ENABLED,
        });

        await _eb.PutTargetsAsync(new PutTargetsRequest
        {
            Rule = "eb-tgt-v2",
            Targets =
            [
                new Target { Id = "t1", Arn = "arn:aws:lambda:us-east-1:000000000000:function:f1" },
                new Target { Id = "t2", Arn = "arn:aws:sqs:us-east-1:000000000000:q1" },
            ],
        });

        var resp = await _eb.ListTargetsByRuleAsync(new ListTargetsByRuleRequest { Rule = "eb-tgt-v2" });
        Assert.Equal(2, resp.Targets.Count);
        var ids = resp.Targets.Select(t => t.Id).ToHashSet();
        Assert.Contains("t1", ids);
        Assert.Contains("t2", ids);
    }

    [Fact]
    public async Task RemoveTargets()
    {
        await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "eb-rm-v2",
            ScheduleExpression = "rate(1 minute)",
            State = RuleState.ENABLED,
        });

        await _eb.PutTargetsAsync(new PutTargetsRequest
        {
            Rule = "eb-rm-v2",
            Targets =
            [
                new Target { Id = "rm1", Arn = "arn:aws:lambda:us-east-1:000000000000:function:f" },
                new Target { Id = "rm2", Arn = "arn:aws:lambda:us-east-1:000000000000:function:g" },
            ],
        });

        var before = await _eb.ListTargetsByRuleAsync(new ListTargetsByRuleRequest { Rule = "eb-rm-v2" });
        Assert.Equal(2, before.Targets.Count);

        await _eb.RemoveTargetsAsync(new RemoveTargetsRequest
        {
            Rule = "eb-rm-v2",
            Ids = ["rm1"],
        });

        var after = await _eb.ListTargetsByRuleAsync(new ListTargetsByRuleRequest { Rule = "eb-rm-v2" });
        Assert.Single(after.Targets);
        Assert.Equal("rm2", after.Targets[0].Id);
    }

    [Fact]
    public async Task ListRuleNamesByTarget()
    {
        var fnArn = "arn:aws:lambda:us-east-1:000000000000:function:list-by-tgt-fn";
        await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "lrt-bus" });

        await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "rule-a",
            EventBusName = "lrt-bus",
            EventPattern = JsonSerializer.Serialize(new { source = new[] { "my.app" } }),
            State = RuleState.ENABLED,
        });
        await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "rule-b",
            EventBusName = "lrt-bus",
            EventPattern = JsonSerializer.Serialize(new { source = new[] { "other.app" } }),
            State = RuleState.ENABLED,
        });

        await _eb.PutTargetsAsync(new PutTargetsRequest
        {
            Rule = "rule-a",
            EventBusName = "lrt-bus",
            Targets = [new Target { Id = "t1", Arn = fnArn }],
        });
        await _eb.PutTargetsAsync(new PutTargetsRequest
        {
            Rule = "rule-b",
            EventBusName = "lrt-bus",
            Targets = [new Target { Id = "t1", Arn = fnArn }],
        });

        var result = await _eb.ListRuleNamesByTargetAsync(new ListRuleNamesByTargetRequest
        {
            TargetArn = fnArn,
            EventBusName = "lrt-bus",
        });

        var sorted = result.RuleNames.OrderBy(x => x).ToList();
        Assert.Equal(["rule-a", "rule-b"], sorted);
    }

    [Fact]
    public async Task ListRuleNamesByTargetPagination()
    {
        var fnArn = "arn:aws:lambda:us-east-1:000000000000:function:page-fn";
        await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "r1",
            ScheduleExpression = "rate(1 hour)",
            State = RuleState.ENABLED,
        });
        await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "r2",
            ScheduleExpression = "rate(1 hour)",
            State = RuleState.ENABLED,
        });

        await _eb.PutTargetsAsync(new PutTargetsRequest
        {
            Rule = "r1",
            Targets = [new Target { Id = "1", Arn = fnArn }],
        });
        await _eb.PutTargetsAsync(new PutTargetsRequest
        {
            Rule = "r2",
            Targets = [new Target { Id = "1", Arn = fnArn }],
        });

        var p1 = await _eb.ListRuleNamesByTargetAsync(new ListRuleNamesByTargetRequest
        {
            TargetArn = fnArn,
            Limit = 1,
        });
        Assert.Single(p1.RuleNames);
        Assert.NotNull(p1.NextToken);

        var p2 = await _eb.ListRuleNamesByTargetAsync(new ListRuleNamesByTargetRequest
        {
            TargetArn = fnArn,
            Limit = 1,
            NextToken = p1.NextToken,
        });
        Assert.Single(p2.RuleNames);
        Assert.NotEqual(p1.RuleNames[0], p2.RuleNames[0]);
    }

    // ── PutEvents ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task PutEvents()
    {
        var resp = await _eb.PutEventsAsync(new PutEventsRequest
        {
            Entries =
            [
                new PutEventsRequestEntry
                {
                    Source = "myapp",
                    DetailType = "UserSignup",
                    Detail = JsonSerializer.Serialize(new { userId = "123" }),
                    EventBusName = "default",
                },
                new PutEventsRequestEntry
                {
                    Source = "myapp",
                    DetailType = "OrderPlaced",
                    Detail = JsonSerializer.Serialize(new { orderId = "456" }),
                    EventBusName = "default",
                },
            ],
        });

        Assert.Equal(0, resp.FailedEntryCount);
        Assert.Equal(2, resp.Entries.Count);
    }

    [Fact]
    public async Task PutEventsMultiple()
    {
        var resp = await _eb.PutEventsAsync(new PutEventsRequest
        {
            Entries =
            [
                new PutEventsRequestEntry
                {
                    Source = "app.v2",
                    DetailType = "Ev1",
                    Detail = JsonSerializer.Serialize(new { a = 1 }),
                    EventBusName = "default",
                },
                new PutEventsRequestEntry
                {
                    Source = "app.v2",
                    DetailType = "Ev2",
                    Detail = JsonSerializer.Serialize(new { b = 2 }),
                    EventBusName = "default",
                },
                new PutEventsRequestEntry
                {
                    Source = "app.v2",
                    DetailType = "Ev3",
                    Detail = JsonSerializer.Serialize(new { c = 3 }),
                    EventBusName = "default",
                },
            ],
        });

        Assert.Equal(0, resp.FailedEntryCount);
        Assert.Equal(3, resp.Entries.Count);
        Assert.All(resp.Entries, e => Assert.NotEmpty(e.EventId));
    }

    // ── TestEventPattern ────────────────────────────────────────────────────────

    [Fact]
    public async Task TestEventPatternMatch()
    {
        var eventJson = JsonSerializer.Serialize(new
        {
            source = "orders.service",
            detail_type = "Order Placed",
            detail = new { orderId = "42", amount = 10 },
        });
        // The AWS SDK sends "detail-type" but our object uses "detail_type" for C#.
        // Use raw JSON to ensure exact field names.
        eventJson = """{"source":"orders.service","detail-type":"Order Placed","detail":{"orderId":"42","amount":10}}""";

        var patternJson = JsonSerializer.Serialize(new
        {
            source = new[] { "orders.service" },
            detail_type = new[] { "Order Placed" },
        });
        patternJson = """{"source":["orders.service"],"detail-type":["Order Placed"]}""";

        var resp = await _eb.TestEventPatternAsync(new TestEventPatternRequest
        {
            Event = eventJson,
            EventPattern = patternJson,
        });

        Assert.True(resp.Result);
    }

    [Fact]
    public async Task TestEventPatternNoMatch()
    {
        var eventJson = """{"source":"other","detail-type":"X","detail":{}}""";
        var patternJson = """{"source":["orders.service"]}""";

        var resp = await _eb.TestEventPatternAsync(new TestEventPatternRequest
        {
            Event = eventJson,
            EventPattern = patternJson,
        });

        Assert.False(resp.Result);
    }

    [Fact]
    public async Task TestEventPatternInvalidEvent()
    {
        var ex = await Assert.ThrowsAsync<InvalidEventPatternException>(
            () => _eb.TestEventPatternAsync(new TestEventPatternRequest
            {
                Event = "not-json",
                EventPattern = "{}",
            }));
        Assert.Equal("InvalidEventPatternException", ex.ErrorCode);
    }

    // ── Tags ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TagAndUntagResource()
    {
        var resp = await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "eb-tag-v2",
            ScheduleExpression = "rate(1 hour)",
            State = RuleState.ENABLED,
        });
        var arn = resp.RuleArn;

        await _eb.TagResourceAsync(new TagResourceRequest
        {
            ResourceARN = arn,
            Tags =
            [
                new Tag { Key = "stage", Value = "dev" },
                new Tag { Key = "team", Value = "ops" },
            ],
        });

        var tags = await _eb.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceARN = arn });
        var tagMap = tags.Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal("dev", tagMap["stage"]);
        Assert.Equal("ops", tagMap["team"]);

        await _eb.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceARN = arn,
            TagKeys = ["stage"],
        });

        var tags2 = await _eb.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceARN = arn });
        Assert.DoesNotContain(tags2.Tags, t => t.Key == "stage");
        Assert.Contains(tags2.Tags, t => t.Key == "team");
    }

    // ── Archives ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ArchiveLifecycle()
    {
        var archiveName = $"intg-archive-{Guid.NewGuid():N}"[..30];

        var resp = await _eb.CreateArchiveAsync(new CreateArchiveRequest
        {
            ArchiveName = archiveName,
            EventSourceArn = "arn:aws:events:us-east-1:000000000000:event-bus/default",
            Description = "test archive",
            RetentionDays = 7,
        });
        Assert.NotEmpty(resp.ArchiveArn);

        var desc = await _eb.DescribeArchiveAsync(new DescribeArchiveRequest { ArchiveName = archiveName });
        Assert.Equal(archiveName, desc.ArchiveName);
        Assert.Equal(7, desc.RetentionDays);

        var archives = await _eb.ListArchivesAsync(new ListArchivesRequest());
        Assert.Contains(archives.Archives, a => a.ArchiveName == archiveName);

        await _eb.DeleteArchiveAsync(new DeleteArchiveRequest { ArchiveName = archiveName });

        var archives2 = await _eb.ListArchivesAsync(new ListArchivesRequest());
        Assert.DoesNotContain(archives2.Archives, a => a.ArchiveName == archiveName);
    }

    [Fact]
    public async Task UpdateArchive()
    {
        var name = $"upd-archive-{Guid.NewGuid():N}"[..26];

        await _eb.CreateArchiveAsync(new CreateArchiveRequest
        {
            ArchiveName = name,
            EventSourceArn = "arn:aws:events:us-east-1:000000000000:event-bus/default",
            Description = "old",
            RetentionDays = 1,
        });

        await _eb.UpdateArchiveAsync(new UpdateArchiveRequest
        {
            ArchiveName = name,
            Description = "new desc",
            RetentionDays = 30,
            EventPattern = JsonSerializer.Serialize(new { source = new[] { "app" } }),
        });

        var desc = await _eb.DescribeArchiveAsync(new DescribeArchiveRequest { ArchiveName = name });
        Assert.Equal("new desc", desc.Description);
        Assert.Equal(30, desc.RetentionDays);
        Assert.Contains("app", desc.EventPattern);
    }

    // ── Replays ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ReplayLifecycle()
    {
        var archName = $"replay-arch-{Guid.NewGuid():N}"[..28];
        await _eb.CreateArchiveAsync(new CreateArchiveRequest
        {
            ArchiveName = archName,
            EventSourceArn = "arn:aws:events:us-east-1:000000000000:event-bus/default",
        });

        var archiveArn = (await _eb.DescribeArchiveAsync(
            new DescribeArchiveRequest { ArchiveName = archName })).ArchiveArn;

        var repName = $"replay-{Guid.NewGuid():N}"[..24];

        var start = await _eb.StartReplayAsync(new StartReplayRequest
        {
            ReplayName = repName,
            EventSourceArn = "arn:aws:events:us-east-1:000000000000:event-bus/default",
            EventStartTime = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            EventEndTime = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc),
            Destination = new ReplayDestination { Arn = archiveArn },
        });
        Assert.Equal("RUNNING", start.State.Value);

        var desc = await _eb.DescribeReplayAsync(new DescribeReplayRequest { ReplayName = repName });
        Assert.Equal(repName, desc.ReplayName);
        Assert.Equal("RUNNING", desc.State.Value);

        var listed = await _eb.ListReplaysAsync(new ListReplaysRequest { NamePrefix = repName });
        Assert.Contains(listed.Replays, r => r.ReplayName == repName);

        var cancel = await _eb.CancelReplayAsync(new CancelReplayRequest { ReplayName = repName });
        Assert.Equal("CANCELLED", cancel.State.Value);

        var desc2 = await _eb.DescribeReplayAsync(new DescribeReplayRequest { ReplayName = repName });
        Assert.Equal("CANCELLED", desc2.State.Value);

        await _eb.DeleteArchiveAsync(new DeleteArchiveRequest { ArchiveName = archName });
    }

    // ── Permissions ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task PutAndRemovePermission()
    {
        await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "perm-bus" });

        await _eb.PutPermissionAsync(new PutPermissionRequest
        {
            EventBusName = "perm-bus",
            Action = "events:PutEvents",
            Principal = "123456789012",
            StatementId = "AllowAcct",
        });

        // No exception = success
        await _eb.RemovePermissionAsync(new RemovePermissionRequest
        {
            EventBusName = "perm-bus",
            StatementId = "AllowAcct",
        });

        await _eb.DeleteEventBusAsync(new DeleteEventBusRequest { Name = "perm-bus" });
    }

    // ── Connections ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task ConnectionLifecycle()
    {
        var resp = await _eb.CreateConnectionAsync(new CreateConnectionRequest
        {
            Name = "test-conn",
            AuthorizationType = ConnectionAuthorizationType.API_KEY,
            AuthParameters = new CreateConnectionAuthRequestParameters
            {
                ApiKeyAuthParameters = new CreateConnectionApiKeyAuthRequestParameters
                {
                    ApiKeyName = "x-api-key",
                    ApiKeyValue = "secret",
                },
            },
        });
        Assert.NotEmpty(resp.ConnectionArn);

        var desc = await _eb.DescribeConnectionAsync(new DescribeConnectionRequest { Name = "test-conn" });
        Assert.Equal("test-conn", desc.Name);

        await _eb.DeleteConnectionAsync(new DeleteConnectionRequest { Name = "test-conn" });
    }

    [Fact]
    public async Task DeauthorizeConnection()
    {
        await _eb.CreateConnectionAsync(new CreateConnectionRequest
        {
            Name = "deauth-conn",
            AuthorizationType = ConnectionAuthorizationType.API_KEY,
            AuthParameters = new CreateConnectionAuthRequestParameters
            {
                ApiKeyAuthParameters = new CreateConnectionApiKeyAuthRequestParameters
                {
                    ApiKeyName = "k",
                    ApiKeyValue = "v",
                },
            },
        });

        var result = await _eb.DeauthorizeConnectionAsync(
            new DeauthorizeConnectionRequest { Name = "deauth-conn" });
        Assert.Equal(ConnectionState.DEAUTHORIZED, result.ConnectionState);

        var desc = await _eb.DescribeConnectionAsync(new DescribeConnectionRequest { Name = "deauth-conn" });
        Assert.Equal(ConnectionState.DEAUTHORIZED, desc.ConnectionState);

        await _eb.DeleteConnectionAsync(new DeleteConnectionRequest { Name = "deauth-conn" });
    }

    // ── API Destinations ────────────────────────────────────────────────────────

    [Fact]
    public async Task ApiDestinationLifecycle()
    {
        await _eb.CreateConnectionAsync(new CreateConnectionRequest
        {
            Name = "apid-conn",
            AuthorizationType = ConnectionAuthorizationType.API_KEY,
            AuthParameters = new CreateConnectionAuthRequestParameters
            {
                ApiKeyAuthParameters = new CreateConnectionApiKeyAuthRequestParameters
                {
                    ApiKeyName = "k",
                    ApiKeyValue = "v",
                },
            },
        });

        var resp = await _eb.CreateApiDestinationAsync(new CreateApiDestinationRequest
        {
            Name = "test-apid",
            ConnectionArn = "arn:aws:events:us-east-1:000000000000:connection/apid-conn/00000000-0000-0000-0000-000000000000",
            InvocationEndpoint = "https://example.com/webhook",
            HttpMethod = ApiDestinationHttpMethod.POST,
        });
        Assert.NotEmpty(resp.ApiDestinationArn);

        var desc = await _eb.DescribeApiDestinationAsync(
            new DescribeApiDestinationRequest { Name = "test-apid" });
        Assert.Equal("test-apid", desc.Name);

        await _eb.DeleteApiDestinationAsync(new DeleteApiDestinationRequest { Name = "test-apid" });
    }

    // ── Endpoints and partner stubs ─────────────────────────────────────────────

    [Fact]
    public async Task EndpointLifecycle()
    {
        await _eb.CreateEndpointAsync(new CreateEndpointRequest
        {
            Name = "my-global-endpoint",
            Description = "stub",
            RoleArn = "arn:aws:iam::000000000000:role/r",
            RoutingConfig = new RoutingConfig
            {
                FailoverConfig = new FailoverConfig
                {
                    Primary = new Primary
                    {
                        HealthCheck = "arn:aws:route53:::healthcheck/primary",
                    },
                    Secondary = new Secondary { Route = "secondary-route" },
                },
            },
            EventBuses =
            [
                new EndpointEventBus { EventBusArn = "arn:aws:events:us-east-1:000000000000:event-bus/default" },
                new EndpointEventBus { EventBusArn = "arn:aws:events:us-east-1:000000000000:event-bus/backup" },
            ],
        });

        var desc = await _eb.DescribeEndpointAsync(new DescribeEndpointRequest
        {
            Name = "my-global-endpoint",
        });
        Assert.Equal(EndpointState.ACTIVE, desc.State);
        Assert.NotEmpty(desc.Arn);

        var listed = await _eb.ListEndpointsAsync(new ListEndpointsRequest());
        Assert.Contains(listed.Endpoints, e => e.Name == "my-global-endpoint");

        await _eb.UpdateEndpointAsync(new UpdateEndpointRequest
        {
            Name = "my-global-endpoint",
            Description = "updated",
        });

        await _eb.DeleteEndpointAsync(new DeleteEndpointRequest { Name = "my-global-endpoint" });
    }

    [Fact]
    public async Task PartnerEventSourceStubs()
    {
        var resp = await _eb.CreatePartnerEventSourceAsync(new CreatePartnerEventSourceRequest
        {
            Name = "aws.partner/saas/src",
            Account = "111111111111",
        });
        Assert.NotEmpty(resp.EventSourceArn);

        await _eb.DescribePartnerEventSourceAsync(
            new DescribePartnerEventSourceRequest { Name = "aws.partner/saas/src" });

        var list = await _eb.ListPartnerEventSourcesAsync(
            new ListPartnerEventSourcesRequest { NamePrefix = "aws.partner/saas/src" });
        Assert.NotEmpty(list.PartnerEventSources);

        await _eb.DeletePartnerEventSourceAsync(new DeletePartnerEventSourceRequest
        {
            Name = "aws.partner/saas/src",
            Account = "111111111111",
        });

        var accounts = await _eb.ListPartnerEventSourceAccountsAsync(
            new ListPartnerEventSourceAccountsRequest { EventSourceName = "aws.partner/saas/accts" });
        Assert.Empty(accounts.PartnerEventSourceAccounts);

        var eventSources = await _eb.ListEventSourcesAsync(new ListEventSourcesRequest());
        Assert.Empty(eventSources.EventSources);

        var pe = await _eb.PutPartnerEventsAsync(new PutPartnerEventsRequest
        {
            Entries =
            [
                new PutPartnerEventsRequestEntry
                {
                    Source = "aws.partner/saas/events",
                    DetailType = "t",
                    Detail = "{}",
                },
            ],
        });
        Assert.Equal(0, pe.FailedEntryCount);
    }

    [Fact]
    public async Task EventSourceStubs()
    {
        await _eb.ActivateEventSourceAsync(new ActivateEventSourceRequest
        {
            Name = "aws.partner/saas/foo",
        });

        await _eb.DeactivateEventSourceAsync(new DeactivateEventSourceRequest
        {
            Name = "aws.partner/saas/foo",
        });

        var src = await _eb.DescribeEventSourceAsync(new DescribeEventSourceRequest
        {
            Name = "aws.partner/saas/foo",
        });
        Assert.Equal("ENABLED", src.State);
    }

    // ── Deleting event bus removes associated rules ─────────────────────────────

    [Fact]
    public async Task DeleteEventBusCleansUpRulesAndTargets()
    {
        await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "cleanup-bus" });
        await _eb.PutRuleAsync(new PutRuleRequest
        {
            Name = "cleanup-rule",
            EventBusName = "cleanup-bus",
            ScheduleExpression = "rate(1 hour)",
            State = RuleState.ENABLED,
        });
        await _eb.PutTargetsAsync(new PutTargetsRequest
        {
            Rule = "cleanup-rule",
            EventBusName = "cleanup-bus",
            Targets = [new Target { Id = "t1", Arn = "arn:aws:lambda:us-east-1:000000000000:function:f" }],
        });

        await _eb.DeleteEventBusAsync(new DeleteEventBusRequest { Name = "cleanup-bus" });

        // Rules for that bus should be gone
        // Re-create the bus, list rules should be empty
        await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "cleanup-bus" });
        var rules = await _eb.ListRulesAsync(new ListRulesRequest { EventBusName = "cleanup-bus" });
        Assert.Empty(rules.Rules);
    }

    // ── Duplicate event bus fails ───────────────────────────────────────────────

    [Fact]
    public async Task CreateDuplicateEventBusFails()
    {
        await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "dup-bus" });

        var ex = await Assert.ThrowsAsync<ResourceAlreadyExistsException>(
            () => _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "dup-bus" }));
        Assert.Equal("ResourceAlreadyExistsException", ex.ErrorCode);
    }
}
