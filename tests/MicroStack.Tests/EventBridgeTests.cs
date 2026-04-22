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

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _eb.Dispose();
        return ValueTask.CompletedTask;
    }

    // ── Event bus lifecycle ─────────────────────────────────────────────────────

    [Fact]
    public async Task CreateEventBusAndDescribe()
    {
        var resp = await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "eb-bus-v2" });
        resp.EventBusArn.ShouldContain("eb-bus-v2");

        var buses = await _eb.ListEventBusesAsync(new ListEventBusesRequest());
        buses.EventBuses.ShouldContain(b => b.Name == "eb-bus-v2");

        var desc = await _eb.DescribeEventBusAsync(new DescribeEventBusRequest { Name = "eb-bus-v2" });
        desc.Name.ShouldBe("eb-bus-v2");
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
        desc.Description.ShouldBe("updated description");
    }

    [Fact]
    public async Task DeleteEventBus()
    {
        await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "eb-del-bus" });
        await _eb.DeleteEventBusAsync(new DeleteEventBusRequest { Name = "eb-del-bus" });

        var buses = await _eb.ListEventBusesAsync(new ListEventBusesRequest());
        buses.EventBuses.ShouldNotContain(b => b.Name == "eb-del-bus");
    }

    [Fact]
    public async Task DeleteDefaultEventBusFails()
    {
        var ex = await Should.ThrowAsync<AmazonEventBridgeException>(
            () => _eb.DeleteEventBusAsync(new DeleteEventBusRequest { Name = "default" }));
        ex.ErrorCode.ShouldBe("ValidationException");
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
        resp.RuleArn.ShouldNotBeEmpty();

        var rules = await _eb.ListRulesAsync(new ListRulesRequest { EventBusName = "eb-rule-bus" });
        rules.Rules.ShouldContain(r => r.Name == "eb-rule-v2");

        var described = await _eb.DescribeRuleAsync(new DescribeRuleRequest
        {
            Name = "eb-rule-v2",
            EventBusName = "eb-rule-bus",
        });
        described.Name.ShouldBe("eb-rule-v2");
        described.State.ShouldBe(RuleState.ENABLED);
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
        rules.Rules.ShouldContain(r => r.Name == "test-rule");
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

        var ex = await Should.ThrowAsync<ResourceNotFoundException>(
            () => _eb.DescribeRuleAsync(new DescribeRuleRequest { Name = "eb-del-v2" }));
        ex.ErrorCode.ShouldBe("ResourceNotFoundException");
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
        desc.State.ShouldBe(RuleState.DISABLED);

        await _eb.EnableRuleAsync(new EnableRuleRequest { Name = "toggle-rule" });
        desc = await _eb.DescribeRuleAsync(new DescribeRuleRequest { Name = "toggle-rule" });
        desc.State.ShouldBe(RuleState.ENABLED);
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
        resp.Targets.Count.ShouldBe(2);
        var ids = resp.Targets.Select(t => t.Id).ToHashSet();
        ids.ShouldContain("t1");
        ids.ShouldContain("t2");
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
        before.Targets.Count.ShouldBe(2);

        await _eb.RemoveTargetsAsync(new RemoveTargetsRequest
        {
            Rule = "eb-rm-v2",
            Ids = ["rm1"],
        });

        var after = await _eb.ListTargetsByRuleAsync(new ListTargetsByRuleRequest { Rule = "eb-rm-v2" });
        after.Targets.ShouldHaveSingleItem();
        after.Targets[0].Id.ShouldBe("rm2");
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
        sorted.ShouldBe(["rule-a", "rule-b"]);
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
        p1.RuleNames.ShouldHaveSingleItem();
        p1.NextToken.ShouldNotBeNull();

        var p2 = await _eb.ListRuleNamesByTargetAsync(new ListRuleNamesByTargetRequest
        {
            TargetArn = fnArn,
            Limit = 1,
            NextToken = p1.NextToken,
        });
        p2.RuleNames.ShouldHaveSingleItem();
        p2.RuleNames[0].ShouldNotBe(p1.RuleNames[0]);
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

        resp.FailedEntryCount.ShouldBe(0);
        resp.Entries.Count.ShouldBe(2);
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

        resp.FailedEntryCount.ShouldBe(0);
        resp.Entries.Count.ShouldBe(3);
        resp.Entries.ShouldAllBe(e => !string.IsNullOrEmpty(e.EventId));
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

        resp.Result.ShouldBe(true);
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

        resp.Result.ShouldBe(false);
    }

    [Fact]
    public async Task TestEventPatternInvalidEvent()
    {
        var ex = await Should.ThrowAsync<InvalidEventPatternException>(
            () => _eb.TestEventPatternAsync(new TestEventPatternRequest
            {
                Event = "not-json",
                EventPattern = "{}",
            }));
        ex.ErrorCode.ShouldBe("InvalidEventPatternException");
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
        tagMap["stage"].ShouldBe("dev");
        tagMap["team"].ShouldBe("ops");

        await _eb.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceARN = arn,
            TagKeys = ["stage"],
        });

        var tags2 = await _eb.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceARN = arn });
        tags2.Tags.ShouldNotContain(t => t.Key == "stage");
        tags2.Tags.ShouldContain(t => t.Key == "team");
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
        resp.ArchiveArn.ShouldNotBeEmpty();

        var desc = await _eb.DescribeArchiveAsync(new DescribeArchiveRequest { ArchiveName = archiveName });
        desc.ArchiveName.ShouldBe(archiveName);
        desc.RetentionDays.ShouldBe(7);

        var archives = await _eb.ListArchivesAsync(new ListArchivesRequest());
        archives.Archives.ShouldContain(a => a.ArchiveName == archiveName);

        await _eb.DeleteArchiveAsync(new DeleteArchiveRequest { ArchiveName = archiveName });

        var archives2 = await _eb.ListArchivesAsync(new ListArchivesRequest());
        archives2.Archives.ShouldNotContain(a => a.ArchiveName == archiveName);
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
        desc.Description.ShouldBe("new desc");
        desc.RetentionDays.ShouldBe(30);
        desc.EventPattern.ShouldContain("app");
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
        start.State.Value.ShouldBe("RUNNING");

        var desc = await _eb.DescribeReplayAsync(new DescribeReplayRequest { ReplayName = repName });
        desc.ReplayName.ShouldBe(repName);
        desc.State.Value.ShouldBe("RUNNING");

        var listed = await _eb.ListReplaysAsync(new ListReplaysRequest { NamePrefix = repName });
        listed.Replays.ShouldContain(r => r.ReplayName == repName);

        var cancel = await _eb.CancelReplayAsync(new CancelReplayRequest { ReplayName = repName });
        cancel.State.Value.ShouldBe("CANCELLED");

        var desc2 = await _eb.DescribeReplayAsync(new DescribeReplayRequest { ReplayName = repName });
        desc2.State.Value.ShouldBe("CANCELLED");

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
        resp.ConnectionArn.ShouldNotBeEmpty();

        var desc = await _eb.DescribeConnectionAsync(new DescribeConnectionRequest { Name = "test-conn" });
        desc.Name.ShouldBe("test-conn");

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
        result.ConnectionState.ShouldBe(ConnectionState.DEAUTHORIZED);

        var desc = await _eb.DescribeConnectionAsync(new DescribeConnectionRequest { Name = "deauth-conn" });
        desc.ConnectionState.ShouldBe(ConnectionState.DEAUTHORIZED);

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
        resp.ApiDestinationArn.ShouldNotBeEmpty();

        var desc = await _eb.DescribeApiDestinationAsync(
            new DescribeApiDestinationRequest { Name = "test-apid" });
        desc.Name.ShouldBe("test-apid");

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
        desc.State.ShouldBe(EndpointState.ACTIVE);
        desc.Arn.ShouldNotBeEmpty();

        var listed = await _eb.ListEndpointsAsync(new ListEndpointsRequest());
        listed.Endpoints.ShouldContain(e => e.Name == "my-global-endpoint");

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
        resp.EventSourceArn.ShouldNotBeEmpty();

        await _eb.DescribePartnerEventSourceAsync(
            new DescribePartnerEventSourceRequest { Name = "aws.partner/saas/src" });

        var list = await _eb.ListPartnerEventSourcesAsync(
            new ListPartnerEventSourcesRequest { NamePrefix = "aws.partner/saas/src" });
        list.PartnerEventSources.ShouldNotBeEmpty();

        await _eb.DeletePartnerEventSourceAsync(new DeletePartnerEventSourceRequest
        {
            Name = "aws.partner/saas/src",
            Account = "111111111111",
        });

        var accounts = await _eb.ListPartnerEventSourceAccountsAsync(
            new ListPartnerEventSourceAccountsRequest { EventSourceName = "aws.partner/saas/accts" });
        accounts.PartnerEventSourceAccounts.ShouldBeEmpty();

        var eventSources = await _eb.ListEventSourcesAsync(new ListEventSourcesRequest());
        eventSources.EventSources.ShouldBeEmpty();

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
        pe.FailedEntryCount.ShouldBe(0);
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
        src.State.Value.ShouldBe("ENABLED");
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
        rules.Rules.ShouldBeEmpty();
    }

    // ── Duplicate event bus fails ───────────────────────────────────────────────

    [Fact]
    public async Task CreateDuplicateEventBusFails()
    {
        await _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "dup-bus" });

        var ex = await Should.ThrowAsync<ResourceAlreadyExistsException>(
            () => _eb.CreateEventBusAsync(new CreateEventBusRequest { Name = "dup-bus" }));
        ex.ErrorCode.ShouldBe("ResourceAlreadyExistsException");
    }
}
