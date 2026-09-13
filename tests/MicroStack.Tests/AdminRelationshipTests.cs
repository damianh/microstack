using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using Microsoft.Extensions.DependencyInjection;

namespace MicroStack.Tests;

public sealed class AdminRelationshipTests(MicroStackFixture fixture)
    : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private const string Account = "111111111111";
    private ServiceRegistry Registry => fixture.Factory.Services.GetRequiredService<ServiceRegistry>();

    public ValueTask InitializeAsync()
    {
        Registry.ResetAll();
        AccountContext.Reset();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        AccountContext.Reset();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task ReverseQueueLinksDeduplicateProjectionViewsAndRetainDistinctConfigurationsAndPaging()
    {
        using var scope = AccountContext.BeginScope(Account);
        var queue = await Queue("destination");
        var sourceQueue = await Queue("source", queue.Resource.Arn);
        await Topic("producer-a", queue.Resource.Arn!);
        await Topic("producer-b", queue.Resource.Arn!);
        await Rule("rule-a", queue.Resource.Arn!);
        await Rule("rule-b", queue.Resource.Arn!);
        var state = Registry.Resolve("sqs")!.GetState()!.Value.GetRawText();

        var detail = await Detail("sqs", [queue.Resource.Key]);
        var links = await Connections("sqs", [queue.Resource.Key]);
        detail.HasConnections.ShouldBeTrue();
        detail.ConnectionCount.ShouldBe(5);
        links.KnownTotal.ShouldBe(detail.ConnectionCount);
        links.Items.Count.ShouldBe(5);
        links.Items.Count(link => link.TargetServiceId == "sns").ShouldBe(2);
        links.Items.Count(link => link.TargetServiceId == "events").ShouldBe(2);
        links.Items.Single(link => link.Relation == "redrive-source").TargetPath.ShouldBe([sourceQueue.Resource.Key]);
        links.Items.ShouldAllBe(link => link.State == "configured");
        links.Items.Where(link => link.TargetServiceId == "events").ShouldAllBe(link => link.TargetPath!.Length == 3);
        links.Items.Where(link => link.TargetServiceId == "sns").ShouldAllBe(link => link.TargetPath!.Length == 2);

        var paged = new List<AdminConnection>();
        string? cursor = null;
        do
        {
            var page = await Connections("sqs", [queue.Resource.Key], "&pageSize=2"
                + (cursor is null ? "" : "&cursor=" + Uri.EscapeDataString(cursor)));
            page.KnownTotal.ShouldBe(5);
            paged.AddRange(page.Items);
            cursor = page.NextCursor;
        } while (cursor is not null);
        JsonSerializer.Serialize(paged.ToArray(), AdminJsonContext.Default.AdminConnectionArray)
            .ShouldBe(JsonSerializer.Serialize(links.Items.ToArray(), AdminJsonContext.Default.AdminConnectionArray));

        foreach (var link in links.Items)
        {
            var destination = await Detail(link.TargetServiceId!, link.TargetPath!);
            destination.Resource.Key.ShouldBe(link.TargetPath![^1]);
        }
        var outgoing = await Connections("sqs", [sourceQueue.Resource.Key]);
        outgoing.Items.Single(link => link.Relation == "redrive").TargetPath.ShouldBe([queue.Resource.Key]);
        Registry.Resolve("sqs")!.GetState()!.Value.GetRawText().ShouldBe(state);
    }

    [Fact]
    public async Task TopicAndBusParentContextRetainsOriginalSubscriptionAndTargetPaths()
    {
        using var scope = AccountContext.BeginScope(Account);
        var queue = await Queue("parent-context");
        var topic = await Topic("parent-topic", queue.Resource.Arn!);
        await Rule("parent-rule", queue.Resource.Arn!);
        var bus = Source("events").GetAdminResources("events").Single();
        var rule = bus.ReadChildren!().Single();
        var target = rule.ReadChildren!().Single();
        var subscription = topic.ReadChildren!().Single();

        var topicLinks = await Connections("sns", [topic.Resource.Key]);
        var busLinks = await Connections("events", [bus.Resource.Key]);
        var ruleLinks = await Connections("events", [bus.Resource.Key, rule.Resource.Key]);
        topicLinks.Items.ShouldHaveSingleItem();
        topicLinks.Items[0].SourceServiceId.ShouldBe("sns");
        topicLinks.Items[0].SourcePath.ShouldBe([topic.Resource.Key, subscription.Resource.Key]);
        topicLinks.Items[0].TargetPath.ShouldBe([queue.Resource.Key]);
        busLinks.Items.ShouldHaveSingleItem();
        busLinks.Items[0].SourceServiceId.ShouldBe("events");
        busLinks.Items[0].SourcePath.ShouldBe([bus.Resource.Key, rule.Resource.Key, target.Resource.Key]);
        ruleLinks.Items.Single(link => link.Relation == "belongs-to").TargetPath.ShouldBe([bus.Resource.Key]);
        ruleLinks.Items.Single(link => link.Relation == "targets").SourcePath
            .ShouldBe([bus.Resource.Key, rule.Resource.Key, target.Resource.Key]);
        (await Detail("sns", [topic.Resource.Key])).ConnectionCount.ShouldBe(topicLinks.KnownTotal);
        (await Detail("events", [bus.Resource.Key])).ConnectionCount.ShouldBe(busLinks.KnownTotal);
    }

    [Fact]
    public async Task ReverseLinksRequireExactFullArnAndSameAccount()
    {
        AdminNode queue;
        using (AccountContext.BeginScope(Account))
        {
            queue = await Queue("same-name");
            await Topic("matching", queue.Resource.Arn!);
            await Topic("different-region", queue.Resource.Arn!.Replace(":us-east-1:", ":eu-west-1:", StringComparison.Ordinal));
            await Topic("similar-name", queue.Resource.Arn + "-suffix");
            await Topic("other-account", queue.Resource.Arn.Replace(Account, "222222222222", StringComparison.Ordinal));
        }
        using (AccountContext.BeginScope("222222222222"))
        {
            var otherQueue = await Queue("same-name");
            await Topic("foreign-producer", queue.Resource.Arn!);
            await Topic("own-producer", otherQueue.Resource.Arn!);
            await Rule("foreign-rule", queue.Resource.Arn!);
        }

        var own = await Connections("sqs", [queue.Resource.Key]);
        own.Items.ShouldHaveSingleItem();
        own.Items[0].Label.ShouldContain("matching");
        using var foreignPath = await fixture.HttpClient.GetAsync(
            Url("sqs", "resource", [queue.Resource.Key], account: "222222222222"));
        foreignPath.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task MissingExternalAndDisabledDestinationsAreTruthful()
    {
        using var scope = AccountContext.BeginScope(Account);
        var queue = await Queue("deleted");
        var topic = await Topic("links", queue.Resource.Arn!);
        await Query(Registry.Resolve("sns")!, ("Action", "Subscribe"), ("TopicArn", topic.Resource.Arn!),
            ("Protocol", "sqs"), ("Endpoint", queue.Resource.Arn!.Replace(Account, "222222222222", StringComparison.Ordinal)));
        await Query(Registry.Resolve("sns")!, ("Action", "Subscribe"), ("TopicArn", topic.Resource.Arn!),
            ("Protocol", "https"), ("Endpoint", "https://example.test/webhook"));
        await Json(Registry.Resolve("sqs")!, "AmazonSQS.DeleteQueue", new
        {
            QueueUrl = queue.ReadFields!().Single(field => field.Name == "Queue URL").Value!
        });

        var links = await Connections("sns", [topic.Resource.Key]);
        links.Items.Count(link => link.State == "missing").ShouldBe(1);
        links.Items.Count(link => link.State == "external").ShouldBe(2);
        links.Items.Single(link => link.State == "missing").TargetPath.ShouldBe([queue.Resource.Key]);

        var disabledRegistry = new ServiceRegistry(new MicroStackOptions { Services = "sns" });
        disabledRegistry.Register(Registry.Resolve("sns")!);
        var current = Source("sns").GetAdminResources("sns").Single();
        var disabled = new AdminRelationshipResolver(disabledRegistry, Account)
            .Read(current, "sns", [current.Resource.Key]);
        disabled.Single(link => link.TargetPath is { } path && path[0] == queue.Resource.Key).State.ShouldBe("disabled");
        disabled.Count(link => link.State == "external").ShouldBe(2);
    }

    [Fact]
    public async Task StepFunctionsTargetUsesCatalogIdAndExactIdentity()
    {
        using var scope = AccountContext.BeginScope(Account);
        await Json(Registry.Resolve("states")!, "AWSStepFunctions.CreateStateMachine", new
        {
            name = "configured-workflow",
            roleArn = $"arn:aws:iam::{Account}:role/test",
            definition = """{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}"""
        });
        var machine = Source("states").GetAdminResources("stepfunctions").Single();
        await Rule("workflow-rule", machine.Resource.Arn!);
        var bus = Source("events").GetAdminResources("events").Single();
        var links = await Connections("events", [bus.Resource.Key]);

        var link = links.Items.Single();
        link.TargetServiceId.ShouldBe("stepfunctions");
        link.TargetPath.ShouldBe([machine.Resource.Key]);
        link.State.ShouldBe("configured");
        (await Detail(link.TargetServiceId!, link.TargetPath!)).Resource.Key.ShouldBe(machine.Resource.Key);
        Source("states").GetAdminResources("stepfunctions").Single().ReadChildren!().ShouldBeEmpty();
    }

    [Fact]
    public async Task DisabledRuleSourceRetainsFullReturnPath()
    {
        using var scope = AccountContext.BeginScope(Account);
        var queue = await Queue("disabled-rule-target");
        await Rule("disabled-source", queue.Resource.Arn!);
        await Json(Registry.Resolve("events")!, "AWSEvents.DisableRule", new { Name = "disabled-source" });
        var links = await Connections("sqs", [queue.Resource.Key]);
        var link = links.Items.Single();
        link.State.ShouldBe("disabled");
        link.TargetPath!.Length.ShouldBe(3);
        link.TargetPath[1].Kind.ShouldBe("rules");
    }

    [Fact]
    public async Task MalformedRetainedRedrivePolicyRemainsVisibleAsUnavailableConfiguration()
    {
        using var scope = AccountContext.BeginScope(Account);
        var queue = await Queue("malformed-redrive");
        await Json(Registry.Resolve("sqs")!, "AmazonSQS.SetQueueAttributes", new
        {
            QueueUrl = queue.ReadFields!().Single(field => field.Name == "Queue URL").Value!,
            Attributes = new { RedrivePolicy = "{invalid" }
        });
        var detail = await Detail("sqs", [queue.Resource.Key]);
        var links = await Connections("sqs", [queue.Resource.Key]);
        detail.Fields.Single(field => field.Name == "RedrivePolicy").Value.ShouldBe("{invalid");
        detail.ConnectionCount.ShouldBe(1);
        links.Items.Single().State.ShouldBe("unavailable");
        links.Items.Single().Label.ShouldContain("Invalid redrive policy");
    }

    [Fact]
    public void OtherProvidersKeepUnverifiedConfiguredLinkSemantics()
    {
        AdminKey[] sourcePath = [new("instances", "database")];
        AdminKey[] destinationPath = [new("vpcs", "vpc-retained-configuration")];
        var configured = new AdminConnection("VPC", "runs-in", "ec2", destinationPath);
        var node = AdminData.Node("instances", "database", "database") with
        {
            ReadConnections = () => [configured]
        };
        var registry = new ServiceRegistry(new MicroStackOptions());

        var links = new AdminRelationshipResolver(registry, Account).Read(node, "rds", sourcePath);

        links.ShouldHaveSingleItem();
        links[0].ShouldBe(configured);
        links[0].State.ShouldBe("configured");
        links[0].TargetPath.ShouldBe(destinationPath);
    }

    [Fact]
    public void RelationshipSnapshotsNeverReadProviderHierarchiesPayloadsOrCallAwsOperations()
    {
        using var scope = AccountContext.BeginScope(Account);
        AdminKey[] queuePath = [new("queues", $"arn:aws:sqs:us-east-1:{Account}:metadata-only")];
        var topic = new AdminKey("topics", $"arn:aws:sns:us-east-1:{Account}:metadata-only");
        AdminKey[] subscriptionA = [topic, new("subscriptions", topic.Id + ":a")];
        AdminKey[] subscriptionB = [topic, new("subscriptions", topic.Id + ":b")];
        var first = new AdminConfiguredRelationship("sns", subscriptionA, "same label",
            new("Queue", "delivers-to", "sqs", queuePath));
        var second = first with { SourcePath = subscriptionB };
        var registry = new ServiceRegistry(new MicroStackOptions());
        registry.Register(new MetadataOnlyHandler("sqs", new([new(queuePath, "queue")], [])));
        registry.Register(new MetadataOnlyHandler("sns", new(
            [new(subscriptionA, "subscription"), new(subscriptionB, "subscription")],
            [first, first, second])));
        var links = new AdminRelationshipResolver(registry, Account)
            .Read(AdminData.Node("queues", queuePath[0].Id, "queue"), "sqs", queuePath);

        links.Count.ShouldBe(2);
        links.Select(link => link.TargetPath![^1].Id).ShouldBe([subscriptionA[^1].Id, subscriptionB[^1].Id]);
        links.ShouldAllBe(link => link.State == "configured");
    }

    private IAdminResourceSource Source(string service) => (IAdminResourceSource)Registry.Resolve(service)!;

    private async Task<AdminNode> Queue(string name, string? deadLetterArn = null)
    {
        var attributes = new Dictionary<string, string>();
        if (deadLetterArn is not null)
            attributes["RedrivePolicy"] = JsonSerializer.Serialize(new { deadLetterTargetArn = deadLetterArn, maxReceiveCount = "3" });
        await Json(Registry.Resolve("sqs")!, "AmazonSQS.CreateQueue", new { QueueName = name, Attributes = attributes });
        return Source("sqs").GetAdminResources("sqs").Single(node => node.Resource.Name == name);
    }

    private async Task<AdminNode> Topic(string name, string targetArn)
    {
        var handler = Registry.Resolve("sns")!;
        await Query(handler, ("Action", "CreateTopic"), ("Name", name));
        var topic = Source("sns").GetAdminResources("sns").Single(node => node.Resource.Name == name);
        await Query(handler, ("Action", "Subscribe"), ("TopicArn", topic.Resource.Arn!),
            ("Protocol", "sqs"), ("Endpoint", targetArn));
        return Source("sns").GetAdminResources("sns").Single(node => node.Resource.Name == name);
    }

    private async Task Rule(string name, string targetArn)
    {
        var handler = Registry.Resolve("events")!;
        await Json(handler, "AWSEvents.PutRule", new { Name = name, EventPattern = """{"source":["orders"]}""" });
        await Json(handler, "AWSEvents.PutTargets", new
        {
            Rule = name, Targets = new[] { new { Id = "configured-target", Arn = targetArn, Input = """{"not":"relationship metadata"}""" } }
        });
    }

    private async Task<AdminResourceDetail> Detail(string service, AdminKey[] path) =>
        (await fixture.HttpClient.GetFromJsonAsync(Url(service, "resource", path),
            AdminJsonContext.Default.AdminResourceDetail))!;

    private async Task<AdminPage<AdminConnection>> Connections(string service, AdminKey[] path, string query = "") =>
        (await fixture.HttpClient.GetFromJsonAsync(Url(service, "connections", path) + query,
            AdminJsonContext.Default.AdminPageAdminConnection))!;

    private static string Url(string service, string operation, AdminKey[] path, string account = Account) =>
        $"/_microstack/admin/v1/services/{service}/{operation}?accountId={account}&path="
        + Uri.EscapeDataString(JsonSerializer.Serialize(path, AdminJsonContext.Default.AdminKeyArray));

    private static async Task Query(IServiceHandler handler, params (string Key, string Value)[] values)
    {
        var body = string.Join("&", values.Select(value =>
            $"{Uri.EscapeDataString(value.Key)}={Uri.EscapeDataString(value.Value)}"));
        var response = await handler.HandleAsync(new("POST", "/", new Dictionary<string, string>(),
            Encoding.UTF8.GetBytes(body), new Dictionary<string, string[]>()));
        response.StatusCode.ShouldBe(200);
    }

    private static async Task Json(IServiceHandler handler, string target, object value)
    {
        var response = await handler.HandleAsync(new("POST", "/", new Dictionary<string, string>
        {
            ["x-amz-target"] = target,
            ["content-type"] = "application/x-amz-json-1.0"
        }, JsonSerializer.SerializeToUtf8Bytes(value), new Dictionary<string, string[]>()));
        response.StatusCode.ShouldBe(200);
    }

    private sealed class MetadataOnlyHandler(string service, AdminRelationshipSnapshot snapshot)
        : IServiceHandler, IAdminResourceSource, IAdminRelationshipSource
    {
        public string ServiceName => service;
        public AdminRelationshipSnapshot GetAdminRelationshipSnapshot() => snapshot;
        public IEnumerable<AdminNode> GetAdminResources(string serviceId) =>
            throw new InvalidOperationException("Relationships must not inspect payload-bearing resource hierarchies.");
        public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) => [];
        public Task<ServiceResponse> HandleAsync(ServiceRequest request) =>
            throw new InvalidOperationException("Inspection must not perform AWS operations.");
        public JsonElement? GetState() => throw new InvalidOperationException("Inspection must not read persisted payloads.");
        public void RestoreState(JsonElement state) => throw new InvalidOperationException("Inspection must not mutate state.");
        public void Reset() { }
    }
}
