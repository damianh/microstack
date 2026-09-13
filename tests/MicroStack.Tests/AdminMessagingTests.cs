using System.Text;
using System.Text.Json;
using System.Net.Http.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using Microsoft.Extensions.DependencyInjection;

namespace MicroStack.Tests;

public sealed class AdminMessagingTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
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
    public async Task QueueRootAndDetailReadsDoNotMaterializeLargeMessageAttributes()
    {
        var handler = Registry.Resolve("sqs")!;
        var source = (IAdminResourceSource)handler;
        await Json(handler, "AmazonSQS.CreateQueue", new { QueueName = "live-read-cost" });
        var queue = source.GetAdminResources("sqs").Single();
        var url = queue.ReadFields!().Single(field => field.Name == "Queue URL").Value!;
        var payload = new
        {
            QueueUrl = url,
            MessageBody = new string('b', 32_000),
            MessageAttributes = new { large = new { DataType = "String", StringValue = new string('a', 32_000) } }
        };
        for (var i = 0; i < 100; i++)
            (await Json(handler, "AmazonSQS.SendMessage", payload)).StatusCode.ShouldBe(200);

        source.GetAdminResources("sqs").Single().ReadFields!();
        var start = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 10; i++)
        {
            var snapshot = source.GetAdminResources("sqs").Single();
            snapshot.ReadFields!();
            snapshot.ReadSummary!();
            snapshot.ReadConnections!();
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - start;
        allocated.ShouldBeLessThan(256_000,
            "Queue metadata reads must not copy/serialize the 6.4 MB of retained message payloads.");
        var messages = source.GetAdminResources("sqs").Single().ReadChildren!().ToArray();
        messages.Length.ShouldBe(100);
        messages[0].ReadFields!().Single(field => field.Name == "Message attributes").Value!
            .ShouldContain(new string('a', 32_000));
        messages[0].ReadContent!().Text.ShouldBe(new string('b', 32_000));
        source.GetAdminResources("sqs").Single().ReadSummary!()
            .Single(field => field.Name == "Visible messages").Value.ShouldBe("100");
    }

    [Fact]
    public async Task SqsSnapshotIsNonConsumingAndDistinguishesMessageStates()
    {
        var handler = Registry.Resolve("sqs")!;
        var source = (IAdminResourceSource)handler;
        await Query(handler, ("Action", "CreateQueue"), ("QueueName", "admin-queue"));
        var queue = source.GetAdminResources("sqs").Single();
        var url = queue.ReadFields!().Single(field => field.Name == "Queue URL").Value!;

        await Query(handler, ("Action", "SendMessage"), ("QueueUrl", url), ("MessageBody", "visible"));
        await Query(handler, ("Action", "SendMessage"), ("QueueUrl", url), ("MessageBody", "delayed"),
            ("DelaySeconds", "60"));
        await Json(handler, "AmazonSQS.ReceiveMessage",
            new { QueueUrl = url, MaxNumberOfMessages = 1, VisibilityTimeout = 60 });

        var first = source.GetAdminResources("sqs").Single();
        var before = first.ReadChildren!().Select(node => (node.Resource.Key.Id, node.Resource.Status)).ToArray();
        var second = source.GetAdminResources("sqs").Single();
        var after = second.ReadChildren!().Select(node => (node.Resource.Key.Id, node.Resource.Status)).ToArray();

        before.ShouldBe(after);
        before.Select(item => item.Status).ShouldContain("in-flight");
        before.Select(item => item.Status).ShouldContain("delayed");
        first.ReadChildren!().Single(node => node.Resource.Status == "delayed")
            .ReadContent!().Text.ShouldBe("delayed");
    }

    [Fact]
    public async Task QueueSummaryCountsWholeSnapshotAndExposesRetainedAttributesAndTags()
    {
        var handler = Registry.Resolve("sqs")!;
        var source = (IAdminResourceSource)handler;
        await Json(handler, "AmazonSQS.CreateQueue", new
        {
            QueueName = "snapshot-counts",
            Attributes = new { VisibilityTimeout = "75", MessageRetentionPeriod = "86400" },
            Tags = new { owner = "team-messaging", empty = "" }
        });
        var queue = source.GetAdminResources("sqs").Single();
        var url = queue.ReadFields!().Single(field => field.Name == "Queue URL").Value!;
        await Json(handler, "AmazonSQS.SendMessage", new { QueueUrl = url, MessageBody = """{"order":42}""" });
        await Json(handler, "AmazonSQS.SetQueueAttributes", new
        {
            QueueUrl = url, Attributes = new { DelaySeconds = "600" }
        });
        await Json(handler, "AmazonSQS.SendMessage", new { QueueUrl = url, MessageBody = "delayed" });
        await Json(handler, "AmazonSQS.SetQueueAttributes", new
        {
            QueueUrl = url, Attributes = new { DelaySeconds = "0" }
        });
        await Json(handler, "AmazonSQS.ReceiveMessage", new { QueueUrl = url, MaxNumberOfMessages = 1, VisibilityTimeout = 600 });
        await Json(handler, "AmazonSQS.SendMessage", new { QueueUrl = url, MessageBody = "visible" });
        var before = handler.GetState()!.Value.GetRawText();

        var detail = await fixture.HttpClient.GetFromJsonAsync(
            AdminUrl("sqs", "resource", [queue.Resource.Key]), AdminJsonContext.Default.AdminResourceDetail);
        var children = await fixture.HttpClient.GetFromJsonAsync(
            AdminUrl("sqs", "children", [queue.Resource.Key]) + "&pageSize=1",
            AdminJsonContext.Default.AdminPageAdminResourceSummary);

        detail.ShouldNotBeNull();
        children.ShouldNotBeNull();
        detail.Resource.Type.ShouldBe("Standard");
        detail.ChildKinds.Single().Id.ShouldBe("messages");
        detail.ChildKinds.Single().IsRoot.ShouldBeFalse();
        detail.Summary.Single(field => field.Name == "Visible messages").Value.ShouldBe("1");
        detail.Summary.Single(field => field.Name == "Delayed messages").Value.ShouldBe("1");
        detail.Summary.Single(field => field.Name == "In-flight messages").Value.ShouldBe("1");
        detail.Fields.Single(field => field.Name == "VisibilityTimeout").Value.ShouldBe("75");
        detail.Fields.Single(field => field.Name == "MessageRetentionPeriod").Secondary.ShouldBeTrue();
        detail.Fields.Single(field => field.Name == "Tag: owner").Value.ShouldBe("team-messaging");
        detail.Fields.Single(field => field.Name == "Tag: empty").Value.ShouldBe("");
        children.Items.Count.ShouldBe(1);
        children.NextCursor.ShouldNotBeNull();
        children.Items[0].Type.ShouldBe("Message");
        children.Items[0].Summary.Single(field => field.Name == "Sent").Format.ShouldBe("datetime");
        JsonSerializer.Serialize(children, AdminJsonContext.Default.AdminPageAdminResourceSummary)
            .ShouldNotContain("order");

        await fixture.HttpClient.GetFromJsonAsync(AdminUrl("sqs", "connections", [queue.Resource.Key]),
            AdminJsonContext.Default.AdminPageAdminConnection);
        handler.GetState()!.Value.GetRawText().ShouldBe(before);
    }

    [Fact]
    public async Task EmptyFifoQueueDeclaresMessagesWithoutInventingMetadata()
    {
        var handler = Registry.Resolve("sqs")!;
        await Json(handler, "AmazonSQS.CreateQueue", new
        {
            QueueName = "empty.fifo", Attributes = new { FifoQueue = "true", ContentBasedDeduplication = "true" }
        });
        var node = ((IAdminResourceSource)handler).GetAdminResources("sqs").Single();
        var detail = await fixture.HttpClient.GetFromJsonAsync(AdminUrl("sqs", "resource", [node.Resource.Key]),
            AdminJsonContext.Default.AdminResourceDetail);

        detail.ShouldNotBeNull();
        detail.Resource.Type.ShouldBe("FIFO");
        detail.ChildKinds.Single().Label.ShouldBe("Messages");
        detail.Summary.Select(field => field.Value).ShouldAllBe(value => value == "0");
        detail.Fields.ShouldNotContain(field => field.Name.StartsWith("Tag:", StringComparison.Ordinal));
        detail.Fields.Single(field => field.Name == "ContentBasedDeduplication").Value.ShouldBe("true");
        ((IAdminResourceSource)handler).GetAdminResourceKinds("sqs")
            .Where(kind => kind.IsRoot).Select(kind => kind.Id).ShouldBe(["queues"]);
    }

    [Theory]
    [InlineData("""{"order":{"id":42}}""", "json", "application/json")]
    [InlineData("{invalid json", "text", "text/plain")]
    [InlineData("plain message", "text", "text/plain")]
    public async Task MessageContentUsesValidatedPrettyJsonOrPlainText(string body, string kind, string contentType)
    {
        var handler = Registry.Resolve("sqs")!;
        await Json(handler, "AmazonSQS.CreateQueue", new { QueueName = "content" });
        var source = (IAdminResourceSource)handler;
        var queue = source.GetAdminResources("sqs").Single();
        var url = queue.ReadFields!().Single(field => field.Name == "Queue URL").Value!;
        await Json(handler, "AmazonSQS.SendMessage", new { QueueUrl = url, MessageBody = body });
        var message = source.GetAdminResources("sqs").Single().ReadChildren!().Single();
        var before = handler.GetState()!.Value.GetRawText();
        var content = await fixture.HttpClient.GetFromJsonAsync(
            AdminUrl("sqs", "content", [queue.Resource.Key, message.Resource.Key]), AdminJsonContext.Default.AdminContent);

        content.ShouldNotBeNull();
        content.Kind.ShouldBe(kind);
        content.ContentType.ShouldBe(contentType);
        if (kind == "json")
        {
            content.Text.ShouldNotBeNull();
            content.Text.ShouldContain("\n");
        }
        else
            content.Text.ShouldBe(body);
        handler.GetState()!.Value.GetRawText().ShouldBe(before);
    }

    [Theory]
    [InlineData(0, "text")]
    [InlineData(1, "oversized")]
    public async Task MessageContentPreservesExactUtf8PreviewLimit(int extraBytes, string expectedKind)
    {
        var handler = Registry.Resolve("sqs")!;
        await Json(handler, "AmazonSQS.CreateQueue", new { QueueName = "limit" });
        var source = (IAdminResourceSource)handler;
        var queue = source.GetAdminResources("sqs").Single();
        var url = queue.ReadFields!().Single(field => field.Name == "Queue URL").Value!;
        var body = new string('é', AdminData.PreviewMaxBytes / 2) + new string('a', extraBytes);
        await Json(handler, "AmazonSQS.SendMessage", new { QueueUrl = url, MessageBody = body });
        var message = source.GetAdminResources("sqs").Single().ReadChildren!().Single();
        var content = await fixture.HttpClient.GetFromJsonAsync(
            AdminUrl("sqs", "content", [queue.Resource.Key, message.Resource.Key]), AdminJsonContext.Default.AdminContent);
        content.ShouldNotBeNull();
        content.Kind.ShouldBe(expectedKind);
        content.Length.ShouldBe(AdminData.PreviewMaxBytes + extraBytes);
        if (extraBytes > 0)
            content.Text.ShouldBeNull();
    }

    [Fact]
    public async Task JsonFormattingCannotBypassInputOrOutputPreviewLimits()
    {
        var handler = Registry.Resolve("sqs")!;
        await Json(handler, "AmazonSQS.CreateQueue", new { QueueName = "json-limits" });
        var source = (IAdminResourceSource)handler;
        var queue = source.GetAdminResources("sqs").Single();
        var url = queue.ReadFields!().Single(field => field.Name == "Queue URL").Value!;
        foreach (var body in new[]
        {
            new string(' ', AdminData.PreviewMaxBytes) + "{}",
            "[" + string.Join(",", Enumerable.Repeat("0", 300_000)) + "]"
        })
        {
            await Json(handler, "AmazonSQS.SendMessage", new { QueueUrl = url, MessageBody = body });
        }
        foreach (var message in source.GetAdminResources("sqs").Single().ReadChildren!())
        {
            var content = await fixture.HttpClient.GetFromJsonAsync(
                AdminUrl("sqs", "content", [queue.Resource.Key, message.Resource.Key]), AdminJsonContext.Default.AdminContent);
            content.ShouldNotBeNull();
            content.Kind.ShouldBe("oversized");
            content.Text.ShouldBeNull();
        }
    }

    [Fact]
    public async Task SnsSubscriptionRowsExposeRetainedFilterAndConfirmationWithParentTopic()
    {
        var handler = Registry.Resolve("sns")!;
        var source = (IAdminResourceSource)handler;
        await Query(handler, ("Action", "CreateTopic"), ("Name", "subscription-metadata"));
        var topic = source.GetAdminResources("sns").Single();
        await Query(handler, ("Action", "Subscribe"), ("TopicArn", topic.Resource.Arn!),
            ("Protocol", "https"), ("Endpoint", "https://example.test/events"),
            ("Attributes.entry.1.key", "FilterPolicy"), ("Attributes.entry.1.value", """{"event":["created"]}"""));
        var child = source.GetAdminResources("sns").Single().ReadChildren!().Single();
        var rows = await fixture.HttpClient.GetFromJsonAsync(AdminUrl("sns", "children", [topic.Resource.Key]),
            AdminJsonContext.Default.AdminPageAdminResourceSummary);
        var connections = await fixture.HttpClient.GetFromJsonAsync(
            AdminUrl("sns", "connections", [topic.Resource.Key, child.Resource.Key]),
            AdminJsonContext.Default.AdminPageAdminConnection);

        rows.ShouldNotBeNull();
        var row = rows.Items.Single();
        row.Type.ShouldBe("https");
        row.Status.ShouldBe("pending");
        row.Summary.Single(field => field.Name == "Endpoint").Value.ShouldBe("https://example.test/events");
        row.Summary.Single(field => field.Name == "Filter policy").Value.ShouldBe("""{"event":["created"]}""");
        connections.ShouldNotBeNull();
        connections.Items.Single(link => link.Relation == "belongs-to").TargetPath.ShouldBe([topic.Resource.Key]);
        connections.Items.Single(link => link.Relation == "delivers-to").State.ShouldBe("external");
        source.GetAdminResourceKinds("sns").Where(kind => kind.IsRoot).Select(kind => kind.Id)
            .ShouldBe(["topics", "platform-applications"]);
    }

    [Fact]
    public async Task ScheduledRuleHasScheduleContentAndStableEmptyTargetCollection()
    {
        var handler = Registry.Resolve("events")!;
        await Json(handler, "AWSEvents.PutRule", new
        {
            Name = "scheduled", ScheduleExpression = "rate(5 minutes)", Description = "Retained schedule", State = "DISABLED"
        });
        var source = (IAdminResourceSource)handler;
        var bus = source.GetAdminResources("events").Single();
        var rule = bus.ReadChildren!().Single();
        var detail = await fixture.HttpClient.GetFromJsonAsync(
            AdminUrl("events", "resource", [bus.Resource.Key, rule.Resource.Key]),
            AdminJsonContext.Default.AdminResourceDetail);
        var content = await fixture.HttpClient.GetFromJsonAsync(
            AdminUrl("events", "content", [bus.Resource.Key, rule.Resource.Key]), AdminJsonContext.Default.AdminContent);

        detail.ShouldNotBeNull();
        detail.Resource.Type.ShouldBe("Scheduled rule");
        detail.Resource.Status.ShouldBe("DISABLED");
        detail.ChildKinds.Single().Label.ShouldBe("Targets");
        detail.ChildKinds.Single().IsRoot.ShouldBeFalse();
        detail.Fields.Single(field => field.Name == "Description").Value.ShouldBe("Retained schedule");
        content.ShouldNotBeNull();
        content.Kind.ShouldBe("text");
        content.Text.ShouldBe("rate(5 minutes)");
        source.GetAdminResourceKinds("events").Where(kind => kind.IsRoot)
            .ShouldNotContain(kind => kind.Id == "rules" || kind.Id == "targets");
    }

    [Fact]
    public async Task SnsShowsConfiguredSubscriptionsButNoMessageInbox()
    {
        var handler = Registry.Resolve("sns")!;
        var source = (IAdminResourceSource)handler;
        await Query(handler, ("Action", "CreateTopic"), ("Name", "admin-topic"));
        var topic = source.GetAdminResources("sns").Single(node => node.Resource.Key.Kind == "topics");
        await Query(handler, ("Action", "Subscribe"), ("TopicArn", topic.Resource.Arn!),
            ("Protocol", "sqs"), ("Endpoint", "arn:aws:sqs:us-east-1:000000000000:orders"));
        await Query(handler, ("Action", "Publish"), ("TopicArn", topic.Resource.Arn!), ("Message", "not-an-inbox"));

        topic = source.GetAdminResources("sns").Single(node => node.Resource.Key.Kind == "topics");
        var children = topic.ReadChildren!().ToArray();

        children.ShouldHaveSingleItem();
        children[0].Resource.Key.Kind.ShouldBe("subscriptions");
        children[0].ReadConnections!().Single().State.ShouldBe("configured");
        source.GetAdminResourceKinds("sns").ShouldNotContain(kind => kind.Id == "messages");
    }

    [Fact]
    public async Task EventBridgeExposesOnlyConfiguredTargetLinksAndMasksAuthentication()
    {
        var handler = Registry.Resolve("events")!;
        var source = (IAdminResourceSource)handler;
        await Json(handler, "AWSEvents.PutRule", new
        {
            Name = "admin-rule",
            EventPattern = """{"source":["orders"]}"""
        });
        await Json(handler, "AWSEvents.PutTargets", new
        {
            Rule = "admin-rule",
            Targets = new[] { new { Id = "queue", Arn = "arn:aws:sqs:us-east-1:000000000000:orders" } }
        });
        await Json(handler, "AWSEvents.CreateConnection", new
        {
            Name = "admin-connection",
            AuthorizationType = "API_KEY",
            AuthParameters = new { ApiKeyAuthParameters = new { ApiKeyName = "x-api-key", ApiKeyValue = "super-secret" } }
        });

        var roots = source.GetAdminResources("events").ToArray();
        var rule = roots.Single(node => node.Resource.Key.Kind == "event-buses"
                && node.Resource.Name == "default")
            .ReadChildren!().Single();
        var link = rule.ReadConnections!().Single();
        var connection = roots.Single(node => node.Resource.Key.Kind == "connections");

        link.State.ShouldBe("configured");
        link.TargetServiceId.ShouldBe("sqs");
        connection.ReadFields!().Single(field => field.Name == "Authentication").Value
            .ShouldBe(AdminData.MaskedValue);
        connection.ReadFields!().Any(field =>
            field.Value is not null && field.Value.Contains("super-secret", StringComparison.Ordinal)).ShouldBeFalse();
    }

    [Fact]
    public async Task SesSnapshotsAreAccountScopedAndUseExactPreviewLimit()
    {
        var handler = Registry.Resolve("ses")!;
        var source = (IAdminResourceSource)handler;
        AdminData.Text(new string('a', AdminData.PreviewMaxBytes)).Kind.ShouldBe("text");
        AdminData.Text(new string('a', AdminData.PreviewMaxBytes + 1)).Kind.ShouldBe("oversized");
        using (AccountContext.BeginScope("111111111111"))
        {
            await Query(handler, ("Action", "VerifyEmailIdentity"), ("EmailAddress", "one@example.test"));
            await Query(handler, ("Action", "CreateTemplate"), ("Template.TemplateName", "large"),
                ("Template.TextPart", new string('a', AdminData.PreviewMaxBytes)));
            source.GetAdminResources("ses").Single(node => node.Resource.Key.Kind == "templates")
                .ReadContent!().Kind.ShouldBe("oversized");
        }
        using (AccountContext.BeginScope("222222222222"))
        {
            await Query(handler, ("Action", "VerifyEmailIdentity"), ("EmailAddress", "two@example.test"));
            source.GetAdminResources("ses").Single(node => node.Resource.Key.Kind == "identities")
                .Resource.Name.ShouldBe("two@example.test");
        }
        using (AccountContext.BeginScope("111111111111"))
        {
            source.GetAdminResources("ses").Single(node => node.Resource.Key.Kind == "identities")
                .Resource.Name.ShouldBe("one@example.test");
        }
    }

    [Fact]
    public async Task StepFunctionsInspectionNeverStartsAnExecution()
    {
        var handler = Registry.Resolve("states")!;
        var source = (IAdminResourceSource)handler;
        const string definition = """{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}""";
        await Json(handler, "AWSStepFunctions.CreateStateMachine", new
        {
            name = "inspect-only",
            roleArn = "arn:aws:iam::000000000000:role/test",
            definition
        });

        var first = source.GetAdminResources("stepfunctions").Single();
        first.ReadContent!().Text.ShouldBe(definition);
        first.ReadChildren!().ShouldBeEmpty();
        source.GetAdminResources("stepfunctions").Single().ReadChildren!().ShouldBeEmpty();
    }

    private static Task<ServiceResponse> Query(IServiceHandler handler, params (string Key, string Value)[] values)
    {
        var body = string.Join("&", values.Select(value =>
            $"{Uri.EscapeDataString(value.Key)}={Uri.EscapeDataString(value.Value)}"));
        return handler.HandleAsync(new("POST", "/", new Dictionary<string, string>(),
            Encoding.UTF8.GetBytes(body), new Dictionary<string, string[]>()));
    }

    private static string AdminUrl(string service, string operation, AdminKey[] path) =>
        $"/_microstack/admin/v1/services/{service}/{operation}?path="
        + Uri.EscapeDataString(JsonSerializer.Serialize(path, AdminJsonContext.Default.AdminKeyArray));

    private static Task<ServiceResponse> Json(IServiceHandler handler, string target, object value) =>
        handler.HandleAsync(new("POST", "/", new Dictionary<string, string>
        {
            ["x-amz-target"] = target,
            ["content-type"] = "application/x-amz-json-1.0"
        }, JsonSerializer.SerializeToUtf8Bytes(value), new Dictionary<string, string[]>()));
}
