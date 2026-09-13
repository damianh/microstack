using System.Text;
using System.Text.Json;
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

    private static Task<ServiceResponse> Json(IServiceHandler handler, string target, object value) =>
        handler.HandleAsync(new("POST", "/", new Dictionary<string, string>
        {
            ["x-amz-target"] = target,
            ["content-type"] = "application/x-amz-json-1.0"
        }, JsonSerializer.SerializeToUtf8Bytes(value), new Dictionary<string, string[]>()));
}
