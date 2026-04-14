using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.AspNetCore.Mvc.Testing;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the SQS service handler.
/// Uses the AWS SDK for .NET v4 pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_sqs.py.
/// </summary>
public sealed class SqsTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonSQSClient _sqs;

    public SqsTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _sqs = CreateSqsClient(fixture);
    }

    private static AmazonSQSClient CreateSqsClient(MicroStackFixture fixture)
    {
        // AWS SDK v4 builds request URIs with DangerousDisablePathAndQueryCanonicalization,
        // which is incompatible with TestServer's ClientHandler (it calls GetComponents() on
        // the URI which throws for such URIs). We work around this by injecting a delegating
        // handler that rewrites the URI to a plain canonical form before forwarding to the
        // test server handler — stripping the dangerous flag.
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient   = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSQSConfig
        {
            RegionEndpoint     = RegionEndpoint.USEast1,
            ServiceURL         = "http://localhost/",
            HttpClientFactory  = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSQSClient(new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        // Reset state before each test class run
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _sqs.Dispose();
        return Task.CompletedTask;
    }

    // ── Basic queue operations ──────────────────────────────────────────────────

    [Fact]
    public async Task CreateQueueAndGetUrl()
    {
        var created = await _sqs.CreateQueueAsync("test-queue-basic");
        created.QueueUrl.ShouldNotBeEmpty();
        created.QueueUrl.ShouldContain("test-queue-basic");

        var getUrl = await _sqs.GetQueueUrlAsync("test-queue-basic");
        getUrl.QueueUrl.ShouldBe(created.QueueUrl);
    }

    [Fact]
    public async Task CreateQueueIsIdempotent()
    {
        await _sqs.CreateQueueAsync("idem-queue");
        var second = await _sqs.CreateQueueAsync("idem-queue");
        second.QueueUrl.ShouldContain("idem-queue");
    }

    [Fact]
    public async Task ListQueues()
    {
        await _sqs.CreateQueueAsync("list-q-1");
        await _sqs.CreateQueueAsync("list-q-2");

        var list = await _sqs.ListQueuesAsync("list-q-");
        list.QueueUrls.Count.ShouldBe(2);
    }

    [Fact]
    public async Task DeleteQueue()
    {
        var q = await _sqs.CreateQueueAsync("del-queue");
        await _sqs.DeleteQueueAsync(q.QueueUrl);

        var list = await _sqs.ListQueuesAsync("del-queue");
        list.QueueUrls.ShouldBeEmpty();
    }

    // ── Message operations ──────────────────────────────────────────────────────

    [Fact]
    public async Task SendAndReceiveMessage()
    {
        var q = await _sqs.CreateQueueAsync("send-recv-queue");

        var sent = await _sqs.SendMessageAsync(q.QueueUrl, "hello world");
        sent.MessageId.ShouldNotBeEmpty();
        sent.MD5OfMessageBody.ShouldNotBeEmpty();

        var recv = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl            = q.QueueUrl,
            MaxNumberOfMessages = 1,
        });
        recv.Messages.ShouldHaveSingleItem();
        recv.Messages[0].Body.ShouldBe("hello world");
        recv.Messages[0].ReceiptHandle.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task DeleteMessage()
    {
        var q = await _sqs.CreateQueueAsync("del-msg-queue");
        await _sqs.SendMessageAsync(q.QueueUrl, "to be deleted");

        var recv = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl            = q.QueueUrl,
            MaxNumberOfMessages = 1,
        });
        recv.Messages.ShouldHaveSingleItem();

        await _sqs.DeleteMessageAsync(q.QueueUrl, recv.Messages[0].ReceiptHandle);

        // Queue should now be empty (visibility timeout still active, but message is gone)
        // Re-receive with visibility=0 to confirm deletion
        var attrs = await _sqs.GetQueueAttributesAsync(q.QueueUrl,
            ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]);
        attrs.Attributes["ApproximateNumberOfMessages"].ShouldBe("0");
        attrs.Attributes["ApproximateNumberOfMessagesNotVisible"].ShouldBe("0");
    }

    [Fact]
    public async Task GetQueueAttributes()
    {
        var q = await _sqs.CreateQueueAsync("attrs-queue");
        var attrs = await _sqs.GetQueueAttributesAsync(q.QueueUrl, ["All"]);

        attrs.Attributes.ContainsKey("QueueArn").ShouldBe(true);
        attrs.Attributes["QueueArn"].ShouldContain("attrs-queue");
        attrs.Attributes["VisibilityTimeout"].ShouldBe("30");
    }

    [Fact]
    public async Task SetQueueAttributes()
    {
        var q = await _sqs.CreateQueueAsync("setattrs-queue");
        await _sqs.SetQueueAttributesAsync(q.QueueUrl,
            new Dictionary<string, string> { ["VisibilityTimeout"] = "60" });

        var attrs = await _sqs.GetQueueAttributesAsync(q.QueueUrl, ["VisibilityTimeout"]);
        attrs.Attributes["VisibilityTimeout"].ShouldBe("60");
    }

    [Fact]
    public async Task PurgeQueue()
    {
        var q = await _sqs.CreateQueueAsync("purge-queue");
        await _sqs.SendMessageAsync(q.QueueUrl, "msg1");
        await _sqs.SendMessageAsync(q.QueueUrl, "msg2");

        await _sqs.PurgeQueueAsync(q.QueueUrl);

        var attrs = await _sqs.GetQueueAttributesAsync(q.QueueUrl,
            ["ApproximateNumberOfMessages"]);
        attrs.Attributes["ApproximateNumberOfMessages"].ShouldBe("0");
    }

    // ── Batch operations ────────────────────────────────────────────────────────

    [Fact]
    public async Task SendMessageBatch()
    {
        var q = await _sqs.CreateQueueAsync("batch-send-queue");

        var resp = await _sqs.SendMessageBatchAsync(q.QueueUrl,
        [
            new SendMessageBatchRequestEntry { Id = "1", MessageBody = "body-1" },
            new SendMessageBatchRequestEntry { Id = "2", MessageBody = "body-2" },
        ]);

        resp.Successful.Count.ShouldBe(2);
        resp.Failed.ShouldBeEmpty();
    }

    [Fact]
    public async Task DeleteMessageBatch()
    {
        var q = await _sqs.CreateQueueAsync("batch-del-queue");
        await _sqs.SendMessageAsync(q.QueueUrl, "msg-a");
        await _sqs.SendMessageAsync(q.QueueUrl, "msg-b");

        var recv = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl            = q.QueueUrl,
            MaxNumberOfMessages = 10,
        });

        var delResp = await _sqs.DeleteMessageBatchAsync(q.QueueUrl,
            recv.Messages.Select((m, i) =>
                new DeleteMessageBatchRequestEntry
                {
                    Id            = i.ToString(),
                    ReceiptHandle = m.ReceiptHandle,
                }).ToList());

        delResp.Successful.Count.ShouldBe(recv.Messages.Count);
        delResp.Failed.ShouldBeEmpty();
    }

    // ── Message attributes ──────────────────────────────────────────────────────

    [Fact]
    public async Task MessageWithUserAttributes()
    {
        var q = await _sqs.CreateQueueAsync("msgattr-queue");

        await _sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl    = q.QueueUrl,
            MessageBody = "body-with-attrs",
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                ["Color"] = new() { DataType = "String", StringValue = "blue" },
                ["Count"] = new() { DataType = "Number", StringValue = "42"   },
            },
        });

        var recv = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl            = q.QueueUrl,
            MaxNumberOfMessages = 1,
            MessageAttributeNames = ["All"],
        });

        recv.Messages.ShouldHaveSingleItem();
        var msg = recv.Messages[0];
        msg.MessageAttributes.ContainsKey("Color").ShouldBe(true);
        msg.MessageAttributes["Color"].StringValue.ShouldBe("blue");
        msg.MessageAttributes["Count"].StringValue.ShouldBe("42");
    }

    // ── System attributes ───────────────────────────────────────────────────────

    [Fact]
    public async Task ReceiveMessageWithSystemAttributes()
    {
        var q = await _sqs.CreateQueueAsync("sysattr-queue");
        await _sqs.SendMessageAsync(q.QueueUrl, "sys-attr-body");

        var recv = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl                    = q.QueueUrl,
            MaxNumberOfMessages         = 1,
            MessageSystemAttributeNames = ["All"],
        });

        recv.Messages.ShouldHaveSingleItem();
        var attrs = recv.Messages[0].Attributes;
        attrs.ContainsKey("SentTimestamp").ShouldBe(true);
        attrs.ContainsKey("ApproximateReceiveCount").ShouldBe(true);
        attrs["ApproximateReceiveCount"].ShouldBe("1");
    }

    // ── Queue tags ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task TagAndUntagQueue()
    {
        var q = await _sqs.CreateQueueAsync("tag-queue");
        await _sqs.TagQueueAsync(new Amazon.SQS.Model.TagQueueRequest
        {
            QueueUrl = q.QueueUrl,
            Tags     = new Dictionary<string, string>
            {
                ["env"] = "test",
                ["app"] = "microstack",
            },
        });

        var tags = await _sqs.ListQueueTagsAsync(new Amazon.SQS.Model.ListQueueTagsRequest
        {
            QueueUrl = q.QueueUrl,
        });
        tags.Tags["env"].ShouldBe("test");
        tags.Tags["app"].ShouldBe("microstack");

        await _sqs.UntagQueueAsync(new Amazon.SQS.Model.UntagQueueRequest
        {
            QueueUrl = q.QueueUrl,
            TagKeys  = ["env"],
        });
        tags = await _sqs.ListQueueTagsAsync(new Amazon.SQS.Model.ListQueueTagsRequest
        {
            QueueUrl = q.QueueUrl,
        });
        tags.Tags.ContainsKey("env").ShouldBe(false);
        tags.Tags.ContainsKey("app").ShouldBe(true);
    }

    // ── FIFO queue ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task FifoQueueBasicOperations()
    {
        var q = await _sqs.CreateQueueAsync("fifo-queue.fifo");

        var s1 = await _sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl               = q.QueueUrl,
            MessageBody            = "fifo-msg-1",
            MessageGroupId         = "g1",
            MessageDeduplicationId = "d1",
        });
        s1.SequenceNumber.ShouldNotBeEmpty();

        var recv = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl            = q.QueueUrl,
            MaxNumberOfMessages = 1,
        });
        recv.Messages.ShouldHaveSingleItem();
        recv.Messages[0].Body.ShouldBe("fifo-msg-1");
    }

    [Fact]
    public async Task FifoQueueDeduplication()
    {
        var q = await _sqs.CreateQueueAsync("dedup-queue.fifo");

        // Send same dedup ID twice
        await _sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl               = q.QueueUrl,
            MessageBody            = "original",
            MessageGroupId         = "g1",
            MessageDeduplicationId = "same-dedup-id",
        });
        await _sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl               = q.QueueUrl,
            MessageBody            = "duplicate",
            MessageGroupId         = "g1",
            MessageDeduplicationId = "same-dedup-id",
        });

        // Should only have 1 message
        var attrs = await _sqs.GetQueueAttributesAsync(q.QueueUrl,
            ["ApproximateNumberOfMessages"]);
        attrs.Attributes["ApproximateNumberOfMessages"].ShouldBe("1");
    }

    // ── Dead-letter queue ───────────────────────────────────────────────────────

    [Fact]
    public async Task DeadLetterQueueRedrive()
    {
        var dlq = await _sqs.CreateQueueAsync("my-dlq");
        var dlqAttrs = await _sqs.GetQueueAttributesAsync(dlq.QueueUrl, ["QueueArn"]);
        var dlqArn   = dlqAttrs.Attributes["QueueArn"];

        var src = await _sqs.CreateQueueAsync("src-queue-dlq");
        await _sqs.SetQueueAttributesAsync(src.QueueUrl, new Dictionary<string, string>
        {
            ["RedrivePolicy"] = System.Text.Json.JsonSerializer.Serialize(new
            {
                maxReceiveCount    = 1,
                deadLetterTargetArn = dlqArn,
            }),
            ["VisibilityTimeout"] = "0",
        });

        // Send a message
        await _sqs.SendMessageAsync(src.QueueUrl, "dlq-test");

        // Receive once (maxReceiveCount = 1, so next sweep → DLQ)
        var recv = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = src.QueueUrl, MaxNumberOfMessages = 1,
        });
        recv.Messages.ShouldHaveSingleItem();

        // Don't delete it — visibility is 0 so it becomes visible immediately.
        // A second receive triggers the DLQ sweep.
        var recv2 = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = src.QueueUrl, MaxNumberOfMessages = 1,
        });

        // Message should now be in DLQ
        var dlqAttrsAfter = await _sqs.GetQueueAttributesAsync(dlq.QueueUrl,
            ["ApproximateNumberOfMessages"]);
        dlqAttrsAfter.Attributes["ApproximateNumberOfMessages"].ShouldBe("1");
    }

    // ── ChangeMessageVisibility ─────────────────────────────────────────────────

    [Fact]
    public async Task ChangeMessageVisibility()
    {
        var q = await _sqs.CreateQueueAsync("vis-queue");
        await _sqs.SendMessageAsync(q.QueueUrl, "vis-test");

        var recv = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = q.QueueUrl, MaxNumberOfMessages = 1,
        });
        recv.Messages.ShouldHaveSingleItem();

        // Extend visibility — no exception = success
        await _sqs.ChangeMessageVisibilityAsync(
            q.QueueUrl, recv.Messages[0].ReceiptHandle, 120);
    }
}

/// <summary>
/// Rewrites the request URI to a canonical form, stripping the
/// <c>DangerousDisablePathAndQueryCanonicalization</c> flag that AWS SDK v4 sets.
/// This is required for compatibility with ASP.NET Core's <see cref="Microsoft.AspNetCore.TestHost.TestServer"/>.
/// </summary>
internal sealed class CanonicalizeUriHandler : DelegatingHandler
{
    internal CanonicalizeUriHandler(HttpMessageHandler innerHandler)
        : base(innerHandler) { }

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (request.RequestUri is not null)
            request.RequestUri = new Uri(request.RequestUri.AbsoluteUri);

        return base.SendAsync(request, cancellationToken);
    }
}

/// <summary>
/// Provides a fixed <see cref="HttpClient"/> to the AWS SDK so it uses the
/// in-process test server instead of making real network calls.
/// </summary>
internal sealed class FixedHttpClientFactory : Amazon.Runtime.HttpClientFactory
{
    private readonly HttpClient _client;

    internal FixedHttpClientFactory(HttpClient client) => _client = client;

    public override HttpClient CreateHttpClient(IClientConfig clientConfig) => _client;

    public override bool DisposeHttpClientsAfterUse(IClientConfig clientConfig) => false;
}
