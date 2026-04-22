using System.Text.Json;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.AspNetCore.Mvc.Testing;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the SNS service handler.
/// Uses the AWS SDK for .NET v4 pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_sns.py.
/// Lambda-dependent tests are skipped until the Lambda handler is ported.
/// </summary>
public sealed class SnsTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonSimpleNotificationServiceClient _sns;
    private readonly AmazonSQSClient _sqs;

    public SnsTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _sns = CreateSnsClient(fixture);
        _sqs = CreateSqsClient(fixture);
    }

    private static AmazonSimpleNotificationServiceClient CreateSnsClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSimpleNotificationServiceConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSimpleNotificationServiceClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    private static AmazonSQSClient CreateSqsClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSQSConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSQSClient(new BasicAWSCredentials("test", "test"), config);
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _sns.Dispose();
        _sqs.Dispose();
        return ValueTask.CompletedTask;
    }

    // ── Topic CRUD ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateTopic()
    {
        var resp = await _sns.CreateTopicAsync("intg-sns-create");
        resp.TopicArn.ShouldNotBeEmpty();
        resp.TopicArn.ShouldContain("intg-sns-create");
    }

    [Fact]
    public async Task DeleteTopic()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-delete")).TopicArn;
        await _sns.DeleteTopicAsync(arn);
        var topics = (await _sns.ListTopicsAsync()).Topics ?? [];
        topics.ShouldNotContain(t => t.TopicArn == arn);
    }

    [Fact]
    public async Task ListTopics()
    {
        await _sns.CreateTopicAsync("intg-sns-list-1");
        await _sns.CreateTopicAsync("intg-sns-list-2");
        var topics = (await _sns.ListTopicsAsync()).Topics;
        var arns = topics.Select(t => t.TopicArn).ToList();
        arns.ShouldContain(a => a.Contains("intg-sns-list-1"));
        arns.ShouldContain(a => a.Contains("intg-sns-list-2"));
    }

    [Fact]
    public async Task GetTopicAttributes()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-getattr")).TopicArn;
        var resp = await _sns.GetTopicAttributesAsync(arn);
        resp.Attributes["TopicArn"].ShouldBe(arn);
        resp.Attributes["DisplayName"].ShouldBe("intg-sns-getattr");
    }

    [Fact]
    public async Task SetTopicAttributes()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-setattr")).TopicArn;
        await _sns.SetTopicAttributesAsync(arn, "DisplayName", "New Display Name");
        var resp = await _sns.GetTopicAttributesAsync(arn);
        resp.Attributes["DisplayName"].ShouldBe("New Display Name");
    }

    // ── Subscriptions ───────────────────────────────────────────────────────────

    [Fact]
    public async Task SubscribeEmail()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-subemail")).TopicArn;
        var resp = await _sns.SubscribeAsync(arn, "email", "user@example.com");
        resp.SubscriptionArn.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task Unsubscribe()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-unsub")).TopicArn;
        var sub = await _sns.SubscribeAsync(arn, "email", "unsub@example.com");
        var subArn = sub.SubscriptionArn;
        await _sns.UnsubscribeAsync(subArn);
        var subs = (await _sns.ListSubscriptionsByTopicAsync(arn)).Subscriptions ?? [];
        subs.ShouldNotContain(s => s.SubscriptionArn == subArn);
    }

    [Fact]
    public async Task ListSubscriptions()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-listsubs")).TopicArn;
        await _sns.SubscribeAsync(arn, "email", "ls1@example.com");
        await _sns.SubscribeAsync(arn, "email", "ls2@example.com");
        var subs = (await _sns.ListSubscriptionsAsync()).Subscriptions;
        var topicSubs = subs.Where(s => s.TopicArn == arn).ToList();
        (topicSubs.Count >= 2).ShouldBe(true);
    }

    [Fact]
    public async Task ListSubscriptionsByTopic()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-listbytopic")).TopicArn;
        await _sns.SubscribeAsync(arn, "email", "bt@example.com");
        var subs = (await _sns.ListSubscriptionsByTopicAsync(arn)).Subscriptions;
        (subs.Count >= 1).ShouldBe(true);
        subs.ShouldAllBe(s => s.TopicArn == arn);
    }

    // ── Publish ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Publish()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-publish")).TopicArn;
        var resp = await _sns.PublishAsync(new PublishRequest
        {
            TopicArn = arn,
            Message = "hello sns",
            Subject = "Test Subject",
        });
        resp.MessageId.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task PublishNonexistentTopic()
    {
        var fakeArn = "arn:aws:sns:us-east-1:000000000000:intg-sns-nonexist";
        var ex = await Should.ThrowAsync<AmazonSimpleNotificationServiceException>(
            () => _sns.PublishAsync(fakeArn, "fail"));
        ex.ErrorCode.ShouldBe("NotFoundException");
    }

    // ── SNS → SQS fanout ────────────────────────────────────────────────────────

    [Fact]
    public async Task SnsSqsFanout()
    {
        var topicArn = (await _sns.CreateTopicAsync("intg-sns-fanout")).TopicArn;
        var qUrl = (await _sqs.CreateQueueAsync("intg-sns-fanout-q")).QueueUrl;
        var qArn = (await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = qUrl,
            AttributeNames = ["QueueArn"],
        })).Attributes["QueueArn"];

        await _sns.SubscribeAsync(topicArn, "sqs", qArn);
        await _sns.PublishAsync(new PublishRequest
        {
            TopicArn = topicArn,
            Message = "fanout msg",
            Subject = "Fan",
        });

        var msgs = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = qUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 1,
        });

        msgs.Messages.ShouldHaveSingleItem();
        var body = JsonDocument.Parse(msgs.Messages[0].Body).RootElement;
        body.GetProperty("Message").GetString().ShouldBe("fanout msg");
        body.GetProperty("TopicArn").GetString().ShouldBe(topicArn);
    }

    [Fact]
    public async Task SnsToSqsFanoutMultipleQueues()
    {
        var topicArn = (await _sns.CreateTopicAsync("intg-fanout-topic")).TopicArn;

        var q1Url = (await _sqs.CreateQueueAsync("intg-fanout-q1")).QueueUrl;
        var q2Url = (await _sqs.CreateQueueAsync("intg-fanout-q2")).QueueUrl;
        var q1Arn = (await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = q1Url,
            AttributeNames = ["QueueArn"],
        })).Attributes["QueueArn"];
        var q2Arn = (await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = q2Url,
            AttributeNames = ["QueueArn"],
        })).Attributes["QueueArn"];

        var sub1 = await _sns.SubscribeAsync(topicArn, "sqs", q1Arn);
        var sub2 = await _sns.SubscribeAsync(topicArn, "sqs", q2Arn);
        sub1.SubscriptionArn.ShouldNotBe("PendingConfirmation");
        sub2.SubscriptionArn.ShouldNotBe("PendingConfirmation");

        await _sns.PublishAsync(new PublishRequest
        {
            TopicArn = topicArn,
            Message = "fanout-test-msg",
            Subject = "IntgTest",
        });

        // Both queues should receive the message
        foreach (var qUrl in new[] { q1Url, q2Url })
        {
            var msgs = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = qUrl,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = 2,
            });

            msgs.Messages.ShouldHaveSingleItem();
            var body = JsonDocument.Parse(msgs.Messages[0].Body).RootElement;
            body.GetProperty("Message").GetString().ShouldBe("fanout-test-msg");
            body.GetProperty("TopicArn").GetString().ShouldBe(topicArn);
            body.GetProperty("Subject").GetString().ShouldBe("IntgTest");
            body.GetProperty("Type").GetString().ShouldBe("Notification");
        }
    }

    // ── Tags ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Tags()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-tags")).TopicArn;
        await _sns.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = arn,
            Tags = [
                new Tag { Key = "env", Value = "staging" },
                new Tag { Key = "team", Value = "infra" },
            ],
        });

        var resp = await _sns.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arn,
        });
        var tags = resp.Tags.ToDictionary(t => t.Key, t => t.Value);
        tags["env"].ShouldBe("staging");
        tags["team"].ShouldBe("infra");

        await _sns.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceArn = arn,
            TagKeys = ["team"],
        });

        resp = await _sns.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arn,
        });
        tags = resp.Tags.ToDictionary(t => t.Key, t => t.Value);
        tags.ContainsKey("team").ShouldBe(false);
        tags["env"].ShouldBe("staging");
    }

    // ── Subscription attributes ─────────────────────────────────────────────────

    [Fact]
    public async Task SubscriptionAttributes()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-subattr")).TopicArn;
        var sub = await _sns.SubscribeAsync(arn, "email", "attrs@example.com");
        var subArn = sub.SubscriptionArn;

        var resp = await _sns.GetSubscriptionAttributesAsync(subArn);
        resp.Attributes["Protocol"].ShouldBe("email");
        resp.Attributes["TopicArn"].ShouldBe(arn);

        await _sns.SetSubscriptionAttributesAsync(new SetSubscriptionAttributesRequest
        {
            SubscriptionArn = subArn,
            AttributeName = "RawMessageDelivery",
            AttributeValue = "true",
        });

        resp = await _sns.GetSubscriptionAttributesAsync(subArn);
        resp.Attributes["RawMessageDelivery"].ShouldBe("true");
    }

    [Fact]
    public async Task SubscribeWithRawMessageDelivery()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-sub-raw")).TopicArn;
        var sub = await _sns.SubscribeAsync(new SubscribeRequest
        {
            TopicArn = arn,
            Protocol = "email",
            Endpoint = "raw@example.com",
            Attributes = new Dictionary<string, string> { ["RawMessageDelivery"] = "true" },
        });
        var subArn = sub.SubscriptionArn;
        var attrs = (await _sns.GetSubscriptionAttributesAsync(subArn)).Attributes;
        attrs["RawMessageDelivery"].ShouldBe("true");
    }

    [Fact]
    public async Task SubscribeWithFilterPolicy()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-sub-filter")).TopicArn;
        var filterPolicy = JsonSerializer.Serialize(new { @event = new[] { "MyEvent" } });
        var sub = await _sns.SubscribeAsync(new SubscribeRequest
        {
            TopicArn = arn,
            Protocol = "email",
            Endpoint = "filter@example.com",
            Attributes = new Dictionary<string, string> { ["FilterPolicy"] = filterPolicy },
        });
        var subArn = sub.SubscriptionArn;
        var attrs = (await _sns.GetSubscriptionAttributesAsync(subArn)).Attributes;
        attrs["FilterPolicy"].ShouldBe(filterPolicy);
    }

    // ── Raw message delivery ────────────────────────────────────────────────────

    [Fact]
    public async Task SnsSqsFanoutRawMessageDelivery()
    {
        var topicArn = (await _sns.CreateTopicAsync("intg-sns-fanout-raw")).TopicArn;
        var qUrl = (await _sqs.CreateQueueAsync("intg-sns-fanout-raw-q")).QueueUrl;
        var qArn = (await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = qUrl,
            AttributeNames = ["QueueArn"],
        })).Attributes["QueueArn"];

        await _sns.SubscribeAsync(new SubscribeRequest
        {
            TopicArn = topicArn,
            Protocol = "sqs",
            Endpoint = qArn,
            Attributes = new Dictionary<string, string> { ["RawMessageDelivery"] = "true" },
        });

        await _sns.PublishAsync(topicArn, "raw fanout msg");

        var msgs = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = qUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 1,
        });

        msgs.Messages.ShouldHaveSingleItem();
        msgs.Messages[0].Body.ShouldBe("raw fanout msg");
    }

    [Fact]
    public async Task RawMessageDeliveryViaSqsFanout()
    {
        var topicArn = (await _sns.CreateTopicAsync("qa-sns-raw")).TopicArn;
        var qUrl = (await _sqs.CreateQueueAsync("qa-sns-raw-q")).QueueUrl;
        var qArn = (await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = qUrl,
            AttributeNames = ["QueueArn"],
        })).Attributes["QueueArn"];

        var sub = await _sns.SubscribeAsync(topicArn, "sqs", qArn);
        await _sns.SetSubscriptionAttributesAsync(new SetSubscriptionAttributesRequest
        {
            SubscriptionArn = sub.SubscriptionArn,
            AttributeName = "RawMessageDelivery",
            AttributeValue = "true",
        });

        await _sns.PublishAsync(topicArn, "raw-body");

        var msgs = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = qUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 1,
        });

        msgs.Messages.ShouldHaveSingleItem();
        msgs.Messages[0].Body.ShouldBe("raw-body");
    }

    // ── Batch publish ───────────────────────────────────────────────────────────

    [Fact]
    public async Task PublishBatch()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-batch")).TopicArn;
        var resp = await _sns.PublishBatchAsync(new PublishBatchRequest
        {
            TopicArn = arn,
            PublishBatchRequestEntries =
            [
                new PublishBatchRequestEntry { Id = "msg1", Message = "batch message 1" },
                new PublishBatchRequestEntry { Id = "msg2", Message = "batch message 2" },
                new PublishBatchRequestEntry { Id = "msg3", Message = "batch message 3" },
            ],
        });

        (resp.Successful?.Count ?? 0).ShouldBe(3);
        (resp.Failed ?? []).ShouldBeEmpty();
    }

    [Fact]
    public async Task PublishBatchDistinctIds()
    {
        var arn = (await _sns.CreateTopicAsync("qa-sns-batch-dup")).TopicArn;
        var ex = await Should.ThrowAsync<Amazon.SimpleNotificationService.Model.BatchEntryIdsNotDistinctException>(
            () => _sns.PublishBatchAsync(new PublishBatchRequest
            {
                TopicArn = arn,
                PublishBatchRequestEntries =
                [
                    new PublishBatchRequestEntry { Id = "same", Message = "msg1" },
                    new PublishBatchRequestEntry { Id = "same", Message = "msg2" },
                ],
            }));
        ex.ErrorCode.ShouldContain("BatchEntryIdsNotDistinct");
    }

    // ── Filter policy ───────────────────────────────────────────────────────────

    [Fact]
    public async Task FilterPolicyBlocksNonMatching()
    {
        var topicArn = (await _sns.CreateTopicAsync("qa-sns-filter")).TopicArn;
        var qUrl = (await _sqs.CreateQueueAsync("qa-sns-filter-q")).QueueUrl;
        var qArn = (await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = qUrl,
            AttributeNames = ["QueueArn"],
        })).Attributes["QueueArn"];

        var sub = await _sns.SubscribeAsync(topicArn, "sqs", qArn);
        await _sns.SetSubscriptionAttributesAsync(new SetSubscriptionAttributesRequest
        {
            SubscriptionArn = sub.SubscriptionArn,
            AttributeName = "FilterPolicy",
            AttributeValue = JsonSerializer.Serialize(new { color = new[] { "blue" } }),
        });

        // Publish non-matching message (red)
        await _sns.PublishAsync(new PublishRequest
        {
            TopicArn = topicArn,
            Message = "red message",
            MessageAttributes = new Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>
            {
                ["color"] = new() { DataType = "String", StringValue = "red" },
            },
        });

        var msgs = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = qUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 0,
        });
        (msgs.Messages ?? []).ShouldBeEmpty();

        // Publish matching message (blue)
        await _sns.PublishAsync(new PublishRequest
        {
            TopicArn = topicArn,
            Message = "blue message",
            MessageAttributes = new Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>
            {
                ["color"] = new() { DataType = "String", StringValue = "blue" },
            },
        });

        var msgs2 = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = qUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 1,
        });
        msgs2.Messages.ShouldHaveSingleItem();
        var body = JsonDocument.Parse(msgs2.Messages[0].Body).RootElement;
        body.GetProperty("Message").GetString().ShouldBe("blue message");
    }

    // ── FIFO dedup passthrough ──────────────────────────────────────────────────

    [Fact]
    public async Task FifoDeduplicationPassthrough()
    {
        var topicArn = (await _sns.CreateTopicAsync(new CreateTopicRequest
        {
            Name = "intg-sns-fifo-dedup.fifo",
            Attributes = new Dictionary<string, string>
            {
                ["FifoTopic"] = "true",
                ["ContentBasedDeduplication"] = "false",
            },
        })).TopicArn;

        var qUrl = (await _sqs.CreateQueueAsync(new CreateQueueRequest
        {
            QueueName = "intg-sns-fifo-dedup-q.fifo",
            Attributes = new Dictionary<string, string> { ["FifoQueue"] = "true" },
        })).QueueUrl;

        var qArn = (await _sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = qUrl,
            AttributeNames = ["QueueArn"],
        })).Attributes["QueueArn"];

        await _sns.SubscribeAsync(topicArn, "sqs", qArn);

        await _sns.PublishAsync(new PublishRequest
        {
            TopicArn = topicArn,
            Message = "fifo-dedup-test",
            MessageGroupId = "grp-1",
            MessageDeduplicationId = "dedup-001",
        });

        var msgs = await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = qUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 2,
            MessageSystemAttributeNames = ["All"],
        });

        msgs.Messages.ShouldHaveSingleItem();
        var msg = msgs.Messages[0];
        var body = JsonDocument.Parse(msg.Body).RootElement;
        body.GetProperty("Message").GetString().ShouldBe("fifo-dedup-test");
        msg.Attributes["MessageGroupId"].ShouldBe("grp-1");
    }

    // ── Lambda fanout tests skipped ─────────────────────────────────────────────
    // test_sns_to_lambda_fanout and test_sns_to_lambda_event_subscription_arn
    // are skipped until the Lambda handler is ported (Task 17+).
}
