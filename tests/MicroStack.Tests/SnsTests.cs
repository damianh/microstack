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

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _sns.Dispose();
        _sqs.Dispose();
        return Task.CompletedTask;
    }

    // ── Topic CRUD ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateTopic()
    {
        var resp = await _sns.CreateTopicAsync("intg-sns-create");
        Assert.NotEmpty(resp.TopicArn);
        Assert.Contains("intg-sns-create", resp.TopicArn);
    }

    [Fact]
    public async Task DeleteTopic()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-delete")).TopicArn;
        await _sns.DeleteTopicAsync(arn);
        var topics = (await _sns.ListTopicsAsync()).Topics ?? [];
        Assert.DoesNotContain(topics, t => t.TopicArn == arn);
    }

    [Fact]
    public async Task ListTopics()
    {
        await _sns.CreateTopicAsync("intg-sns-list-1");
        await _sns.CreateTopicAsync("intg-sns-list-2");
        var topics = (await _sns.ListTopicsAsync()).Topics;
        var arns = topics.Select(t => t.TopicArn).ToList();
        Assert.Contains(arns, a => a.Contains("intg-sns-list-1"));
        Assert.Contains(arns, a => a.Contains("intg-sns-list-2"));
    }

    [Fact]
    public async Task GetTopicAttributes()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-getattr")).TopicArn;
        var resp = await _sns.GetTopicAttributesAsync(arn);
        Assert.Equal(arn, resp.Attributes["TopicArn"]);
        Assert.Equal("intg-sns-getattr", resp.Attributes["DisplayName"]);
    }

    [Fact]
    public async Task SetTopicAttributes()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-setattr")).TopicArn;
        await _sns.SetTopicAttributesAsync(arn, "DisplayName", "New Display Name");
        var resp = await _sns.GetTopicAttributesAsync(arn);
        Assert.Equal("New Display Name", resp.Attributes["DisplayName"]);
    }

    // ── Subscriptions ───────────────────────────────────────────────────────────

    [Fact]
    public async Task SubscribeEmail()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-subemail")).TopicArn;
        var resp = await _sns.SubscribeAsync(arn, "email", "user@example.com");
        Assert.NotEmpty(resp.SubscriptionArn);
    }

    [Fact]
    public async Task Unsubscribe()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-unsub")).TopicArn;
        var sub = await _sns.SubscribeAsync(arn, "email", "unsub@example.com");
        var subArn = sub.SubscriptionArn;
        await _sns.UnsubscribeAsync(subArn);
        var subs = (await _sns.ListSubscriptionsByTopicAsync(arn)).Subscriptions ?? [];
        Assert.DoesNotContain(subs, s => s.SubscriptionArn == subArn);
    }

    [Fact]
    public async Task ListSubscriptions()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-listsubs")).TopicArn;
        await _sns.SubscribeAsync(arn, "email", "ls1@example.com");
        await _sns.SubscribeAsync(arn, "email", "ls2@example.com");
        var subs = (await _sns.ListSubscriptionsAsync()).Subscriptions;
        var topicSubs = subs.Where(s => s.TopicArn == arn).ToList();
        Assert.True(topicSubs.Count >= 2);
    }

    [Fact]
    public async Task ListSubscriptionsByTopic()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-listbytopic")).TopicArn;
        await _sns.SubscribeAsync(arn, "email", "bt@example.com");
        var subs = (await _sns.ListSubscriptionsByTopicAsync(arn)).Subscriptions;
        Assert.True(subs.Count >= 1);
        Assert.All(subs, s => Assert.Equal(arn, s.TopicArn));
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
        Assert.NotEmpty(resp.MessageId);
    }

    [Fact]
    public async Task PublishNonexistentTopic()
    {
        var fakeArn = "arn:aws:sns:us-east-1:000000000000:intg-sns-nonexist";
        var ex = await Assert.ThrowsAsync<AmazonSimpleNotificationServiceException>(
            () => _sns.PublishAsync(fakeArn, "fail"));
        Assert.Equal("NotFoundException", ex.ErrorCode);
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

        Assert.Single(msgs.Messages);
        var body = JsonDocument.Parse(msgs.Messages[0].Body).RootElement;
        Assert.Equal("fanout msg", body.GetProperty("Message").GetString());
        Assert.Equal(topicArn, body.GetProperty("TopicArn").GetString());
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
        Assert.NotEqual("PendingConfirmation", sub1.SubscriptionArn);
        Assert.NotEqual("PendingConfirmation", sub2.SubscriptionArn);

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

            Assert.Single(msgs.Messages);
            var body = JsonDocument.Parse(msgs.Messages[0].Body).RootElement;
            Assert.Equal("fanout-test-msg", body.GetProperty("Message").GetString());
            Assert.Equal(topicArn, body.GetProperty("TopicArn").GetString());
            Assert.Equal("IntgTest", body.GetProperty("Subject").GetString());
            Assert.Equal("Notification", body.GetProperty("Type").GetString());
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
        Assert.Equal("staging", tags["env"]);
        Assert.Equal("infra", tags["team"]);

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
        Assert.False(tags.ContainsKey("team"));
        Assert.Equal("staging", tags["env"]);
    }

    // ── Subscription attributes ─────────────────────────────────────────────────

    [Fact]
    public async Task SubscriptionAttributes()
    {
        var arn = (await _sns.CreateTopicAsync("intg-sns-subattr")).TopicArn;
        var sub = await _sns.SubscribeAsync(arn, "email", "attrs@example.com");
        var subArn = sub.SubscriptionArn;

        var resp = await _sns.GetSubscriptionAttributesAsync(subArn);
        Assert.Equal("email", resp.Attributes["Protocol"]);
        Assert.Equal(arn, resp.Attributes["TopicArn"]);

        await _sns.SetSubscriptionAttributesAsync(new SetSubscriptionAttributesRequest
        {
            SubscriptionArn = subArn,
            AttributeName = "RawMessageDelivery",
            AttributeValue = "true",
        });

        resp = await _sns.GetSubscriptionAttributesAsync(subArn);
        Assert.Equal("true", resp.Attributes["RawMessageDelivery"]);
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
        Assert.Equal("true", attrs["RawMessageDelivery"]);
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
        Assert.Equal(filterPolicy, attrs["FilterPolicy"]);
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

        Assert.Single(msgs.Messages);
        Assert.Equal("raw fanout msg", msgs.Messages[0].Body);
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

        Assert.Single(msgs.Messages);
        Assert.Equal("raw-body", msgs.Messages[0].Body);
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

        Assert.Equal(3, resp.Successful?.Count ?? 0);
        Assert.Empty(resp.Failed ?? []);
    }

    [Fact]
    public async Task PublishBatchDistinctIds()
    {
        var arn = (await _sns.CreateTopicAsync("qa-sns-batch-dup")).TopicArn;
        var ex = await Assert.ThrowsAsync<Amazon.SimpleNotificationService.Model.BatchEntryIdsNotDistinctException>(
            () => _sns.PublishBatchAsync(new PublishBatchRequest
            {
                TopicArn = arn,
                PublishBatchRequestEntries =
                [
                    new PublishBatchRequestEntry { Id = "same", Message = "msg1" },
                    new PublishBatchRequestEntry { Id = "same", Message = "msg2" },
                ],
            }));
        Assert.Contains("BatchEntryIdsNotDistinct", ex.ErrorCode);
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
        Assert.Empty(msgs.Messages ?? []);

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
        Assert.Single(msgs2.Messages);
        var body = JsonDocument.Parse(msgs2.Messages[0].Body).RootElement;
        Assert.Equal("blue message", body.GetProperty("Message").GetString());
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

        Assert.Single(msgs.Messages);
        var msg = msgs.Messages[0];
        var body = JsonDocument.Parse(msg.Body).RootElement;
        Assert.Equal("fifo-dedup-test", body.GetProperty("Message").GetString());
        Assert.Equal("grp-1", msg.Attributes["MessageGroupId"]);
    }

    // ── Lambda fanout tests skipped ─────────────────────────────────────────────
    // test_sns_to_lambda_fanout and test_sns_to_lambda_event_subscription_arn
    // are skipped until the Lambda handler is ported (Task 17+).
}
