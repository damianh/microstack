---
title: SNS
description: SNS topic emulation — publish/subscribe, SQS and Lambda fanout, FIFO topics.
order: 8
section: Services
---

# SNS

MicroStack's SNS handler supports topics, subscriptions, publish/fanout to SQS queues and Lambda functions, FIFO topics with deduplication, platform applications, and batch publish.

## Supported Operations

- CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes, SetTopicAttributes
- Subscribe, ConfirmSubscription, Unsubscribe, ListSubscriptions, ListSubscriptionsByTopic, GetSubscriptionAttributes, SetSubscriptionAttributes
- Publish, PublishBatch
- CreatePlatformApplication, CreatePlatformEndpoint
- TagResource, UntagResource, ListTagsForResource

## Usage

```csharp
var sns = new AmazonSimpleNotificationServiceClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonSimpleNotificationServiceConfig { ServiceURL = "http://localhost:4566" });

// Create a topic
var topic = await sns.CreateTopicAsync("my-topic");

// Publish a message
var publish = await sns.PublishAsync(new PublishRequest
{
    TopicArn = topic.TopicArn,
    Message = "Hello from MicroStack",
    Subject = "Test",
});

Console.WriteLine(publish.MessageId);
```

## SNS to SQS Fanout

```csharp
var sqs = new AmazonSQSClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonSQSConfig { ServiceURL = "http://localhost:4566" });

// Create topic and queue
var topicArn = (await sns.CreateTopicAsync("fanout-topic")).TopicArn;
var queueUrl = (await sqs.CreateQueueAsync("fanout-queue")).QueueUrl;

// Get the queue ARN
var queueArn = (await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
{
    QueueUrl = queueUrl,
    AttributeNames = ["QueueArn"],
})).Attributes["QueueArn"];

// Subscribe the queue to the topic
await sns.SubscribeAsync(topicArn, "sqs", queueArn);

// Publish — message is delivered to the subscribed queue
await sns.PublishAsync(new PublishRequest
{
    TopicArn = topicArn,
    Message = "fanout message",
});

// Receive from SQS — body is a JSON envelope with Message, TopicArn, etc.
var messages = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
{
    QueueUrl = queueUrl,
    MaxNumberOfMessages = 1,
});
```

## FIFO Topics

```csharp
var fifoTopic = await sns.CreateTopicAsync(new CreateTopicRequest
{
    Name = "my-topic.fifo",
    Attributes = new Dictionary<string, string>
    {
        ["FifoTopic"] = "true",
        ["ContentBasedDeduplication"] = "true",
    },
});

await sns.PublishAsync(new PublishRequest
{
    TopicArn = fifoTopic.TopicArn,
    Message = "ordered message",
    MessageGroupId = "group-1",
});
```

:::aside{type="note" title="Lambda fanout"}
SNS-to-Lambda subscriptions are supported. Lambda functions subscribed to an SNS topic are invoked synchronously when a message is published.
:::
