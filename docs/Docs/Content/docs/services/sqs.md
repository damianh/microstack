---
title: SQS
description: SQS message queue emulation — standard and FIFO queues, dead-letter queues.
order: 3
section: Services
---

# SQS

Full SQS emulation including standard queues, FIFO queues with deduplication, dead-letter queues, message attributes, and visibility timeouts.

## Supported Operations

- CreateQueue, DeleteQueue, ListQueues, GetQueueUrl, GetQueueAttributes, SetQueueAttributes
- SendMessage, SendMessageBatch, ReceiveMessage, DeleteMessage, DeleteMessageBatch
- ChangeMessageVisibility, PurgeQueue
- TagQueue, UntagQueue, ListQueueTags
- FIFO queues with MessageGroupId and MessageDeduplicationId

## Usage

```csharp
var sqs = new AmazonSQSClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonSQSConfig { ServiceURL = "http://localhost:4566" });

// Create queue
var queue = await sqs.CreateQueueAsync("my-queue");

// Send message
await sqs.SendMessageAsync(new SendMessageRequest
{
    QueueUrl = queue.QueueUrl,
    MessageBody = "Hello from MicroStack",
});

// Receive message
var messages = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
{
    QueueUrl = queue.QueueUrl,
    MaxNumberOfMessages = 10,
});

foreach (var msg in messages.Messages)
{
    Console.WriteLine(msg.Body);
    await sqs.DeleteMessageAsync(queue.QueueUrl, msg.ReceiptHandle);
}
```

## FIFO Queues

```csharp
var fifo = await sqs.CreateQueueAsync(new CreateQueueRequest
{
    QueueName = "my-queue.fifo",
    Attributes = new Dictionary<string, string>
    {
        ["FifoQueue"] = "true",
        ["ContentBasedDeduplication"] = "true",
    },
});

await sqs.SendMessageAsync(new SendMessageRequest
{
    QueueUrl = fifo.QueueUrl,
    MessageBody = "ordered message",
    MessageGroupId = "group-1",
});
```
