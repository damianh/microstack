---
title: Kinesis
description: Kinesis emulation — streams, shards, records, consumers, shard splitting and merging, and stream encryption.
order: 27
section: Services
---

# Kinesis

MicroStack emulates Amazon Kinesis Data Streams, supporting stream and shard management, record production and consumption, registered consumers, and shard-level operations (split, merge, update shard count). Records are routed to shards by partition key using consistent hash assignment. Default retention is 24 hours; it can be increased or decreased per stream.

## Supported Operations

CreateStream, DeleteStream, DescribeStream, DescribeStreamSummary, ListStreams, ListShards, PutRecord, PutRecords, GetShardIterator, GetRecords, IncreaseStreamRetentionPeriod, DecreaseStreamRetentionPeriod, AddTagsToStream, RemoveTagsFromStream, ListTagsForStream, MergeShards, SplitShard, UpdateShardCount, RegisterStreamConsumer, DeregisterStreamConsumer, ListStreamConsumers, DescribeStreamConsumer, StartStreamEncryption, StopStreamEncryption, EnableEnhancedMonitoring, DisableEnhancedMonitoring

## Usage

```csharp
var client = new AmazonKinesisClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonKinesisConfig
    {
        RegionEndpoint = RegionEndpoint.USEast1,
        ServiceURL = "http://localhost:4566",
    });

// Create a stream with 2 shards
await client.CreateStreamAsync(new CreateStreamRequest
{
    StreamName = "my-stream",
    ShardCount = 2,
});

var desc = await client.DescribeStreamAsync(new DescribeStreamRequest
{
    StreamName = "my-stream",
});
Console.WriteLine(desc.StreamDescription.StreamStatus); // ACTIVE
Console.WriteLine(desc.StreamDescription.Shards.Count); // 2

// Put individual records
await client.PutRecordAsync(new PutRecordRequest
{
    StreamName = "my-stream",
    Data = new MemoryStream(Encoding.UTF8.GetBytes("hello kinesis")),
    PartitionKey = "user-123",
});

// Batch put records
var entries = Enumerable.Range(0, 10).Select(i => new PutRecordsRequestEntry
{
    Data = new MemoryStream(Encoding.UTF8.GetBytes($"record-{i}")),
    PartitionKey = $"pk-{i}",
}).ToList();

var batchResp = await client.PutRecordsAsync(new PutRecordsRequest
{
    StreamName = "my-stream",
    Records = entries,
});
Console.WriteLine(batchResp.FailedRecordCount); // 0
```

## Reading Records

```csharp
// Get a shard iterator from the start of the stream
var shardId = desc.StreamDescription.Shards[0].ShardId;

var iterResp = await client.GetShardIteratorAsync(new GetShardIteratorRequest
{
    StreamName = "my-stream",
    ShardId = shardId,
    ShardIteratorType = ShardIteratorType.TRIM_HORIZON,
});

// Read records from the shard
var records = await client.GetRecordsAsync(new GetRecordsRequest
{
    ShardIterator = iterResp.ShardIterator,
    Limit = 100,
});

foreach (var record in records.Records)
{
    using var reader = new StreamReader(record.Data);
    Console.WriteLine(await reader.ReadToEndAsync());
}

// Use NextShardIterator to continue polling
string nextIterator = records.NextShardIterator;
```

## Shard Operations

```csharp
// Split a shard at its midpoint
var shard = desc.StreamDescription.Shards[0];
var startHash = System.Numerics.BigInteger.Parse(shard.HashKeyRange.StartingHashKey);
var endHash   = System.Numerics.BigInteger.Parse(shard.HashKeyRange.EndingHashKey);
var midHash   = ((startHash + endHash) / 2).ToString();

await client.SplitShardAsync(new SplitShardRequest
{
    StreamName = "my-stream",
    ShardToSplit = shard.ShardId,
    NewStartingHashKey = midHash,
});

// Increase retention period
await client.IncreaseStreamRetentionPeriodAsync(new IncreaseStreamRetentionPeriodRequest
{
    StreamName = "my-stream",
    RetentionPeriodHours = 48,
});
```
