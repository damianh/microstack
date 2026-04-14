---
title: Firehose
description: Firehose emulation — delivery streams with S3 destination support, record ingestion, and version-based optimistic concurrency for updates.
order: 28
section: Services
---

# Firehose

MicroStack emulates Amazon Kinesis Data Firehose with delivery stream management, single and batch record ingestion, and S3 destination integration. S3 destinations write records to the local S3 emulator. `UpdateDestination` uses version-based optimistic concurrency — passing a stale `CurrentDeliveryStreamVersionId` returns a `ConcurrentModificationException`.

## Supported Operations

CreateDeliveryStream, DeleteDeliveryStream, DescribeDeliveryStream, ListDeliveryStreams, PutRecord, PutRecordBatch, UpdateDestination, TagDeliveryStream, UntagDeliveryStream, ListTagsForDeliveryStream, StartDeliveryStreamEncryption, StopDeliveryStreamEncryption

## Usage

```csharp
using Amazon.KinesisFirehose;
using Amazon.KinesisFirehose.Model;
using Amazon.Runtime;
using FirehoseRecord = Amazon.KinesisFirehose.Model.Record;

var config = new AmazonKinesisFirehoseConfig
{
    ServiceURL = "http://localhost:4566",
};
using var client = new AmazonKinesisFirehoseClient(
    new BasicAWSCredentials("test", "test"), config);

// Create a delivery stream with an S3 destination
var create = await client.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
{
    DeliveryStreamName = "my-stream",
    DeliveryStreamType = DeliveryStreamType.DirectPut,
    ExtendedS3DestinationConfiguration = new ExtendedS3DestinationConfiguration
    {
        BucketARN = "arn:aws:s3:::my-bucket",
        RoleARN = "arn:aws:iam::000000000000:role/firehose-role",
    },
});

Console.WriteLine($"ARN: {create.DeliveryStreamARN}");

// Describe the stream
var desc = await client.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
{
    DeliveryStreamName = "my-stream",
});
Console.WriteLine($"Status: {desc.DeliveryStreamDescription.DeliveryStreamStatus}");
// => ACTIVE
```

## Sending Records

```csharp
// Put a single record
using var data = new MemoryStream("hello firehose"u8.ToArray());
var put = await client.PutRecordAsync(new PutRecordRequest
{
    DeliveryStreamName = "my-stream",
    Record = new FirehoseRecord { Data = data },
});
Console.WriteLine($"RecordId: {put.RecordId}");

// Put a batch of records
var batch = await client.PutRecordBatchAsync(new PutRecordBatchRequest
{
    DeliveryStreamName = "my-stream",
    Records =
    [
        new FirehoseRecord { Data = new MemoryStream("record-0"u8.ToArray()) },
        new FirehoseRecord { Data = new MemoryStream("record-1"u8.ToArray()) },
        new FirehoseRecord { Data = new MemoryStream("record-2"u8.ToArray()) },
    ],
});
Console.WriteLine($"Failed: {batch.FailedPutCount}");
// => 0
```

## Updating Destinations

`UpdateDestination` requires the current version ID from `DescribeDeliveryStream`. The version increments with each successful update.

```csharp
var desc = await client.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
{
    DeliveryStreamName = "my-stream",
});
var destId = desc.DeliveryStreamDescription.Destinations[0].DestinationId;
var versionId = desc.DeliveryStreamDescription.VersionId; // e.g. "1"

await client.UpdateDestinationAsync(new UpdateDestinationRequest
{
    DeliveryStreamName = "my-stream",
    DestinationId = destId,
    CurrentDeliveryStreamVersionId = versionId,
    ExtendedS3DestinationUpdate = new ExtendedS3DestinationUpdate
    {
        BucketARN = "arn:aws:s3:::updated-bucket",
        RoleARN = "arn:aws:iam::000000000000:role/firehose-role",
    },
});
// VersionId is now "2"
```

:::aside{type="note" title="Version-Based Concurrency"}
`UpdateDestination` enforces optimistic concurrency via `CurrentDeliveryStreamVersionId`. Passing a stale version ID throws `ConcurrentModificationException`. Always fetch the latest `VersionId` from `DescribeDeliveryStream` before calling `UpdateDestination`.
:::
