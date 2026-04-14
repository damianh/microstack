using System.Text;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the Kinesis service handler.
/// Uses the AWS SDK for .NET v4 pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_kinesis.py.
/// </summary>
public sealed class KinesisTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonKinesisClient _kin;

    public KinesisTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _kin = CreateKinesisClient(fixture);
    }

    private static AmazonKinesisClient CreateKinesisClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonKinesisConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonKinesisClient(new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _kin.Dispose();
        return Task.CompletedTask;
    }

    // ── Stream lifecycle ────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateStream()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest
        {
            StreamName = "kin-cs-v2",
            ShardCount = 2,
        });

        var desc = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-cs-v2" });
        var sd = desc.StreamDescription;
        sd.StreamName.ShouldBe("kin-cs-v2");
        sd.StreamStatus.ShouldBe(StreamStatus.ACTIVE);
        sd.Shards.Count.ShouldBe(2);
    }

    [Fact]
    public async Task DeleteStream()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-del-v2", ShardCount = 1 });
        await _kin.DeleteStreamAsync(new DeleteStreamRequest { StreamName = "kin-del-v2" });

        var ex = await Should.ThrowAsync<ResourceNotFoundException>(
            () => _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-del-v2" }));
        ex.ErrorCode.ShouldBe("ResourceNotFoundException");
    }

    [Fact]
    public async Task ListStreams()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-ls-v2a", ShardCount = 1 });
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-ls-v2b", ShardCount = 1 });

        var resp = await _kin.ListStreamsAsync(new ListStreamsRequest());
        resp.StreamNames.ShouldContain("kin-ls-v2a");
        resp.StreamNames.ShouldContain("kin-ls-v2b");
    }

    [Fact]
    public async Task DescribeStreamAndSummary()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-desc-v2", ShardCount = 1 });

        var resp = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-desc-v2" });
        var sd = resp.StreamDescription;
        sd.StreamName.ShouldBe("kin-desc-v2");
        sd.RetentionPeriodHours.ShouldBe(24);
        sd.StreamARN.ShouldNotBeEmpty();
        sd.Shards.ShouldHaveSingleItem();

        var summary = await _kin.DescribeStreamSummaryAsync(
            new DescribeStreamSummaryRequest { StreamName = "kin-desc-v2" });
        summary.StreamDescriptionSummary.StreamName.ShouldBe("kin-desc-v2");
    }

    [Fact]
    public async Task ListShards()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-lsh-v2", ShardCount = 3 });

        var resp = await _kin.ListShardsAsync(new ListShardsRequest { StreamName = "kin-lsh-v2" });
        resp.Shards.Count.ShouldBe(3);
        foreach (var s in resp.Shards)
        {
            s.ShardId.ShouldNotBeEmpty();
            s.HashKeyRange.ShouldNotBeNull();
        }
    }

    // ── PutRecord / GetRecords ──────────────────────────────────────────────────

    [Fact]
    public async Task PutAndGetRecords()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-pgr-v2", ShardCount = 1 });

        await _kin.PutRecordAsync(new PutRecordRequest
        {
            StreamName = "kin-pgr-v2",
            Data = new MemoryStream(Encoding.UTF8.GetBytes("rec1")),
            PartitionKey = "pk1",
        });
        await _kin.PutRecordAsync(new PutRecordRequest
        {
            StreamName = "kin-pgr-v2",
            Data = new MemoryStream(Encoding.UTF8.GetBytes("rec2")),
            PartitionKey = "pk2",
        });
        await _kin.PutRecordAsync(new PutRecordRequest
        {
            StreamName = "kin-pgr-v2",
            Data = new MemoryStream(Encoding.UTF8.GetBytes("rec3")),
            PartitionKey = "pk3",
        });

        var desc = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-pgr-v2" });
        var shardId = desc.StreamDescription.Shards[0].ShardId;

        var it = await _kin.GetShardIteratorAsync(new GetShardIteratorRequest
        {
            StreamName = "kin-pgr-v2",
            ShardId = shardId,
            ShardIteratorType = ShardIteratorType.TRIM_HORIZON,
        });

        var records = await _kin.GetRecordsAsync(new GetRecordsRequest
        {
            ShardIterator = it.ShardIterator,
        });
        records.Records.Count.ShouldBe(3);

        // Verify first record data
        using var reader = new StreamReader(records.Records[0].Data);
        var data = await reader.ReadToEndAsync();
        data.ShouldBe("rec1");
    }

    [Fact]
    public async Task PutRecordsBatch()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-batch-v2", ShardCount = 1 });

        var entries = Enumerable.Range(0, 7).Select(i => new PutRecordsRequestEntry
        {
            Data = new MemoryStream(Encoding.UTF8.GetBytes($"b{i}")),
            PartitionKey = $"pk{i}",
        }).ToList();

        var resp = await _kin.PutRecordsAsync(new PutRecordsRequest
        {
            StreamName = "kin-batch-v2",
            Records = entries,
        });

        resp.FailedRecordCount.ShouldBe(0);
        resp.Records.Count.ShouldBe(7);
        foreach (var r in resp.Records)
        {
            r.ShardId.ShouldNotBeEmpty();
            r.SequenceNumber.ShouldNotBeEmpty();
        }
    }

    // ── Tags ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task AddAndRemoveTags()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-tag-v2", ShardCount = 1 });

        await _kin.AddTagsToStreamAsync(new AddTagsToStreamRequest
        {
            StreamName = "kin-tag-v2",
            Tags = new Dictionary<string, string>
            {
                ["env"] = "test",
                ["team"] = "data",
            },
        });

        var resp = await _kin.ListTagsForStreamAsync(new ListTagsForStreamRequest
        {
            StreamName = "kin-tag-v2",
        });
        var tagMap = resp.Tags.ToDictionary(t => t.Key, t => t.Value);
        tagMap["env"].ShouldBe("test");
        tagMap["team"].ShouldBe("data");

        await _kin.RemoveTagsFromStreamAsync(new RemoveTagsFromStreamRequest
        {
            StreamName = "kin-tag-v2",
            TagKeys = ["team"],
        });

        var resp2 = await _kin.ListTagsForStreamAsync(new ListTagsForStreamRequest
        {
            StreamName = "kin-tag-v2",
        });
        var tagMap2 = resp2.Tags.ToDictionary(t => t.Key, t => t.Value);
        tagMap2.Keys.ShouldNotContain("team");
        tagMap2["env"].ShouldBe("test");
    }

    // ── Retention period ────────────────────────────────────────────────────────

    [Fact]
    public async Task IncreaseAndDecreaseRetentionPeriod()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-retention", ShardCount = 1 });

        await _kin.IncreaseStreamRetentionPeriodAsync(new IncreaseStreamRetentionPeriodRequest
        {
            StreamName = "kin-retention",
            RetentionPeriodHours = 48,
        });
        var desc = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-retention" });
        desc.StreamDescription.RetentionPeriodHours.ShouldBe(48);

        await _kin.DecreaseStreamRetentionPeriodAsync(new DecreaseStreamRetentionPeriodRequest
        {
            StreamName = "kin-retention",
            RetentionPeriodHours = 24,
        });
        var desc2 = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-retention" });
        desc2.StreamDescription.RetentionPeriodHours.ShouldBe(24);
    }

    // ── Encryption ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task StreamEncryptionToggle()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-enc", ShardCount = 1 });

        await _kin.StartStreamEncryptionAsync(new StartStreamEncryptionRequest
        {
            StreamName = "kin-enc",
            EncryptionType = EncryptionType.KMS,
            KeyId = "alias/aws/kinesis",
        });
        var desc = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-enc" });
        desc.StreamDescription.EncryptionType.ShouldBe(EncryptionType.KMS);

        await _kin.StopStreamEncryptionAsync(new StopStreamEncryptionRequest
        {
            StreamName = "kin-enc",
            EncryptionType = EncryptionType.KMS,
            KeyId = "alias/aws/kinesis",
        });
        var desc2 = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-enc" });
        desc2.StreamDescription.EncryptionType.ShouldBe(EncryptionType.NONE);
    }

    // ── Enhanced monitoring ─────────────────────────────────────────────────────

    [Fact]
    public async Task EnhancedMonitoringToggle()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-mon", ShardCount = 1 });

        var enable = await _kin.EnableEnhancedMonitoringAsync(new EnableEnhancedMonitoringRequest
        {
            StreamName = "kin-mon",
            ShardLevelMetrics = ["IncomingBytes", "OutgoingBytes"],
        });
        enable.DesiredShardLevelMetrics.ShouldContain("IncomingBytes");

        var disable = await _kin.DisableEnhancedMonitoringAsync(new DisableEnhancedMonitoringRequest
        {
            StreamName = "kin-mon",
            ShardLevelMetrics = ["IncomingBytes"],
        });
        disable.DesiredShardLevelMetrics.ShouldNotContain("IncomingBytes");
    }

    // ── Split / Merge / UpdateShardCount ────────────────────────────────────────

    [Fact]
    public async Task SplitShard()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-split", ShardCount = 1 });

        var desc = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-split" });
        var shard = desc.StreamDescription.Shards[0];
        var startHash = System.Numerics.BigInteger.Parse(shard.HashKeyRange.StartingHashKey);
        var endHash = System.Numerics.BigInteger.Parse(shard.HashKeyRange.EndingHashKey);
        var mid = ((startHash + endHash) / 2).ToString();

        await _kin.SplitShardAsync(new SplitShardRequest
        {
            StreamName = "kin-split",
            ShardToSplit = shard.ShardId,
            NewStartingHashKey = mid,
        });

        var desc2 = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-split" });
        desc2.StreamDescription.Shards.Count.ShouldBe(2);
    }

    [Fact]
    public async Task MergeShards()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-merge", ShardCount = 2 });

        var desc = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-merge" });
        var shards = desc.StreamDescription.Shards;
        shards.Count.ShouldBe(2);

        var sorted = shards.OrderBy(s =>
            System.Numerics.BigInteger.Parse(s.HashKeyRange.StartingHashKey)).ToList();

        await _kin.MergeShardsAsync(new MergeShardsRequest
        {
            StreamName = "kin-merge",
            ShardToMerge = sorted[0].ShardId,
            AdjacentShardToMerge = sorted[1].ShardId,
        });

        var desc2 = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-merge" });
        desc2.StreamDescription.Shards.ShouldHaveSingleItem();
    }

    [Fact]
    public async Task UpdateShardCount()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-usc", ShardCount = 1 });

        var resp = await _kin.UpdateShardCountAsync(new UpdateShardCountRequest
        {
            StreamName = "kin-usc",
            TargetShardCount = 2,
            ScalingType = ScalingType.UNIFORM_SCALING,
        });
        resp.TargetShardCount.ShouldBe(2);
    }

    // ── Consumers ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task RegisterAndDeregisterConsumer()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-consumer", ShardCount = 1 });
        var desc = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-consumer" });
        var streamArn = desc.StreamDescription.StreamARN;

        var reg = await _kin.RegisterStreamConsumerAsync(new RegisterStreamConsumerRequest
        {
            StreamARN = streamArn,
            ConsumerName = "my-consumer",
        });
        reg.Consumer.ConsumerName.ShouldBe("my-consumer");
        reg.Consumer.ConsumerStatus.ShouldBe(ConsumerStatus.ACTIVE);
        var consumerArn = reg.Consumer.ConsumerARN;

        var consumers = await _kin.ListStreamConsumersAsync(new ListStreamConsumersRequest
        {
            StreamARN = streamArn,
        });
        consumers.Consumers.ShouldContain(c => c.ConsumerName == "my-consumer");

        var descC = await _kin.DescribeStreamConsumerAsync(new DescribeStreamConsumerRequest
        {
            ConsumerARN = consumerArn,
        });
        descC.ConsumerDescription.ConsumerName.ShouldBe("my-consumer");

        await _kin.DeregisterStreamConsumerAsync(new DeregisterStreamConsumerRequest
        {
            ConsumerARN = consumerArn,
        });

        var consumers2 = await _kin.ListStreamConsumersAsync(new ListStreamConsumersRequest
        {
            StreamARN = streamArn,
        });
        consumers2.Consumers.ShouldNotContain(c => c.ConsumerName == "my-consumer");
    }

    // ── AT_TIMESTAMP iterator ───────────────────────────────────────────────────

    [Fact]
    public async Task AtTimestampShardIterator()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-ts", ShardCount = 1 });

        // Use a timestamp slightly in the past to avoid precision issues
        var before = DateTime.UtcNow.AddSeconds(-1);

        await _kin.PutRecordAsync(new PutRecordRequest
        {
            StreamName = "kin-ts",
            Data = new MemoryStream(Encoding.UTF8.GetBytes("after-ts")),
            PartitionKey = "pk",
        });

        var shards = (await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-ts" }))
            .StreamDescription.Shards;
        var shardId = shards[0].ShardId;

        var it = await _kin.GetShardIteratorAsync(new GetShardIteratorRequest
        {
            StreamName = "kin-ts",
            ShardId = shardId,
            ShardIteratorType = ShardIteratorType.AT_TIMESTAMP,
            Timestamp = before,
        });

        var records = await _kin.GetRecordsAsync(new GetRecordsRequest
        {
            ShardIterator = it.ShardIterator,
            Limit = 10,
        });
        records.Records.ShouldNotBeEmpty();

        using var reader = new StreamReader(records.Records[0].Data);
        var data = await reader.ReadToEndAsync();
        data.ShouldBe("after-ts");
    }

    // ── Duplicate stream fails ──────────────────────────────────────────────────

    [Fact]
    public async Task CreateDuplicateStreamFails()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-dup", ShardCount = 1 });

        var ex = await Should.ThrowAsync<ResourceInUseException>(
            () => _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-dup", ShardCount = 1 }));
        ex.ErrorCode.ShouldBe("ResourceInUseException");
    }

    // ── Stream not found ────────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeNonexistentStreamFails()
    {
        var ex = await Should.ThrowAsync<ResourceNotFoundException>(
            () => _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "no-such-stream" }));
        ex.ErrorCode.ShouldBe("ResourceNotFoundException");
    }

    // ── PutRecord to two records then get ────────────────────────────────────────

    [Fact]
    public async Task PutAndGetTwoRecords()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "test-stream", ShardCount = 1 });

        await _kin.PutRecordAsync(new PutRecordRequest
        {
            StreamName = "test-stream",
            Data = new MemoryStream(Encoding.UTF8.GetBytes("hello kinesis")),
            PartitionKey = "pk1",
        });
        await _kin.PutRecordAsync(new PutRecordRequest
        {
            StreamName = "test-stream",
            Data = new MemoryStream(Encoding.UTF8.GetBytes("second record")),
            PartitionKey = "pk2",
        });

        var desc = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "test-stream" });
        var shardId = desc.StreamDescription.Shards[0].ShardId;

        var it = await _kin.GetShardIteratorAsync(new GetShardIteratorRequest
        {
            StreamName = "test-stream",
            ShardId = shardId,
            ShardIteratorType = ShardIteratorType.TRIM_HORIZON,
        });

        var records = await _kin.GetRecordsAsync(new GetRecordsRequest
        {
            ShardIterator = it.ShardIterator,
        });
        records.Records.Count.ShouldBe(2);
    }

    // ── GetRecords returns NextShardIterator ─────────────────────────────────────

    [Fact]
    public async Task GetRecordsReturnsNextIterator()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-next-it", ShardCount = 1 });

        await _kin.PutRecordAsync(new PutRecordRequest
        {
            StreamName = "kin-next-it",
            Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
            PartitionKey = "pk",
        });

        var desc = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-next-it" });
        var shardId = desc.StreamDescription.Shards[0].ShardId;

        var it = await _kin.GetShardIteratorAsync(new GetShardIteratorRequest
        {
            StreamName = "kin-next-it",
            ShardId = shardId,
            ShardIteratorType = ShardIteratorType.TRIM_HORIZON,
        });

        var result = await _kin.GetRecordsAsync(new GetRecordsRequest
        {
            ShardIterator = it.ShardIterator,
        });

        result.NextShardIterator.ShouldNotBeEmpty();

        // Using next iterator should return 0 records (no new data)
        var result2 = await _kin.GetRecordsAsync(new GetRecordsRequest
        {
            ShardIterator = result.NextShardIterator,
        });
        result2.Records.ShouldBeEmpty();
    }
}
