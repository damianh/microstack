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
        Assert.Equal("kin-cs-v2", sd.StreamName);
        Assert.Equal(StreamStatus.ACTIVE, sd.StreamStatus);
        Assert.Equal(2, sd.Shards.Count);
    }

    [Fact]
    public async Task DeleteStream()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-del-v2", ShardCount = 1 });
        await _kin.DeleteStreamAsync(new DeleteStreamRequest { StreamName = "kin-del-v2" });

        var ex = await Assert.ThrowsAsync<ResourceNotFoundException>(
            () => _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-del-v2" }));
        Assert.Equal("ResourceNotFoundException", ex.ErrorCode);
    }

    [Fact]
    public async Task ListStreams()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-ls-v2a", ShardCount = 1 });
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-ls-v2b", ShardCount = 1 });

        var resp = await _kin.ListStreamsAsync(new ListStreamsRequest());
        Assert.Contains("kin-ls-v2a", resp.StreamNames);
        Assert.Contains("kin-ls-v2b", resp.StreamNames);
    }

    [Fact]
    public async Task DescribeStreamAndSummary()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-desc-v2", ShardCount = 1 });

        var resp = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-desc-v2" });
        var sd = resp.StreamDescription;
        Assert.Equal("kin-desc-v2", sd.StreamName);
        Assert.Equal(24, sd.RetentionPeriodHours);
        Assert.NotEmpty(sd.StreamARN);
        Assert.Single(sd.Shards);

        var summary = await _kin.DescribeStreamSummaryAsync(
            new DescribeStreamSummaryRequest { StreamName = "kin-desc-v2" });
        Assert.Equal("kin-desc-v2", summary.StreamDescriptionSummary.StreamName);
    }

    [Fact]
    public async Task ListShards()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-lsh-v2", ShardCount = 3 });

        var resp = await _kin.ListShardsAsync(new ListShardsRequest { StreamName = "kin-lsh-v2" });
        Assert.Equal(3, resp.Shards.Count);
        Assert.All(resp.Shards, s =>
        {
            Assert.NotEmpty(s.ShardId);
            Assert.NotNull(s.HashKeyRange);
        });
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
        Assert.Equal(3, records.Records.Count);

        // Verify first record data
        using var reader = new StreamReader(records.Records[0].Data);
        var data = await reader.ReadToEndAsync();
        Assert.Equal("rec1", data);
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

        Assert.Equal(0, resp.FailedRecordCount);
        Assert.Equal(7, resp.Records.Count);
        Assert.All(resp.Records, r =>
        {
            Assert.NotEmpty(r.ShardId);
            Assert.NotEmpty(r.SequenceNumber);
        });
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
        Assert.Equal("test", tagMap["env"]);
        Assert.Equal("data", tagMap["team"]);

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
        Assert.DoesNotContain("team", tagMap2.Keys);
        Assert.Equal("test", tagMap2["env"]);
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
        Assert.Equal(48, desc.StreamDescription.RetentionPeriodHours);

        await _kin.DecreaseStreamRetentionPeriodAsync(new DecreaseStreamRetentionPeriodRequest
        {
            StreamName = "kin-retention",
            RetentionPeriodHours = 24,
        });
        var desc2 = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-retention" });
        Assert.Equal(24, desc2.StreamDescription.RetentionPeriodHours);
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
        Assert.Equal(EncryptionType.KMS, desc.StreamDescription.EncryptionType);

        await _kin.StopStreamEncryptionAsync(new StopStreamEncryptionRequest
        {
            StreamName = "kin-enc",
            EncryptionType = EncryptionType.KMS,
            KeyId = "alias/aws/kinesis",
        });
        var desc2 = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-enc" });
        Assert.Equal(EncryptionType.NONE, desc2.StreamDescription.EncryptionType);
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
        Assert.Contains("IncomingBytes", enable.DesiredShardLevelMetrics);

        var disable = await _kin.DisableEnhancedMonitoringAsync(new DisableEnhancedMonitoringRequest
        {
            StreamName = "kin-mon",
            ShardLevelMetrics = ["IncomingBytes"],
        });
        Assert.DoesNotContain("IncomingBytes", disable.DesiredShardLevelMetrics);
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
        Assert.Equal(2, desc2.StreamDescription.Shards.Count);
    }

    [Fact]
    public async Task MergeShards()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-merge", ShardCount = 2 });

        var desc = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-merge" });
        var shards = desc.StreamDescription.Shards;
        Assert.Equal(2, shards.Count);

        var sorted = shards.OrderBy(s =>
            System.Numerics.BigInteger.Parse(s.HashKeyRange.StartingHashKey)).ToList();

        await _kin.MergeShardsAsync(new MergeShardsRequest
        {
            StreamName = "kin-merge",
            ShardToMerge = sorted[0].ShardId,
            AdjacentShardToMerge = sorted[1].ShardId,
        });

        var desc2 = await _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "kin-merge" });
        Assert.Single(desc2.StreamDescription.Shards);
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
        Assert.Equal(2, resp.TargetShardCount);
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
        Assert.Equal("my-consumer", reg.Consumer.ConsumerName);
        Assert.Equal(ConsumerStatus.ACTIVE, reg.Consumer.ConsumerStatus);
        var consumerArn = reg.Consumer.ConsumerARN;

        var consumers = await _kin.ListStreamConsumersAsync(new ListStreamConsumersRequest
        {
            StreamARN = streamArn,
        });
        Assert.Contains(consumers.Consumers, c => c.ConsumerName == "my-consumer");

        var descC = await _kin.DescribeStreamConsumerAsync(new DescribeStreamConsumerRequest
        {
            ConsumerARN = consumerArn,
        });
        Assert.Equal("my-consumer", descC.ConsumerDescription.ConsumerName);

        await _kin.DeregisterStreamConsumerAsync(new DeregisterStreamConsumerRequest
        {
            ConsumerARN = consumerArn,
        });

        var consumers2 = await _kin.ListStreamConsumersAsync(new ListStreamConsumersRequest
        {
            StreamARN = streamArn,
        });
        Assert.DoesNotContain(consumers2.Consumers, c => c.ConsumerName == "my-consumer");
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
        Assert.NotEmpty(records.Records);

        using var reader = new StreamReader(records.Records[0].Data);
        var data = await reader.ReadToEndAsync();
        Assert.Equal("after-ts", data);
    }

    // ── Duplicate stream fails ──────────────────────────────────────────────────

    [Fact]
    public async Task CreateDuplicateStreamFails()
    {
        await _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-dup", ShardCount = 1 });

        var ex = await Assert.ThrowsAsync<ResourceInUseException>(
            () => _kin.CreateStreamAsync(new CreateStreamRequest { StreamName = "kin-dup", ShardCount = 1 }));
        Assert.Equal("ResourceInUseException", ex.ErrorCode);
    }

    // ── Stream not found ────────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeNonexistentStreamFails()
    {
        var ex = await Assert.ThrowsAsync<ResourceNotFoundException>(
            () => _kin.DescribeStreamAsync(new DescribeStreamRequest { StreamName = "no-such-stream" }));
        Assert.Equal("ResourceNotFoundException", ex.ErrorCode);
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
        Assert.Equal(2, records.Records.Count);
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

        Assert.NotEmpty(result.NextShardIterator);

        // Using next iterator should return 0 records (no new data)
        var result2 = await _kin.GetRecordsAsync(new GetRecordsRequest
        {
            ShardIterator = result.NextShardIterator,
        });
        Assert.Empty(result2.Records);
    }
}
