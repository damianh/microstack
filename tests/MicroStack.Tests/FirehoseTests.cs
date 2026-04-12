using Amazon;
using Amazon.KinesisFirehose;
using Amazon.KinesisFirehose.Model;
using Amazon.Runtime;
using FirehoseRecord = Amazon.KinesisFirehose.Model.Record;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the Firehose service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_firehose.py.
/// </summary>
public sealed class FirehoseTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonKinesisFirehoseClient _fh;

    public FirehoseTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _fh = CreateClient(fixture);
    }

    private static AmazonKinesisFirehoseClient CreateClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonKinesisFirehoseConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonKinesisFirehoseClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _fh.Dispose();
        return Task.CompletedTask;
    }

    // -- CreateDeliveryStream / DescribeDeliveryStream -------------------------

    [Fact]
    public async Task CreateAndDescribeDeliveryStream()
    {
        var create = await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-basic",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
            ExtendedS3DestinationConfiguration = new ExtendedS3DestinationConfiguration
            {
                BucketARN = "arn:aws:s3:::my-bucket",
                RoleARN = "arn:aws:iam::000000000000:role/firehose-role",
            },
        });

        Assert.Contains("firehose", create.DeliveryStreamARN);
        Assert.Contains("test-fh-basic", create.DeliveryStreamARN);

        var desc = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-basic",
        });

        var stream = desc.DeliveryStreamDescription;
        Assert.Equal("test-fh-basic", stream.DeliveryStreamName);
        Assert.Equal(DeliveryStreamStatus.ACTIVE, stream.DeliveryStreamStatus);
        Assert.Equal(DeliveryStreamType.DirectPut, stream.DeliveryStreamType);
        Assert.Single(stream.Destinations);
        Assert.NotNull(stream.Destinations[0].ExtendedS3DestinationDescription);
        Assert.Equal("1", stream.VersionId);
    }

    // -- ListDeliveryStreams ---------------------------------------------------

    [Fact]
    public async Task ListDeliveryStreams()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-list-a",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
        });
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-list-b",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
        });

        var resp = await _fh.ListDeliveryStreamsAsync(new ListDeliveryStreamsRequest());
        Assert.Contains("test-fh-list-a", resp.DeliveryStreamNames);
        Assert.Contains("test-fh-list-b", resp.DeliveryStreamNames);
        Assert.False(resp.HasMoreDeliveryStreams);
    }

    // -- PutRecord ------------------------------------------------------------

    [Fact]
    public async Task PutRecord()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-put",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
        });

        using var ms = new MemoryStream("hello firehose"u8.ToArray());
        var resp = await _fh.PutRecordAsync(new PutRecordRequest
        {
            DeliveryStreamName = "test-fh-put",
            Record = new FirehoseRecord { Data = ms },
        });

        Assert.NotEmpty(resp.RecordId);
        Assert.False(resp.Encrypted);
    }

    // -- PutRecordBatch -------------------------------------------------------

    [Fact]
    public async Task PutRecordBatch()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-batch",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
        });

        var records = new List<FirehoseRecord>();
        for (var i = 0; i < 5; i++)
        {
            records.Add(new FirehoseRecord { Data = new MemoryStream(System.Text.Encoding.UTF8.GetBytes($"record-{i}")) });
        }

        var resp = await _fh.PutRecordBatchAsync(new PutRecordBatchRequest
        {
            DeliveryStreamName = "test-fh-batch",
            Records = records,
        });

        Assert.Equal(0, resp.FailedPutCount);
        Assert.Equal(5, resp.RequestResponses.Count);
        foreach (var r in resp.RequestResponses)
        {
            Assert.NotEmpty(r.RecordId);
        }
    }

    // -- DeleteDeliveryStream -------------------------------------------------

    [Fact]
    public async Task DeleteDeliveryStream()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-delete",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
        });

        await _fh.DeleteDeliveryStreamAsync(new DeleteDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-delete",
        });

        var ex = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
            {
                DeliveryStreamName = "test-fh-delete",
            }));
        Assert.Equal("ResourceNotFoundException", ex.ErrorCode);
    }

    // -- Tags -----------------------------------------------------------------

    [Fact]
    public async Task TagAndUntagDeliveryStream()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-tags",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
        });

        await _fh.TagDeliveryStreamAsync(new TagDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-tags",
            Tags =
            [
                new Tag { Key = "Env", Value = "test" },
                new Tag { Key = "Team", Value = "data" },
            ],
        });

        var resp = await _fh.ListTagsForDeliveryStreamAsync(new ListTagsForDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-tags",
        });
        var tagMap = resp.Tags.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal("test", tagMap["Env"]);
        Assert.Equal("data", tagMap["Team"]);

        await _fh.UntagDeliveryStreamAsync(new UntagDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-tags",
            TagKeys = ["Env"],
        });

        var resp2 = await _fh.ListTagsForDeliveryStreamAsync(new ListTagsForDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-tags",
        });
        var keys = resp2.Tags.Select(t => t.Key).ToList();
        Assert.DoesNotContain("Env", keys);
        Assert.Contains("Team", keys);
    }

    // -- UpdateDestination ----------------------------------------------------

    [Fact]
    public async Task UpdateDestination()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-update-dest",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
            ExtendedS3DestinationConfiguration = new ExtendedS3DestinationConfiguration
            {
                BucketARN = "arn:aws:s3:::original-bucket",
                RoleARN = "arn:aws:iam::000000000000:role/firehose-role",
            },
        });

        var desc = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-update-dest",
        });
        var destId = desc.DeliveryStreamDescription.Destinations[0].DestinationId;
        var versionId = desc.DeliveryStreamDescription.VersionId;

        await _fh.UpdateDestinationAsync(new UpdateDestinationRequest
        {
            DeliveryStreamName = "test-fh-update-dest",
            DestinationId = destId,
            CurrentDeliveryStreamVersionId = versionId,
            ExtendedS3DestinationUpdate = new ExtendedS3DestinationUpdate
            {
                BucketARN = "arn:aws:s3:::updated-bucket",
                RoleARN = "arn:aws:iam::000000000000:role/firehose-role",
            },
        });

        var desc2 = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-update-dest",
        });
        Assert.Equal("2", desc2.DeliveryStreamDescription.VersionId);
        Assert.Equal("arn:aws:s3:::updated-bucket",
            desc2.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription.BucketARN);
    }

    // -- Encryption -----------------------------------------------------------

    [Fact]
    public async Task StartAndStopEncryption()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-enc",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
        });

        await _fh.StartDeliveryStreamEncryptionAsync(new StartDeliveryStreamEncryptionRequest
        {
            DeliveryStreamName = "test-fh-enc",
            DeliveryStreamEncryptionConfigurationInput = new DeliveryStreamEncryptionConfigurationInput
            {
                KeyType = KeyType.AWS_OWNED_CMK,
            },
        });

        var desc = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-enc",
        });
        Assert.Equal(DeliveryStreamEncryptionStatus.ENABLED,
            desc.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.Status);

        await _fh.StopDeliveryStreamEncryptionAsync(new StopDeliveryStreamEncryptionRequest
        {
            DeliveryStreamName = "test-fh-enc",
        });

        var desc2 = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-enc",
        });
        Assert.Equal(DeliveryStreamEncryptionStatus.DISABLED,
            desc2.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.Status);
    }

    // -- Duplicate create error -----------------------------------------------

    [Fact]
    public async Task DuplicateCreateError()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-dup",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
        });

        var ex = await Assert.ThrowsAsync<ResourceInUseException>(() =>
            _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
            {
                DeliveryStreamName = "test-fh-dup",
                DeliveryStreamType = DeliveryStreamType.DirectPut,
            }));
        Assert.Equal("ResourceInUseException", ex.ErrorCode);
    }

    // -- Not found error ------------------------------------------------------

    [Fact]
    public async Task DescribeNotFoundError()
    {
        var ex = await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
            _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
            {
                DeliveryStreamName = "no-such-stream-xyz",
            }));
        Assert.Equal("ResourceNotFoundException", ex.ErrorCode);
    }

    // -- List with type filter ------------------------------------------------

    [Fact]
    public async Task ListWithTypeFilter()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-type-dp",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
        });

        var resp = await _fh.ListDeliveryStreamsAsync(new ListDeliveryStreamsRequest
        {
            DeliveryStreamType = DeliveryStreamType.DirectPut,
        });
        Assert.Contains("test-fh-type-dp", resp.DeliveryStreamNames);
    }

    // -- S3 destination has encryption config ---------------------------------

    [Fact]
    public async Task S3DestinationHasEncryptionConfig()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-enc-cfg",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
            ExtendedS3DestinationConfiguration = new ExtendedS3DestinationConfiguration
            {
                BucketARN = "arn:aws:s3:::my-bucket",
                RoleARN = "arn:aws:iam::000000000000:role/firehose-role",
            },
        });

        var desc = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-enc-cfg",
        });
        var s3Desc = desc.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription;
        Assert.NotNull(s3Desc.EncryptionConfiguration);
        Assert.Equal("NoEncryption", s3Desc.EncryptionConfiguration.NoEncryptionConfig?.Value);
    }

    // -- No encryption config when not set ------------------------------------

    [Fact]
    public async Task NoEncryptionConfigWhenNotSet()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-no-enc",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
        });

        var desc = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-no-enc",
        });
        Assert.Null(desc.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration?.Status);
    }

    // -- Update destination merges same type -----------------------------------

    [Fact]
    public async Task UpdateDestinationMergesSameType()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-merge",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
            ExtendedS3DestinationConfiguration = new ExtendedS3DestinationConfiguration
            {
                BucketARN = "arn:aws:s3:::original-bucket",
                RoleARN = "arn:aws:iam::000000000000:role/firehose-role",
                Prefix = "original/",
            },
        });

        var desc = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-merge",
        });
        var destId = desc.DeliveryStreamDescription.Destinations[0].DestinationId;

        await _fh.UpdateDestinationAsync(new UpdateDestinationRequest
        {
            DeliveryStreamName = "test-fh-merge",
            DestinationId = destId,
            CurrentDeliveryStreamVersionId = desc.DeliveryStreamDescription.VersionId,
            ExtendedS3DestinationUpdate = new ExtendedS3DestinationUpdate
            {
                BucketARN = "arn:aws:s3:::updated-bucket",
            },
        });

        var desc2 = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-merge",
        });
        var s3Desc = desc2.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription;
        Assert.Equal("arn:aws:s3:::updated-bucket", s3Desc.BucketARN);
        Assert.Equal("original/", s3Desc.Prefix);
        Assert.Equal("arn:aws:iam::000000000000:role/firehose-role", s3Desc.RoleARN);
    }

    // -- Update destination version mismatch error ----------------------------

    [Fact]
    public async Task UpdateDestinationVersionMismatch()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-version-check",
            DeliveryStreamType = DeliveryStreamType.DirectPut,
            ExtendedS3DestinationConfiguration = new ExtendedS3DestinationConfiguration
            {
                BucketARN = "arn:aws:s3:::qa-fh-bucket2",
                RoleARN = "arn:aws:iam::000000000000:role/r",
            },
        });

        var desc = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-version-check",
        });
        var destId = desc.DeliveryStreamDescription.Destinations[0].DestinationId;

        var ex = await Assert.ThrowsAsync<ConcurrentModificationException>(() =>
            _fh.UpdateDestinationAsync(new UpdateDestinationRequest
            {
                DeliveryStreamName = "test-fh-version-check",
                CurrentDeliveryStreamVersionId = "999",
                DestinationId = destId,
                ExtendedS3DestinationUpdate = new ExtendedS3DestinationUpdate
                {
                    BucketARN = "arn:aws:s3:::qa-fh-bucket2-updated",
                    RoleARN = "arn:aws:iam::000000000000:role/r",
                },
            }));
        Assert.Equal("ConcurrentModificationException", ex.ErrorCode);
    }

    // -- PutRecordBatch with valid records returns FailedPutCount 0 -----------

    [Fact]
    public async Task PutRecordBatchFailureCountZero()
    {
        await _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-batch-fail",
            ExtendedS3DestinationConfiguration = new ExtendedS3DestinationConfiguration
            {
                BucketARN = "arn:aws:s3:::qa-fh-bucket",
                RoleARN = "arn:aws:iam::000000000000:role/r",
            },
        });

        var resp = await _fh.PutRecordBatchAsync(new PutRecordBatchRequest
        {
            DeliveryStreamName = "test-fh-batch-fail",
            Records =
            [
                new FirehoseRecord { Data = new MemoryStream("hello"u8.ToArray()) },
                new FirehoseRecord { Data = new MemoryStream("world"u8.ToArray()) },
            ],
        });

        Assert.Equal(0, resp.FailedPutCount);
        Assert.Equal(2, resp.RequestResponses.Count);
    }
}
