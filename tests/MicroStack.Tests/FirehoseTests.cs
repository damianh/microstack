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

        create.DeliveryStreamARN.ShouldContain("firehose");
        create.DeliveryStreamARN.ShouldContain("test-fh-basic");

        var desc = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-basic",
        });

        var stream = desc.DeliveryStreamDescription;
        stream.DeliveryStreamName.ShouldBe("test-fh-basic");
        stream.DeliveryStreamStatus.ShouldBe(DeliveryStreamStatus.ACTIVE);
        stream.DeliveryStreamType.ShouldBe(DeliveryStreamType.DirectPut);
        stream.Destinations.ShouldHaveSingleItem();
        stream.Destinations[0].ExtendedS3DestinationDescription.ShouldNotBeNull();
        stream.VersionId.ShouldBe("1");
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
        resp.DeliveryStreamNames.ShouldContain("test-fh-list-a");
        resp.DeliveryStreamNames.ShouldContain("test-fh-list-b");
        resp.HasMoreDeliveryStreams.ShouldBe(false);
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

        resp.RecordId.ShouldNotBeEmpty();
        resp.Encrypted.ShouldBe(false);
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

        resp.FailedPutCount.ShouldBe(0);
        resp.RequestResponses.Count.ShouldBe(5);
        foreach (var r in resp.RequestResponses)
        {
            r.RecordId.ShouldNotBeEmpty();
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

        var ex = await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
            {
                DeliveryStreamName = "test-fh-delete",
            }));
        ex.ErrorCode.ShouldBe("ResourceNotFoundException");
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
        tagMap["Env"].ShouldBe("test");
        tagMap["Team"].ShouldBe("data");

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
        keys.ShouldNotContain("Env");
        keys.ShouldContain("Team");
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
        desc2.DeliveryStreamDescription.VersionId.ShouldBe("2");
        desc2.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription.BucketARN.ShouldBe("arn:aws:s3:::updated-bucket");
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
        desc.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.Status.ShouldBe(DeliveryStreamEncryptionStatus.ENABLED);

        await _fh.StopDeliveryStreamEncryptionAsync(new StopDeliveryStreamEncryptionRequest
        {
            DeliveryStreamName = "test-fh-enc",
        });

        var desc2 = await _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = "test-fh-enc",
        });
        desc2.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.Status.ShouldBe(DeliveryStreamEncryptionStatus.DISABLED);
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

        var ex = await Should.ThrowAsync<ResourceInUseException>(() =>
            _fh.CreateDeliveryStreamAsync(new CreateDeliveryStreamRequest
            {
                DeliveryStreamName = "test-fh-dup",
                DeliveryStreamType = DeliveryStreamType.DirectPut,
            }));
        ex.ErrorCode.ShouldBe("ResourceInUseException");
    }

    // -- Not found error ------------------------------------------------------

    [Fact]
    public async Task DescribeNotFoundError()
    {
        var ex = await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _fh.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
            {
                DeliveryStreamName = "no-such-stream-xyz",
            }));
        ex.ErrorCode.ShouldBe("ResourceNotFoundException");
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
        resp.DeliveryStreamNames.ShouldContain("test-fh-type-dp");
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
        s3Desc.EncryptionConfiguration.ShouldNotBeNull();
        s3Desc.EncryptionConfiguration.NoEncryptionConfig?.Value.ShouldBe("NoEncryption");
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
        desc.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration?.Status.ShouldBeNull();
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
        s3Desc.BucketARN.ShouldBe("arn:aws:s3:::updated-bucket");
        s3Desc.Prefix.ShouldBe("original/");
        s3Desc.RoleARN.ShouldBe("arn:aws:iam::000000000000:role/firehose-role");
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

        var ex = await Should.ThrowAsync<ConcurrentModificationException>(() =>
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
        ex.ErrorCode.ShouldBe("ConcurrentModificationException");
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

        resp.FailedPutCount.ShouldBe(0);
        resp.RequestResponses.Count.ShouldBe(2);
    }
}
