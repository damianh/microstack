using Amazon;
using Amazon.ElasticFileSystem;
using Amazon.ElasticFileSystem.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the EFS (Elastic File System) service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_efs.py.
/// </summary>
public sealed class EfsTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly AmazonElasticFileSystemClient _efs = CreateEfsClient(fixture);

    private static AmazonElasticFileSystemClient CreateEfsClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonElasticFileSystemConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonElasticFileSystemClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async ValueTask InitializeAsync()
    {
        await fixture.HttpClient.PostAsync("/_microstack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _efs.Dispose();
        return ValueTask.CompletedTask;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CreateFileSystem + DescribeFileSystems
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateAndDescribeFileSystem()
    {
        var createResp = await _efs.CreateFileSystemAsync(new CreateFileSystemRequest
        {
            PerformanceMode = PerformanceMode.GeneralPurpose,
            ThroughputMode = ThroughputMode.Bursting,
            Encrypted = false,
            Tags =
            [
                new Tag { Key = "Name", Value = "test-fs" },
            ],
        });

        createResp.FileSystemId.ShouldStartWith("fs-");
        createResp.LifeCycleState.ShouldBe(LifeCycleState.Available);
        createResp.ThroughputMode.ShouldBe(ThroughputMode.Bursting);

        var descResp = await _efs.DescribeFileSystemsAsync(new DescribeFileSystemsRequest
        {
            FileSystemId = createResp.FileSystemId,
        });

        descResp.FileSystems.ShouldHaveSingleItem();
        descResp.FileSystems[0].FileSystemId.ShouldBe(createResp.FileSystemId);
        descResp.FileSystems[0].Name.ShouldBe("test-fs");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CreationToken idempotency
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreationTokenIdempotency()
    {
        var token = "unique-token-abc123";

        var r1 = await _efs.CreateFileSystemAsync(new CreateFileSystemRequest
        {
            CreationToken = token,
        });

        var r2 = await _efs.CreateFileSystemAsync(new CreateFileSystemRequest
        {
            CreationToken = token,
        });

        r2.FileSystemId.ShouldBe(r1.FileSystemId);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DeleteFileSystem
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DeleteFileSystem()
    {
        var createResp = await _efs.CreateFileSystemAsync(new CreateFileSystemRequest());
        var fsId = createResp.FileSystemId;

        await _efs.DeleteFileSystemAsync(new DeleteFileSystemRequest
        {
            FileSystemId = fsId,
        });

        var descResp = await _efs.DescribeFileSystemsAsync(new DescribeFileSystemsRequest
        {
            FileSystemId = fsId,
        });

        descResp.FileSystems.ShouldBeEmpty();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // MountTarget
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MountTargetCrud()
    {
        var fs = await _efs.CreateFileSystemAsync(new CreateFileSystemRequest());
        var fsId = fs.FileSystemId;

        var mt = await _efs.CreateMountTargetAsync(new CreateMountTargetRequest
        {
            FileSystemId = fsId,
            SubnetId = "subnet-00000001",
        });

        mt.MountTargetId.ShouldStartWith("fsmt-");
        mt.LifeCycleState.ShouldBe(LifeCycleState.Available);

        var descResp = await _efs.DescribeMountTargetsAsync(new DescribeMountTargetsRequest
        {
            FileSystemId = fsId,
        });
        descResp.MountTargets.ShouldHaveSingleItem();
        descResp.MountTargets[0].MountTargetId.ShouldBe(mt.MountTargetId);

        // Cannot delete file system with mount targets
        var ex = await Should.ThrowAsync<FileSystemInUseException>(() =>
            _efs.DeleteFileSystemAsync(new DeleteFileSystemRequest
            {
                FileSystemId = fsId,
            }));
        ex.ShouldNotBeNull();

        await _efs.DeleteMountTargetAsync(new DeleteMountTargetRequest
        {
            MountTargetId = mt.MountTargetId,
        });

        var desc2 = await _efs.DescribeMountTargetsAsync(new DescribeMountTargetsRequest
        {
            FileSystemId = fsId,
        });
        desc2.MountTargets.ShouldBeEmpty();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // AccessPoint
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AccessPointCrud()
    {
        var fs = await _efs.CreateFileSystemAsync(new CreateFileSystemRequest());
        var fsId = fs.FileSystemId;

        var ap = await _efs.CreateAccessPointAsync(new CreateAccessPointRequest
        {
            FileSystemId = fsId,
            Tags =
            [
                new Tag { Key = "Name", Value = "my-ap" },
            ],
            RootDirectory = new RootDirectory { Path = "/data" },
        });

        ap.AccessPointId.ShouldStartWith("fsap-");
        ap.LifeCycleState.ShouldBe(LifeCycleState.Available);

        var descResp = await _efs.DescribeAccessPointsAsync(new DescribeAccessPointsRequest
        {
            FileSystemId = fsId,
        });
        descResp.AccessPoints.ShouldContain(a => a.AccessPointId == ap.AccessPointId);

        await _efs.DeleteAccessPointAsync(new DeleteAccessPointRequest
        {
            AccessPointId = ap.AccessPointId,
        });

        var desc2 = await _efs.DescribeAccessPointsAsync(new DescribeAccessPointsRequest
        {
            FileSystemId = fsId,
        });
        desc2.AccessPoints.ShouldNotContain(a => a.AccessPointId == ap.AccessPointId);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tags
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TagOperations()
    {
        var fs = await _efs.CreateFileSystemAsync(new CreateFileSystemRequest
        {
            Tags =
            [
                new Tag { Key = "env", Value = "test" },
            ],
        });
        var fsArn = fs.FileSystemArn;

        await _efs.TagResourceAsync(new TagResourceRequest
        {
            ResourceId = fsArn,
            Tags = [new Tag { Key = "team", Value = "data" }],
        });

        var tagsResp = await _efs.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceId = fsArn,
        });
        var tagMap = tagsResp.Tags.ToDictionary(t => t.Key, t => t.Value);
        tagMap["env"].ShouldBe("test");
        tagMap["team"].ShouldBe("data");

        await _efs.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceId = fsArn,
            TagKeys = ["env"],
        });

        var tags2 = await _efs.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceId = fsArn,
        });
        var keys = tags2.Tags.ConvertAll(t => t.Key);
        keys.ShouldNotContain("env");
        keys.ShouldContain("team");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // LifecycleConfiguration
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task LifecycleConfiguration()
    {
        var fs = await _efs.CreateFileSystemAsync(new CreateFileSystemRequest());
        var fsId = fs.FileSystemId;

        await _efs.PutLifecycleConfigurationAsync(new PutLifecycleConfigurationRequest
        {
            FileSystemId = fsId,
            LifecyclePolicies =
            [
                new LifecyclePolicy { TransitionToIA = TransitionToIARules.AFTER_30_DAYS },
            ],
        });

        var resp = await _efs.DescribeLifecycleConfigurationAsync(
            new DescribeLifecycleConfigurationRequest
            {
                FileSystemId = fsId,
            });

        resp.LifecyclePolicies.ShouldHaveSingleItem();
        resp.LifecyclePolicies[0].TransitionToIA.ShouldBe(TransitionToIARules.AFTER_30_DAYS);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // BackupPolicy
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BackupPolicy()
    {
        var fs = await _efs.CreateFileSystemAsync(new CreateFileSystemRequest());
        var fsId = fs.FileSystemId;

        await _efs.PutBackupPolicyAsync(new PutBackupPolicyRequest
        {
            FileSystemId = fsId,
            BackupPolicy = new BackupPolicy { Status = Status.ENABLED },
        });

        var resp = await _efs.DescribeBackupPolicyAsync(new DescribeBackupPolicyRequest
        {
            FileSystemId = fsId,
        });

        resp.BackupPolicy.Status.ShouldBe(Status.ENABLED);
    }
}
