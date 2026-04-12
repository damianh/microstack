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
public sealed class EfsTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonElasticFileSystemClient _efs;

    public EfsTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _efs = CreateEfsClient(fixture);
    }

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

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _efs.Dispose();
        return Task.CompletedTask;
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

        Assert.StartsWith("fs-", createResp.FileSystemId);
        Assert.Equal(LifeCycleState.Available, createResp.LifeCycleState);
        Assert.Equal(ThroughputMode.Bursting, createResp.ThroughputMode);

        var descResp = await _efs.DescribeFileSystemsAsync(new DescribeFileSystemsRequest
        {
            FileSystemId = createResp.FileSystemId,
        });

        Assert.Single(descResp.FileSystems);
        Assert.Equal(createResp.FileSystemId, descResp.FileSystems[0].FileSystemId);
        Assert.Equal("test-fs", descResp.FileSystems[0].Name);
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

        Assert.Equal(r1.FileSystemId, r2.FileSystemId);
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

        Assert.Empty(descResp.FileSystems);
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

        Assert.StartsWith("fsmt-", mt.MountTargetId);
        Assert.Equal(LifeCycleState.Available, mt.LifeCycleState);

        var descResp = await _efs.DescribeMountTargetsAsync(new DescribeMountTargetsRequest
        {
            FileSystemId = fsId,
        });
        Assert.Single(descResp.MountTargets);
        Assert.Equal(mt.MountTargetId, descResp.MountTargets[0].MountTargetId);

        // Cannot delete file system with mount targets
        var ex = await Assert.ThrowsAsync<FileSystemInUseException>(() =>
            _efs.DeleteFileSystemAsync(new DeleteFileSystemRequest
            {
                FileSystemId = fsId,
            }));
        Assert.NotNull(ex);

        await _efs.DeleteMountTargetAsync(new DeleteMountTargetRequest
        {
            MountTargetId = mt.MountTargetId,
        });

        var desc2 = await _efs.DescribeMountTargetsAsync(new DescribeMountTargetsRequest
        {
            FileSystemId = fsId,
        });
        Assert.Empty(desc2.MountTargets);
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

        Assert.StartsWith("fsap-", ap.AccessPointId);
        Assert.Equal(LifeCycleState.Available, ap.LifeCycleState);

        var descResp = await _efs.DescribeAccessPointsAsync(new DescribeAccessPointsRequest
        {
            FileSystemId = fsId,
        });
        Assert.Contains(descResp.AccessPoints, a => a.AccessPointId == ap.AccessPointId);

        await _efs.DeleteAccessPointAsync(new DeleteAccessPointRequest
        {
            AccessPointId = ap.AccessPointId,
        });

        var desc2 = await _efs.DescribeAccessPointsAsync(new DescribeAccessPointsRequest
        {
            FileSystemId = fsId,
        });
        Assert.DoesNotContain(desc2.AccessPoints, a => a.AccessPointId == ap.AccessPointId);
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
        Assert.Equal("test", tagMap["env"]);
        Assert.Equal("data", tagMap["team"]);

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
        Assert.DoesNotContain("env", keys);
        Assert.Contains("team", keys);
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

        Assert.Single(resp.LifecyclePolicies);
        Assert.Equal(TransitionToIARules.AFTER_30_DAYS, resp.LifecyclePolicies[0].TransitionToIA);
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

        Assert.Equal(Status.ENABLED, resp.BackupPolicy.Status);
    }
}
