using Amazon;
using Amazon.ElastiCache;
using Amazon.ElastiCache.Model;
using Amazon.Runtime;
using Task = System.Threading.Tasks.Task;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the ElastiCache service handler.
/// Mirrors coverage from ministack/tests/test_elasticache.py.
/// </summary>
public sealed class ElastiCacheTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonElastiCacheClient _ec;

    public ElastiCacheTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _ec = CreateClient(fixture);
    }

    private static AmazonElastiCacheClient CreateClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        var config = new AmazonElastiCacheConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };
        return new AmazonElastiCacheClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _ec.Dispose();
        return Task.CompletedTask;
    }

    // ── Cache Cluster CRUD ────────────────────────────────────────────────────

    [Fact]
    public async Task CreateCacheCluster()
    {
        var resp = await _ec.CreateCacheClusterAsync(new CreateCacheClusterRequest
        {
            CacheClusterId = "test-redis",
            Engine = "redis",
            CacheNodeType = "cache.t3.micro",
            NumCacheNodes = 1,
        });

        Assert.Equal("test-redis", resp.CacheCluster.CacheClusterId);
        Assert.Equal("redis", resp.CacheCluster.Engine);
        Assert.Equal("available", resp.CacheCluster.CacheClusterStatus);
        Assert.Single(resp.CacheCluster.CacheNodes);
    }

    [Fact]
    public async Task DescribeCacheClusters()
    {
        await _ec.CreateCacheClusterAsync(new CreateCacheClusterRequest
        {
            CacheClusterId = "dc-a",
            Engine = "redis",
            CacheNodeType = "cache.t3.micro",
            NumCacheNodes = 1,
        });
        await _ec.CreateCacheClusterAsync(new CreateCacheClusterRequest
        {
            CacheClusterId = "dc-b",
            Engine = "memcached",
            CacheNodeType = "cache.t3.micro",
            NumCacheNodes = 1,
        });

        var resp = await _ec.DescribeCacheClustersAsync(new DescribeCacheClustersRequest());
        var ids = resp.CacheClusters.Select(c => c.CacheClusterId).ToList();
        Assert.Contains("dc-a", ids);
        Assert.Contains("dc-b", ids);

        var resp2 = await _ec.DescribeCacheClustersAsync(new DescribeCacheClustersRequest
        {
            CacheClusterId = "dc-b",
        });
        Assert.Equal("memcached", resp2.CacheClusters[0].Engine);
    }

    [Fact]
    public async Task DescribeCacheClusterNotFound()
    {
        var ex = await Assert.ThrowsAsync<CacheClusterNotFoundException>(() =>
            _ec.DescribeCacheClustersAsync(new DescribeCacheClustersRequest
            {
                CacheClusterId = "nonexistent",
            }));
        Assert.Contains("nonexistent", ex.Message);
    }

    [Fact]
    public async Task DeleteCacheCluster()
    {
        await _ec.CreateCacheClusterAsync(new CreateCacheClusterRequest
        {
            CacheClusterId = "to-delete",
            Engine = "redis",
            CacheNodeType = "cache.t3.micro",
            NumCacheNodes = 1,
        });

        var resp = await _ec.DeleteCacheClusterAsync(new DeleteCacheClusterRequest
        {
            CacheClusterId = "to-delete",
        });
        Assert.Equal("to-delete", resp.CacheCluster.CacheClusterId);
        Assert.Equal("deleting", resp.CacheCluster.CacheClusterStatus);

        await Assert.ThrowsAsync<CacheClusterNotFoundException>(() =>
            _ec.DescribeCacheClustersAsync(new DescribeCacheClustersRequest
            {
                CacheClusterId = "to-delete",
            }));
    }

    [Fact]
    public async Task ModifyCacheCluster()
    {
        await _ec.CreateCacheClusterAsync(new CreateCacheClusterRequest
        {
            CacheClusterId = "mod-cluster",
            Engine = "redis",
            CacheNodeType = "cache.t3.micro",
            NumCacheNodes = 1,
        });

        await _ec.ModifyCacheClusterAsync(new ModifyCacheClusterRequest
        {
            CacheClusterId = "mod-cluster",
            CacheNodeType = "cache.m5.large",
        });

        var desc = await _ec.DescribeCacheClustersAsync(new DescribeCacheClustersRequest
        {
            CacheClusterId = "mod-cluster",
        });
        Assert.Equal("cache.m5.large", desc.CacheClusters[0].CacheNodeType);
    }

    [Fact]
    public async Task CreateCacheClusterDuplicate()
    {
        await _ec.CreateCacheClusterAsync(new CreateCacheClusterRequest
        {
            CacheClusterId = "dup-cluster",
            Engine = "redis",
            CacheNodeType = "cache.t3.micro",
            NumCacheNodes = 1,
        });

        await Assert.ThrowsAsync<CacheClusterAlreadyExistsException>(() =>
            _ec.CreateCacheClusterAsync(new CreateCacheClusterRequest
            {
                CacheClusterId = "dup-cluster",
                Engine = "redis",
                CacheNodeType = "cache.t3.micro",
                NumCacheNodes = 1,
            }));
    }

    // ── Replication Groups ────────────────────────────────────────────────────

    [Fact]
    public async Task CreateReplicationGroup()
    {
        var resp = await _ec.CreateReplicationGroupAsync(new CreateReplicationGroupRequest
        {
            ReplicationGroupId = "test-rg",
            ReplicationGroupDescription = "Test RG",
            CacheNodeType = "cache.t3.micro",
            NumNodeGroups = 1,
            ReplicasPerNodeGroup = 1,
        });

        Assert.Equal("test-rg", resp.ReplicationGroup.ReplicationGroupId);
        Assert.Equal("available", resp.ReplicationGroup.Status);
        Assert.Single(resp.ReplicationGroup.NodeGroups);
    }

    [Fact]
    public async Task DescribeReplicationGroups()
    {
        await _ec.CreateReplicationGroupAsync(new CreateReplicationGroupRequest
        {
            ReplicationGroupId = "desc-rg",
            ReplicationGroupDescription = "Describe test",
            CacheNodeType = "cache.t3.micro",
        });

        var resp = await _ec.DescribeReplicationGroupsAsync(new DescribeReplicationGroupsRequest
        {
            ReplicationGroupId = "desc-rg",
        });
        Assert.Equal("desc-rg", resp.ReplicationGroups[0].ReplicationGroupId);
    }

    [Fact]
    public async Task DeleteReplicationGroup()
    {
        await _ec.CreateReplicationGroupAsync(new CreateReplicationGroupRequest
        {
            ReplicationGroupId = "del-rg",
            ReplicationGroupDescription = "Delete test",
            CacheNodeType = "cache.t3.micro",
        });

        await _ec.DeleteReplicationGroupAsync(new DeleteReplicationGroupRequest
        {
            ReplicationGroupId = "del-rg",
        });

        await Assert.ThrowsAsync<ReplicationGroupNotFoundException>(() =>
            _ec.DescribeReplicationGroupsAsync(new DescribeReplicationGroupsRequest
            {
                ReplicationGroupId = "del-rg",
            }));
    }

    [Fact]
    public async Task ModifyReplicationGroup()
    {
        await _ec.CreateReplicationGroupAsync(new CreateReplicationGroupRequest
        {
            ReplicationGroupId = "mod-rg",
            ReplicationGroupDescription = "Original desc",
            CacheNodeType = "cache.t3.micro",
        });

        await _ec.ModifyReplicationGroupAsync(new ModifyReplicationGroupRequest
        {
            ReplicationGroupId = "mod-rg",
            ReplicationGroupDescription = "Updated desc",
        });

        var desc = await _ec.DescribeReplicationGroupsAsync(new DescribeReplicationGroupsRequest
        {
            ReplicationGroupId = "mod-rg",
        });
        Assert.Equal("Updated desc", desc.ReplicationGroups[0].Description);
    }

    // ── Subnet Groups ─────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeCacheSubnetGroup()
    {
        await _ec.CreateCacheSubnetGroupAsync(new CreateCacheSubnetGroupRequest
        {
            CacheSubnetGroupName = "test-sg",
            CacheSubnetGroupDescription = "Test SG",
            SubnetIds = ["subnet-aaa"],
        });

        var resp = await _ec.DescribeCacheSubnetGroupsAsync(new DescribeCacheSubnetGroupsRequest
        {
            CacheSubnetGroupName = "test-sg",
        });
        Assert.Single(resp.CacheSubnetGroups);
        Assert.Equal("test-sg", resp.CacheSubnetGroups[0].CacheSubnetGroupName);
    }

    [Fact]
    public async Task ModifyCacheSubnetGroup()
    {
        await _ec.CreateCacheSubnetGroupAsync(new CreateCacheSubnetGroupRequest
        {
            CacheSubnetGroupName = "mod-sg",
            CacheSubnetGroupDescription = "Original",
            SubnetIds = ["subnet-aaa"],
        });

        await _ec.ModifyCacheSubnetGroupAsync(new ModifyCacheSubnetGroupRequest
        {
            CacheSubnetGroupName = "mod-sg",
            CacheSubnetGroupDescription = "Updated",
            SubnetIds = ["subnet-bbb"],
        });

        var resp = await _ec.DescribeCacheSubnetGroupsAsync(new DescribeCacheSubnetGroupsRequest
        {
            CacheSubnetGroupName = "mod-sg",
        });
        Assert.Equal("Updated", resp.CacheSubnetGroups[0].CacheSubnetGroupDescription);
    }

    [Fact]
    public async Task DeleteCacheSubnetGroup()
    {
        await _ec.CreateCacheSubnetGroupAsync(new CreateCacheSubnetGroupRequest
        {
            CacheSubnetGroupName = "del-sg",
            CacheSubnetGroupDescription = "To delete",
            SubnetIds = ["subnet-aaa"],
        });

        await _ec.DeleteCacheSubnetGroupAsync(new DeleteCacheSubnetGroupRequest
        {
            CacheSubnetGroupName = "del-sg",
        });

        // After deletion, describe all should not contain it
        var resp = await _ec.DescribeCacheSubnetGroupsAsync(new DescribeCacheSubnetGroupsRequest());
        Assert.True(
            resp.CacheSubnetGroups is null || !resp.CacheSubnetGroups.Any(g => g.CacheSubnetGroupName == "del-sg"));
    }

    // ── Parameter Groups ──────────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeCacheParameterGroup()
    {
        await _ec.CreateCacheParameterGroupAsync(new CreateCacheParameterGroupRequest
        {
            CacheParameterGroupName = "test-pg",
            CacheParameterGroupFamily = "redis7.0",
            Description = "Test param group",
        });

        var resp = await _ec.DescribeCacheParameterGroupsAsync(new DescribeCacheParameterGroupsRequest
        {
            CacheParameterGroupName = "test-pg",
        });
        Assert.Single(resp.CacheParameterGroups);
        Assert.Equal("test-pg", resp.CacheParameterGroups[0].CacheParameterGroupName);
        Assert.Equal("redis7.0", resp.CacheParameterGroups[0].CacheParameterGroupFamily);
    }

    [Fact]
    public async Task DeleteCacheParameterGroup()
    {
        await _ec.CreateCacheParameterGroupAsync(new CreateCacheParameterGroupRequest
        {
            CacheParameterGroupName = "del-pg",
            CacheParameterGroupFamily = "redis7.0",
            Description = "To delete",
        });

        await _ec.DeleteCacheParameterGroupAsync(new DeleteCacheParameterGroupRequest
        {
            CacheParameterGroupName = "del-pg",
        });

        var all = await _ec.DescribeCacheParameterGroupsAsync(new DescribeCacheParameterGroupsRequest());
        Assert.True(
            all.CacheParameterGroups is null || !all.CacheParameterGroups.Any(g => g.CacheParameterGroupName == "del-pg"));
    }

    [Fact]
    public async Task DescribeCacheParameters()
    {
        await _ec.CreateCacheParameterGroupAsync(new CreateCacheParameterGroupRequest
        {
            CacheParameterGroupName = "params-pg",
            CacheParameterGroupFamily = "redis7.0",
            Description = "test",
        });

        var resp = await _ec.DescribeCacheParametersAsync(new DescribeCacheParametersRequest
        {
            CacheParameterGroupName = "params-pg",
        });
        Assert.NotEmpty(resp.Parameters);
    }

    [Fact]
    public async Task ModifyCacheParameterGroup()
    {
        await _ec.CreateCacheParameterGroupAsync(new CreateCacheParameterGroupRequest
        {
            CacheParameterGroupName = "modify-pg",
            CacheParameterGroupFamily = "redis7.0",
            Description = "test",
        });

        await _ec.ModifyCacheParameterGroupAsync(new ModifyCacheParameterGroupRequest
        {
            CacheParameterGroupName = "modify-pg",
            ParameterNameValues =
            [
                new ParameterNameValue { ParameterName = "maxmemory-policy", ParameterValue = "allkeys-lru" },
            ],
        });

        var resp = await _ec.DescribeCacheParametersAsync(new DescribeCacheParametersRequest
        {
            CacheParameterGroupName = "modify-pg",
        });
        var maxmem = resp.Parameters.FirstOrDefault(p => p.ParameterName == "maxmemory-policy");
        Assert.NotNull(maxmem);
        Assert.Equal("allkeys-lru", maxmem.ParameterValue);
    }

    // ── Engine Versions ───────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeCacheEngineVersionsRedis()
    {
        var resp = await _ec.DescribeCacheEngineVersionsAsync(new DescribeCacheEngineVersionsRequest
        {
            Engine = "redis",
        });
        Assert.NotEmpty(resp.CacheEngineVersions);
        Assert.All(resp.CacheEngineVersions, v => Assert.Equal("redis", v.Engine));
    }

    [Fact]
    public async Task DescribeCacheEngineVersionsMemcached()
    {
        var resp = await _ec.DescribeCacheEngineVersionsAsync(new DescribeCacheEngineVersionsRequest
        {
            Engine = "memcached",
        });
        Assert.NotEmpty(resp.CacheEngineVersions);
    }

    // ── Snapshots ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeSnapshot()
    {
        await _ec.CreateCacheClusterAsync(new CreateCacheClusterRequest
        {
            CacheClusterId = "snap-cluster",
            Engine = "redis",
            CacheNodeType = "cache.t3.micro",
            NumCacheNodes = 1,
        });

        var resp = await _ec.CreateSnapshotAsync(new CreateSnapshotRequest
        {
            SnapshotName = "test-snap",
            CacheClusterId = "snap-cluster",
        });
        Assert.Equal("test-snap", resp.Snapshot.SnapshotName);
        Assert.Equal("available", resp.Snapshot.SnapshotStatus);

        var desc = await _ec.DescribeSnapshotsAsync(new DescribeSnapshotsRequest
        {
            SnapshotName = "test-snap",
        });
        Assert.Single(desc.Snapshots);
        Assert.Equal("test-snap", desc.Snapshots[0].SnapshotName);
    }

    [Fact]
    public async Task DeleteSnapshot()
    {
        await _ec.CreateSnapshotAsync(new CreateSnapshotRequest
        {
            SnapshotName = "del-snap",
        });

        var resp = await _ec.DeleteSnapshotAsync(new DeleteSnapshotRequest
        {
            SnapshotName = "del-snap",
        });
        Assert.Equal("deleting", resp.Snapshot.SnapshotStatus);
    }

    // ── Tags ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task AddAndListAndRemoveTags()
    {
        await _ec.CreateCacheClusterAsync(new CreateCacheClusterRequest
        {
            CacheClusterId = "tag-cluster",
            Engine = "redis",
            CacheNodeType = "cache.t3.micro",
            NumCacheNodes = 1,
        });

        var descResp = await _ec.DescribeCacheClustersAsync(new DescribeCacheClustersRequest
        {
            CacheClusterId = "tag-cluster",
        });
        var arn = descResp.CacheClusters[0].ARN;

        await _ec.AddTagsToResourceAsync(new AddTagsToResourceRequest
        {
            ResourceName = arn,
            Tags =
            [
                new Tag { Key = "env", Value = "prod" },
                new Tag { Key = "tier", Value = "cache" },
            ],
        });

        var tagsResp = await _ec.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceName = arn,
        });
        var tagMap = tagsResp.TagList.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal("prod", tagMap["env"]);
        Assert.Equal("cache", tagMap["tier"]);

        await _ec.RemoveTagsFromResourceAsync(new RemoveTagsFromResourceRequest
        {
            ResourceName = arn,
            TagKeys = ["env"],
        });

        var tags2 = await _ec.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceName = arn,
        });
        Assert.DoesNotContain(tags2.TagList, t => t.Key == "env");
        Assert.Contains(tags2.TagList, t => t.Key == "tier");
    }

    // ── Users ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeAndDeleteUser()
    {
        await _ec.CreateUserAsync(new CreateUserRequest
        {
            UserId = "test-user",
            UserName = "test-user",
            Engine = "redis",
            AccessString = "on ~* +@all",
            NoPasswordRequired = true,
        });

        var desc = await _ec.DescribeUsersAsync(new DescribeUsersRequest
        {
            UserId = "test-user",
        });
        Assert.Single(desc.Users);
        Assert.Equal("test-user", desc.Users[0].UserId);

        await _ec.ModifyUserAsync(new ModifyUserRequest
        {
            UserId = "test-user",
            AccessString = "on ~keys:* +get",
        });

        await _ec.DeleteUserAsync(new DeleteUserRequest
        {
            UserId = "test-user",
        });

        await Assert.ThrowsAsync<UserNotFoundException>(() =>
            _ec.DescribeUsersAsync(new DescribeUsersRequest { UserId = "test-user" }));
    }

    // ── User Groups ───────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeAndDeleteUserGroup()
    {
        await _ec.CreateUserAsync(new CreateUserRequest
        {
            UserId = "ug-user",
            UserName = "ug-user",
            Engine = "redis",
            AccessString = "on ~* +@all",
            NoPasswordRequired = true,
        });

        await _ec.CreateUserGroupAsync(new CreateUserGroupRequest
        {
            UserGroupId = "test-ug",
            Engine = "redis",
            UserIds = ["ug-user"],
        });

        var desc = await _ec.DescribeUserGroupsAsync(new DescribeUserGroupsRequest
        {
            UserGroupId = "test-ug",
        });
        Assert.Single(desc.UserGroups);
        Assert.Equal("test-ug", desc.UserGroups[0].UserGroupId);

        await _ec.DeleteUserGroupAsync(new DeleteUserGroupRequest
        {
            UserGroupId = "test-ug",
        });
        await _ec.DeleteUserAsync(new DeleteUserRequest { UserId = "ug-user" });
    }

    // ── Reboot ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RebootCacheCluster()
    {
        await _ec.CreateCacheClusterAsync(new CreateCacheClusterRequest
        {
            CacheClusterId = "reboot-cluster",
            Engine = "redis",
            CacheNodeType = "cache.t3.micro",
            NumCacheNodes = 1,
        });

        var resp = await _ec.RebootCacheClusterAsync(new RebootCacheClusterRequest
        {
            CacheClusterId = "reboot-cluster",
            CacheNodeIdsToReboot = ["0001"],
        });
        Assert.Equal("reboot-cluster", resp.CacheCluster.CacheClusterId);
    }
}
