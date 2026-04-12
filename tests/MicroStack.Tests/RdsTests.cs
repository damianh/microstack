using Amazon;
using Amazon.RDS;
using Amazon.RDS.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

public sealed class RdsTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonRDSClient _rds;

    public RdsTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _rds = CreateRdsClient(fixture);
    }

    private static AmazonRDSClient CreateRdsClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        var config = new AmazonRDSConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };
        return new AmazonRDSClient(new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _rds.Dispose();
        return Task.CompletedTask;
    }

    // ── DB Instance CRUD ────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateDbInstance()
    {
        var resp = await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "test-db",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "password123",
            DBName = "testdb",
            AllocatedStorage = 20,
        });

        var inst = resp.DBInstance;
        Assert.Equal("test-db", inst.DBInstanceIdentifier);
        Assert.Equal("available", inst.DBInstanceStatus);
        Assert.Equal("postgres", inst.Engine);
        Assert.NotNull(inst.Endpoint);
        Assert.NotEmpty(inst.Endpoint.Address);
        Assert.True(inst.Endpoint.Port > 0);
    }

    [Fact]
    public async Task DescribeDbInstances()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-di-v2a",
            DBInstanceClass = "db.t3.micro",
            Engine = "mysql",
            MasterUsername = "admin",
            MasterUserPassword = "pass",
            AllocatedStorage = 10,
        });
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-di-v2b",
            DBInstanceClass = "db.t3.small",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "pass",
            AllocatedStorage = 20,
        });

        var resp = await _rds.DescribeDBInstancesAsync(new DescribeDBInstancesRequest());
        var ids = resp.DBInstances.Select(i => i.DBInstanceIdentifier).ToList();
        Assert.Contains("rds-di-v2a", ids);
        Assert.Contains("rds-di-v2b", ids);

        var resp2 = await _rds.DescribeDBInstancesAsync(new DescribeDBInstancesRequest
        {
            DBInstanceIdentifier = "rds-di-v2a",
        });
        Assert.Single(resp2.DBInstances);
        Assert.Equal("mysql", resp2.DBInstances[0].Engine);
    }

    [Fact]
    public async Task ModifyDbInstance()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-mod-v2",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "pass",
            AllocatedStorage = 20,
        });

        await _rds.ModifyDBInstanceAsync(new ModifyDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-mod-v2",
            DBInstanceClass = "db.t3.small",
            AllocatedStorage = 50,
            ApplyImmediately = true,
        });

        var resp = await _rds.DescribeDBInstancesAsync(new DescribeDBInstancesRequest
        {
            DBInstanceIdentifier = "rds-mod-v2",
        });
        var inst = resp.DBInstances[0];
        Assert.Equal("db.t3.small", inst.DBInstanceClass);
        Assert.Equal(50, inst.AllocatedStorage);
    }

    [Fact]
    public async Task DeleteDbInstance()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-del-v2",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "pass",
            AllocatedStorage = 10,
        });

        await _rds.DeleteDBInstanceAsync(new DeleteDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-del-v2",
            SkipFinalSnapshot = true,
        });

        var ex = await Assert.ThrowsAsync<Amazon.RDS.Model.DBInstanceNotFoundException>(() =>
            _rds.DescribeDBInstancesAsync(new DescribeDBInstancesRequest
            {
                DBInstanceIdentifier = "rds-del-v2",
            }));
        Assert.Contains("rds-del-v2", ex.Message);
    }

    [Fact]
    public async Task RebootDbInstance()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-reboot",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "pass",
            AllocatedStorage = 10,
        });

        var resp = await _rds.RebootDBInstanceAsync(new RebootDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-reboot",
        });
        Assert.Equal("available", resp.DBInstance.DBInstanceStatus);
    }

    [Fact]
    public async Task StopAndStartDbInstance()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-stop-start",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "pass",
            AllocatedStorage = 10,
        });

        var stopResp = await _rds.StopDBInstanceAsync(new StopDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-stop-start",
        });
        Assert.Equal("stopped", stopResp.DBInstance.DBInstanceStatus);

        var startResp = await _rds.StartDBInstanceAsync(new StartDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-stop-start",
        });
        Assert.Equal("available", startResp.DBInstance.DBInstanceStatus);
    }

    [Fact]
    public async Task CreateDbInstanceAlreadyExistsThrows()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "dup-inst",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "pass",
            AllocatedStorage = 10,
        });

        await Assert.ThrowsAsync<AmazonRDSException>(() =>
            _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
            {
                DBInstanceIdentifier = "dup-inst",
                DBInstanceClass = "db.t3.micro",
                Engine = "postgres",
                MasterUsername = "admin",
                MasterUserPassword = "pass",
                AllocatedStorage = 10,
            }));
    }

    [Fact]
    public async Task DescribeDbInstanceNotFoundThrows()
    {
        await Assert.ThrowsAsync<Amazon.RDS.Model.DBInstanceNotFoundException>(() =>
            _rds.DescribeDBInstancesAsync(new DescribeDBInstancesRequest
            {
                DBInstanceIdentifier = "nonexistent",
            }));
    }

    [Fact]
    public async Task DeletionProtectionPreventsDelete()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "qa-rds-protected",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "password",
            AllocatedStorage = 20,
            DeletionProtection = true,
        });

        var ex = await Assert.ThrowsAsync<AmazonRDSException>(() =>
            _rds.DeleteDBInstanceAsync(new DeleteDBInstanceRequest
            {
                DBInstanceIdentifier = "qa-rds-protected",
            }));
        Assert.Equal("InvalidParameterCombination", ex.ErrorCode);

        // Disable protection then delete
        await _rds.ModifyDBInstanceAsync(new ModifyDBInstanceRequest
        {
            DBInstanceIdentifier = "qa-rds-protected",
            DeletionProtection = false,
            ApplyImmediately = true,
        });
        await _rds.DeleteDBInstanceAsync(new DeleteDBInstanceRequest
        {
            DBInstanceIdentifier = "qa-rds-protected",
            SkipFinalSnapshot = true,
        });
    }

    // ── DB Cluster CRUD ─────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateDbCluster()
    {
        var resp = await _rds.CreateDBClusterAsync(new CreateDBClusterRequest
        {
            DBClusterIdentifier = "rds-cc-v2",
            Engine = "aurora-postgresql",
            MasterUsername = "admin",
            MasterUserPassword = "pass123",
        });

        var cluster = resp.DBCluster;
        Assert.Equal("rds-cc-v2", cluster.DBClusterIdentifier);
        Assert.Equal("available", cluster.Status);
        Assert.Equal("aurora-postgresql", cluster.Engine);
        Assert.NotEmpty(cluster.DBClusterArn);

        var desc = await _rds.DescribeDBClustersAsync(new DescribeDBClustersRequest
        {
            DBClusterIdentifier = "rds-cc-v2",
        });
        Assert.Equal("rds-cc-v2", desc.DBClusters[0].DBClusterIdentifier);
    }

    [Fact]
    public async Task DescribeDbClusterNotFoundThrows()
    {
        await Assert.ThrowsAsync<Amazon.RDS.Model.DBClusterNotFoundException>(() =>
            _rds.DescribeDBClustersAsync(new DescribeDBClustersRequest
            {
                DBClusterIdentifier = "nonexistent-cluster",
            }));
    }

    [Fact]
    public async Task ModifyDbCluster()
    {
        await _rds.CreateDBClusterAsync(new CreateDBClusterRequest
        {
            DBClusterIdentifier = "mod-cluster",
            Engine = "aurora-postgresql",
            MasterUsername = "admin",
            MasterUserPassword = "pass123",
        });

        await _rds.ModifyDBClusterAsync(new ModifyDBClusterRequest
        {
            DBClusterIdentifier = "mod-cluster",
            DeletionProtection = true,
        });

        var resp = await _rds.DescribeDBClustersAsync(new DescribeDBClustersRequest
        {
            DBClusterIdentifier = "mod-cluster",
        });
        Assert.True(resp.DBClusters[0].DeletionProtection);
    }

    [Fact]
    public async Task DeleteDbCluster()
    {
        await _rds.CreateDBClusterAsync(new CreateDBClusterRequest
        {
            DBClusterIdentifier = "del-cluster",
            Engine = "aurora-postgresql",
            MasterUsername = "admin",
            MasterUserPassword = "pass123",
        });

        await _rds.DeleteDBClusterAsync(new DeleteDBClusterRequest
        {
            DBClusterIdentifier = "del-cluster",
            SkipFinalSnapshot = true,
        });

        await Assert.ThrowsAsync<Amazon.RDS.Model.DBClusterNotFoundException>(() =>
            _rds.DescribeDBClustersAsync(new DescribeDBClustersRequest
            {
                DBClusterIdentifier = "del-cluster",
            }));
    }

    [Fact]
    public async Task StopAndStartDbCluster()
    {
        await _rds.CreateDBClusterAsync(new CreateDBClusterRequest
        {
            DBClusterIdentifier = "ss-cl",
            Engine = "aurora-mysql",
            MasterUsername = "admin",
            MasterUserPassword = "password123",
        });

        await _rds.StopDBClusterAsync(new StopDBClusterRequest
        {
            DBClusterIdentifier = "ss-cl",
        });
        var resp = await _rds.DescribeDBClustersAsync(new DescribeDBClustersRequest
        {
            DBClusterIdentifier = "ss-cl",
        });
        Assert.Equal("stopped", resp.DBClusters[0].Status);

        await _rds.StartDBClusterAsync(new StartDBClusterRequest
        {
            DBClusterIdentifier = "ss-cl",
        });
        var resp2 = await _rds.DescribeDBClustersAsync(new DescribeDBClustersRequest
        {
            DBClusterIdentifier = "ss-cl",
        });
        Assert.Equal("available", resp2.DBClusters[0].Status);
    }

    // ── DB Subnet Group CRUD ────────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeDbSubnetGroup()
    {
        await _rds.CreateDBSubnetGroupAsync(new CreateDBSubnetGroupRequest
        {
            DBSubnetGroupName = "test-sg",
            DBSubnetGroupDescription = "Test SG",
            SubnetIds = ["subnet-111"],
        });

        var resp = await _rds.DescribeDBSubnetGroupsAsync(new DescribeDBSubnetGroupsRequest
        {
            DBSubnetGroupName = "test-sg",
        });
        Assert.Single(resp.DBSubnetGroups);
        Assert.Equal("test-sg", resp.DBSubnetGroups[0].DBSubnetGroupName);
        Assert.Equal("Test SG", resp.DBSubnetGroups[0].DBSubnetGroupDescription);
    }

    [Fact]
    public async Task ModifyDbSubnetGroup()
    {
        await _rds.CreateDBSubnetGroupAsync(new CreateDBSubnetGroupRequest
        {
            DBSubnetGroupName = "test-mod-sg",
            DBSubnetGroupDescription = "Test SG",
            SubnetIds = ["subnet-111"],
        });

        await _rds.ModifyDBSubnetGroupAsync(new ModifyDBSubnetGroupRequest
        {
            DBSubnetGroupName = "test-mod-sg",
            DBSubnetGroupDescription = "Updated SG",
            SubnetIds = ["subnet-222", "subnet-333"],
        });

        var resp = await _rds.DescribeDBSubnetGroupsAsync(new DescribeDBSubnetGroupsRequest
        {
            DBSubnetGroupName = "test-mod-sg",
        });
        Assert.Equal("Updated SG", resp.DBSubnetGroups[0].DBSubnetGroupDescription);
    }

    [Fact]
    public async Task DeleteDbSubnetGroup()
    {
        await _rds.CreateDBSubnetGroupAsync(new CreateDBSubnetGroupRequest
        {
            DBSubnetGroupName = "del-sg",
            DBSubnetGroupDescription = "Delete me",
            SubnetIds = ["subnet-111"],
        });

        await _rds.DeleteDBSubnetGroupAsync(new DeleteDBSubnetGroupRequest
        {
            DBSubnetGroupName = "del-sg",
        });

        await Assert.ThrowsAsync<Amazon.RDS.Model.DBSubnetGroupNotFoundException>(() =>
            _rds.DescribeDBSubnetGroupsAsync(new DescribeDBSubnetGroupsRequest
            {
                DBSubnetGroupName = "del-sg",
            }));
    }

    // ── DB Parameter Group CRUD ─────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeDbParameterGroup()
    {
        await _rds.CreateDBParameterGroupAsync(new CreateDBParameterGroupRequest
        {
            DBParameterGroupName = "test-pg",
            DBParameterGroupFamily = "mysql8.0",
            Description = "Test param group",
        });

        var resp = await _rds.DescribeDBParameterGroupsAsync(new DescribeDBParameterGroupsRequest
        {
            DBParameterGroupName = "test-pg",
        });
        Assert.Single(resp.DBParameterGroups);
        Assert.Equal("test-pg", resp.DBParameterGroups[0].DBParameterGroupName);
    }

    [Fact]
    public async Task ModifyDbParameterGroup()
    {
        await _rds.CreateDBParameterGroupAsync(new CreateDBParameterGroupRequest
        {
            DBParameterGroupName = "test-mpg",
            DBParameterGroupFamily = "mysql8.0",
            Description = "Test param group for modify",
        });

        var resp = await _rds.ModifyDBParameterGroupAsync(new ModifyDBParameterGroupRequest
        {
            DBParameterGroupName = "test-mpg",
            Parameters =
            [
                new Parameter
                {
                    ParameterName = "max_connections",
                    ParameterValue = "100",
                    ApplyMethod = ApplyMethod.Immediate,
                },
            ],
        });
        Assert.Equal("test-mpg", resp.DBParameterGroupName);
    }

    [Fact]
    public async Task DeleteDbParameterGroup()
    {
        await _rds.CreateDBParameterGroupAsync(new CreateDBParameterGroupRequest
        {
            DBParameterGroupName = "del-pg",
            DBParameterGroupFamily = "postgres15",
            Description = "Delete me",
        });

        await _rds.DeleteDBParameterGroupAsync(new DeleteDBParameterGroupRequest
        {
            DBParameterGroupName = "del-pg",
        });

        await Assert.ThrowsAsync<AmazonRDSException>(() =>
            _rds.DescribeDBParameterGroupsAsync(new DescribeDBParameterGroupsRequest
            {
                DBParameterGroupName = "del-pg",
            }));
    }

    // ── DB Cluster Parameter Group CRUD ─────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeClusterParameterGroup()
    {
        await _rds.CreateDBClusterParameterGroupAsync(new CreateDBClusterParameterGroupRequest
        {
            DBClusterParameterGroupName = "test-cpg",
            DBParameterGroupFamily = "aurora-mysql8.0",
            Description = "Test cluster param group",
        });

        var resp = await _rds.DescribeDBClusterParameterGroupsAsync(new DescribeDBClusterParameterGroupsRequest
        {
            DBClusterParameterGroupName = "test-cpg",
        });
        Assert.True(resp.DBClusterParameterGroups.Count >= 1);
        Assert.Equal("test-cpg", resp.DBClusterParameterGroups[0].DBClusterParameterGroupName);
    }

    [Fact]
    public async Task DeleteClusterParameterGroup()
    {
        await _rds.CreateDBClusterParameterGroupAsync(new CreateDBClusterParameterGroupRequest
        {
            DBClusterParameterGroupName = "del-cpg",
            DBParameterGroupFamily = "aurora-mysql8.0",
            Description = "Delete me",
        });

        await _rds.DeleteDBClusterParameterGroupAsync(new DeleteDBClusterParameterGroupRequest
        {
            DBClusterParameterGroupName = "del-cpg",
        });

        await Assert.ThrowsAsync<AmazonRDSException>(() =>
            _rds.DescribeDBClusterParameterGroupsAsync(new DescribeDBClusterParameterGroupsRequest
            {
                DBClusterParameterGroupName = "del-cpg",
            }));
    }

    // ── DB Snapshot CRUD ────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeDbSnapshot()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-snap-v2",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "pass",
            AllocatedStorage = 10,
        });

        var resp = await _rds.CreateDBSnapshotAsync(new CreateDBSnapshotRequest
        {
            DBSnapshotIdentifier = "rds-snap-v2-s1",
            DBInstanceIdentifier = "rds-snap-v2",
        });
        Assert.Equal("rds-snap-v2-s1", resp.DBSnapshot.DBSnapshotIdentifier);
        Assert.Equal("available", resp.DBSnapshot.Status);

        var desc = await _rds.DescribeDBSnapshotsAsync(new DescribeDBSnapshotsRequest
        {
            DBSnapshotIdentifier = "rds-snap-v2-s1",
        });
        Assert.Single(desc.DBSnapshots);
    }

    [Fact]
    public async Task DeleteDbSnapshot()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "snap-del-inst",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "pass",
            AllocatedStorage = 10,
        });

        await _rds.CreateDBSnapshotAsync(new CreateDBSnapshotRequest
        {
            DBSnapshotIdentifier = "snap-del",
            DBInstanceIdentifier = "snap-del-inst",
        });

        await _rds.DeleteDBSnapshotAsync(new DeleteDBSnapshotRequest
        {
            DBSnapshotIdentifier = "snap-del",
        });

        await Assert.ThrowsAsync<Amazon.RDS.Model.DBSnapshotNotFoundException>(() =>
            _rds.DescribeDBSnapshotsAsync(new DescribeDBSnapshotsRequest
            {
                DBSnapshotIdentifier = "snap-del",
            }));
    }

    [Fact]
    public async Task SnapshotCrud()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "qa-rds-snap-db",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "password",
            AllocatedStorage = 20,
        });

        await _rds.CreateDBSnapshotAsync(new CreateDBSnapshotRequest
        {
            DBSnapshotIdentifier = "qa-rds-snap-1",
            DBInstanceIdentifier = "qa-rds-snap-db",
        });

        var snaps = await _rds.DescribeDBSnapshotsAsync(new DescribeDBSnapshotsRequest
        {
            DBSnapshotIdentifier = "qa-rds-snap-1",
        });
        Assert.Single(snaps.DBSnapshots);
        Assert.Equal("qa-rds-snap-1", snaps.DBSnapshots[0].DBSnapshotIdentifier);
        Assert.Equal("available", snaps.DBSnapshots[0].Status);

        await _rds.DeleteDBSnapshotAsync(new DeleteDBSnapshotRequest
        {
            DBSnapshotIdentifier = "qa-rds-snap-1",
        });

        var snaps2 = await _rds.DescribeDBSnapshotsAsync(new DescribeDBSnapshotsRequest());
        Assert.DoesNotContain(snaps2.DBSnapshots ?? [], s => s.DBSnapshotIdentifier == "qa-rds-snap-1");

        await _rds.DeleteDBInstanceAsync(new DeleteDBInstanceRequest
        {
            DBInstanceIdentifier = "qa-rds-snap-db",
            SkipFinalSnapshot = true,
        });
    }

    [Fact]
    public async Task CopyDbSnapshot()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "copy-snap-inst",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "pass",
            AllocatedStorage = 10,
        });

        await _rds.CreateDBSnapshotAsync(new CreateDBSnapshotRequest
        {
            DBSnapshotIdentifier = "orig-snap",
            DBInstanceIdentifier = "copy-snap-inst",
        });

        var copyResp = await _rds.CopyDBSnapshotAsync(new CopyDBSnapshotRequest
        {
            SourceDBSnapshotIdentifier = "orig-snap",
            TargetDBSnapshotIdentifier = "copied-snap",
        });
        Assert.Equal("copied-snap", copyResp.DBSnapshot.DBSnapshotIdentifier);
        Assert.Equal("available", copyResp.DBSnapshot.Status);

        var desc = await _rds.DescribeDBSnapshotsAsync(new DescribeDBSnapshotsRequest
        {
            DBSnapshotIdentifier = "copied-snap",
        });
        Assert.Single(desc.DBSnapshots);
    }

    // ── DB Cluster Snapshot CRUD ────────────────────────────────────────────────

    [Fact]
    public async Task CreateAndDescribeClusterSnapshot()
    {
        await _rds.CreateDBClusterAsync(new CreateDBClusterRequest
        {
            DBClusterIdentifier = "snap-cl",
            Engine = "aurora-mysql",
            MasterUsername = "admin",
            MasterUserPassword = "password123",
        });

        await _rds.CreateDBClusterSnapshotAsync(new CreateDBClusterSnapshotRequest
        {
            DBClusterSnapshotIdentifier = "snap-cl-snap",
            DBClusterIdentifier = "snap-cl",
        });

        var resp = await _rds.DescribeDBClusterSnapshotsAsync(new DescribeDBClusterSnapshotsRequest
        {
            DBClusterSnapshotIdentifier = "snap-cl-snap",
        });
        Assert.True(resp.DBClusterSnapshots.Count >= 1);
        Assert.Equal("snap-cl-snap", resp.DBClusterSnapshots[0].DBClusterSnapshotIdentifier);
    }

    [Fact]
    public async Task DeleteClusterSnapshot()
    {
        await _rds.CreateDBClusterAsync(new CreateDBClusterRequest
        {
            DBClusterIdentifier = "snap-cl-del",
            Engine = "aurora-mysql",
            MasterUsername = "admin",
            MasterUserPassword = "password123",
        });

        await _rds.CreateDBClusterSnapshotAsync(new CreateDBClusterSnapshotRequest
        {
            DBClusterSnapshotIdentifier = "snap-cl-del-snap",
            DBClusterIdentifier = "snap-cl-del",
        });

        await _rds.DeleteDBClusterSnapshotAsync(new DeleteDBClusterSnapshotRequest
        {
            DBClusterSnapshotIdentifier = "snap-cl-del-snap",
        });

        await Assert.ThrowsAsync<Amazon.RDS.Model.DBClusterSnapshotNotFoundException>(() =>
            _rds.DescribeDBClusterSnapshotsAsync(new DescribeDBClusterSnapshotsRequest
            {
                DBClusterSnapshotIdentifier = "snap-cl-del-snap",
            }));
    }

    // ── Tags ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TagsAddListRemove()
    {
        await _rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-tag-v2",
            DBInstanceClass = "db.t3.micro",
            Engine = "postgres",
            MasterUsername = "admin",
            MasterUserPassword = "pass",
            AllocatedStorage = 10,
            Tags = [new Tag { Key = "env", Value = "dev" }],
        });

        var descResp = await _rds.DescribeDBInstancesAsync(new DescribeDBInstancesRequest
        {
            DBInstanceIdentifier = "rds-tag-v2",
        });
        var arn = descResp.DBInstances[0].DBInstanceArn;

        var tags = await _rds.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceName = arn,
        });
        Assert.Contains(tags.TagList, t => t.Key == "env" && t.Value == "dev");

        await _rds.AddTagsToResourceAsync(new AddTagsToResourceRequest
        {
            ResourceName = arn,
            Tags = [new Tag { Key = "team", Value = "dba" }],
        });

        var tags2 = await _rds.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceName = arn,
        });
        Assert.Contains(tags2.TagList, t => t.Key == "team" && t.Value == "dba");

        await _rds.RemoveTagsFromResourceAsync(new RemoveTagsFromResourceRequest
        {
            ResourceName = arn,
            TagKeys = ["env"],
        });

        var tags3 = await _rds.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceName = arn,
        });
        Assert.DoesNotContain(tags3.TagList, t => t.Key == "env");
        Assert.Contains(tags3.TagList, t => t.Key == "team");
    }

    // ── Event Subscriptions ─────────────────────────────────────────────────────

    [Fact]
    public async Task EventSubscriptionCrud()
    {
        var createResp = await _rds.CreateEventSubscriptionAsync(new CreateEventSubscriptionRequest
        {
            SubscriptionName = "test-sub",
            SnsTopicArn = "arn:aws:sns:us-east-1:123456789012:my-topic",
            SourceType = "db-instance",
            Enabled = true,
        });
        Assert.Equal("test-sub", createResp.EventSubscription.CustSubscriptionId);
        Assert.Equal("active", createResp.EventSubscription.Status);

        var descResp = await _rds.DescribeEventSubscriptionsAsync(new DescribeEventSubscriptionsRequest
        {
            SubscriptionName = "test-sub",
        });
        Assert.Single(descResp.EventSubscriptionsList);

        await _rds.DeleteEventSubscriptionAsync(new DeleteEventSubscriptionRequest
        {
            SubscriptionName = "test-sub",
        });

        var ex = await Assert.ThrowsAsync<AmazonRDSException>(() =>
            _rds.DescribeEventSubscriptionsAsync(new DescribeEventSubscriptionsRequest
            {
                SubscriptionName = "test-sub",
            }));
        Assert.Equal("SubscriptionNotFoundFault", ex.ErrorCode);
    }

    // ── DB Proxy ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DbProxyCrud()
    {
        var createResp = await _rds.CreateDBProxyAsync(new CreateDBProxyRequest
        {
            DBProxyName = "test-proxy",
            EngineFamily = EngineFamily.POSTGRESQL,
            RoleArn = "arn:aws:iam::123456789012:role/test-role",
            Auth =
            [
                new UserAuthConfig
                {
                    AuthScheme = AuthScheme.SECRETS,
                    SecretArn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
                },
            ],
            VpcSubnetIds = ["subnet-111"],
        });
        Assert.Equal("test-proxy", createResp.DBProxy.DBProxyName);
        Assert.Equal(DBProxyStatus.Available, createResp.DBProxy.Status);

        var descResp = await _rds.DescribeDBProxiesAsync(new DescribeDBProxiesRequest
        {
            DBProxyName = "test-proxy",
        });
        Assert.Single(descResp.DBProxies);

        await _rds.DeleteDBProxyAsync(new DeleteDBProxyRequest
        {
            DBProxyName = "test-proxy",
        });

        await Assert.ThrowsAsync<Amazon.RDS.Model.DBProxyNotFoundException>(() =>
            _rds.DescribeDBProxiesAsync(new DescribeDBProxiesRequest
            {
                DBProxyName = "test-proxy",
            }));
    }

    // ── Option Group CRUD ───────────────────────────────────────────────────────

    [Fact]
    public async Task OptionGroupCrud()
    {
        await _rds.CreateOptionGroupAsync(new CreateOptionGroupRequest
        {
            OptionGroupName = "test-og",
            EngineName = "mysql",
            MajorEngineVersion = "8.0",
            OptionGroupDescription = "Test option group",
        });

        var resp = await _rds.DescribeOptionGroupsAsync(new DescribeOptionGroupsRequest
        {
            OptionGroupName = "test-og",
        });
        Assert.True(resp.OptionGroupsList.Count >= 1);
        Assert.Equal("test-og", resp.OptionGroupsList[0].OptionGroupName);

        await _rds.DeleteOptionGroupAsync(new DeleteOptionGroupRequest
        {
            OptionGroupName = "test-og",
        });

        await Assert.ThrowsAsync<Amazon.RDS.Model.OptionGroupNotFoundException>(() =>
            _rds.DescribeOptionGroupsAsync(new DescribeOptionGroupsRequest
            {
                OptionGroupName = "test-og",
            }));
    }

    // ── Describe Helpers ────────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeOrderableDbInstanceOptions()
    {
        var resp = await _rds.DescribeOrderableDBInstanceOptionsAsync(new DescribeOrderableDBInstanceOptionsRequest
        {
            Engine = "postgres",
        });
        Assert.NotEmpty(resp.OrderableDBInstanceOptions);
        Assert.All(resp.OrderableDBInstanceOptions, opt => Assert.Equal("postgres", opt.Engine));
    }

    [Fact]
    public async Task DescribeDbEngineVersionsPostgres()
    {
        var resp = await _rds.DescribeDBEngineVersionsAsync(new DescribeDBEngineVersionsRequest
        {
            Engine = "postgres",
        });
        Assert.NotEmpty(resp.DBEngineVersions);
        Assert.All(resp.DBEngineVersions, v => Assert.Equal("postgres", v.Engine));
    }

    [Fact]
    public async Task DescribeDbEngineVersionsMysql()
    {
        var resp = await _rds.DescribeDBEngineVersionsAsync(new DescribeDBEngineVersionsRequest
        {
            Engine = "mysql",
        });
        Assert.NotEmpty(resp.DBEngineVersions);
        Assert.All(resp.DBEngineVersions, v => Assert.Equal("mysql", v.Engine));
    }

    // ── Global Cluster Lifecycle ────────────────────────────────────────────────

    [Fact]
    public async Task GlobalClusterLifecycle()
    {
        await _rds.CreateGlobalClusterAsync(new CreateGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-1",
            Engine = "aurora-postgresql",
            EngineVersion = "15.3",
        });

        var resp = await _rds.DescribeGlobalClustersAsync(new DescribeGlobalClustersRequest
        {
            GlobalClusterIdentifier = "test-global-1",
        });
        Assert.Single(resp.GlobalClusters);
        var gc = resp.GlobalClusters[0];
        Assert.Equal("test-global-1", gc.GlobalClusterIdentifier);
        Assert.Equal("aurora-postgresql", gc.Engine);
        Assert.Equal("available", gc.Status);
        Assert.NotEmpty(gc.GlobalClusterArn);
        Assert.NotEmpty(gc.GlobalClusterResourceId);

        await _rds.DeleteGlobalClusterAsync(new DeleteGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-1",
        });

        await Assert.ThrowsAsync<Amazon.RDS.Model.GlobalClusterNotFoundException>(() =>
            _rds.DescribeGlobalClustersAsync(new DescribeGlobalClustersRequest
            {
                GlobalClusterIdentifier = "test-global-1",
            }));
    }

    [Fact]
    public async Task GlobalClusterWithSource()
    {
        await _rds.CreateDBClusterAsync(new CreateDBClusterRequest
        {
            DBClusterIdentifier = "gc-source-cluster",
            Engine = "aurora-postgresql",
            MasterUsername = "admin",
            MasterUserPassword = "password123",
        });

        await _rds.CreateGlobalClusterAsync(new CreateGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-src",
            SourceDBClusterIdentifier = "gc-source-cluster",
        });

        var resp = await _rds.DescribeGlobalClustersAsync(new DescribeGlobalClustersRequest
        {
            GlobalClusterIdentifier = "test-global-src",
        });
        var gc = resp.GlobalClusters[0];
        Assert.Equal("aurora-postgresql", gc.Engine);
        Assert.Single(gc.GlobalClusterMembers);
        Assert.True(gc.GlobalClusterMembers[0].IsWriter);

        await _rds.RemoveFromGlobalClusterAsync(new RemoveFromGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-src",
            DbClusterIdentifier = "gc-source-cluster",
        });

        var resp2 = await _rds.DescribeGlobalClustersAsync(new DescribeGlobalClustersRequest
        {
            GlobalClusterIdentifier = "test-global-src",
        });
        Assert.Empty(resp2.GlobalClusters[0].GlobalClusterMembers ?? []);

        await _rds.DeleteGlobalClusterAsync(new DeleteGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-src",
        });

        await _rds.DeleteDBClusterAsync(new DeleteDBClusterRequest
        {
            DBClusterIdentifier = "gc-source-cluster",
            SkipFinalSnapshot = true,
        });
    }

    [Fact]
    public async Task GlobalClusterDeleteWithMembersFails()
    {
        await _rds.CreateDBClusterAsync(new CreateDBClusterRequest
        {
            DBClusterIdentifier = "gc-member-cluster",
            Engine = "aurora-postgresql",
            MasterUsername = "admin",
            MasterUserPassword = "password123",
        });

        await _rds.CreateGlobalClusterAsync(new CreateGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-members",
            SourceDBClusterIdentifier = "gc-member-cluster",
        });

        var ex = await Assert.ThrowsAsync<Amazon.RDS.Model.InvalidGlobalClusterStateException>(() =>
            _rds.DeleteGlobalClusterAsync(new DeleteGlobalClusterRequest
            {
                GlobalClusterIdentifier = "test-global-members",
            }));

        // Cleanup
        await _rds.RemoveFromGlobalClusterAsync(new RemoveFromGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-members",
            DbClusterIdentifier = "gc-member-cluster",
        });
        await _rds.DeleteGlobalClusterAsync(new DeleteGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-members",
        });
        await _rds.DeleteDBClusterAsync(new DeleteDBClusterRequest
        {
            DBClusterIdentifier = "gc-member-cluster",
            SkipFinalSnapshot = true,
        });
    }

    [Fact]
    public async Task GlobalClusterModify()
    {
        await _rds.CreateGlobalClusterAsync(new CreateGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-mod",
            Engine = "aurora-postgresql",
        });

        await _rds.ModifyGlobalClusterAsync(new ModifyGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-mod",
            DeletionProtection = true,
        });

        var gc = (await _rds.DescribeGlobalClustersAsync(new DescribeGlobalClustersRequest
        {
            GlobalClusterIdentifier = "test-global-mod",
        })).GlobalClusters[0];
        Assert.True(gc.DeletionProtection);

        // Cannot delete while protected
        var ex = await Assert.ThrowsAsync<AmazonRDSException>(() =>
            _rds.DeleteGlobalClusterAsync(new DeleteGlobalClusterRequest
            {
                GlobalClusterIdentifier = "test-global-mod",
            }));
        Assert.Equal("InvalidParameterCombination", ex.ErrorCode);

        // Rename
        await _rds.ModifyGlobalClusterAsync(new ModifyGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-mod",
            NewGlobalClusterIdentifier = "test-global-renamed",
            DeletionProtection = false,
        });

        var resp = await _rds.DescribeGlobalClustersAsync(new DescribeGlobalClustersRequest
        {
            GlobalClusterIdentifier = "test-global-renamed",
        });
        Assert.Equal("test-global-renamed", resp.GlobalClusters[0].GlobalClusterIdentifier);

        await Assert.ThrowsAsync<Amazon.RDS.Model.GlobalClusterNotFoundException>(() =>
            _rds.DescribeGlobalClustersAsync(new DescribeGlobalClustersRequest
            {
                GlobalClusterIdentifier = "test-global-mod",
            }));

        // Cleanup
        await _rds.DeleteGlobalClusterAsync(new DeleteGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-renamed",
        });
    }
}
