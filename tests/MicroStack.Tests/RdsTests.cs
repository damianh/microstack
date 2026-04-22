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

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _rds.Dispose();
        return ValueTask.CompletedTask;
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
        inst.DBInstanceIdentifier.ShouldBe("test-db");
        inst.DBInstanceStatus.ShouldBe("available");
        inst.Engine.ShouldBe("postgres");
        inst.Endpoint.ShouldNotBeNull();
        inst.Endpoint.Address.ShouldNotBeEmpty();
        (inst.Endpoint.Port > 0).ShouldBe(true);
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
        ids.ShouldContain("rds-di-v2a");
        ids.ShouldContain("rds-di-v2b");

        var resp2 = await _rds.DescribeDBInstancesAsync(new DescribeDBInstancesRequest
        {
            DBInstanceIdentifier = "rds-di-v2a",
        });
        resp2.DBInstances.ShouldHaveSingleItem();
        resp2.DBInstances[0].Engine.ShouldBe("mysql");
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
        inst.DBInstanceClass.ShouldBe("db.t3.small");
        inst.AllocatedStorage.ShouldBe(50);
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

        var ex = await Should.ThrowAsync<Amazon.RDS.Model.DBInstanceNotFoundException>(() =>
            _rds.DescribeDBInstancesAsync(new DescribeDBInstancesRequest
            {
                DBInstanceIdentifier = "rds-del-v2",
            }));
        ex.Message.ShouldContain("rds-del-v2");
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
        resp.DBInstance.DBInstanceStatus.ShouldBe("available");
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
        stopResp.DBInstance.DBInstanceStatus.ShouldBe("stopped");

        var startResp = await _rds.StartDBInstanceAsync(new StartDBInstanceRequest
        {
            DBInstanceIdentifier = "rds-stop-start",
        });
        startResp.DBInstance.DBInstanceStatus.ShouldBe("available");
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

        await Should.ThrowAsync<AmazonRDSException>(() =>
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
        await Should.ThrowAsync<Amazon.RDS.Model.DBInstanceNotFoundException>(() =>
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

        var ex = await Should.ThrowAsync<AmazonRDSException>(() =>
            _rds.DeleteDBInstanceAsync(new DeleteDBInstanceRequest
            {
                DBInstanceIdentifier = "qa-rds-protected",
            }));
        ex.ErrorCode.ShouldBe("InvalidParameterCombination");

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
        cluster.DBClusterIdentifier.ShouldBe("rds-cc-v2");
        cluster.Status.ShouldBe("available");
        cluster.Engine.ShouldBe("aurora-postgresql");
        cluster.DBClusterArn.ShouldNotBeEmpty();

        var desc = await _rds.DescribeDBClustersAsync(new DescribeDBClustersRequest
        {
            DBClusterIdentifier = "rds-cc-v2",
        });
        desc.DBClusters[0].DBClusterIdentifier.ShouldBe("rds-cc-v2");
    }

    [Fact]
    public async Task DescribeDbClusterNotFoundThrows()
    {
        await Should.ThrowAsync<Amazon.RDS.Model.DBClusterNotFoundException>(() =>
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
        resp.DBClusters[0].DeletionProtection.ShouldBe(true);
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

        await Should.ThrowAsync<Amazon.RDS.Model.DBClusterNotFoundException>(() =>
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
        resp.DBClusters[0].Status.ShouldBe("stopped");

        await _rds.StartDBClusterAsync(new StartDBClusterRequest
        {
            DBClusterIdentifier = "ss-cl",
        });
        var resp2 = await _rds.DescribeDBClustersAsync(new DescribeDBClustersRequest
        {
            DBClusterIdentifier = "ss-cl",
        });
        resp2.DBClusters[0].Status.ShouldBe("available");
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
        resp.DBSubnetGroups.ShouldHaveSingleItem();
        resp.DBSubnetGroups[0].DBSubnetGroupName.ShouldBe("test-sg");
        resp.DBSubnetGroups[0].DBSubnetGroupDescription.ShouldBe("Test SG");
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
        resp.DBSubnetGroups[0].DBSubnetGroupDescription.ShouldBe("Updated SG");
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

        await Should.ThrowAsync<Amazon.RDS.Model.DBSubnetGroupNotFoundException>(() =>
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
        resp.DBParameterGroups.ShouldHaveSingleItem();
        resp.DBParameterGroups[0].DBParameterGroupName.ShouldBe("test-pg");
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
        resp.DBParameterGroupName.ShouldBe("test-mpg");
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

        await Should.ThrowAsync<AmazonRDSException>(() =>
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
        (resp.DBClusterParameterGroups.Count >= 1).ShouldBe(true);
        resp.DBClusterParameterGroups[0].DBClusterParameterGroupName.ShouldBe("test-cpg");
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

        await Should.ThrowAsync<AmazonRDSException>(() =>
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
        resp.DBSnapshot.DBSnapshotIdentifier.ShouldBe("rds-snap-v2-s1");
        resp.DBSnapshot.Status.ShouldBe("available");

        var desc = await _rds.DescribeDBSnapshotsAsync(new DescribeDBSnapshotsRequest
        {
            DBSnapshotIdentifier = "rds-snap-v2-s1",
        });
        desc.DBSnapshots.ShouldHaveSingleItem();
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

        await Should.ThrowAsync<Amazon.RDS.Model.DBSnapshotNotFoundException>(() =>
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
        snaps.DBSnapshots.ShouldHaveSingleItem();
        snaps.DBSnapshots[0].DBSnapshotIdentifier.ShouldBe("qa-rds-snap-1");
        snaps.DBSnapshots[0].Status.ShouldBe("available");

        await _rds.DeleteDBSnapshotAsync(new DeleteDBSnapshotRequest
        {
            DBSnapshotIdentifier = "qa-rds-snap-1",
        });

        var snaps2 = await _rds.DescribeDBSnapshotsAsync(new DescribeDBSnapshotsRequest());
        (snaps2.DBSnapshots ?? []).ShouldNotContain(s => s.DBSnapshotIdentifier == "qa-rds-snap-1");

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
        copyResp.DBSnapshot.DBSnapshotIdentifier.ShouldBe("copied-snap");
        copyResp.DBSnapshot.Status.ShouldBe("available");

        var desc = await _rds.DescribeDBSnapshotsAsync(new DescribeDBSnapshotsRequest
        {
            DBSnapshotIdentifier = "copied-snap",
        });
        desc.DBSnapshots.ShouldHaveSingleItem();
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
        (resp.DBClusterSnapshots.Count >= 1).ShouldBe(true);
        resp.DBClusterSnapshots[0].DBClusterSnapshotIdentifier.ShouldBe("snap-cl-snap");
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

        await Should.ThrowAsync<Amazon.RDS.Model.DBClusterSnapshotNotFoundException>(() =>
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
        tags.TagList.ShouldContain(t => t.Key == "env" && t.Value == "dev");

        await _rds.AddTagsToResourceAsync(new AddTagsToResourceRequest
        {
            ResourceName = arn,
            Tags = [new Tag { Key = "team", Value = "dba" }],
        });

        var tags2 = await _rds.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceName = arn,
        });
        tags2.TagList.ShouldContain(t => t.Key == "team" && t.Value == "dba");

        await _rds.RemoveTagsFromResourceAsync(new RemoveTagsFromResourceRequest
        {
            ResourceName = arn,
            TagKeys = ["env"],
        });

        var tags3 = await _rds.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceName = arn,
        });
        tags3.TagList.ShouldNotContain(t => t.Key == "env");
        tags3.TagList.ShouldContain(t => t.Key == "team");
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
        createResp.EventSubscription.CustSubscriptionId.ShouldBe("test-sub");
        createResp.EventSubscription.Status.ShouldBe("active");

        var descResp = await _rds.DescribeEventSubscriptionsAsync(new DescribeEventSubscriptionsRequest
        {
            SubscriptionName = "test-sub",
        });
        descResp.EventSubscriptionsList.ShouldHaveSingleItem();

        await _rds.DeleteEventSubscriptionAsync(new DeleteEventSubscriptionRequest
        {
            SubscriptionName = "test-sub",
        });

        var ex = await Should.ThrowAsync<AmazonRDSException>(() =>
            _rds.DescribeEventSubscriptionsAsync(new DescribeEventSubscriptionsRequest
            {
                SubscriptionName = "test-sub",
            }));
        ex.ErrorCode.ShouldBe("SubscriptionNotFoundFault");
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
        createResp.DBProxy.DBProxyName.ShouldBe("test-proxy");
        createResp.DBProxy.Status.ShouldBe(DBProxyStatus.Available);

        var descResp = await _rds.DescribeDBProxiesAsync(new DescribeDBProxiesRequest
        {
            DBProxyName = "test-proxy",
        });
        descResp.DBProxies.ShouldHaveSingleItem();

        await _rds.DeleteDBProxyAsync(new DeleteDBProxyRequest
        {
            DBProxyName = "test-proxy",
        });

        await Should.ThrowAsync<Amazon.RDS.Model.DBProxyNotFoundException>(() =>
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
        (resp.OptionGroupsList.Count >= 1).ShouldBe(true);
        resp.OptionGroupsList[0].OptionGroupName.ShouldBe("test-og");

        await _rds.DeleteOptionGroupAsync(new DeleteOptionGroupRequest
        {
            OptionGroupName = "test-og",
        });

        await Should.ThrowAsync<Amazon.RDS.Model.OptionGroupNotFoundException>(() =>
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
        resp.OrderableDBInstanceOptions.ShouldNotBeEmpty();
        resp.OrderableDBInstanceOptions.ShouldAllBe(opt => opt.Engine == "postgres");
    }

    [Fact]
    public async Task DescribeDbEngineVersionsPostgres()
    {
        var resp = await _rds.DescribeDBEngineVersionsAsync(new DescribeDBEngineVersionsRequest
        {
            Engine = "postgres",
        });
        resp.DBEngineVersions.ShouldNotBeEmpty();
        resp.DBEngineVersions.ShouldAllBe(v => v.Engine == "postgres");
    }

    [Fact]
    public async Task DescribeDbEngineVersionsMysql()
    {
        var resp = await _rds.DescribeDBEngineVersionsAsync(new DescribeDBEngineVersionsRequest
        {
            Engine = "mysql",
        });
        resp.DBEngineVersions.ShouldNotBeEmpty();
        resp.DBEngineVersions.ShouldAllBe(v => v.Engine == "mysql");
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
        resp.GlobalClusters.ShouldHaveSingleItem();
        var gc = resp.GlobalClusters[0];
        gc.GlobalClusterIdentifier.ShouldBe("test-global-1");
        gc.Engine.ShouldBe("aurora-postgresql");
        gc.Status.ShouldBe("available");
        gc.GlobalClusterArn.ShouldNotBeEmpty();
        gc.GlobalClusterResourceId.ShouldNotBeEmpty();

        await _rds.DeleteGlobalClusterAsync(new DeleteGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-1",
        });

        await Should.ThrowAsync<Amazon.RDS.Model.GlobalClusterNotFoundException>(() =>
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
        gc.Engine.ShouldBe("aurora-postgresql");
        gc.GlobalClusterMembers.ShouldHaveSingleItem();
        gc.GlobalClusterMembers[0].IsWriter.ShouldBe(true);

        await _rds.RemoveFromGlobalClusterAsync(new RemoveFromGlobalClusterRequest
        {
            GlobalClusterIdentifier = "test-global-src",
            DbClusterIdentifier = "gc-source-cluster",
        });

        var resp2 = await _rds.DescribeGlobalClustersAsync(new DescribeGlobalClustersRequest
        {
            GlobalClusterIdentifier = "test-global-src",
        });
        (resp2.GlobalClusters[0].GlobalClusterMembers ?? []).ShouldBeEmpty();

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

        var ex = await Should.ThrowAsync<Amazon.RDS.Model.InvalidGlobalClusterStateException>(() =>
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
        gc.DeletionProtection.ShouldBe(true);

        // Cannot delete while protected
        var ex = await Should.ThrowAsync<AmazonRDSException>(() =>
            _rds.DeleteGlobalClusterAsync(new DeleteGlobalClusterRequest
            {
                GlobalClusterIdentifier = "test-global-mod",
            }));
        ex.ErrorCode.ShouldBe("InvalidParameterCombination");

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
        resp.GlobalClusters[0].GlobalClusterIdentifier.ShouldBe("test-global-renamed");

        await Should.ThrowAsync<Amazon.RDS.Model.GlobalClusterNotFoundException>(() =>
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
