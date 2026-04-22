using Amazon;
using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the EMR (Elastic MapReduce) service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_emr.py.
/// </summary>
public sealed class EmrTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly AmazonElasticMapReduceClient _emr = CreateEmrClient(fixture);

    private static AmazonElasticMapReduceClient CreateEmrClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonElasticMapReduceConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonElasticMapReduceClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async ValueTask InitializeAsync()
    {
        await fixture.HttpClient.PostAsync("/_microstack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _emr.Dispose();
        return ValueTask.CompletedTask;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // RunJobFlow + DescribeCluster
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task RunJobFlowAndDescribeCluster()
    {
        var resp = await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "test-cluster",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                SlaveInstanceType = "m5.xlarge",
                InstanceCount = 3,
                KeepJobFlowAliveWhenNoSteps = true,
            },
            Tags =
            [
                new Tag { Key = "env", Value = "test" },
            ],
        });

        var clusterId = resp.JobFlowId;
        clusterId.ShouldStartWith("j-");

        var descResp = await _emr.DescribeClusterAsync(new DescribeClusterRequest
        {
            ClusterId = clusterId,
        });

        var cluster = descResp.Cluster;
        cluster.Name.ShouldBe("test-cluster");
        cluster.ReleaseLabel.ShouldBe("emr-6.10.0");
        cluster.Tags.ShouldContain(t => t.Key == "env" && t.Value == "test");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // ListClusters
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ListClusters()
    {
        await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "cluster-a",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                InstanceCount = 1,
            },
        });

        await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "cluster-b",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                InstanceCount = 1,
            },
        });

        var listResp = await _emr.ListClustersAsync(new ListClustersRequest());
        (listResp.Clusters.Count >= 2).ShouldBe(true);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // TerminateJobFlows
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TerminateJobFlows()
    {
        var resp = await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "to-terminate",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                InstanceCount = 1,
            },
        });

        await _emr.TerminateJobFlowsAsync(new TerminateJobFlowsRequest
        {
            JobFlowIds = [resp.JobFlowId],
        });

        var descResp = await _emr.DescribeClusterAsync(new DescribeClusterRequest
        {
            ClusterId = resp.JobFlowId,
        });

        descResp.Cluster.Status.State.Value.ShouldBe("TERMINATED");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Steps CRUD
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task StepsCrud()
    {
        var runResp = await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "steps-cluster",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                InstanceCount = 1,
            },
        });
        var clusterId = runResp.JobFlowId;

        var addResp = await _emr.AddJobFlowStepsAsync(new AddJobFlowStepsRequest
        {
            JobFlowId = clusterId,
            Steps =
            [
                new StepConfig
                {
                    Name = "step-1",
                    ActionOnFailure = ActionOnFailure.CONTINUE,
                    HadoopJarStep = new HadoopJarStepConfig
                    {
                        Jar = "command-runner.jar",
                        Args = ["spark-submit", "--class", "Main"],
                    },
                },
            ],
        });

        addResp.StepIds.ShouldHaveSingleItem();

        var listResp = await _emr.ListStepsAsync(new ListStepsRequest
        {
            ClusterId = clusterId,
        });

        listResp.Steps.ShouldHaveSingleItem();
        listResp.Steps[0].Name.ShouldBe("step-1");

        var descResp = await _emr.DescribeStepAsync(new DescribeStepRequest
        {
            ClusterId = clusterId,
            StepId = addResp.StepIds[0],
        });

        descResp.Step.Name.ShouldBe("step-1");

        await _emr.CancelStepsAsync(new CancelStepsRequest
        {
            ClusterId = clusterId,
            StepIds = [addResp.StepIds[0]],
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tags
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TagOperations()
    {
        var runResp = await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "tag-cluster",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                InstanceCount = 1,
            },
        });
        var clusterId = runResp.JobFlowId;

        await _emr.AddTagsAsync(new AddTagsRequest
        {
            ResourceId = clusterId,
            Tags = [new Tag { Key = "Owner", Value = "TeamA" }],
        });

        var descResp = await _emr.DescribeClusterAsync(new DescribeClusterRequest
        {
            ClusterId = clusterId,
        });

        descResp.Cluster.Tags.ShouldContain(t => t.Key == "Owner" && t.Value == "TeamA");

        await _emr.RemoveTagsAsync(new RemoveTagsRequest
        {
            ResourceId = clusterId,
            TagKeys = ["Owner"],
        });

        var descResp2 = await _emr.DescribeClusterAsync(new DescribeClusterRequest
        {
            ClusterId = clusterId,
        });

        descResp2.Cluster.Tags.ShouldNotContain(t => t.Key == "Owner");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SetTerminationProtection
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SetTerminationProtection()
    {
        var runResp = await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "prot-cluster",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                InstanceCount = 1,
            },
        });

        await _emr.SetTerminationProtectionAsync(new SetTerminationProtectionRequest
        {
            JobFlowIds = [runResp.JobFlowId],
            TerminationProtected = true,
        });

        var descResp = await _emr.DescribeClusterAsync(new DescribeClusterRequest
        {
            ClusterId = runResp.JobFlowId,
        });

        descResp.Cluster.TerminationProtected.ShouldBe(true);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SetVisibleToAllUsers
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SetVisibleToAllUsers()
    {
        var runResp = await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "vis-cluster",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                InstanceCount = 1,
            },
        });

        await _emr.SetVisibleToAllUsersAsync(new SetVisibleToAllUsersRequest
        {
            JobFlowIds = [runResp.JobFlowId],
            VisibleToAllUsers = true,
        });

        var descResp = await _emr.DescribeClusterAsync(new DescribeClusterRequest
        {
            ClusterId = runResp.JobFlowId,
        });

        descResp.Cluster.VisibleToAllUsers.ShouldBe(true);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // ModifyCluster
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ModifyCluster()
    {
        var runResp = await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "mod-cluster",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                InstanceCount = 1,
            },
        });

        var modResp = await _emr.ModifyClusterAsync(new ModifyClusterRequest
        {
            ClusterId = runResp.JobFlowId,
            StepConcurrencyLevel = 5,
        });

        modResp.StepConcurrencyLevel.ShouldBe(5);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // InstanceFleets
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task InstanceFleetsCrud()
    {
        var runResp = await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "fleet-cluster",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                InstanceCount = 1,
            },
        });
        var clusterId = runResp.JobFlowId;

        var addResp = await _emr.AddInstanceFleetAsync(new AddInstanceFleetRequest
        {
            ClusterId = clusterId,
            InstanceFleet = new InstanceFleetConfig
            {
                Name = "task-fleet",
                InstanceFleetType = InstanceFleetType.TASK,
                TargetOnDemandCapacity = 2,
                TargetSpotCapacity = 0,
                InstanceTypeConfigs =
                [
                    new InstanceTypeConfig { InstanceType = "m5.xlarge" },
                ],
            },
        });

        addResp.InstanceFleetId.ShouldNotBeEmpty();

        var listResp = await _emr.ListInstanceFleetsAsync(new ListInstanceFleetsRequest
        {
            ClusterId = clusterId,
        });

        listResp.InstanceFleets.ShouldContain(f => f.Id == addResp.InstanceFleetId);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // InstanceGroups
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task InstanceGroupsCrud()
    {
        var runResp = await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "group-cluster",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                InstanceCount = 1,
            },
        });
        var clusterId = runResp.JobFlowId;

        var addResp = await _emr.AddInstanceGroupsAsync(new AddInstanceGroupsRequest
        {
            JobFlowId = clusterId,
            InstanceGroups =
            [
                new InstanceGroupConfig
                {
                    Name = "task-group",
                    InstanceRole = InstanceRoleType.TASK,
                    InstanceType = "m5.xlarge",
                    InstanceCount = 2,
                },
            ],
        });

        addResp.InstanceGroupIds.ShouldHaveSingleItem();

        var listResp = await _emr.ListInstanceGroupsAsync(new ListInstanceGroupsRequest
        {
            ClusterId = clusterId,
        });

        listResp.InstanceGroups.ShouldContain(g => g.Id == addResp.InstanceGroupIds[0]);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // BootstrapActions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BootstrapActions()
    {
        var runResp = await _emr.RunJobFlowAsync(new RunJobFlowRequest
        {
            Name = "bootstrap-cluster",
            ReleaseLabel = "emr-6.10.0",
            Instances = new JobFlowInstancesConfig
            {
                MasterInstanceType = "m5.xlarge",
                InstanceCount = 1,
            },
            BootstrapActions =
            [
                new BootstrapActionConfig
                {
                    Name = "install-libs",
                    ScriptBootstrapAction = new ScriptBootstrapActionConfig
                    {
                        Path = "s3://my-bucket/bootstrap.sh",
                        Args = ["--arg1"],
                    },
                },
            ],
        });

        var listResp = await _emr.ListBootstrapActionsAsync(new ListBootstrapActionsRequest
        {
            ClusterId = runResp.JobFlowId,
        });

        listResp.BootstrapActions.ShouldHaveSingleItem();
        listResp.BootstrapActions[0].Name.ShouldBe("install-libs");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // BlockPublicAccessConfiguration
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BlockPublicAccessConfiguration()
    {
        var getResp = await _emr.GetBlockPublicAccessConfigurationAsync(
            new GetBlockPublicAccessConfigurationRequest());

        getResp.BlockPublicAccessConfiguration.ShouldNotBeNull();

        await _emr.PutBlockPublicAccessConfigurationAsync(
            new PutBlockPublicAccessConfigurationRequest
            {
                BlockPublicAccessConfiguration = new BlockPublicAccessConfiguration
                {
                    BlockPublicSecurityGroupRules = false,
                },
            });

        var getResp2 = await _emr.GetBlockPublicAccessConfigurationAsync(
            new GetBlockPublicAccessConfigurationRequest());

        getResp2.BlockPublicAccessConfiguration.BlockPublicSecurityGroupRules.ShouldBe(false);
    }
}
