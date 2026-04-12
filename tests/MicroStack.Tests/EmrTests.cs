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
public sealed class EmrTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonElasticMapReduceClient _emr;

    public EmrTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _emr = CreateEmrClient(fixture);
    }

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

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _emr.Dispose();
        return Task.CompletedTask;
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
        Assert.StartsWith("j-", clusterId);

        var descResp = await _emr.DescribeClusterAsync(new DescribeClusterRequest
        {
            ClusterId = clusterId,
        });

        var cluster = descResp.Cluster;
        Assert.Equal("test-cluster", cluster.Name);
        Assert.Equal("emr-6.10.0", cluster.ReleaseLabel);
        Assert.Contains(cluster.Tags, t => t.Key == "env" && t.Value == "test");
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
        Assert.True(listResp.Clusters.Count >= 2);
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

        Assert.Equal("TERMINATED", descResp.Cluster.Status.State.Value);
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

        Assert.Single(addResp.StepIds);

        var listResp = await _emr.ListStepsAsync(new ListStepsRequest
        {
            ClusterId = clusterId,
        });

        Assert.Single(listResp.Steps);
        Assert.Equal("step-1", listResp.Steps[0].Name);

        var descResp = await _emr.DescribeStepAsync(new DescribeStepRequest
        {
            ClusterId = clusterId,
            StepId = addResp.StepIds[0],
        });

        Assert.Equal("step-1", descResp.Step.Name);

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

        Assert.Contains(descResp.Cluster.Tags, t => t.Key == "Owner" && t.Value == "TeamA");

        await _emr.RemoveTagsAsync(new RemoveTagsRequest
        {
            ResourceId = clusterId,
            TagKeys = ["Owner"],
        });

        var descResp2 = await _emr.DescribeClusterAsync(new DescribeClusterRequest
        {
            ClusterId = clusterId,
        });

        Assert.DoesNotContain(descResp2.Cluster.Tags, t => t.Key == "Owner");
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

        Assert.True(descResp.Cluster.TerminationProtected);
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

        Assert.True(descResp.Cluster.VisibleToAllUsers);
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

        Assert.Equal(5, modResp.StepConcurrencyLevel);
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

        Assert.NotEmpty(addResp.InstanceFleetId);

        var listResp = await _emr.ListInstanceFleetsAsync(new ListInstanceFleetsRequest
        {
            ClusterId = clusterId,
        });

        Assert.Contains(listResp.InstanceFleets, f => f.Id == addResp.InstanceFleetId);
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

        Assert.Single(addResp.InstanceGroupIds);

        var listResp = await _emr.ListInstanceGroupsAsync(new ListInstanceGroupsRequest
        {
            ClusterId = clusterId,
        });

        Assert.Contains(listResp.InstanceGroups, g => g.Id == addResp.InstanceGroupIds[0]);
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

        Assert.Single(listResp.BootstrapActions);
        Assert.Equal("install-libs", listResp.BootstrapActions[0].Name);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // BlockPublicAccessConfiguration
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BlockPublicAccessConfiguration()
    {
        var getResp = await _emr.GetBlockPublicAccessConfigurationAsync(
            new GetBlockPublicAccessConfigurationRequest());

        Assert.NotNull(getResp.BlockPublicAccessConfiguration);

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

        Assert.False(getResp2.BlockPublicAccessConfiguration.BlockPublicSecurityGroupRules);
    }
}
