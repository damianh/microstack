using Amazon;
using Amazon.ECS;
using Amazon.ECS.Model;
using Amazon.Runtime;
using Task = System.Threading.Tasks.Task;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the ECS service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/services/ecs.py.
/// </summary>
public sealed class EcsTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonECSClient _ecs;

    public EcsTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _ecs = CreateClient(fixture);
    }

    private static AmazonECSClient CreateClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonECSConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonECSClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _ecs.Dispose();
        return Task.CompletedTask;
    }

    // -- Cluster tests ---------------------------------------------------------

    [Fact]
    public async Task CreateCluster()
    {
        var resp = await _ecs.CreateClusterAsync(new CreateClusterRequest
        {
            ClusterName = "test-cluster",
        });

        Assert.Equal("test-cluster", resp.Cluster.ClusterName);
        Assert.Equal("ACTIVE", resp.Cluster.Status);
        Assert.Contains("test-cluster", resp.Cluster.ClusterArn);
    }

    [Fact]
    public async Task CreateClusterIdempotent()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "idem-cluster" });
        var resp2 = await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "idem-cluster" });

        Assert.Equal("idem-cluster", resp2.Cluster.ClusterName);
        Assert.Equal("ACTIVE", resp2.Cluster.Status);
    }

    [Fact]
    public async Task ListClusters()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "lc-a" });
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "lc-b" });

        var resp = await _ecs.ListClustersAsync(new ListClustersRequest());

        Assert.Contains(resp.ClusterArns, a => a.Contains("lc-a"));
        Assert.Contains(resp.ClusterArns, a => a.Contains("lc-b"));
    }

    [Fact]
    public async Task DescribeClusters()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "desc-cluster" });

        var resp = await _ecs.DescribeClustersAsync(new DescribeClustersRequest
        {
            Clusters = ["desc-cluster"],
        });

        Assert.Single(resp.Clusters);
        Assert.Equal("desc-cluster", resp.Clusters[0].ClusterName);
        Assert.Empty(resp.Failures);
    }

    [Fact]
    public async Task DescribeClustersNotFound()
    {
        var resp = await _ecs.DescribeClustersAsync(new DescribeClustersRequest
        {
            Clusters = ["nonexistent"],
        });

        Assert.Empty(resp.Clusters);
        Assert.Single(resp.Failures);
        Assert.Equal("MISSING", resp.Failures[0].Reason);
    }

    [Fact]
    public async Task DeleteCluster()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "del-cluster" });

        var resp = await _ecs.DeleteClusterAsync(new DeleteClusterRequest { Cluster = "del-cluster" });
        Assert.Equal("INACTIVE", resp.Cluster.Status);

        var listResp = await _ecs.ListClustersAsync(new ListClustersRequest());
        Assert.DoesNotContain(listResp.ClusterArns, a => a.Contains("del-cluster"));
    }

    [Fact]
    public async Task DeleteClusterNotFound()
    {
        var ex = await Assert.ThrowsAsync<ClusterNotFoundException>(() =>
            _ecs.DeleteClusterAsync(new DeleteClusterRequest { Cluster = "no-such-cluster" }));
        Assert.Contains("not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task UpdateCluster()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "upd-cl" });

        var resp = await _ecs.UpdateClusterAsync(new UpdateClusterRequest
        {
            Cluster = "upd-cl",
            Settings = [new ClusterSetting { Name = ClusterSettingName.ContainerInsights, Value = "enabled" }],
        });

        Assert.Equal("upd-cl", resp.Cluster.ClusterName);
    }

    [Fact]
    public async Task PutClusterCapacityProviders()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "cp-cluster" });

        var resp = await _ecs.PutClusterCapacityProvidersAsync(new PutClusterCapacityProvidersRequest
        {
            Cluster = "cp-cluster",
            CapacityProviders = ["FARGATE"],
            DefaultCapacityProviderStrategy =
            [
                new CapacityProviderStrategyItem { CapacityProvider = "FARGATE", Weight = 1 },
            ],
        });

        Assert.Equal("cp-cluster", resp.Cluster.ClusterName);
        Assert.Contains("FARGATE", resp.Cluster.CapacityProviders);
    }

    // -- Task Definition tests -------------------------------------------------

    [Fact]
    public async Task RegisterTaskDefinition()
    {
        var resp = await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "test-task",
            ContainerDefinitions =
            [
                new ContainerDefinition
                {
                    Name = "web",
                    Image = "nginx:alpine",
                    Cpu = 128,
                    Memory = 256,
                    PortMappings = [new PortMapping { ContainerPort = 80, HostPort = 8080 }],
                },
            ],
            RequiresCompatibilities = [Compatibility.EC2],
            Cpu = "256",
            Memory = "512",
        });

        Assert.Equal("test-task", resp.TaskDefinition.Family);
        Assert.Equal(1, resp.TaskDefinition.Revision);
        Assert.Equal("ACTIVE", resp.TaskDefinition.Status.Value);
    }

    [Fact]
    public async Task RegisterTaskDefinitionRevisions()
    {
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "rev-task",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "web", Image = "nginx:alpine", Cpu = 256, Memory = 512 },
            ],
        });

        var resp2 = await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "rev-task",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "web", Image = "nginx:latest", Cpu = 256, Memory = 512 },
            ],
        });

        Assert.Equal(2, resp2.TaskDefinition.Revision);
    }

    [Fact]
    public async Task RegisterTaskDefinitionMultipleContainers()
    {
        var resp = await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "multi-container",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "web", Image = "nginx:alpine", Cpu = 256, Memory = 512 },
                new ContainerDefinition { Name = "sidecar", Image = "envoy:latest", Cpu = 128, Memory = 256 },
            ],
            Cpu = "512",
            Memory = "1024",
        });

        Assert.Equal(2, resp.TaskDefinition.ContainerDefinitions.Count);
    }

    [Fact]
    public async Task DescribeTaskDefinition()
    {
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "desc-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "app", Image = "img", Cpu = 64, Memory = 128 },
            ],
        });

        var resp = await _ecs.DescribeTaskDefinitionAsync(new DescribeTaskDefinitionRequest
        {
            TaskDefinition = "desc-td",
        });

        Assert.Equal("desc-td", resp.TaskDefinition.Family);
        Assert.Equal(1, resp.TaskDefinition.Revision);
    }

    [Fact]
    public async Task DeregisterTaskDefinition()
    {
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "dereg-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "app", Image = "img", Cpu = 64, Memory = 128 },
            ],
        });

        var resp = await _ecs.DeregisterTaskDefinitionAsync(new DeregisterTaskDefinitionRequest
        {
            TaskDefinition = "dereg-td:1",
        });

        Assert.Equal("INACTIVE", resp.TaskDefinition.Status.Value);
    }

    [Fact]
    public async Task ListTaskDefinitions()
    {
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "ltd-task",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "app", Image = "img", Cpu = 64, Memory = 128 },
            ],
        });

        var resp = await _ecs.ListTaskDefinitionsAsync(new ListTaskDefinitionsRequest
        {
            FamilyPrefix = "ltd-task",
        });

        Assert.True(resp.TaskDefinitionArns.Count >= 1);
        Assert.All(resp.TaskDefinitionArns, a => Assert.Contains("ltd-task", a));
    }

    [Fact]
    public async Task ListTaskDefinitionFamilies()
    {
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "fam-a",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "app", Image = "img", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "fam-b",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "app", Image = "img", Cpu = 64, Memory = 128 },
            ],
        });

        var resp = await _ecs.ListTaskDefinitionFamiliesAsync(new ListTaskDefinitionFamiliesRequest
        {
            FamilyPrefix = "fam-",
        });

        Assert.Contains("fam-a", resp.Families);
        Assert.Contains("fam-b", resp.Families);
    }

    // -- Service tests ---------------------------------------------------------

    [Fact]
    public async Task CreateService()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "svc-cluster" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "svc-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        var resp = await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "svc-cluster",
            ServiceName = "test-svc",
            TaskDefinition = "svc-td",
            DesiredCount = 2,
        });

        Assert.Equal("test-svc", resp.Service.ServiceName);
        Assert.Equal("ACTIVE", resp.Service.Status);
        Assert.Equal(2, resp.Service.DesiredCount);
    }

    [Fact]
    public async Task CreateServiceDuplicateFails()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "dup-svc-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "dup-svc-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "dup-svc-c",
            ServiceName = "dup-svc",
            TaskDefinition = "dup-svc-td",
            DesiredCount = 1,
        });

        await Assert.ThrowsAsync<InvalidParameterException>(() =>
            _ecs.CreateServiceAsync(new CreateServiceRequest
            {
                Cluster = "dup-svc-c",
                ServiceName = "dup-svc",
                TaskDefinition = "dup-svc-td",
                DesiredCount = 1,
            }));
    }

    [Fact]
    public async Task DescribeServices()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "ds-cluster" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "ds-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "ds-cluster",
            ServiceName = "ds-svc-a",
            TaskDefinition = "ds-td",
            DesiredCount = 1,
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "ds-cluster",
            ServiceName = "ds-svc-b",
            TaskDefinition = "ds-td",
            DesiredCount = 3,
        });

        var resp = await _ecs.DescribeServicesAsync(new DescribeServicesRequest
        {
            Cluster = "ds-cluster",
            Services = ["ds-svc-a", "ds-svc-b"],
        });

        Assert.Equal(2, resp.Services.Count);
        var svcMap = resp.Services.ToDictionary(s => s.ServiceName);
        Assert.Equal(1, svcMap["ds-svc-a"].DesiredCount);
        Assert.Equal(3, svcMap["ds-svc-b"].DesiredCount);
    }

    [Fact]
    public async Task DescribeServicesNotFound()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "ds-miss-c" });

        var resp = await _ecs.DescribeServicesAsync(new DescribeServicesRequest
        {
            Cluster = "ds-miss-c",
            Services = ["nonexistent"],
        });

        Assert.Empty(resp.Services);
        Assert.Single(resp.Failures);
        Assert.Equal("MISSING", resp.Failures[0].Reason);
    }

    [Fact]
    public async Task UpdateServiceDesiredCount()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "us-cluster" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "us-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "us-cluster",
            ServiceName = "us-svc",
            TaskDefinition = "us-td",
            DesiredCount = 1,
        });

        await _ecs.UpdateServiceAsync(new UpdateServiceRequest
        {
            Cluster = "us-cluster",
            Service = "us-svc",
            DesiredCount = 5,
        });

        var desc = await _ecs.DescribeServicesAsync(new DescribeServicesRequest
        {
            Cluster = "us-cluster",
            Services = ["us-svc"],
        });

        Assert.Equal(5, desc.Services[0].DesiredCount);
    }

    [Fact]
    public async Task UpdateServiceTaskDefinition()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "us-td-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "us-td-v1",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx:v1", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "us-td-v2",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx:v2", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "us-td-c",
            ServiceName = "us-td-svc",
            TaskDefinition = "us-td-v1",
            DesiredCount = 1,
        });

        var updateResp = await _ecs.UpdateServiceAsync(new UpdateServiceRequest
        {
            Cluster = "us-td-c",
            Service = "us-td-svc",
            TaskDefinition = "us-td-v2",
        });

        Assert.Contains("us-td-v2", updateResp.Service.TaskDefinition);
        Assert.True(updateResp.Service.Deployments.Count >= 2);
    }

    [Fact]
    public async Task DeleteServiceForceDelete()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "del-svc-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "del-svc-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "del-svc-c",
            ServiceName = "del-svc",
            TaskDefinition = "del-svc-td",
            DesiredCount = 1,
        });

        var resp = await _ecs.DeleteServiceAsync(new DeleteServiceRequest
        {
            Cluster = "del-svc-c",
            Service = "del-svc",
            Force = true,
        });

        Assert.Equal("DRAINING", resp.Service.Status);
    }

    [Fact]
    public async Task DeleteServiceWithoutForceRequiresZeroDesiredCount()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "del-nof-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "del-nof-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "del-nof-c",
            ServiceName = "del-nof-svc",
            TaskDefinition = "del-nof-td",
            DesiredCount = 1,
        });

        await Assert.ThrowsAsync<InvalidParameterException>(() =>
            _ecs.DeleteServiceAsync(new DeleteServiceRequest
            {
                Cluster = "del-nof-c",
                Service = "del-nof-svc",
            }));
    }

    [Fact]
    public async Task ListServices()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "ls-cluster" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "ls-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "ls-cluster",
            ServiceName = "ls-svc-a",
            TaskDefinition = "ls-td",
            DesiredCount = 1,
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "ls-cluster",
            ServiceName = "ls-svc-b",
            TaskDefinition = "ls-td",
            DesiredCount = 1,
        });

        var resp = await _ecs.ListServicesAsync(new ListServicesRequest
        {
            Cluster = "ls-cluster",
        });

        Assert.Equal(2, resp.ServiceArns.Count);
    }

    // -- Task tests ------------------------------------------------------------

    [Fact]
    public async Task RunTaskAndDescribe()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "rt-cluster" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "rt-td",
            ContainerDefinitions =
            [
                new ContainerDefinition
                {
                    Name = "worker",
                    Image = "alpine:latest",
                    Essential = true,
                    Cpu = 128,
                    Memory = 256,
                },
            ],
        });

        var resp = await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "rt-cluster",
            TaskDefinition = "rt-td",
            Count = 1,
        });

        Assert.Single(resp.Tasks);
        Assert.Equal("RUNNING", resp.Tasks[0].LastStatus);
        Assert.NotEmpty(resp.Tasks[0].TaskArn);
        Assert.NotEmpty(resp.Tasks[0].Containers);

        // Describe the task
        var desc = await _ecs.DescribeTasksAsync(new DescribeTasksRequest
        {
            Cluster = "rt-cluster",
            Tasks = [resp.Tasks[0].TaskArn],
        });

        Assert.Single(desc.Tasks);
        Assert.Equal(resp.Tasks[0].TaskArn, desc.Tasks[0].TaskArn);
    }

    [Fact]
    public async Task RunTaskMultiple()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "rtm-cluster" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "rtm-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "worker", Image = "alpine", Cpu = 64, Memory = 128 },
            ],
        });

        var resp = await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "rtm-cluster",
            TaskDefinition = "rtm-td",
            Count = 3,
        });

        Assert.Equal(3, resp.Tasks.Count);
    }

    [Fact]
    public async Task StopTask()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "st-cluster" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "st-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "worker", Image = "alpine", Cpu = 64, Memory = 128 },
            ],
        });

        var run = await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "st-cluster",
            TaskDefinition = "st-td",
        });

        var taskArn = run.Tasks[0].TaskArn;
        var resp = await _ecs.StopTaskAsync(new StopTaskRequest
        {
            Cluster = "st-cluster",
            Task = taskArn,
            Reason = "Unit test",
        });

        Assert.Equal("STOPPED", resp.Task.LastStatus);
        Assert.Equal("STOPPED", resp.Task.DesiredStatus);
        Assert.Equal("UserInitiated", resp.Task.StopCode.Value);
        Assert.All(resp.Task.Containers, c => Assert.Equal("STOPPED", c.LastStatus));
    }

    [Fact]
    public async Task StopTaskAlreadyStopped()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "ss-cluster" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "ss-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "worker", Image = "alpine", Cpu = 64, Memory = 128 },
            ],
        });

        var run = await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "ss-cluster",
            TaskDefinition = "ss-td",
        });

        var taskArn = run.Tasks[0].TaskArn;
        await _ecs.StopTaskAsync(new StopTaskRequest { Cluster = "ss-cluster", Task = taskArn });

        // Stop again should return STOPPED without error
        var resp = await _ecs.StopTaskAsync(new StopTaskRequest { Cluster = "ss-cluster", Task = taskArn });
        Assert.Equal("STOPPED", resp.Task.LastStatus);
    }

    [Fact]
    public async Task ListTasks()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "lt-cluster" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "lt-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "worker", Image = "alpine", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "lt-cluster",
            TaskDefinition = "lt-td",
            Count = 2,
        });

        var resp = await _ecs.ListTasksAsync(new ListTasksRequest
        {
            Cluster = "lt-cluster",
        });

        Assert.Equal(2, resp.TaskArns.Count);
    }

    [Fact]
    public async Task ListTasksFilterByDesiredStatus()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "ltf-cluster" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "ltf-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "worker", Image = "alpine", Cpu = 64, Memory = 128 },
            ],
        });

        var run = await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "ltf-cluster",
            TaskDefinition = "ltf-td",
            Count = 2,
        });

        // Stop one task
        await _ecs.StopTaskAsync(new StopTaskRequest
        {
            Cluster = "ltf-cluster",
            Task = run.Tasks[0].TaskArn,
        });

        var running = await _ecs.ListTasksAsync(new ListTasksRequest
        {
            Cluster = "ltf-cluster",
            DesiredStatus = DesiredStatus.RUNNING,
        });

        var stopped = await _ecs.ListTasksAsync(new ListTasksRequest
        {
            Cluster = "ltf-cluster",
            DesiredStatus = DesiredStatus.STOPPED,
        });

        Assert.Single(running.TaskArns);
        Assert.Single(stopped.TaskArns);
    }

    [Fact]
    public async Task DescribeTasksNotFound()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "dtm-cluster" });

        var resp = await _ecs.DescribeTasksAsync(new DescribeTasksRequest
        {
            Cluster = "dtm-cluster",
            Tasks = ["nonexistent-task-id"],
        });

        Assert.Empty(resp.Tasks);
        Assert.Single(resp.Failures);
        Assert.Equal("MISSING", resp.Failures[0].Reason);
    }

    [Fact]
    public async Task RunTaskAutoCreatesCluster()
    {
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "auto-cluster-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "worker", Image = "alpine", Cpu = 64, Memory = 128 },
            ],
        });

        var resp = await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "auto-created-cluster",
            TaskDefinition = "auto-cluster-td",
        });

        Assert.Single(resp.Tasks);

        // Cluster should have been auto-created
        var clusters = await _ecs.ListClustersAsync(new ListClustersRequest());
        Assert.Contains(clusters.ClusterArns, a => a.Contains("auto-created-cluster"));
    }

    // -- Tag tests -------------------------------------------------------------

    [Fact]
    public async Task TagResourceAndListTags()
    {
        var resp = await _ecs.CreateClusterAsync(new CreateClusterRequest
        {
            ClusterName = "tag-cluster",
            Tags = [new Tag { Key = "env", Value = "staging" }],
        });

        var arn = resp.Cluster.ClusterArn;

        var tags = await _ecs.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arn,
        });

        Assert.Contains(tags.Tags, t => t.Key == "env" && t.Value == "staging");
    }

    [Fact]
    public async Task TagAndUntagResource()
    {
        var resp = await _ecs.CreateClusterAsync(new CreateClusterRequest
        {
            ClusterName = "tag-untag-c",
            Tags = [new Tag { Key = "env", Value = "staging" }],
        });

        var arn = resp.Cluster.ClusterArn;

        await _ecs.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = arn,
            Tags = [new Tag { Key = "team", Value = "platform" }],
        });

        var tags2 = await _ecs.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceArn = arn });
        Assert.Equal(2, tags2.Tags.Count);

        await _ecs.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceArn = arn,
            TagKeys = ["env"],
        });

        var tags3 = await _ecs.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceArn = arn });
        Assert.Single(tags3.Tags);
        Assert.Equal("team", tags3.Tags[0].Key);
    }

    [Fact]
    public async Task TagResourceOverwriteExistingKey()
    {
        var resp = await _ecs.CreateClusterAsync(new CreateClusterRequest
        {
            ClusterName = "tag-overwrite-c",
            Tags = [new Tag { Key = "env", Value = "staging" }],
        });

        var arn = resp.Cluster.ClusterArn;

        await _ecs.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = arn,
            Tags = [new Tag { Key = "env", Value = "production" }],
        });

        var tags = await _ecs.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceArn = arn });
        Assert.Single(tags.Tags);
        Assert.Equal("production", tags.Tags[0].Value);
    }

    // -- Capacity Provider tests -----------------------------------------------

    [Fact]
    public async Task CreateAndDescribeCapacityProvider()
    {
        var resp = await _ecs.CreateCapacityProviderAsync(new CreateCapacityProviderRequest
        {
            Name = "test-cp",
            AutoScalingGroupProvider = new AutoScalingGroupProvider
            {
                AutoScalingGroupArn = "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:xxx:autoScalingGroupName/asg-1",
                ManagedScaling = new ManagedScaling { Status = ManagedScalingStatus.ENABLED },
            },
        });

        Assert.Equal("test-cp", resp.CapacityProvider.Name);

        var desc = await _ecs.DescribeCapacityProvidersAsync(new DescribeCapacityProvidersRequest
        {
            CapacityProviders = ["test-cp"],
        });

        Assert.Contains(desc.CapacityProviders, cp => cp.Name == "test-cp");
    }

    [Fact]
    public async Task DescribeCapacityProvidersIncludesDefaults()
    {
        var resp = await _ecs.DescribeCapacityProvidersAsync(new DescribeCapacityProvidersRequest());

        Assert.Contains(resp.CapacityProviders, cp => cp.Name == "FARGATE");
        Assert.Contains(resp.CapacityProviders, cp => cp.Name == "FARGATE_SPOT");
    }

    [Fact]
    public async Task DeleteCapacityProvider()
    {
        await _ecs.CreateCapacityProviderAsync(new CreateCapacityProviderRequest
        {
            Name = "del-cp",
            AutoScalingGroupProvider = new AutoScalingGroupProvider
            {
                AutoScalingGroupArn = "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:xxx:autoScalingGroupName/asg-del",
            },
        });

        var resp = await _ecs.DeleteCapacityProviderAsync(new DeleteCapacityProviderRequest
        {
            CapacityProvider = "del-cp",
        });

        Assert.Equal("INACTIVE", resp.CapacityProvider.Status);
    }

    [Fact]
    public async Task CreateCapacityProviderDuplicateFails()
    {
        await _ecs.CreateCapacityProviderAsync(new CreateCapacityProviderRequest
        {
            Name = "dup-cp",
            AutoScalingGroupProvider = new AutoScalingGroupProvider
            {
                AutoScalingGroupArn = "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:xxx:autoScalingGroupName/asg-dup",
            },
        });

        await Assert.ThrowsAsync<InvalidParameterException>(() =>
            _ecs.CreateCapacityProviderAsync(new CreateCapacityProviderRequest
            {
                Name = "dup-cp",
                AutoScalingGroupProvider = new AutoScalingGroupProvider
                {
                    AutoScalingGroupArn = "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:xxx:autoScalingGroupName/asg-dup2",
                },
            }));
    }

    // -- Account Settings tests ------------------------------------------------

    [Fact]
    public async Task ListAccountSettings()
    {
        var resp = await _ecs.ListAccountSettingsAsync(new ListAccountSettingsRequest());

        Assert.NotEmpty(resp.Settings);
        Assert.Contains(resp.Settings, s => s.Name.Value == "serviceLongArnFormat");
    }

    [Fact]
    public async Task PutAccountSetting()
    {
        var resp = await _ecs.PutAccountSettingAsync(new PutAccountSettingRequest
        {
            Name = SettingName.ContainerInsights,
            Value = "enhanced",
        });

        Assert.Equal("containerInsights", resp.Setting.Name.Value);
        Assert.Equal("enhanced", resp.Setting.Value);
    }

    // -- Cluster recount tests -------------------------------------------------

    [Fact]
    public async Task ClusterRecountsTasksAndServices()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "recount-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "recount-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "worker", Image = "alpine", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "recount-c",
            TaskDefinition = "recount-td",
            Count = 2,
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "recount-c",
            ServiceName = "recount-svc",
            TaskDefinition = "recount-td",
            DesiredCount = 1,
        });

        var desc = await _ecs.DescribeClustersAsync(new DescribeClustersRequest
        {
            Clusters = ["recount-c"],
        });

        Assert.Equal(2, desc.Clusters[0].RunningTasksCount);
        Assert.Equal(1, desc.Clusters[0].ActiveServicesCount);
    }

    // -- RunTask with task definition not found ---------------------------------

    [Fact]
    public async Task RunTaskWithInvalidTaskDefinitionFails()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "rt-invalid-c" });

        var ex = await Assert.ThrowsAsync<ClientException>(() =>
            _ecs.RunTaskAsync(new RunTaskRequest
            {
                Cluster = "rt-invalid-c",
                TaskDefinition = "nonexistent-td",
            }));

        Assert.Contains("Unable to find task definition", ex.Message);
    }

    // -- Task ARN format tests -------------------------------------------------

    [Fact]
    public async Task TaskArnFormat()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "arn-fmt-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "arn-fmt-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "worker", Image = "alpine", Cpu = 64, Memory = 128 },
            ],
        });

        var run = await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "arn-fmt-c",
            TaskDefinition = "arn-fmt-td",
        });

        var arn = run.Tasks[0].TaskArn;
        Assert.StartsWith("arn:aws:ecs:", arn);
        Assert.Contains("task/arn-fmt-c/", arn);
    }

    // -- Service ARN format tests ----------------------------------------------

    [Fact]
    public async Task ServiceArnFormat()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "sarn-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "sarn-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        var resp = await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "sarn-c",
            ServiceName = "sarn-svc",
            TaskDefinition = "sarn-td",
            DesiredCount = 1,
        });

        var arn = resp.Service.ServiceArn;
        Assert.StartsWith("arn:aws:ecs:", arn);
        Assert.Contains("service/sarn-c/sarn-svc", arn);
    }

    // -- RunTask with overrides ------------------------------------------------

    [Fact]
    public async Task RunTaskWithOverrides()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "ov-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "ov-td",
            ContainerDefinitions =
            [
                new ContainerDefinition
                {
                    Name = "worker",
                    Image = "alpine",
                    Cpu = 64,
                    Memory = 128,
                    Environment = [new Amazon.ECS.Model.KeyValuePair { Name = "FOO", Value = "bar" }],
                },
            ],
        });

        var resp = await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "ov-c",
            TaskDefinition = "ov-td",
            Overrides = new TaskOverride
            {
                ContainerOverrides =
                [
                    new ContainerOverride
                    {
                        Name = "worker",
                        Environment = [new Amazon.ECS.Model.KeyValuePair { Name = "FOO", Value = "overridden" }],
                    },
                ],
            },
        });

        Assert.Single(resp.Tasks);
        Assert.Equal("RUNNING", resp.Tasks[0].LastStatus);
    }

    // -- RunTask with startedBy ------------------------------------------------

    [Fact]
    public async Task ListTasksFilterByStartedBy()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "sb-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "sb-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "worker", Image = "alpine", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "sb-c",
            TaskDefinition = "sb-td",
            StartedBy = "my-deployment",
        });

        await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "sb-c",
            TaskDefinition = "sb-td",
            StartedBy = "other-deployment",
        });

        var resp = await _ecs.ListTasksAsync(new ListTasksRequest
        {
            Cluster = "sb-c",
            StartedBy = "my-deployment",
        });

        Assert.Single(resp.TaskArns);
    }

    // -- Task container details ------------------------------------------------

    [Fact]
    public async Task RunTaskContainerDetails()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "cd-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "cd-td",
            ContainerDefinitions =
            [
                new ContainerDefinition
                {
                    Name = "web",
                    Image = "nginx:latest",
                    Cpu = 256,
                    Memory = 512,
                },
            ],
        });

        var run = await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "cd-c",
            TaskDefinition = "cd-td",
        });

        var container = run.Tasks[0].Containers[0];
        Assert.Equal("web", container.Name);
        Assert.Equal("nginx:latest", container.Image);
        Assert.Equal("RUNNING", container.LastStatus);
        Assert.NotEmpty(container.ContainerArn);
    }

    // -- Delete service then list confirms removed ----------------------------

    [Fact]
    public async Task DeleteServiceRemovesFromList()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "del-list-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "del-list-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "del-list-c",
            ServiceName = "del-list-svc",
            TaskDefinition = "del-list-td",
            DesiredCount = 0,
        });

        await _ecs.DeleteServiceAsync(new DeleteServiceRequest
        {
            Cluster = "del-list-c",
            Service = "del-list-svc",
        });

        var list = await _ecs.ListServicesAsync(new ListServicesRequest { Cluster = "del-list-c" });
        Assert.Empty(list.ServiceArns);
    }

    // -- Task Definition registration with no family fails --------------------

    [Fact]
    public async Task RegisterTaskDefinitionNoFamilyFails()
    {
        var ex = await Assert.ThrowsAsync<ClientException>(() =>
            _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
            {
                ContainerDefinitions =
                [
                    new ContainerDefinition { Name = "app", Image = "img", Cpu = 64, Memory = 128 },
                ],
            }));

        Assert.Contains("family is required", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // -- Task Definition registration with no container definitions fails -----

    [Fact]
    public async Task RegisterTaskDefinitionNoContainersFails()
    {
        var ex = await Assert.ThrowsAsync<ClientException>(() =>
            _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
            {
                Family = "no-containers",
                ContainerDefinitions = [],
            }));

        Assert.Contains("at least one container definition", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // -- Cluster cleanup: delete removes tags ---------------------------------

    [Fact]
    public async Task DeleteClusterRemovesTags()
    {
        var createResp = await _ecs.CreateClusterAsync(new CreateClusterRequest
        {
            ClusterName = "tag-del-c",
            Tags = [new Tag { Key = "env", Value = "test" }],
        });

        var arn = createResp.Cluster.ClusterArn;

        // Tags should exist before delete
        var tags1 = await _ecs.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceArn = arn });
        Assert.Single(tags1.Tags);

        await _ecs.DeleteClusterAsync(new DeleteClusterRequest { Cluster = "tag-del-c" });

        // Tags should be gone after delete
        var tags2 = await _ecs.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceArn = arn });
        Assert.Empty(tags2.Tags);
    }

    // -- Update service not found fails ---------------------------------------

    [Fact]
    public async Task UpdateServiceNotFoundFails()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "us-nf-c" });

        var ex = await Assert.ThrowsAsync<ServiceNotFoundException>(() =>
            _ecs.UpdateServiceAsync(new UpdateServiceRequest
            {
                Cluster = "us-nf-c",
                Service = "nonexistent",
                DesiredCount = 1,
            }));

        Assert.Contains("not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // -- Service not found on delete fails ------------------------------------

    [Fact]
    public async Task DeleteServiceNotFoundFails()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "ds-nf-c" });

        var ex = await Assert.ThrowsAsync<ServiceNotFoundException>(() =>
            _ecs.DeleteServiceAsync(new DeleteServiceRequest
            {
                Cluster = "ds-nf-c",
                Service = "nonexistent",
                Force = true,
            }));

        Assert.Contains("not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // -- Deregister task definition not found fails ----------------------------

    [Fact]
    public async Task DeregisterTaskDefinitionNotFoundFails()
    {
        var ex = await Assert.ThrowsAsync<ClientException>(() =>
            _ecs.DeregisterTaskDefinitionAsync(new DeregisterTaskDefinitionRequest
            {
                TaskDefinition = "nonexistent:1",
            }));

        Assert.Contains("Unable to describe task definition", ex.Message);
    }

    // -- Describe task definition not found fails -----------------------------

    [Fact]
    public async Task DescribeTaskDefinitionNotFoundFails()
    {
        var ex = await Assert.ThrowsAsync<ClientException>(() =>
            _ecs.DescribeTaskDefinitionAsync(new DescribeTaskDefinitionRequest
            {
                TaskDefinition = "nonexistent:1",
            }));

        Assert.Contains("Unable to describe task definition", ex.Message);
    }

    // -- RunTask with tags ----------------------------------------------------

    [Fact]
    public async Task RunTaskWithTags()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "rt-tags-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "rt-tags-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "worker", Image = "alpine", Cpu = 64, Memory = 128 },
            ],
        });

        var run = await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "rt-tags-c",
            TaskDefinition = "rt-tags-td",
            Tags = [new Tag { Key = "batch", Value = "123" }],
        });

        var taskArn = run.Tasks[0].TaskArn;
        var tags = await _ecs.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = taskArn,
        });

        Assert.Contains(tags.Tags, t => t.Key == "batch" && t.Value == "123");
    }

    // -- StopTask not found fails ---------------------------------------------

    [Fact]
    public async Task StopTaskNotFoundFails()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "st-nf-c" });

        var ex = await Assert.ThrowsAsync<InvalidParameterException>(() =>
            _ecs.StopTaskAsync(new StopTaskRequest
            {
                Cluster = "st-nf-c",
                Task = "nonexistent-task",
            }));

        Assert.Contains("not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // -- DeleteTaskDefinitions ------------------------------------------------

    [Fact]
    public async Task DeleteTaskDefinitions()
    {
        var reg = await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "del-td-fam",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "app", Image = "img", Cpu = 64, Memory = 128 },
            ],
        });

        var arn = reg.TaskDefinition.TaskDefinitionArn;
        var resp = await _ecs.DeleteTaskDefinitionsAsync(new DeleteTaskDefinitionsRequest
        {
            TaskDefinitions = [arn],
        });

        Assert.Single(resp.TaskDefinitions);
        Assert.Empty(resp.Failures);
    }

    // -- Describe services with include TAGS ----------------------------------

    [Fact]
    public async Task DescribeServicesWithTags()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "ds-tags-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "ds-tags-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "ds-tags-c",
            ServiceName = "ds-tags-svc",
            TaskDefinition = "ds-tags-td",
            DesiredCount = 1,
            Tags = [new Tag { Key = "env", Value = "prod" }],
        });

        var resp = await _ecs.DescribeServicesAsync(new DescribeServicesRequest
        {
            Cluster = "ds-tags-c",
            Services = ["ds-tags-svc"],
            Include = ["TAGS"],
        });

        Assert.Single(resp.Services);
        Assert.Contains(resp.Services[0].Tags, t => t.Key == "env" && t.Value == "prod");
    }

    // -- UpdateClusterSettings ------------------------------------------------

    [Fact]
    public async Task UpdateClusterSettings()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "ucs-c" });

        var resp = await _ecs.UpdateClusterSettingsAsync(new UpdateClusterSettingsRequest
        {
            Cluster = "ucs-c",
            Settings = [new ClusterSetting { Name = ClusterSettingName.ContainerInsights, Value = "enabled" }],
        });

        Assert.Equal("ucs-c", resp.Cluster.ClusterName);
    }

    // -- Service create with cluster auto-creation ----------------------------

    [Fact]
    public async Task CreateServiceAutoCreatesCluster()
    {
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "auto-c-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        var resp = await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "auto-created-c",
            ServiceName = "auto-c-svc",
            TaskDefinition = "auto-c-td",
            DesiredCount = 1,
        });

        Assert.Equal("ACTIVE", resp.Service.Status);

        var clusters = await _ecs.ListClustersAsync(new ListClustersRequest());
        Assert.Contains(clusters.ClusterArns, a => a.Contains("auto-created-c"));
    }

    // -- Register task def with tags ------------------------------------------

    [Fact]
    public async Task RegisterTaskDefinitionWithTags()
    {
        var resp = await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "tagged-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "app", Image = "img", Cpu = 64, Memory = 128 },
            ],
            Tags = [new Tag { Key = "team", Value = "infra" }],
        });

        Assert.Single(resp.Tags);
        Assert.Equal("team", resp.Tags[0].Key);
    }

    // -- Describe task definition with tags -----------------------------------

    [Fact]
    public async Task DescribeTaskDefinitionWithTags()
    {
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "desc-tag-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "app", Image = "img", Cpu = 64, Memory = 128 },
            ],
            Tags = [new Tag { Key = "dept", Value = "eng" }],
        });

        var resp = await _ecs.DescribeTaskDefinitionAsync(new DescribeTaskDefinitionRequest
        {
            TaskDefinition = "desc-tag-td",
            Include = ["TAGS"],
        });

        Assert.Contains(resp.Tags, t => t.Key == "dept" && t.Value == "eng");
    }

    // -- Fargate compatibility ------------------------------------------------

    [Fact]
    public async Task RegisterTaskDefinitionFargateNetworkMode()
    {
        var resp = await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "fargate-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "app", Image = "img", Cpu = 256, Memory = 512 },
            ],
            RequiresCompatibilities = [Compatibility.FARGATE],
            Cpu = "256",
            Memory = "512",
        });

        Assert.Equal(NetworkMode.Awsvpc, resp.TaskDefinition.NetworkMode);
        Assert.Contains("FARGATE", resp.TaskDefinition.Compatibilities);
        Assert.Contains("EC2", resp.TaskDefinition.Compatibilities);
    }

    // -- RunTask with FARGATE launch type ------------------------------------

    [Fact]
    public async Task RunTaskWithFargateLaunchType()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "fg-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "fg-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "app", Image = "img", Cpu = 256, Memory = 512 },
            ],
            RequiresCompatibilities = [Compatibility.FARGATE],
            Cpu = "256",
            Memory = "512",
        });

        var resp = await _ecs.RunTaskAsync(new RunTaskRequest
        {
            Cluster = "fg-c",
            TaskDefinition = "fg-td",
            LaunchType = LaunchType.FARGATE,
        });

        Assert.Single(resp.Tasks);
        Assert.Equal(LaunchType.FARGATE, resp.Tasks[0].LaunchType);
    }

    // -- Service deployment details -------------------------------------------

    [Fact]
    public async Task ServiceHasDeployments()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "dep-c" });
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "dep-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "w", Image = "nginx", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.CreateServiceAsync(new CreateServiceRequest
        {
            Cluster = "dep-c",
            ServiceName = "dep-svc",
            TaskDefinition = "dep-td",
            DesiredCount = 2,
        });

        var desc = await _ecs.DescribeServicesAsync(new DescribeServicesRequest
        {
            Cluster = "dep-c",
            Services = ["dep-svc"],
        });

        Assert.Single(desc.Services[0].Deployments);
        Assert.Equal("PRIMARY", desc.Services[0].Deployments[0].Status);
        Assert.Equal(2, desc.Services[0].Deployments[0].DesiredCount);
    }

    // -- Cluster with capacity providers on create ----------------------------

    [Fact]
    public async Task CreateClusterWithCapacityProviders()
    {
        var resp = await _ecs.CreateClusterAsync(new CreateClusterRequest
        {
            ClusterName = "cp-create-c",
            CapacityProviders = ["FARGATE", "FARGATE_SPOT"],
            DefaultCapacityProviderStrategy =
            [
                new CapacityProviderStrategyItem { CapacityProvider = "FARGATE", Weight = 1 },
            ],
        });

        Assert.Equal(2, resp.Cluster.CapacityProviders.Count);
    }

    // -- ListTaskDefinitions with status filter --------------------------------

    [Fact]
    public async Task ListTaskDefinitionsWithStatusFilter()
    {
        await _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
        {
            Family = "ltds-td",
            ContainerDefinitions =
            [
                new ContainerDefinition { Name = "app", Image = "img", Cpu = 64, Memory = 128 },
            ],
        });

        await _ecs.DeregisterTaskDefinitionAsync(new DeregisterTaskDefinitionRequest
        {
            TaskDefinition = "ltds-td:1",
        });

        var active = await _ecs.ListTaskDefinitionsAsync(new ListTaskDefinitionsRequest
        {
            FamilyPrefix = "ltds-td",
            Status = TaskDefinitionStatus.ACTIVE,
        });

        var inactive = await _ecs.ListTaskDefinitionsAsync(new ListTaskDefinitionsRequest
        {
            FamilyPrefix = "ltds-td",
            Status = TaskDefinitionStatus.INACTIVE,
        });

        Assert.Empty(active.TaskDefinitionArns);
        Assert.Single(inactive.TaskDefinitionArns);
    }
}
