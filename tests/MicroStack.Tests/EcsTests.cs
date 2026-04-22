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

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _ecs.Dispose();
        return ValueTask.CompletedTask;
    }

    // -- Cluster tests ---------------------------------------------------------

    [Fact]
    public async Task CreateCluster()
    {
        var resp = await _ecs.CreateClusterAsync(new CreateClusterRequest
        {
            ClusterName = "test-cluster",
        });

        resp.Cluster.ClusterName.ShouldBe("test-cluster");
        resp.Cluster.Status.ShouldBe("ACTIVE");
        resp.Cluster.ClusterArn.ShouldContain("test-cluster");
    }

    [Fact]
    public async Task CreateClusterIdempotent()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "idem-cluster" });
        var resp2 = await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "idem-cluster" });

        resp2.Cluster.ClusterName.ShouldBe("idem-cluster");
        resp2.Cluster.Status.ShouldBe("ACTIVE");
    }

    [Fact]
    public async Task ListClusters()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "lc-a" });
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "lc-b" });

        var resp = await _ecs.ListClustersAsync(new ListClustersRequest());

        resp.ClusterArns.ShouldContain(a => a.Contains("lc-a"));
        resp.ClusterArns.ShouldContain(a => a.Contains("lc-b"));
    }

    [Fact]
    public async Task DescribeClusters()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "desc-cluster" });

        var resp = await _ecs.DescribeClustersAsync(new DescribeClustersRequest
        {
            Clusters = ["desc-cluster"],
        });

        resp.Clusters.ShouldHaveSingleItem();
        resp.Clusters[0].ClusterName.ShouldBe("desc-cluster");
        resp.Failures.ShouldBeEmpty();
    }

    [Fact]
    public async Task DescribeClustersNotFound()
    {
        var resp = await _ecs.DescribeClustersAsync(new DescribeClustersRequest
        {
            Clusters = ["nonexistent"],
        });

        resp.Clusters.ShouldBeEmpty();
        resp.Failures.ShouldHaveSingleItem();
        resp.Failures[0].Reason.ShouldBe("MISSING");
    }

    [Fact]
    public async Task DeleteCluster()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "del-cluster" });

        var resp = await _ecs.DeleteClusterAsync(new DeleteClusterRequest { Cluster = "del-cluster" });
        resp.Cluster.Status.ShouldBe("INACTIVE");

        var listResp = await _ecs.ListClustersAsync(new ListClustersRequest());
        listResp.ClusterArns.ShouldNotContain(a => a.Contains("del-cluster"));
    }

    [Fact]
    public async Task DeleteClusterNotFound()
    {
        var ex = await Should.ThrowAsync<ClusterNotFoundException>(() =>
            _ecs.DeleteClusterAsync(new DeleteClusterRequest { Cluster = "no-such-cluster" }));
        ex.Message.ShouldContain("not found", Case.Insensitive);
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

        resp.Cluster.ClusterName.ShouldBe("upd-cl");
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

        resp.Cluster.ClusterName.ShouldBe("cp-cluster");
        resp.Cluster.CapacityProviders.ShouldContain("FARGATE");
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

        resp.TaskDefinition.Family.ShouldBe("test-task");
        resp.TaskDefinition.Revision.ShouldBe(1);
        resp.TaskDefinition.Status.Value.ShouldBe("ACTIVE");
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

        resp2.TaskDefinition.Revision.ShouldBe(2);
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

        resp.TaskDefinition.ContainerDefinitions.Count.ShouldBe(2);
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

        resp.TaskDefinition.Family.ShouldBe("desc-td");
        resp.TaskDefinition.Revision.ShouldBe(1);
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

        resp.TaskDefinition.Status.Value.ShouldBe("INACTIVE");
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

        (resp.TaskDefinitionArns.Count >= 1).ShouldBe(true);
        resp.TaskDefinitionArns.ShouldAllBe(a => a.Contains("ltd-task"));
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

        resp.Families.ShouldContain("fam-a");
        resp.Families.ShouldContain("fam-b");
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

        resp.Service.ServiceName.ShouldBe("test-svc");
        resp.Service.Status.ShouldBe("ACTIVE");
        resp.Service.DesiredCount.ShouldBe(2);
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

        await Should.ThrowAsync<InvalidParameterException>(() =>
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

        resp.Services.Count.ShouldBe(2);
        var svcMap = resp.Services.ToDictionary(s => s.ServiceName);
        svcMap["ds-svc-a"].DesiredCount.ShouldBe(1);
        svcMap["ds-svc-b"].DesiredCount.ShouldBe(3);
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

        resp.Services.ShouldBeEmpty();
        resp.Failures.ShouldHaveSingleItem();
        resp.Failures[0].Reason.ShouldBe("MISSING");
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

        desc.Services[0].DesiredCount.ShouldBe(5);
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

        updateResp.Service.TaskDefinition.ShouldContain("us-td-v2");
        (updateResp.Service.Deployments.Count >= 2).ShouldBe(true);
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

        resp.Service.Status.ShouldBe("DRAINING");
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

        await Should.ThrowAsync<InvalidParameterException>(() =>
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

        resp.ServiceArns.Count.ShouldBe(2);
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

        resp.Tasks.ShouldHaveSingleItem();
        resp.Tasks[0].LastStatus.ShouldBe("RUNNING");
        resp.Tasks[0].TaskArn.ShouldNotBeEmpty();
        resp.Tasks[0].Containers.ShouldNotBeEmpty();

        // Describe the task
        var desc = await _ecs.DescribeTasksAsync(new DescribeTasksRequest
        {
            Cluster = "rt-cluster",
            Tasks = [resp.Tasks[0].TaskArn],
        });

        desc.Tasks.ShouldHaveSingleItem();
        desc.Tasks[0].TaskArn.ShouldBe(resp.Tasks[0].TaskArn);
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

        resp.Tasks.Count.ShouldBe(3);
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

        resp.Task.LastStatus.ShouldBe("STOPPED");
        resp.Task.DesiredStatus.ShouldBe("STOPPED");
        resp.Task.StopCode.Value.ShouldBe("UserInitiated");
        resp.Task.Containers.ShouldAllBe(c => c.LastStatus == "STOPPED");
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
        resp.Task.LastStatus.ShouldBe("STOPPED");
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

        resp.TaskArns.Count.ShouldBe(2);
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

        running.TaskArns.ShouldHaveSingleItem();
        stopped.TaskArns.ShouldHaveSingleItem();
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

        resp.Tasks.ShouldBeEmpty();
        resp.Failures.ShouldHaveSingleItem();
        resp.Failures[0].Reason.ShouldBe("MISSING");
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

        resp.Tasks.ShouldHaveSingleItem();

        // Cluster should have been auto-created
        var clusters = await _ecs.ListClustersAsync(new ListClustersRequest());
        clusters.ClusterArns.ShouldContain(a => a.Contains("auto-created-cluster"));
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

        tags.Tags.ShouldContain(t => t.Key == "env" && t.Value == "staging");
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
        tags2.Tags.Count.ShouldBe(2);

        await _ecs.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceArn = arn,
            TagKeys = ["env"],
        });

        var tags3 = await _ecs.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceArn = arn });
        tags3.Tags.ShouldHaveSingleItem();
        tags3.Tags[0].Key.ShouldBe("team");
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
        tags.Tags.ShouldHaveSingleItem();
        tags.Tags[0].Value.ShouldBe("production");
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

        resp.CapacityProvider.Name.ShouldBe("test-cp");

        var desc = await _ecs.DescribeCapacityProvidersAsync(new DescribeCapacityProvidersRequest
        {
            CapacityProviders = ["test-cp"],
        });

        desc.CapacityProviders.ShouldContain(cp => cp.Name == "test-cp");
    }

    [Fact]
    public async Task DescribeCapacityProvidersIncludesDefaults()
    {
        var resp = await _ecs.DescribeCapacityProvidersAsync(new DescribeCapacityProvidersRequest());

        resp.CapacityProviders.ShouldContain(cp => cp.Name == "FARGATE");
        resp.CapacityProviders.ShouldContain(cp => cp.Name == "FARGATE_SPOT");
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

        resp.CapacityProvider.Status.Value.ShouldBe("INACTIVE");
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

        await Should.ThrowAsync<InvalidParameterException>(() =>
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

        resp.Settings.ShouldNotBeEmpty();
        resp.Settings.ShouldContain(s => s.Name.Value == "serviceLongArnFormat");
    }

    [Fact]
    public async Task PutAccountSetting()
    {
        var resp = await _ecs.PutAccountSettingAsync(new PutAccountSettingRequest
        {
            Name = SettingName.ContainerInsights,
            Value = "enhanced",
        });

        resp.Setting.Name.Value.ShouldBe("containerInsights");
        resp.Setting.Value.ShouldBe("enhanced");
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

        desc.Clusters[0].RunningTasksCount.ShouldBe(2);
        desc.Clusters[0].ActiveServicesCount.ShouldBe(1);
    }

    // -- RunTask with task definition not found ---------------------------------

    [Fact]
    public async Task RunTaskWithInvalidTaskDefinitionFails()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "rt-invalid-c" });

        var ex = await Should.ThrowAsync<ClientException>(() =>
            _ecs.RunTaskAsync(new RunTaskRequest
            {
                Cluster = "rt-invalid-c",
                TaskDefinition = "nonexistent-td",
            }));

        ex.Message.ShouldContain("Unable to find task definition");
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
        arn.ShouldStartWith("arn:aws:ecs:");
        arn.ShouldContain("task/arn-fmt-c/");
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
        arn.ShouldStartWith("arn:aws:ecs:");
        arn.ShouldContain("service/sarn-c/sarn-svc");
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

        resp.Tasks.ShouldHaveSingleItem();
        resp.Tasks[0].LastStatus.ShouldBe("RUNNING");
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

        resp.TaskArns.ShouldHaveSingleItem();
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
        container.Name.ShouldBe("web");
        container.Image.ShouldBe("nginx:latest");
        container.LastStatus.ShouldBe("RUNNING");
        container.ContainerArn.ShouldNotBeEmpty();
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
        list.ServiceArns.ShouldBeEmpty();
    }

    // -- Task Definition registration with no family fails --------------------

    [Fact]
    public async Task RegisterTaskDefinitionNoFamilyFails()
    {
        var ex = await Should.ThrowAsync<ClientException>(() =>
            _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
            {
                ContainerDefinitions =
                [
                    new ContainerDefinition { Name = "app", Image = "img", Cpu = 64, Memory = 128 },
                ],
            }));

        ex.Message.ShouldContain("family is required", Case.Insensitive);
    }

    // -- Task Definition registration with no container definitions fails -----

    [Fact]
    public async Task RegisterTaskDefinitionNoContainersFails()
    {
        var ex = await Should.ThrowAsync<ClientException>(() =>
            _ecs.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
            {
                Family = "no-containers",
                ContainerDefinitions = [],
            }));

        ex.Message.ShouldContain("at least one container definition", Case.Insensitive);
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
        tags1.Tags.ShouldHaveSingleItem();

        await _ecs.DeleteClusterAsync(new DeleteClusterRequest { Cluster = "tag-del-c" });

        // Tags should be gone after delete
        var tags2 = await _ecs.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceArn = arn });
        tags2.Tags.ShouldBeEmpty();
    }

    // -- Update service not found fails ---------------------------------------

    [Fact]
    public async Task UpdateServiceNotFoundFails()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "us-nf-c" });

        var ex = await Should.ThrowAsync<ServiceNotFoundException>(() =>
            _ecs.UpdateServiceAsync(new UpdateServiceRequest
            {
                Cluster = "us-nf-c",
                Service = "nonexistent",
                DesiredCount = 1,
            }));

        ex.Message.ShouldContain("not found", Case.Insensitive);
    }

    // -- Service not found on delete fails ------------------------------------

    [Fact]
    public async Task DeleteServiceNotFoundFails()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "ds-nf-c" });

        var ex = await Should.ThrowAsync<ServiceNotFoundException>(() =>
            _ecs.DeleteServiceAsync(new DeleteServiceRequest
            {
                Cluster = "ds-nf-c",
                Service = "nonexistent",
                Force = true,
            }));

        ex.Message.ShouldContain("not found", Case.Insensitive);
    }

    // -- Deregister task definition not found fails ----------------------------

    [Fact]
    public async Task DeregisterTaskDefinitionNotFoundFails()
    {
        var ex = await Should.ThrowAsync<ClientException>(() =>
            _ecs.DeregisterTaskDefinitionAsync(new DeregisterTaskDefinitionRequest
            {
                TaskDefinition = "nonexistent:1",
            }));

        ex.Message.ShouldContain("Unable to describe task definition");
    }

    // -- Describe task definition not found fails -----------------------------

    [Fact]
    public async Task DescribeTaskDefinitionNotFoundFails()
    {
        var ex = await Should.ThrowAsync<ClientException>(() =>
            _ecs.DescribeTaskDefinitionAsync(new DescribeTaskDefinitionRequest
            {
                TaskDefinition = "nonexistent:1",
            }));

        ex.Message.ShouldContain("Unable to describe task definition");
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

        tags.Tags.ShouldContain(t => t.Key == "batch" && t.Value == "123");
    }

    // -- StopTask not found fails ---------------------------------------------

    [Fact]
    public async Task StopTaskNotFoundFails()
    {
        await _ecs.CreateClusterAsync(new CreateClusterRequest { ClusterName = "st-nf-c" });

        var ex = await Should.ThrowAsync<InvalidParameterException>(() =>
            _ecs.StopTaskAsync(new StopTaskRequest
            {
                Cluster = "st-nf-c",
                Task = "nonexistent-task",
            }));

        ex.Message.ShouldContain("not found", Case.Insensitive);
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

        resp.TaskDefinitions.ShouldHaveSingleItem();
        resp.Failures.ShouldBeEmpty();
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

        resp.Services.ShouldHaveSingleItem();
        resp.Services[0].Tags.ShouldContain(t => t.Key == "env" && t.Value == "prod");
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

        resp.Cluster.ClusterName.ShouldBe("ucs-c");
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

        resp.Service.Status.ShouldBe("ACTIVE");

        var clusters = await _ecs.ListClustersAsync(new ListClustersRequest());
        clusters.ClusterArns.ShouldContain(a => a.Contains("auto-created-c"));
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

        resp.Tags.ShouldHaveSingleItem();
        resp.Tags[0].Key.ShouldBe("team");
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

        resp.Tags.ShouldContain(t => t.Key == "dept" && t.Value == "eng");
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

        resp.TaskDefinition.NetworkMode.ShouldBe(NetworkMode.Awsvpc);
        resp.TaskDefinition.Compatibilities.ShouldContain("FARGATE");
        resp.TaskDefinition.Compatibilities.ShouldContain("EC2");
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

        resp.Tasks.ShouldHaveSingleItem();
        resp.Tasks[0].LaunchType.ShouldBe(LaunchType.FARGATE);
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

        desc.Services[0].Deployments.ShouldHaveSingleItem();
        desc.Services[0].Deployments[0].Status.ShouldBe("PRIMARY");
        desc.Services[0].Deployments[0].DesiredCount.ShouldBe(2);
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

        resp.Cluster.CapacityProviders.Count.ShouldBe(2);
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

        active.TaskDefinitionArns.ShouldBeEmpty();
        inactive.TaskDefinitionArns.ShouldHaveSingleItem();
    }
}
