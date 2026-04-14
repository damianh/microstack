---
title: ECS
description: ECS emulation — clusters, task definitions, tasks, services, capacity providers, and account settings.
order: 24
section: Services
---

# ECS

MicroStack emulates Amazon ECS (Elastic Container Service) for clusters, task definitions, tasks, and services. All resources are metadata-only — no actual containers are launched. Tasks start immediately in `RUNNING` status; clusters and services are auto-created on demand when referenced by `RunTask` or `CreateService`.

## Supported Operations

**Clusters:** CreateCluster, DeleteCluster, DescribeClusters, ListClusters, UpdateCluster, UpdateClusterSettings, PutClusterCapacityProviders

**Task Definitions:** RegisterTaskDefinition, DeregisterTaskDefinition, DescribeTaskDefinition, ListTaskDefinitions, ListTaskDefinitionFamilies, DeleteTaskDefinitions

**Services:** CreateService, DeleteService, DescribeServices, UpdateService, ListServices, ListServicesByNamespace

**Tasks:** RunTask, StopTask, DescribeTasks, ListTasks, ExecuteCommand, UpdateTaskProtection, GetTaskProtection, SubmitTaskStateChange, SubmitContainerStateChange, SubmitAttachmentStateChanges, DiscoverPollEndpoint

**Capacity Providers:** CreateCapacityProvider, DeleteCapacityProvider, UpdateCapacityProvider, DescribeCapacityProviders

**Tags & Settings:** TagResource, UntagResource, ListTagsForResource, ListAccountSettings, PutAccountSetting, PutAccountSettingDefault, DeleteAccountSetting, PutAttributes, DeleteAttributes, ListAttributes, DescribeServiceDeployments, ListServiceDeployments, DescribeServiceRevisions

## Usage

```csharp
var client = new AmazonECSClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonECSConfig
    {
        RegionEndpoint = RegionEndpoint.USEast1,
        ServiceURL = "http://localhost:4566",
    });

// Create a cluster
var clusterResp = await client.CreateClusterAsync(new CreateClusterRequest
{
    ClusterName = "my-cluster",
});
Console.WriteLine(clusterResp.Cluster.Status); // ACTIVE

// Register a task definition
var tdResp = await client.RegisterTaskDefinitionAsync(new RegisterTaskDefinitionRequest
{
    Family = "my-task",
    ContainerDefinitions =
    [
        new ContainerDefinition
        {
            Name = "web",
            Image = "nginx:alpine",
            Cpu = 256,
            Memory = 512,
            PortMappings = [new PortMapping { ContainerPort = 80, HostPort = 8080 }],
        },
    ],
    RequiresCompatibilities = [Compatibility.FARGATE],
    Cpu = "256",
    Memory = "512",
});

Console.WriteLine(tdResp.TaskDefinition.Revision); // 1
Console.WriteLine(tdResp.TaskDefinition.Status);   // ACTIVE

// Run tasks
var runResp = await client.RunTaskAsync(new RunTaskRequest
{
    Cluster = "my-cluster",
    TaskDefinition = "my-task",
    Count = 2,
});

Console.WriteLine(runResp.Tasks.Count);              // 2
Console.WriteLine(runResp.Tasks[0].LastStatus);      // RUNNING

// Stop a task
var taskArn = runResp.Tasks[0].TaskArn;
var stopResp = await client.StopTaskAsync(new StopTaskRequest
{
    Cluster = "my-cluster",
    Task = taskArn,
    Reason = "Shutting down for maintenance",
});
Console.WriteLine(stopResp.Task.LastStatus); // STOPPED
```

## Services

```csharp
// Create a service
var svcResp = await client.CreateServiceAsync(new CreateServiceRequest
{
    Cluster = "my-cluster",
    ServiceName = "my-service",
    TaskDefinition = "my-task",
    DesiredCount = 3,
});

Console.WriteLine(svcResp.Service.Status);       // ACTIVE
Console.WriteLine(svcResp.Service.DesiredCount); // 3

// Scale the service
await client.UpdateServiceAsync(new UpdateServiceRequest
{
    Cluster = "my-cluster",
    Service = "my-service",
    DesiredCount = 1,
});

// List services in a cluster
var listResp = await client.ListServicesAsync(new ListServicesRequest
{
    Cluster = "my-cluster",
});
Console.WriteLine(listResp.ServiceArns.Count); // 1

// Delete the service (force=true bypasses DesiredCount=0 requirement)
await client.DeleteServiceAsync(new DeleteServiceRequest
{
    Cluster = "my-cluster",
    Service = "my-service",
    Force = true,
});
```

:::aside{type="note" title="Metadata-only emulation"}
ECS in MicroStack is purely metadata — no actual containers are started. Tasks are created in `RUNNING` status immediately. `RunTask` and `CreateService` auto-create the referenced cluster if it does not exist. Registering a new revision of the same task definition family increments the revision number.
:::
