---
title: EMR
description: EMR emulation — cluster lifecycle, job steps, instance groups and fleets, bootstrap actions, and termination protection.
order: 32
section: Services
---

# EMR

MicroStack emulates Amazon EMR (Elastic MapReduce) cluster management. Clusters are created synchronously with a `WAITING` status — no actual Hadoop/Spark computation occurs. Steps, instance fleets, instance groups, and bootstrap actions are stored in memory and tracked by cluster ID.

## Supported Operations

**Clusters:** RunJobFlow, DescribeCluster, ListClusters, TerminateJobFlows, ModifyCluster, SetTerminationProtection, SetVisibleToAllUsers

**Steps:** AddJobFlowSteps, DescribeStep, ListSteps, CancelSteps

**Instance Fleets:** AddInstanceFleet, ListInstanceFleets, ModifyInstanceFleet

**Instance Groups:** AddInstanceGroups, ListInstanceGroups, ModifyInstanceGroups

**Bootstrap Actions:** ListBootstrapActions

**Tags:** AddTags, RemoveTags

**Configuration:** GetBlockPublicAccessConfiguration, PutBlockPublicAccessConfiguration

## Usage

```csharp
using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using Amazon.Runtime;

var config = new AmazonElasticMapReduceConfig
{
    ServiceURL = "http://localhost:4566",
};
using var client = new AmazonElasticMapReduceClient(
    new BasicAWSCredentials("test", "test"), config);

// Launch a cluster
var resp = await client.RunJobFlowAsync(new RunJobFlowRequest
{
    Name = "analytics-cluster",
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
        new Tag { Key = "team", Value = "data" },
    ],
});

var clusterId = resp.JobFlowId;
Console.WriteLine($"Cluster ID: {clusterId}");
// => j-XXXXXXXXXXXX

var desc = await client.DescribeClusterAsync(new DescribeClusterRequest
{
    ClusterId = clusterId,
});
Console.WriteLine($"Name: {desc.Cluster.Name}, Release: {desc.Cluster.ReleaseLabel}");
```

## Steps and Instance Groups

```csharp
// Add a Spark job step
var stepsResp = await client.AddJobFlowStepsAsync(new AddJobFlowStepsRequest
{
    JobFlowId = clusterId,
    Steps =
    [
        new StepConfig
        {
            Name = "run-spark-job",
            ActionOnFailure = ActionOnFailure.CONTINUE,
            HadoopJarStep = new HadoopJarStepConfig
            {
                Jar = "command-runner.jar",
                Args = ["spark-submit", "--class", "com.example.Main", "s3://my-bucket/job.jar"],
            },
        },
    ],
});

Console.WriteLine($"Step ID: {stepsResp.StepIds[0]}");

// Add a task instance group for scaling
var groupsResp = await client.AddInstanceGroupsAsync(new AddInstanceGroupsRequest
{
    JobFlowId = clusterId,
    InstanceGroups =
    [
        new InstanceGroupConfig
        {
            Name = "task-nodes",
            InstanceRole = InstanceRoleType.TASK,
            InstanceType = "m5.xlarge",
            InstanceCount = 4,
        },
    ],
});

Console.WriteLine($"Instance Group ID: {groupsResp.InstanceGroupIds[0]}");

// Terminate the cluster when done
await client.TerminateJobFlowsAsync(new TerminateJobFlowsRequest
{
    JobFlowIds = [clusterId],
});
```

:::aside{type="note" title="Metadata-Only Emulation"}
EMR in MicroStack is metadata-only — no actual Hadoop, Spark, or compute resources are provisioned. Clusters start in a `WAITING` state. Steps are tracked but not executed. Use MicroStack to test your cluster-management and orchestration code without cloud costs.
:::
