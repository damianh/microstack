---
title: ElastiCache
description: ElastiCache emulation — cache clusters, replication groups, subnet groups, parameter groups, snapshots, users, and user groups.
order: 35
section: Services
---

# ElastiCache

MicroStack emulates Amazon ElastiCache with support for Redis and Memcached cache clusters, replication groups, subnet groups, parameter groups, snapshots, users, and user groups. All resources are metadata-only — no actual Redis or Memcached processes are launched.

## Supported Operations

**Cache Clusters:** CreateCacheCluster, DescribeCacheClusters, DeleteCacheCluster, ModifyCacheCluster, RebootCacheCluster

**Replication Groups:** CreateReplicationGroup, DescribeReplicationGroups, DeleteReplicationGroup, ModifyReplicationGroup, IncreaseReplicaCount, DecreaseReplicaCount

**Subnet Groups:** CreateCacheSubnetGroup, DescribeCacheSubnetGroups, DeleteCacheSubnetGroup, ModifyCacheSubnetGroup

**Parameter Groups:** CreateCacheParameterGroup, DescribeCacheParameterGroups, DeleteCacheParameterGroup, DescribeCacheParameters, ModifyCacheParameterGroup, ResetCacheParameterGroup

**Snapshots:** CreateSnapshot, DescribeSnapshots, DeleteSnapshot

**Users:** CreateUser, DescribeUsers, DeleteUser, ModifyUser

**User Groups:** CreateUserGroup, DescribeUserGroups, DeleteUserGroup, ModifyUserGroup

**Engine Versions:** DescribeCacheEngineVersions

**Events:** DescribeEvents

**Tags:** AddTagsToResource, RemoveTagsFromResource, ListTagsForResource

## Usage

```csharp
using Amazon.ElastiCache;
using Amazon.ElastiCache.Model;
using Amazon.Runtime;

var config = new AmazonElastiCacheConfig
{
    ServiceURL = "http://localhost:4566",
};
using var client = new AmazonElastiCacheClient(
    new BasicAWSCredentials("test", "test"), config);

// Create a Redis cache cluster
var resp = await client.CreateCacheClusterAsync(new CreateCacheClusterRequest
{
    CacheClusterId = "my-redis",
    Engine = "redis",
    CacheNodeType = "cache.t3.micro",
    NumCacheNodes = 1,
});

Console.WriteLine($"Cluster ID: {resp.CacheCluster.CacheClusterId}");
Console.WriteLine($"Status: {resp.CacheCluster.CacheClusterStatus}");
// => available

// Describe cache clusters
var desc = await client.DescribeCacheClustersAsync(new DescribeCacheClustersRequest
{
    CacheClusterId = "my-redis",
});
Console.WriteLine($"Engine: {desc.CacheClusters[0].Engine}");
Console.WriteLine($"Nodes: {desc.CacheClusters[0].CacheNodes.Count}");
```

## Replication Groups

```csharp
// Create a Redis replication group (multi-node)
var rg = await client.CreateReplicationGroupAsync(new CreateReplicationGroupRequest
{
    ReplicationGroupId = "my-rg",
    ReplicationGroupDescription = "Production cache",
    CacheNodeType = "cache.t3.micro",
    NumNodeGroups = 1,
    ReplicasPerNodeGroup = 2,
});

Console.WriteLine($"Status: {rg.ReplicationGroup.Status}");
// => available

// Create a cache subnet group for VPC placement
await client.CreateCacheSubnetGroupAsync(new CreateCacheSubnetGroupRequest
{
    CacheSubnetGroupName = "my-subnet-group",
    CacheSubnetGroupDescription = "Cache subnet group",
    SubnetIds = ["subnet-aaa", "subnet-bbb"],
});

// Create a parameter group to tune Redis settings
await client.CreateCacheParameterGroupAsync(new CreateCacheParameterGroupRequest
{
    CacheParameterGroupName = "my-redis-params",
    CacheParameterGroupFamily = "redis7.0",
    Description = "Custom Redis parameters",
});

await client.ModifyCacheParameterGroupAsync(new ModifyCacheParameterGroupRequest
{
    CacheParameterGroupName = "my-redis-params",
    ParameterNameValues =
    [
        new ParameterNameValue { ParameterName = "maxmemory-policy", ParameterValue = "allkeys-lru" },
    ],
});
```

:::aside{type="note" title="Metadata-Only Emulation"}
ElastiCache in MicroStack is metadata-only — no actual Redis or Memcached processes are launched. Clusters have `available` status immediately after creation. Use MicroStack to test your caching infrastructure code and IaC without provisioning real cache clusters.
:::
