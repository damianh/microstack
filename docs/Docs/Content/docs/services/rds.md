---
title: RDS
description: RDS emulation — DB instances, clusters, parameter groups, subnet groups, snapshots, proxies.
order: 18
section: Services
---

# RDS

MicroStack's RDS handler provides metadata-only emulation of the RDS API — DB instances, clusters, global clusters, proxies, parameter groups, subnet groups, snapshots, and event subscriptions. No actual database engine is started.

## Supported Operations

**DB Instances**
- CreateDBInstance, DescribeDBInstances, ModifyDBInstance, DeleteDBInstance
- RebootDBInstance, StopDBInstance, StartDBInstance

**DB Clusters**
- CreateDBCluster, DescribeDBClusters, ModifyDBCluster, DeleteDBCluster
- StopDBCluster, StartDBCluster

**Subnet Groups**
- CreateDBSubnetGroup, DescribeDBSubnetGroups, ModifyDBSubnetGroup, DeleteDBSubnetGroup

**Parameter Groups**
- CreateDBParameterGroup, DescribeDBParameterGroups, DeleteDBParameterGroup, DescribeDBParameters, ModifyDBParameterGroup
- CreateDBClusterParameterGroup, DescribeDBClusterParameterGroups, DeleteDBClusterParameterGroup

**Snapshots**
- CreateDBSnapshot, DescribeDBSnapshots, DeleteDBSnapshot, CopyDBSnapshot
- CreateDBClusterSnapshot, DescribeDBClusterSnapshots, DeleteDBClusterSnapshot

**Proxies and Global Clusters**
- CreateDBProxy, DescribeDBProxies, DeleteDBProxy
- CreateGlobalCluster, DescribeGlobalClusters, ModifyGlobalCluster, DeleteGlobalCluster, RemoveFromGlobalCluster

**Other**
- CreateOptionGroup, DescribeOptionGroups, DeleteOptionGroup
- CreateEventSubscription, DescribeEventSubscriptions, DeleteEventSubscription
- AddTagsToResource, ListTagsForResource, RemoveTagsFromResource
- DescribeOrderableDBInstanceOptions, DescribeEngineDefaultClusterParameters, DescribeDBEngineVersions

## Usage

```csharp
var rds = new AmazonRDSClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonRDSConfig { ServiceURL = "http://localhost:4566" });

// Create a DB instance
var instance = await rds.CreateDBInstanceAsync(new CreateDBInstanceRequest
{
    DBInstanceIdentifier = "my-postgres-db",
    DBInstanceClass = "db.t3.micro",
    Engine = "postgres",
    MasterUsername = "admin",
    MasterUserPassword = "password123",
    DBName = "appdb",
    AllocatedStorage = 20,
});

Console.WriteLine(instance.DBInstance.DBInstanceStatus);   // available
Console.WriteLine(instance.DBInstance.Endpoint.Address);   // synthetic endpoint

// Describe instances
var describe = await rds.DescribeDBInstancesAsync(new DescribeDBInstancesRequest());
foreach (var db in describe.DBInstances)
{
    Console.WriteLine($"{db.DBInstanceIdentifier}: {db.Engine} ({db.DBInstanceStatus})");
}
```

## DB Clusters (Aurora)

```csharp
// Create a cluster
var cluster = await rds.CreateDBClusterAsync(new CreateDBClusterRequest
{
    DBClusterIdentifier = "my-aurora-cluster",
    Engine = "aurora-postgresql",
    MasterUsername = "admin",
    MasterUserPassword = "password123",
    DatabaseName = "appdb",
});

Console.WriteLine(cluster.DBCluster.Status); // available
```

## Subnet Groups

```csharp
// Create a DB subnet group (requires existing subnets via EC2)
var subnetGroup = await rds.CreateDBSubnetGroupAsync(new CreateDBSubnetGroupRequest
{
    DBSubnetGroupName = "my-db-subnets",
    DBSubnetGroupDescription = "Subnets for RDS",
    SubnetIds = ["subnet-abc123", "subnet-def456"],
});

Console.WriteLine(subnetGroup.DBSubnetGroup.DBSubnetGroupName);
```

:::aside{type="note" title="Metadata only"}
RDS is metadata-only — no actual database engine is started. Endpoint addresses are synthetic. Use [RDS Data API](rds-data) or a real database for actual SQL execution.
:::
