---
title: EFS
description: EFS emulation — file systems, mount targets, access points, lifecycle configuration, and backup policies.
order: 33
section: Services
---

# EFS

MicroStack emulates Amazon Elastic File System with file system lifecycle management, mount targets, access points, lifecycle configuration, and backup policies. All state is in memory — no actual file storage or NFS mounts are provisioned. File system IDs begin with `fs-` and mount target IDs with `fsmt-`.

## Supported Operations

**File Systems:** CreateFileSystem, DescribeFileSystems, DeleteFileSystem, UpdateFileSystem

**Mount Targets:** CreateMountTarget, DescribeMountTargets, DeleteMountTarget, DescribeMountTargetSecurityGroups, ModifyMountTargetSecurityGroups

**Access Points:** CreateAccessPoint, DescribeAccessPoints, DeleteAccessPoint

**Lifecycle:** PutLifecycleConfiguration, DescribeLifecycleConfiguration

**Backup Policy:** PutBackupPolicy, DescribeBackupPolicy

**Tags:** TagResource, UntagResource, ListTagsForResource

## Usage

```csharp
using Amazon.ElasticFileSystem;
using Amazon.ElasticFileSystem.Model;
using Amazon.Runtime;

var config = new AmazonElasticFileSystemConfig
{
    ServiceURL = "http://localhost:4566",
};
using var client = new AmazonElasticFileSystemClient(
    new BasicAWSCredentials("test", "test"), config);

// Create a file system
var fs = await client.CreateFileSystemAsync(new CreateFileSystemRequest
{
    PerformanceMode = PerformanceMode.GeneralPurpose,
    ThroughputMode = ThroughputMode.Bursting,
    Encrypted = false,
    Tags =
    [
        new Tag { Key = "Name", Value = "my-efs" },
    ],
});

Console.WriteLine($"File System ID: {fs.FileSystemId}");
// => fs-XXXXXXXX

// Describe the file system
var desc = await client.DescribeFileSystemsAsync(new DescribeFileSystemsRequest
{
    FileSystemId = fs.FileSystemId,
});
Console.WriteLine($"State: {desc.FileSystems[0].LifeCycleState}");
// => Available

// Create a mount target in a subnet
var mt = await client.CreateMountTargetAsync(new CreateMountTargetRequest
{
    FileSystemId = fs.FileSystemId,
    SubnetId = "subnet-00000001",
});
Console.WriteLine($"Mount Target ID: {mt.MountTargetId}");
// => fsmt-XXXXXXXX
```

## Access Points

```csharp
// Create an access point with a dedicated root directory
var ap = await client.CreateAccessPointAsync(new CreateAccessPointRequest
{
    FileSystemId = fs.FileSystemId,
    Tags =
    [
        new Tag { Key = "Name", Value = "data-access-point" },
    ],
    RootDirectory = new RootDirectory { Path = "/data" },
    PosixUser = new PosixUser { Uid = 1000, Gid = 1000 },
});

Console.WriteLine($"Access Point ID: {ap.AccessPointId}");
// => fsap-XXXXXXXX

// Describe all access points for a file system
var aps = await client.DescribeAccessPointsAsync(new DescribeAccessPointsRequest
{
    FileSystemId = fs.FileSystemId,
});
Console.WriteLine($"Access points: {aps.AccessPoints.Count}");

// Configure lifecycle to transition infrequently accessed files to IA storage
await client.PutLifecycleConfigurationAsync(new PutLifecycleConfigurationRequest
{
    FileSystemId = fs.FileSystemId,
    LifecyclePolicies =
    [
        new LifecyclePolicy { TransitionToIA = TransitionToIARules.AFTER_30_DAYS },
    ],
});
```

:::aside{type="note" title="Mount Target Dependency"}
A file system with active mount targets cannot be deleted — you must delete all mount targets first. Attempting to delete a file system with mount targets returns a `FileSystemInUseException`. Use `CreationToken` for idempotent file system creation: repeated calls with the same token return the existing file system.
:::
