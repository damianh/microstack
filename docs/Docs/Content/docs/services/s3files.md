---
title: S3Files
description: S3Files emulation — S3-backed virtual file systems, mount targets, access points, policies, and synchronization configuration via a REST/JSON API.
order: 39
section: Services
---

# S3Files

MicroStack's S3Files handler emulates an S3-backed file system service (S3 Access Grants / S3-backed file systems). It provides REST/JSON endpoints for managing file systems, mount targets, access points, resource policies, and synchronization configuration. All state is held in-memory.

## Supported Operations

**File Systems:** CreateFileSystem, GetFileSystem, ListFileSystems, DeleteFileSystem

**Mount Targets:** CreateMountTarget, GetMountTarget, ListMountTargets, DeleteMountTarget, UpdateMountTarget

**Access Points:** CreateAccessPoint, GetAccessPoint, ListAccessPoints, DeleteAccessPoint

**Policies:** GetFileSystemPolicy, PutFileSystemPolicy, DeleteFileSystemPolicy

**Synchronization:** GetSynchronizationConfiguration, PutSynchronizationConfiguration

**Tags:** TagResource, UntagResource, ListTagsForResource

## Usage

S3Files uses a path-based REST/JSON protocol. Use `HttpClient` (or any HTTP library) pointed at MicroStack's base URL.

```csharp
using System.Net.Http.Json;
using System.Text.Json;

using var http = new HttpClient { BaseAddress = new Uri("http://localhost:4566") };

// Create a file system backed by an S3 bucket
var createResp = await http.PostAsJsonAsync("/file-systems", new
{
    BucketName = "my-data-bucket",
});
createResp.EnsureSuccessStatusCode();

var fs = await createResp.Content.ReadFromJsonAsync<JsonElement>();
var fileSystemId = fs.GetProperty("FileSystemId").GetString();
Console.WriteLine($"Created: {fileSystemId}"); // e.g. fs-a1b2c3d4e5f6g7h8i

// Get the file system
var getResp = await http.GetFromJsonAsync<JsonElement>($"/file-systems/{fileSystemId}");
Console.WriteLine(getResp.GetProperty("LifeCycleState").GetString()); // available

// List all file systems
var listResp = await http.GetFromJsonAsync<JsonElement>("/file-systems");
var fileSystems = listResp.GetProperty("FileSystems");
Console.WriteLine($"Total: {fileSystems.GetArrayLength()}");

// Delete the file system
var deleteResp = await http.DeleteAsync($"/file-systems/{fileSystemId}");
deleteResp.EnsureSuccessStatusCode(); // 204 No Content
```

## Mount Targets and Access Points

```csharp
// Create a mount target for the file system
var mtResp = await http.PostAsJsonAsync("/mount-targets", new
{
    FileSystemId = fileSystemId,
    SubnetId     = "subnet-abc123",
    VpcId        = "vpc-def456",
    IpAddress    = "10.0.1.10",
});
var mt = await mtResp.Content.ReadFromJsonAsync<JsonElement>();
var mountTargetId = mt.GetProperty("MountTargetId").GetString();

// List mount targets for a specific file system
var mtListResp = await http.GetFromJsonAsync<JsonElement>(
    $"/mount-targets?FileSystemId={fileSystemId}");
Console.WriteLine(mtListResp.GetProperty("MountTargets").GetArrayLength());

// Create an access point
var apResp = await http.PostAsJsonAsync("/access-points", new
{
    FileSystemId = fileSystemId,
    Name         = "my-access-point",
});
var ap = await apResp.Content.ReadFromJsonAsync<JsonElement>();
var accessPointId = ap.GetProperty("AccessPointId").GetString();
Console.WriteLine(ap.GetProperty("LifeCycleState").GetString()); // available

// Attach a resource policy to the file system
using var policyContent = JsonContent.Create(new
{
    Policy = """{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3files:*"}]}""",
});
var policyResp = await http.PutAsync($"/file-systems/{fileSystemId}/policy", policyContent);
policyResp.EnsureSuccessStatusCode();
```

:::aside{type="note" title="Internal REST API"}
S3Files does not correspond to a single named AWS service with an official SDK client. Requests are plain REST/JSON calls to path-based endpoints served at the MicroStack base URL. There is no `AmazonS3FilesClient` in the AWS SDK — use `HttpClient` directly or any HTTP library.
:::
