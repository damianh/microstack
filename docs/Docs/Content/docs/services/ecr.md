---
title: ECR
description: ECR emulation — repositories, image management, lifecycle policies, repository policies, and authorization tokens.
order: 34
section: Services
---

# ECR

MicroStack emulates Amazon Elastic Container Registry with repository lifecycle management, image push/pull metadata, authorization tokens, lifecycle policies, and repository policies. Images are tracked as metadata — no actual container layer storage occurs. Repositories with images cannot be deleted without `Force = true`.

## Supported Operations

**Repositories:** CreateRepository, DescribeRepositories, DeleteRepository, DescribeRegistry

**Images:** PutImage, ListImages, DescribeImages, BatchGetImage, BatchDeleteImage

**Authorization:** GetAuthorizationToken

**Lifecycle Policies:** PutLifecyclePolicy, GetLifecyclePolicy, DeleteLifecyclePolicy

**Repository Policies:** SetRepositoryPolicy, GetRepositoryPolicy, DeleteRepositoryPolicy

**Configuration:** PutImageTagMutability, PutImageScanningConfiguration

**Layer Upload (stubs):** GetDownloadUrlForLayer, BatchCheckLayerAvailability, InitiateLayerUpload, UploadLayerPart, CompleteLayerUpload

**Tags:** TagResource, UntagResource, ListTagsForResource

## Usage

```csharp
using Amazon.ECR;
using Amazon.ECR.Model;
using Amazon.Runtime;

var config = new AmazonECRConfig
{
    ServiceURL = "http://localhost:4566",
};
using var client = new AmazonECRClient(
    new BasicAWSCredentials("test", "test"), config);

// Create a repository
var repo = await client.CreateRepositoryAsync(new CreateRepositoryRequest
{
    RepositoryName = "my-app",
});
Console.WriteLine($"URI: {repo.Repository.RepositoryUri}");
Console.WriteLine($"ARN: {repo.Repository.RepositoryArn}");

// Push an image (metadata only)
var manifest = """{"schemaVersion": 2, "config": {"digest": "sha256:abc123"}}""";
var image = await client.PutImageAsync(new PutImageRequest
{
    RepositoryName = "my-app",
    ImageManifest = manifest,
    ImageTag = "v1.0.0",
});
Console.WriteLine($"Digest: {image.Image.ImageId.ImageDigest}");

// List images
var images = await client.ListImagesAsync(new ListImagesRequest
{
    RepositoryName = "my-app",
});
Console.WriteLine($"Images: {images.ImageIds.Count}");

// Get authorization token (for docker login)
var auth = await client.GetAuthorizationTokenAsync(new GetAuthorizationTokenRequest());
Console.WriteLine($"Token: {auth.AuthorizationData[0].AuthorizationToken}");
```

## Lifecycle Policies and Image Management

```csharp
// Set a lifecycle policy to expire untagged images after 14 days
var policy = """
    {
      "rules": [{
        "rulePriority": 1,
        "selection": {
          "tagStatus": "untagged",
          "countType": "sinceImagePushed",
          "countUnit": "days",
          "countNumber": 14
        },
        "action": { "type": "expire" }
      }]
    }
    """;

await client.PutLifecyclePolicyAsync(new PutLifecyclePolicyRequest
{
    RepositoryName = "my-app",
    LifecyclePolicyText = policy,
});

// Batch retrieve images
var batch = await client.BatchGetImageAsync(new BatchGetImageRequest
{
    RepositoryName = "my-app",
    ImageIds = [new ImageIdentifier { ImageTag = "v1.0.0" }],
});
Console.WriteLine($"Found: {batch.Images.Count}, Failures: {batch.Failures.Count}");

// Force-delete a repository with images
await client.DeleteRepositoryAsync(new DeleteRepositoryRequest
{
    RepositoryName = "my-app",
    Force = true,
});
```

:::aside{type="note" title="Image Storage"}
ECR in MicroStack is metadata-only — image manifests and tags are stored in memory, but no actual container layers are stored or served. `GetDownloadUrlForLayer` and layer upload operations are accepted but do nothing. Repositories that contain images return `RepositoryNotEmptyException` on delete unless `Force = true` is specified.
:::
