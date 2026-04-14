---
title: CloudFront
description: CloudFront emulation — distributions, invalidations, origin access controls, and tag management with ETag-based optimistic concurrency.
order: 21
section: Services
---

# CloudFront

MicroStack emulates Amazon CloudFront distributions, cache invalidations, and origin access controls. Every distribution is created in the `Deployed` state immediately. Updates and deletes use ETag-based optimistic concurrency — you must pass the current ETag in `IfMatch`, or the request will fail with `PreconditionFailed`. Distributions must be disabled before they can be deleted.

## Supported Operations

CreateDistribution, GetDistribution, GetDistributionConfig, ListDistributions, UpdateDistribution, DeleteDistribution, CreateInvalidation, ListInvalidations, GetInvalidation, CreateOriginAccessControl, GetOriginAccessControl, GetOriginAccessControlConfig, ListOriginAccessControls, UpdateOriginAccessControl, DeleteOriginAccessControl, TagResource, UntagResource, ListTagsForResource

## Usage

```csharp
var client = new AmazonCloudFrontClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonCloudFrontConfig
    {
        RegionEndpoint = RegionEndpoint.USEast1,
        ServiceURL = "http://localhost:4566",
    });

// Create a distribution
var distConfig = new DistributionConfig
{
    CallerReference = "my-ref-1",
    Comment = "my test distribution",
    Enabled = true,
    Origins = new Origins
    {
        Quantity = 1,
        Items =
        [
            new Origin
            {
                Id = "myS3Origin",
                DomainName = "mybucket.s3.amazonaws.com",
                S3OriginConfig = new S3OriginConfig { OriginAccessIdentity = "" },
            },
        ],
    },
    DefaultCacheBehavior = new DefaultCacheBehavior
    {
        TargetOriginId = "myS3Origin",
        ViewerProtocolPolicy = ViewerProtocolPolicy.RedirectToHttps,
        CachePolicyId = "658327ea-f89d-4fab-a63d-7e88639e58f6",
    },
};

var createResp = await client.CreateDistributionAsync(new CreateDistributionRequest
{
    DistributionConfig = distConfig,
});

string distId  = createResp.Distribution.Id;
string etag    = createResp.ETag;

Console.WriteLine(distId);                    // e.g. E1ABCDEF1234567
Console.WriteLine(createResp.Distribution.DomainName); // e.g. E1ABCDEF1234567.cloudfront.net
Console.WriteLine(createResp.Distribution.Status);     // Deployed

// Get the distribution
var getResp = await client.GetDistributionAsync(new GetDistributionRequest { Id = distId });
Console.WriteLine(getResp.Distribution.Status); // Deployed
```

## Updating Distributions

All update operations require the current ETag returned by `CreateDistribution` or `GetDistribution`. A stale or incorrect ETag results in a `PreconditionFailed` error.

```csharp
// Update a distribution — IfMatch must carry the current ETag
var updResp = await client.UpdateDistributionAsync(new UpdateDistributionRequest
{
    Id = distId,
    IfMatch = etag,
    DistributionConfig = new DistributionConfig
    {
        CallerReference = "my-ref-1",
        Comment = "updated comment",
        Enabled = true,
        Origins = distConfig.Origins,
        DefaultCacheBehavior = distConfig.DefaultCacheBehavior,
    },
});

string newEtag = updResp.ETag; // use this for subsequent updates or delete

// Delete the distribution — must be disabled first
await client.UpdateDistributionAsync(new UpdateDistributionRequest
{
    Id = distId,
    IfMatch = newEtag,
    DistributionConfig = new DistributionConfig
    {
        CallerReference = "my-ref-1",
        Comment = "disabled",
        Enabled = false,
        Origins = distConfig.Origins,
        DefaultCacheBehavior = distConfig.DefaultCacheBehavior,
    },
});

var disabledEtag = (await client.GetDistributionAsync(
    new GetDistributionRequest { Id = distId })).ETag;

await client.DeleteDistributionAsync(new DeleteDistributionRequest
{
    Id = distId,
    IfMatch = disabledEtag,
});
```

## Cache Invalidations

```csharp
var invResp = await client.CreateInvalidationAsync(new CreateInvalidationRequest
{
    DistributionId = distId,
    InvalidationBatch = new InvalidationBatch
    {
        CallerReference = "inv-ref-1",
        Paths = new Paths
        {
            Quantity = 2,
            Items = ["/index.html", "/static/*"],
        },
    },
});

Console.WriteLine(invResp.Invalidation.Status); // Completed

// List all invalidations for a distribution
var listResp = await client.ListInvalidationsAsync(new ListInvalidationsRequest
{
    DistributionId = distId,
});
Console.WriteLine(listResp.InvalidationList.Quantity); // 1
```

:::aside{type="note" title="ETag-based optimistic concurrency"}
Every mutating operation on a distribution (`UpdateDistribution`, `DeleteDistribution`) requires the `IfMatch` header set to the distribution's current ETag. Passing an incorrect ETag returns `PreconditionFailed`. Attempting to delete an enabled distribution returns `DistributionNotDisabled`.
:::
