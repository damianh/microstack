---
title: WAF v2
description: WAF v2 emulation — web ACLs, IP sets, rule groups, resource associations, and LockToken-based optimistic concurrency.
order: 23
section: Services
---

# WAF v2

MicroStack emulates AWS WAF v2 (Web Application Firewall), supporting web ACLs, IP sets, and rule groups at `REGIONAL` or `CLOUDFRONT` scope. All mutating operations (`UpdateWebACL`, `DeleteWebACL`, `UpdateIPSet`, etc.) require a valid `LockToken` returned by the preceding Create or Get response — passing an incorrect token causes the request to fail.

## Supported Operations

**Web ACLs:** CreateWebACL, GetWebACL, UpdateWebACL, DeleteWebACL, ListWebACLs, AssociateWebACL, DisassociateWebACL, GetWebACLForResource, ListResourcesForWebACL

**IP Sets:** CreateIPSet, GetIPSet, UpdateIPSet, DeleteIPSet, ListIPSets

**Rule Groups:** CreateRuleGroup, GetRuleGroup, UpdateRuleGroup, DeleteRuleGroup, ListRuleGroups

**Tags & Utilities:** TagResource, UntagResource, ListTagsForResource, CheckCapacity, DescribeManagedRuleGroup

## Usage

```csharp
var client = new AmazonWAFV2Client(
    new BasicAWSCredentials("test", "test"),
    new AmazonWAFV2Config
    {
        RegionEndpoint = RegionEndpoint.USEast1,
        ServiceURL = "http://localhost:4566",
    });

// Create a web ACL
var createResp = await client.CreateWebACLAsync(new CreateWebACLRequest
{
    Name = "my-web-acl",
    Scope = "REGIONAL",
    DefaultAction = new DefaultAction { Allow = new AllowAction() },
    VisibilityConfig = new VisibilityConfig
    {
        SampledRequestsEnabled = true,
        CloudWatchMetricsEnabled = false,
        MetricName = "my-web-acl",
    },
});

string aclId    = createResp.Summary.Id;
string lockToken = createResp.Summary.LockToken;

// Get the web ACL
var getResp = await client.GetWebACLAsync(new GetWebACLRequest
{
    Name = "my-web-acl",
    Scope = "REGIONAL",
    Id = aclId,
});
Console.WriteLine(getResp.WebACL.Name); // my-web-acl

// Update the web ACL — LockToken is required
var updateResp = await client.UpdateWebACLAsync(new UpdateWebACLRequest
{
    Name = "my-web-acl",
    Scope = "REGIONAL",
    Id = aclId,
    LockToken = lockToken,
    DefaultAction = new DefaultAction { Block = new BlockAction() },
    VisibilityConfig = new VisibilityConfig
    {
        SampledRequestsEnabled = false,
        CloudWatchMetricsEnabled = false,
        MetricName = "my-web-acl",
    },
});

string newLockToken = updateResp.NextLockToken;

// Delete the web ACL — LockToken must be current
await client.DeleteWebACLAsync(new DeleteWebACLRequest
{
    Name = "my-web-acl",
    Scope = "REGIONAL",
    Id = aclId,
    LockToken = newLockToken,
});
```

## IP Sets

```csharp
// Create an IP set
var ipSetResp = await client.CreateIPSetAsync(new CreateIPSetRequest
{
    Name = "blocked-ips",
    Scope = "REGIONAL",
    IPAddressVersion = "IPV4",
    Addresses = ["192.0.2.0/24", "198.51.100.1/32"],
});

string ipSetId    = ipSetResp.Summary.Id;
string ipLockToken = ipSetResp.Summary.LockToken;

// Update the IP set
var updateIpResp = await client.UpdateIPSetAsync(new UpdateIPSetRequest
{
    Name = "blocked-ips",
    Scope = "REGIONAL",
    Id = ipSetId,
    LockToken = ipLockToken,
    Addresses = ["10.0.0.0/8"],
});

// Associate the web ACL with a load balancer ARN
string albArn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-alb/abc123";

await client.AssociateWebACLAsync(new AssociateWebACLRequest
{
    WebACLArn = createResp.Summary.ARN,
    ResourceArn = albArn,
});

var assocResp = await client.GetWebACLForResourceAsync(new GetWebACLForResourceRequest
{
    ResourceArn = albArn,
});
Console.WriteLine(assocResp.WebACL.ARN); // ARN of the associated web ACL
```

:::aside{type="note" title="LockToken enforcement"}
Every update or delete operation requires the `LockToken` returned by the most recent Create, Get, or Update response. Tokens are rotated on every successful mutation — always store the `NextLockToken` from update responses for the next call.
:::
