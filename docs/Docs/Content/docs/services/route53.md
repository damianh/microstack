---
title: Route 53
description: Route 53 emulation — hosted zones, record sets, health checks, tags.
order: 20
section: Services
---

# Route 53

MicroStack's Route 53 handler supports hosted zones, resource record sets (A, AAAA, CNAME, MX, TXT, NS, SOA, etc.), health checks, and tagging. Each new hosted zone is seeded with SOA and NS records.

## Supported Operations

- CreateHostedZone, GetHostedZone, ListHostedZones, DeleteHostedZone
- ChangeResourceRecordSets, ListResourceRecordSets, GetChange
- CreateHealthCheck, GetHealthCheck, ListHealthChecks, UpdateHealthCheck, DeleteHealthCheck
- ChangeTagsForResource, ListTagsForResource

## Usage

```csharp
var r53 = new AmazonRoute53Client(
    new BasicAWSCredentials("test", "test"),
    new AmazonRoute53Config { ServiceURL = "http://localhost:4566" });

// Create a hosted zone
var zone = await r53.CreateHostedZoneAsync(new CreateHostedZoneRequest
{
    Name = "example.com",
    CallerReference = "unique-ref-001",
});

var zoneId = zone.HostedZone.Id.Split('/')[^1];
Console.WriteLine(zone.DelegationSet.NameServers.Count); // 4

// Verify the zone has SOA and NS records by default
var records = await r53.ListResourceRecordSetsAsync(new ListResourceRecordSetsRequest
{
    HostedZoneId = zoneId,
});

Console.WriteLine(records.ResourceRecordSets.Count); // 2 (SOA + NS)
```

## Creating Record Sets

```csharp
// Add an A record
await r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
{
    HostedZoneId = zoneId,
    ChangeBatch = new ChangeBatch
    {
        Changes =
        [
            new Change
            {
                Action = ChangeAction.CREATE,
                ResourceRecordSet = new ResourceRecordSet
                {
                    Name = "www.example.com",
                    Type = RRType.A,
                    TTL = 300,
                    ResourceRecords =
                    [
                        new ResourceRecord { Value = "1.2.3.4" },
                    ],
                },
            },
        ],
    },
});

// Add an MX record
await r53.ChangeResourceRecordSetsAsync(new ChangeResourceRecordSetsRequest
{
    HostedZoneId = zoneId,
    ChangeBatch = new ChangeBatch
    {
        Changes =
        [
            new Change
            {
                Action = ChangeAction.CREATE,
                ResourceRecordSet = new ResourceRecordSet
                {
                    Name = "example.com",
                    Type = RRType.MX,
                    TTL = 300,
                    ResourceRecords =
                    [
                        new ResourceRecord { Value = "10 mail.example.com." },
                    ],
                },
            },
        ],
    },
});
```

## Health Checks

```csharp
// Create an HTTP health check
var health = await r53.CreateHealthCheckAsync(new CreateHealthCheckRequest
{
    CallerReference = "hc-ref-001",
    HealthCheckConfig = new HealthCheckConfig
    {
        Type = HealthCheckType.HTTP,
        IPAddress = "1.2.3.4",
        Port = 80,
        ResourcePath = "/health",
    },
});

Console.WriteLine(health.HealthCheck.Id);
```
