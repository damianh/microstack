---
title: Service Discovery
description: Service Discovery (Cloud Map) emulation — namespaces, services, instance registration, and health-aware discovery with Route 53 integration.
order: 37
section: Services
---

# Service Discovery

MicroStack emulates AWS Cloud Map (Service Discovery) for registering and discovering services within HTTP, Private DNS, and Public DNS namespaces. Private and Public DNS namespaces automatically create a Route 53 hosted zone. All operations complete synchronously — operations resolve with status `SUCCESS` immediately.

## Supported Operations

CreateHttpNamespace, CreatePrivateDnsNamespace, CreatePublicDnsNamespace, CreateService, DeleteNamespace, DeleteService, DeregisterInstance, DiscoverInstances, DiscoverInstancesRevision, GetInstance, GetInstancesHealthStatus, GetNamespace, GetOperation, GetService, GetServiceAttributes, ListInstances, ListNamespaces, ListOperations, ListServices, ListTagsForResource, RegisterInstance, TagResource, UntagResource, UpdateHttpNamespace, UpdateInstanceCustomHealthStatus, UpdatePrivateDnsNamespace, UpdatePublicDnsNamespace, UpdateService, UpdateServiceAttributes, DeleteServiceAttributes

## Usage

```csharp
using Amazon.Runtime;
using Amazon.ServiceDiscovery;
using Amazon.ServiceDiscovery.Model;

var config = new AmazonServiceDiscoveryConfig
{
    ServiceURL = "http://localhost:4566",
};

using var sd = new AmazonServiceDiscoveryClient(
    new BasicAWSCredentials("test", "test"), config);

// Create a Private DNS namespace (also creates a Route 53 hosted zone)
var nsResp = await sd.CreatePrivateDnsNamespaceAsync(new CreatePrivateDnsNamespaceRequest
{
    Name = "myapp.local",
    Vpc = "vpc-12345",
    Description = "Internal service namespace",
});

// Get the operation to retrieve the namespace ID
var op = (await sd.GetOperationAsync(new GetOperationRequest
{
    OperationId = nsResp.OperationId,
})).Operation;
var namespaceId = op.Targets["NAMESPACE"];

// Create a service within the namespace
var svcResp = await sd.CreateServiceAsync(new CreateServiceRequest
{
    Name = "auth-service",
    NamespaceId = namespaceId,
    DnsConfig = new DnsConfig
    {
        DnsRecords = [new DnsRecord { Type = RecordType.A, TTL = 10 }],
        RoutingPolicy = RoutingPolicy.MULTIVALUE,
    },
});
var serviceId = svcResp.Service.Id;

// Register an instance
await sd.RegisterInstanceAsync(new RegisterInstanceRequest
{
    ServiceId = serviceId,
    InstanceId = "auth-instance-1",
    Attributes = new Dictionary<string, string>
    {
        ["AWS_INSTANCE_IPV4"] = "10.0.1.5",
        ["version"] = "2.3.0",
    },
});

// Discover instances
var discovered = await sd.DiscoverInstancesAsync(new DiscoverInstancesRequest
{
    NamespaceName = "myapp.local",
    ServiceName = "auth-service",
});

foreach (var inst in discovered.Instances)
{
    Console.WriteLine($"{inst.InstanceId}: {inst.Attributes["AWS_INSTANCE_IPV4"]}");
}
```

## Health-Aware Discovery

Instances can be marked healthy or unhealthy. `DiscoverInstances` accepts a `HealthStatus` filter (`HEALTHY`, `UNHEALTHY`, `ALL`).

```csharp
// Mark an instance as unhealthy
await sd.UpdateInstanceCustomHealthStatusAsync(new UpdateInstanceCustomHealthStatusRequest
{
    ServiceId = serviceId,
    InstanceId = "auth-instance-1",
    Status = CustomHealthStatus.UNHEALTHY,
});

// Discover only healthy instances (unhealthy ones are excluded by default)
var healthyOnly = await sd.DiscoverInstancesAsync(new DiscoverInstancesRequest
{
    NamespaceName = "myapp.local",
    ServiceName = "auth-service",
    HealthStatus = HealthStatusFilter.HEALTHY,
});

// Discover all instances regardless of health
var allInstances = await sd.DiscoverInstancesAsync(new DiscoverInstancesRequest
{
    NamespaceName = "myapp.local",
    ServiceName = "auth-service",
    HealthStatus = HealthStatusFilter.ALL,
});

// Check health status of all instances in a service
var healthStatus = (await sd.GetInstancesHealthStatusAsync(new GetInstancesHealthStatusRequest
{
    ServiceId = serviceId,
})).Status;

foreach (var (instanceId, status) in healthStatus)
{
    Console.WriteLine($"{instanceId}: {status}"); // e.g. auth-instance-1: UNHEALTHY
}
```

## Route 53 Integration

Creating a Private or Public DNS namespace automatically provisions a Route 53 hosted zone. The hosted zone ID is available in the namespace's DNS properties.

```csharp
using Amazon.Route53;
using Amazon.Route53.Model;

// The namespace carries the hosted zone ID
var ns = (await sd.GetNamespaceAsync(new GetNamespaceRequest { Id = namespaceId })).Namespace;
var hostedZoneId = ns.Properties.DnsProperties.HostedZoneId;

// Query the hosted zone via Route 53
var r53Config = new AmazonRoute53Config { ServiceURL = "http://localhost:4566" };
using var r53 = new AmazonRoute53Client(new BasicAWSCredentials("test", "test"), r53Config);

var hz = (await r53.GetHostedZoneAsync(new GetHostedZoneRequest { Id = hostedZoneId })).HostedZone;
Console.WriteLine(hz.Name);      // myapp.local.
Console.WriteLine(hz.Config.PrivateZone); // True
```

:::aside{type="note" title="Synchronous operations"}
All Cloud Map namespace and service mutations return an `OperationId`. In MicroStack, the operation is already in `SUCCESS` state by the time `GetOperation` is called — there is no polling delay.
:::
