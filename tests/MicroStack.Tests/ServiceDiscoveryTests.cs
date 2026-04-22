using Amazon;
using Amazon.Route53;
using Amazon.ServiceDiscovery;
using Amazon.ServiceDiscovery.Model;
using Amazon.Runtime;
using Tag = Amazon.ServiceDiscovery.Model.Tag;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the ServiceDiscovery (Cloud Map) service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_servicediscovery.py.
/// </summary>
public sealed class ServiceDiscoveryTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonServiceDiscoveryClient _sd;
    private readonly AmazonRoute53Client _r53;

    public ServiceDiscoveryTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _sd = CreateServiceDiscoveryClient(fixture);
        _r53 = CreateRoute53Client(fixture);
    }

    private static AmazonServiceDiscoveryClient CreateServiceDiscoveryClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonServiceDiscoveryConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonServiceDiscoveryClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    private static AmazonRoute53Client CreateRoute53Client(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonRoute53Config
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonRoute53Client(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _sd.Dispose();
        _r53.Dispose();
        return ValueTask.CompletedTask;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    private async Task<(string NamespaceId, string OperationId)> CreatePrivateDnsNamespace(
        string name,
        string? description = null)
    {
        var resp = await _sd.CreatePrivateDnsNamespaceAsync(new CreatePrivateDnsNamespaceRequest
        {
            Name = name,
            Vpc = "vpc-12345",
            Description = description,
        });
        var opId = resp.OperationId;
        var op = (await _sd.GetOperationAsync(new GetOperationRequest { OperationId = opId })).Operation;
        var nsId = op.Targets["NAMESPACE"];
        return (nsId, opId);
    }

    private async Task<(string NamespaceId, string OperationId)> CreateHttpNamespace(
        string name,
        List<Tag>? tags = null)
    {
        var request = new CreateHttpNamespaceRequest { Name = name };
        if (tags is not null)
        {
            request.Tags = tags;
        }

        var resp = await _sd.CreateHttpNamespaceAsync(request);
        var opId = resp.OperationId;
        var op = (await _sd.GetOperationAsync(new GetOperationRequest { OperationId = opId })).Operation;
        var nsId = op.Targets["NAMESPACE"];
        return (nsId, opId);
    }

    private async Task<string> CreateService(string name, string namespaceId)
    {
        var resp = await _sd.CreateServiceAsync(new CreateServiceRequest
        {
            Name = name,
            NamespaceId = namespaceId,
            DnsConfig = new DnsConfig
            {
                DnsRecords = [new DnsRecord { Type = RecordType.A, TTL = 10 }],
                RoutingPolicy = RoutingPolicy.MULTIVALUE,
            },
        });
        return resp.Service.Id;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Full flow: Create namespace -> service -> instance -> discover -> cleanup
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task FullServiceDiscoveryFlow()
    {
        // 1. Create Private DNS Namespace
        var nsName = "example.terraform.local";
        var (nsId, nsOpId) = await CreatePrivateDnsNamespace(nsName, "example");
        nsOpId.ShouldNotBeEmpty();

        // Verify Operation
        var op = (await _sd.GetOperationAsync(new GetOperationRequest { OperationId = nsOpId })).Operation;
        op.Status.Value.ShouldBe("SUCCESS");
        op.Targets["NAMESPACE"].ShouldBe(nsId);

        // Verify Namespace
        var ns = (await _sd.GetNamespaceAsync(new GetNamespaceRequest { Id = nsId })).Namespace;
        ns.Name.ShouldBe(nsName);

        // Verify Hosted Zone integration
        var dnsProps = ns.Properties.DnsProperties;
        dnsProps.ShouldNotBeNull();
        var hzId = dnsProps.HostedZoneId;
        hzId.ShouldNotBeEmpty();

        var hz = (await _r53.GetHostedZoneAsync(new Amazon.Route53.Model.GetHostedZoneRequest { Id = hzId })).HostedZone;
        hz.Name.ShouldBe(nsName + ".");
        hz.Config.PrivateZone.ShouldBe(true);

        // 2. Create Service
        var svcName = "example-service";
        var svcId = await CreateService(svcName, nsId);
        svcId.ShouldNotBeEmpty();

        // 3. Register Instance
        var instId = "example-instance-id";
        var regResp = await _sd.RegisterInstanceAsync(new RegisterInstanceRequest
        {
            ServiceId = svcId,
            InstanceId = instId,
            Attributes = new Dictionary<string, string>
            {
                ["AWS_INSTANCE_IPV4"] = "172.18.0.1",
                ["custom_attribute"] = "custom",
            },
        });
        regResp.OperationId.ShouldNotBeEmpty();

        // 4. Discover Instances
        var discoverResp = await _sd.DiscoverInstancesAsync(new DiscoverInstancesRequest
        {
            NamespaceName = nsName,
            ServiceName = svcName,
        });
        discoverResp.Instances.ShouldHaveSingleItem();
        discoverResp.Instances[0].InstanceId.ShouldBe(instId);
        discoverResp.Instances[0].Attributes["AWS_INSTANCE_IPV4"].ShouldBe("172.18.0.1");

        // 5. List operations
        var namespaces = (await _sd.ListNamespacesAsync(new ListNamespacesRequest())).Namespaces;
        namespaces.ShouldContain(n => n.Id == nsId);

        var services = (await _sd.ListServicesAsync(new ListServicesRequest())).Services;
        services.ShouldContain(s => s.Id == svcId);

        var instances = (await _sd.ListInstancesAsync(new ListInstancesRequest { ServiceId = svcId })).Instances;
        instances.ShouldContain(i => i.Id == instId);

        // 6. Deregister & Delete
        await _sd.DeregisterInstanceAsync(new DeregisterInstanceRequest
        {
            ServiceId = svcId,
            InstanceId = instId,
        });
        instances = (await _sd.ListInstancesAsync(new ListInstancesRequest { ServiceId = svcId })).Instances;
        instances.ShouldBeEmpty();

        await _sd.DeleteServiceAsync(new DeleteServiceRequest { Id = svcId });
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tagging
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TagOperations()
    {
        // 1. Create Namespace with tags
        var (nsId, _) = await CreateHttpNamespace("tag-test-ns",
            [new Tag { Key = "Owner", Value = "TeamA" }]);

        var ns = (await _sd.GetNamespaceAsync(new GetNamespaceRequest { Id = nsId })).Namespace;
        var nsArn = ns.Arn;

        // 2. List tags
        var tags = (await _sd.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = nsArn,
        })).Tags;
        tags.ShouldContain(t => t.Key == "Owner" && t.Value == "TeamA");

        // 3. Add more tags
        await _sd.TagResourceAsync(new TagResourceRequest
        {
            ResourceARN = nsArn,
            Tags = [new Tag { Key = "Env", Value = "Dev" }],
        });
        tags = (await _sd.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = nsArn,
        })).Tags;
        tags.Count.ShouldBe(2);

        // 4. Untag
        await _sd.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceARN = nsArn,
            TagKeys = ["Owner"],
        });
        tags = (await _sd.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = nsArn,
        })).Tags;
        tags.ShouldHaveSingleItem();
        tags[0].Key.ShouldBe("Env");

        // Cleanup
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Additional operations: attributes, updates, health, revision
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ServiceAttributesCrud()
    {
        var (nsId, _) = await CreatePrivateDnsNamespace("attrs-test.local");
        var svcId = await CreateService("attrs-service", nsId);

        // Update service attributes
        await _sd.UpdateServiceAttributesAsync(new UpdateServiceAttributesRequest
        {
            ServiceId = svcId,
            Attributes = new Dictionary<string, string> { ["team"] = "core", ["env"] = "test" },
        });

        var attrs = (await _sd.GetServiceAttributesAsync(new GetServiceAttributesRequest
        {
            ServiceId = svcId,
        })).ServiceAttributes.Attributes;
        attrs["team"].ShouldBe("core");
        attrs["env"].ShouldBe("test");

        // Delete one attribute
        await _sd.DeleteServiceAttributesAsync(new DeleteServiceAttributesRequest
        {
            ServiceId = svcId,
            Attributes = ["env"],
        });

        attrs = (await _sd.GetServiceAttributesAsync(new GetServiceAttributesRequest
        {
            ServiceId = svcId,
        })).ServiceAttributes.Attributes;
        attrs.ShouldNotContainKey("env");
        attrs["team"].ShouldBe("core");

        // Cleanup
        await _sd.DeleteServiceAsync(new DeleteServiceRequest { Id = svcId });
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    [Fact]
    public async Task NamespaceAndServiceUpdate()
    {
        var (nsId, _) = await CreatePrivateDnsNamespace("update-test.local");
        var svcId = await CreateService("update-service", nsId);

        // Update namespace
        var nsUpdateResp = await _sd.UpdatePrivateDnsNamespaceAsync(new UpdatePrivateDnsNamespaceRequest
        {
            Id = nsId,
            Namespace = new PrivateDnsNamespaceChange { Description = "updated namespace" },
        });
        var nsUpdateOpId = nsUpdateResp.OperationId;
        var nsUpdateOp = (await _sd.GetOperationAsync(new GetOperationRequest
        {
            OperationId = nsUpdateOpId,
        })).Operation;
        nsUpdateOp.Targets["NAMESPACE"].ShouldBe(nsId);

        // Update service
        var svcUpdateResp = await _sd.UpdateServiceAsync(new UpdateServiceRequest
        {
            Id = svcId,
            Service = new ServiceChange { Description = "updated service" },
        });
        var svcUpdateOpId = svcUpdateResp.OperationId;
        var svcUpdateOp = (await _sd.GetOperationAsync(new GetOperationRequest
        {
            OperationId = svcUpdateOpId,
        })).Operation;
        svcUpdateOp.Targets["SERVICE"].ShouldBe(svcId);

        // List operations should contain both
        var ops = (await _sd.ListOperationsAsync(new ListOperationsRequest
        {
            MaxResults = 50,
        })).Operations;
        ops.ShouldContain(o => o.Id == nsUpdateOpId);
        ops.ShouldContain(o => o.Id == svcUpdateOpId);

        // Cleanup
        await _sd.DeleteServiceAsync(new DeleteServiceRequest { Id = svcId });
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    [Fact]
    public async Task InstanceHealthAndRevision()
    {
        var nsName = "health-test.local";
        var (nsId, _) = await CreatePrivateDnsNamespace(nsName);
        var svcId = await CreateService("health-service", nsId);

        // Register instance
        await _sd.RegisterInstanceAsync(new RegisterInstanceRequest
        {
            ServiceId = svcId,
            InstanceId = "inst-1",
            Attributes = new Dictionary<string, string> { ["AWS_INSTANCE_IPV4"] = "10.0.0.1" },
        });

        // Get revision before health update
        var revBefore = (await _sd.DiscoverInstancesRevisionAsync(new DiscoverInstancesRevisionRequest
        {
            NamespaceName = nsName,
            ServiceName = "health-service",
        })).InstancesRevision;

        // Update health status
        await _sd.UpdateInstanceCustomHealthStatusAsync(new UpdateInstanceCustomHealthStatusRequest
        {
            ServiceId = svcId,
            InstanceId = "inst-1",
            Status = CustomHealthStatus.UNHEALTHY,
        });

        // Verify health status
        var health = (await _sd.GetInstancesHealthStatusAsync(new GetInstancesHealthStatusRequest
        {
            ServiceId = svcId,
        })).Status;
        health["inst-1"].ShouldBe("UNHEALTHY");

        // Discover with HealthStatus=ALL should include unhealthy
        var discovered = (await _sd.DiscoverInstancesAsync(new DiscoverInstancesRequest
        {
            NamespaceName = nsName,
            ServiceName = "health-service",
            HealthStatus = HealthStatusFilter.ALL,
        })).Instances;
        discovered.ShouldHaveSingleItem();
        discovered[0].HealthStatus.Value.ShouldBe("UNHEALTHY");

        // Revision should have increased
        var revAfter = (await _sd.DiscoverInstancesRevisionAsync(new DiscoverInstancesRevisionRequest
        {
            NamespaceName = nsName,
            ServiceName = "health-service",
        })).InstancesRevision;
        (revAfter > revBefore).ShouldBe(true);

        // Cleanup
        await _sd.DeregisterInstanceAsync(new DeregisterInstanceRequest
        {
            ServiceId = svcId,
            InstanceId = "inst-1",
        });
        await _sd.DeleteServiceAsync(new DeleteServiceRequest { Id = svcId });
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // HTTP Namespace
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateHttpNamespaceWithProperties()
    {
        var nsName = "http-ns-test";
        var (nsId, _) = await CreateHttpNamespace(nsName);

        var ns = (await _sd.GetNamespaceAsync(new GetNamespaceRequest { Id = nsId })).Namespace;
        ns.Name.ShouldBe(nsName);
        ns.Type.ShouldBe(NamespaceType.HTTP);

        // Cleanup
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Public DNS Namespace
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreatePublicDnsNamespace()
    {
        var nsName = "public-dns-test.com";
        var resp = await _sd.CreatePublicDnsNamespaceAsync(new CreatePublicDnsNamespaceRequest
        {
            Name = nsName,
        });
        var opId = resp.OperationId;
        opId.ShouldNotBeEmpty();

        var op = (await _sd.GetOperationAsync(new GetOperationRequest { OperationId = opId })).Operation;
        var nsId = op.Targets["NAMESPACE"];

        var ns = (await _sd.GetNamespaceAsync(new GetNamespaceRequest { Id = nsId })).Namespace;
        ns.Name.ShouldBe(nsName);
        ns.Type.ShouldBe(NamespaceType.DNS_PUBLIC);

        // Verify hosted zone was created
        var dnsProps = ns.Properties.DnsProperties;
        dnsProps.ShouldNotBeNull();
        dnsProps.HostedZoneId.ShouldNotBeEmpty();

        // Cleanup
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // GetInstance
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GetInstance()
    {
        var (nsId, _) = await CreateHttpNamespace("getinst-ns");
        var svcId = await CreateService("getinst-svc", nsId);

        await _sd.RegisterInstanceAsync(new RegisterInstanceRequest
        {
            ServiceId = svcId,
            InstanceId = "inst-get-1",
            Attributes = new Dictionary<string, string> { ["key"] = "value" },
        });

        var inst = (await _sd.GetInstanceAsync(new GetInstanceRequest
        {
            ServiceId = svcId,
            InstanceId = "inst-get-1",
        })).Instance;
        inst.Id.ShouldBe("inst-get-1");
        inst.Attributes["key"].ShouldBe("value");

        // Cleanup
        await _sd.DeregisterInstanceAsync(new DeregisterInstanceRequest
        {
            ServiceId = svcId,
            InstanceId = "inst-get-1",
        });
        await _sd.DeleteServiceAsync(new DeleteServiceRequest { Id = svcId });
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // GetService
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GetService()
    {
        var (nsId, _) = await CreateHttpNamespace("getsvc-ns");
        var svcId = await CreateService("getsvc-svc", nsId);

        var svc = (await _sd.GetServiceAsync(new GetServiceRequest { Id = svcId })).Service;
        svc.Id.ShouldBe(svcId);
        svc.Name.ShouldBe("getsvc-svc");

        // Cleanup
        await _sd.DeleteServiceAsync(new DeleteServiceRequest { Id = svcId });
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DiscoverInstances with HealthStatus filter
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DiscoverInstancesHealthFilter()
    {
        var nsName = "discover-filter.local";
        var (nsId, _) = await CreatePrivateDnsNamespace(nsName);
        var svcId = await CreateService("filter-svc", nsId);

        // Register two instances
        await _sd.RegisterInstanceAsync(new RegisterInstanceRequest
        {
            ServiceId = svcId,
            InstanceId = "healthy-inst",
            Attributes = new Dictionary<string, string> { ["AWS_INSTANCE_IPV4"] = "10.0.0.1" },
        });
        await _sd.RegisterInstanceAsync(new RegisterInstanceRequest
        {
            ServiceId = svcId,
            InstanceId = "unhealthy-inst",
            Attributes = new Dictionary<string, string> { ["AWS_INSTANCE_IPV4"] = "10.0.0.2" },
        });

        // Mark one unhealthy
        await _sd.UpdateInstanceCustomHealthStatusAsync(new UpdateInstanceCustomHealthStatusRequest
        {
            ServiceId = svcId,
            InstanceId = "unhealthy-inst",
            Status = CustomHealthStatus.UNHEALTHY,
        });

        // Discover only HEALTHY instances
        var healthyInstances = (await _sd.DiscoverInstancesAsync(new DiscoverInstancesRequest
        {
            NamespaceName = nsName,
            ServiceName = "filter-svc",
            HealthStatus = HealthStatusFilter.HEALTHY,
        })).Instances;
        healthyInstances.ShouldHaveSingleItem();
        healthyInstances[0].InstanceId.ShouldBe("healthy-inst");

        // Discover only UNHEALTHY instances
        var unhealthyInstances = (await _sd.DiscoverInstancesAsync(new DiscoverInstancesRequest
        {
            NamespaceName = nsName,
            ServiceName = "filter-svc",
            HealthStatus = HealthStatusFilter.UNHEALTHY,
        })).Instances;
        unhealthyInstances.ShouldHaveSingleItem();
        unhealthyInstances[0].InstanceId.ShouldBe("unhealthy-inst");

        // Discover ALL instances
        var allInstances = (await _sd.DiscoverInstancesAsync(new DiscoverInstancesRequest
        {
            NamespaceName = nsName,
            ServiceName = "filter-svc",
            HealthStatus = HealthStatusFilter.ALL,
        })).Instances;
        allInstances.Count.ShouldBe(2);

        // Cleanup
        await _sd.DeregisterInstanceAsync(new DeregisterInstanceRequest
        {
            ServiceId = svcId,
            InstanceId = "healthy-inst",
        });
        await _sd.DeregisterInstanceAsync(new DeregisterInstanceRequest
        {
            ServiceId = svcId,
            InstanceId = "unhealthy-inst",
        });
        await _sd.DeleteServiceAsync(new DeleteServiceRequest { Id = svcId });
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Service with tags on create
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateServiceWithTags()
    {
        var (nsId, _) = await CreateHttpNamespace("svctag-ns");

        var resp = await _sd.CreateServiceAsync(new CreateServiceRequest
        {
            Name = "tagged-svc",
            NamespaceId = nsId,
            Tags = [new Tag { Key = "Team", Value = "Platform" }],
        });
        var svcArn = resp.Service.Arn;

        var tags = (await _sd.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceARN = svcArn,
        })).Tags;
        tags.ShouldContain(t => t.Key == "Team" && t.Value == "Platform");

        // Cleanup
        await _sd.DeleteServiceAsync(new DeleteServiceRequest { Id = resp.Service.Id });
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // UpdateHttpNamespace
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task UpdateHttpNamespace()
    {
        var (nsId, _) = await CreateHttpNamespace("update-http-ns");

        var resp = await _sd.UpdateHttpNamespaceAsync(new UpdateHttpNamespaceRequest
        {
            Id = nsId,
            Namespace = new HttpNamespaceChange { Description = "updated description" },
        });
        resp.OperationId.ShouldNotBeEmpty();

        var ns = (await _sd.GetNamespaceAsync(new GetNamespaceRequest { Id = nsId })).Namespace;
        ns.Description.ShouldBe("updated description");

        // Cleanup
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // UpdatePublicDnsNamespace
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task UpdatePublicDnsNamespace()
    {
        var resp = await _sd.CreatePublicDnsNamespaceAsync(new CreatePublicDnsNamespaceRequest
        {
            Name = "update-public.com",
        });
        var op = (await _sd.GetOperationAsync(new GetOperationRequest
        {
            OperationId = resp.OperationId,
        })).Operation;
        var nsId = op.Targets["NAMESPACE"];

        var updateResp = await _sd.UpdatePublicDnsNamespaceAsync(new UpdatePublicDnsNamespaceRequest
        {
            Id = nsId,
            Namespace = new PublicDnsNamespaceChange { Description = "updated public" },
        });
        updateResp.OperationId.ShouldNotBeEmpty();

        // Cleanup
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Delete namespace not found
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DeleteNamespaceNotFound()
    {
        var ex = await Should.ThrowAsync<NamespaceNotFoundException>(() =>
            _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = "ns-nonexistent" }));
        ex.Message.ShouldContain("not found", Case.Insensitive);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Delete service not found
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DeleteServiceNotFound()
    {
        var ex = await Should.ThrowAsync<ServiceNotFoundException>(() =>
            _sd.DeleteServiceAsync(new DeleteServiceRequest { Id = "srv-nonexistent" }));
        ex.Message.ShouldContain("not found", Case.Insensitive);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // ListOperations with filter
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ListOperationsWithFilter()
    {
        var (nsId, nsOpId) = await CreateHttpNamespace("filter-ops-ns");
        var svcId = await CreateService("filter-ops-svc", nsId);

        var ops = (await _sd.ListOperationsAsync(new ListOperationsRequest
        {
            MaxResults = 100,
            Filters =
            [
                new OperationFilter
                {
                    Name = OperationFilterName.STATUS,
                    Values = ["SUCCESS"],
                },
            ],
        })).Operations;
        (ops.Count >= 1).ShouldBe(true);

        // Cleanup
        await _sd.DeleteServiceAsync(new DeleteServiceRequest { Id = svcId });
        await _sd.DeleteNamespaceAsync(new DeleteNamespaceRequest { Id = nsId });
    }
}
