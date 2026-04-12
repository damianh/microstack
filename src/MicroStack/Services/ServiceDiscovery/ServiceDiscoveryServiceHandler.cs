using System.Text;
using System.Text.Json;
using System.Xml.Linq;
using MicroStack.Internal;
using MicroStack.Services.Route53;

namespace MicroStack.Services.ServiceDiscovery;

/// <summary>
/// AWS Cloud Map (Service Discovery) service handler -- JSON protocol via X-Amz-Target.
///
/// Port of ministack/services/servicediscovery.py.
///
/// Supports: CreateHttpNamespace, CreatePrivateDnsNamespace, CreatePublicDnsNamespace,
///           CreateService, DeleteNamespace, DeleteService, DeleteServiceAttributes,
///           DeregisterInstance, DiscoverInstances, DiscoverInstancesRevision,
///           GetInstance, GetInstancesHealthStatus, GetNamespace, GetOperation,
///           GetService, GetServiceAttributes, ListInstances, ListNamespaces,
///           ListOperations, ListServices, ListTagsForResource, RegisterInstance,
///           TagResource, UntagResource, UpdateHttpNamespace, UpdateInstanceCustomHealthStatus,
///           UpdatePrivateDnsNamespace, UpdatePublicDnsNamespace, UpdateService,
///           UpdateServiceAttributes.
/// </summary>
internal sealed class ServiceDiscoveryServiceHandler : IServiceHandler
{
    private static string Region => MicroStackOptions.Instance.Region;

    private readonly Route53ServiceHandler _route53;
    private readonly Lock _lock = new();

    // In-memory state
    private readonly AccountScopedDictionary<string, SdNamespace> _namespaces = new();
    private readonly AccountScopedDictionary<string, SdService> _services = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, SdInstance>> _instances = new();
    private readonly AccountScopedDictionary<string, SdOperation> _operations = new();
    private readonly AccountScopedDictionary<string, List<SdTag>> _resourceTags = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _serviceAttributes = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _instanceHealthStatus = new();
    private readonly AccountScopedDictionary<string, int> _instancesRevision = new();

    internal ServiceDiscoveryServiceHandler(Route53ServiceHandler route53)
    {
        _route53 = route53;
    }

    // ── IServiceHandler ──────────────────────────────────────────────────────────

    public string ServiceName => "servicediscovery";

    public async Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var target = request.GetHeader("x-amz-target") ?? "";
        var action = target.Contains('.', StringComparison.Ordinal)
            ? target[(target.LastIndexOf('.') + 1)..]
            : "";

        JsonElement data;
        if (request.Body.Length > 0)
        {
            try
            {
                using var doc = JsonDocument.Parse(request.Body);
                data = doc.RootElement.Clone();
            }
            catch (JsonException)
            {
                return AwsResponseHelpers.ErrorResponseJson("SerializationException", "Invalid JSON", 400);
            }
        }
        else
        {
            data = JsonDocument.Parse("{}"u8.ToArray()).RootElement.Clone();
        }

        return action switch
        {
            "CreateHttpNamespace" => await CreateNamespace(data, action),
            "CreatePrivateDnsNamespace" => await CreateNamespace(data, action),
            "CreatePublicDnsNamespace" => await CreateNamespace(data, action),
            "CreateService" => CreateService(data),
            "DeleteNamespace" => DeleteNamespace(data),
            "DeleteService" => DeleteService(data),
            "DeleteServiceAttributes" => DeleteServiceAttributes(data),
            "DeregisterInstance" => DeregisterInstance(data),
            "DiscoverInstances" => DiscoverInstances(data),
            "DiscoverInstancesRevision" => DiscoverInstancesRevision(data),
            "GetInstance" => GetInstance(data),
            "GetInstancesHealthStatus" => GetInstancesHealthStatus(data),
            "GetNamespace" => GetNamespace(data),
            "GetOperation" => GetOperation(data),
            "GetService" => GetService(data),
            "GetServiceAttributes" => GetServiceAttributes(data),
            "ListInstances" => ListInstances(data),
            "ListNamespaces" => ListNamespaces(),
            "ListOperations" => ListOperations(data),
            "ListServices" => ListServices(),
            "ListTagsForResource" => ListTagsForResource(data),
            "RegisterInstance" => RegisterInstance(data),
            "TagResource" => TagResource(data),
            "UntagResource" => UntagResource(data),
            "UpdateHttpNamespace" => UpdateNamespace(data),
            "UpdateInstanceCustomHealthStatus" => UpdateInstanceCustomHealthStatus(data),
            "UpdatePrivateDnsNamespace" => UpdateNamespace(data),
            "UpdatePublicDnsNamespace" => UpdateNamespace(data),
            "UpdateService" => UpdateService(data),
            "UpdateServiceAttributes" => UpdateServiceAttributes(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown action: {action}", 400),
        };
    }

    public void Reset()
    {
        lock (_lock)
        {
            _namespaces.Clear();
            _services.Clear();
            _instances.Clear();
            _operations.Clear();
            _resourceTags.Clear();
            _serviceAttributes.Clear();
            _instanceHealthStatus.Clear();
            _instancesRevision.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state)
    {
        // Not implementing restore in Phase 1.
    }

    // ── JSON helpers ─────────────────────────────────────────────────────────────

    private static string? GetString(JsonElement el, string propertyName)
    {
        return el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;
    }

    private static int GetInt(JsonElement el, string propertyName, int defaultValue)
    {
        if (!el.TryGetProperty(propertyName, out var prop))
        {
            return defaultValue;
        }

        return prop.TryGetInt32(out var val) ? val : defaultValue;
    }

    private static JsonElement? GetObject(JsonElement el, string propertyName)
    {
        return el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.Object
            ? prop
            : null;
    }

    private static List<SdTag>? GetTags(JsonElement el)
    {
        if (!el.TryGetProperty("Tags", out var tagsEl) || tagsEl.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        var tags = new List<SdTag>();
        foreach (var tagEl in tagsEl.EnumerateArray())
        {
            var key = GetString(tagEl, "Key");
            var value = GetString(tagEl, "Value") ?? "";
            if (key is not null)
            {
                tags.Add(new SdTag { Key = key, Value = value });
            }
        }

        return tags.Count > 0 ? tags : null;
    }

    // ── ARN helpers ──────────────────────────────────────────────────────────────

    private static string NamespaceArn(string nsId)
    {
        return $"arn:aws:servicediscovery:{Region}:{AccountContext.GetAccountId()}:namespace/{nsId}";
    }

    private static string ServiceArn(string svcId)
    {
        return $"arn:aws:servicediscovery:{Region}:{AccountContext.GetAccountId()}:service/{svcId}";
    }

    // ── Operation helpers ────────────────────────────────────────────────────────

    private string CreateOperation(string opType, Dictionary<string, object?> targets)
    {
        var opId = HashHelpers.NewUuid();
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;
        _operations[opId] = new SdOperation
        {
            Id = opId,
            Status = "SUCCESS",
            Type = opType,
            Targets = targets,
            CreateDate = now,
            UpdateDate = now,
        };
        return opId;
    }

    private void TouchInstancesRevision(string serviceId)
    {
        if (_instancesRevision.TryGetValue(serviceId, out var rev))
        {
            _instancesRevision[serviceId] = rev + 1;
        }
        else
        {
            _instancesRevision[serviceId] = 2;
        }
    }

    // ── Namespace operations ─────────────────────────────────────────────────────

    private async Task<ServiceResponse> CreateNamespace(JsonElement data, string action)
    {
        var nsName = GetString(data, "Name");
        if (string.IsNullOrEmpty(nsName))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "Name is required", 400);
        }

        var nsId = $"ns-{HashHelpers.NewUuid()[..8]}";

        var isPrivate = action == "CreatePrivateDnsNamespace";
        var isHttp = action == "CreateHttpNamespace";

        string nsType;
        if (isPrivate)
        {
            nsType = "DNS_PRIVATE";
        }
        else if (isHttp)
        {
            nsType = "HTTP";
        }
        else
        {
            nsType = "DNS_PUBLIC";
        }

        Dictionary<string, object?>? properties;

        if (nsType != "HTTP")
        {
            var zoneName = nsName.EndsWith('.') ? nsName : nsName + ".";
            var callerReference = HashHelpers.NewUuid();
            var xmlBody = $"""
                <CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
                    <Name>{zoneName}</Name>
                    <CallerReference>{callerReference}</CallerReference>
                    <HostedZoneConfig>
                        <Comment>Created by Cloud Map</Comment>
                        <PrivateZone>{(isPrivate ? "true" : "false")}</PrivateZone>
                    </HostedZoneConfig>
                </CreateHostedZoneRequest>
                """;

            var r53Request = new ServiceRequest(
                "POST",
                "/2013-04-01/hostedzone",
                new Dictionary<string, string>(),
                Encoding.UTF8.GetBytes(xmlBody),
                new Dictionary<string, string[]>());

            var r53Response = await _route53.HandleAsync(r53Request);

            if (r53Response.StatusCode >= 300)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InternalFailure", "Failed to create Route53 hosted zone", 500);
            }

            var responseXml = Encoding.UTF8.GetString(r53Response.Body);
            var doc = XDocument.Parse(responseXml);
            XNamespace r53Ns = "https://route53.amazonaws.com/doc/2013-04-01/";
            var zoneIdEl = doc.Descendants(r53Ns + "Id").FirstOrDefault();
            if (zoneIdEl is null || string.IsNullOrEmpty(zoneIdEl.Value))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InternalFailure", "Hosted zone ID missing in Route53 response", 500);
            }

            var zoneId = zoneIdEl.Value.Split('/')[^1];
            properties = new Dictionary<string, object?>
            {
                ["DnsProperties"] = new Dictionary<string, object?>
                {
                    ["HostedZoneId"] = zoneId,
                },
            };
        }
        else
        {
            properties = new Dictionary<string, object?>
            {
                ["HttpProperties"] = new Dictionary<string, object?>
                {
                    ["HttpName"] = nsName,
                },
            };
        }

        var ns = new SdNamespace
        {
            Id = nsId,
            Arn = NamespaceArn(nsId),
            Name = nsName,
            Type = nsType,
            Description = GetString(data, "Description"),
            CreateDate = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0,
            Properties = properties,
        };

        lock (_lock)
        {
            _namespaces[nsId] = ns;
            var tags = GetTags(data);
            if (tags is not null)
            {
                _resourceTags[ns.Arn] = tags;
            }

            var opId = CreateOperation("CREATE_NAMESPACE", new Dictionary<string, object?> { ["NAMESPACE"] = nsId });
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["OperationId"] = opId });
        }
    }

    private ServiceResponse DeleteNamespace(JsonElement data)
    {
        var nsId = GetString(data, "Id");
        if (string.IsNullOrEmpty(nsId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "Id is required", 400);
        }

        lock (_lock)
        {
            if (!_namespaces.TryGetValue(nsId, out var ns))
            {
                return AwsResponseHelpers.ErrorResponseJson("NamespaceNotFound", "Namespace not found", 404);
            }

            _namespaces.TryRemove(nsId, out _);
            _resourceTags.TryRemove(ns.Arn, out _);

            var opId = CreateOperation("DELETE_NAMESPACE", new Dictionary<string, object?> { ["NAMESPACE"] = nsId });
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["OperationId"] = opId });
        }
    }

    private ServiceResponse GetNamespace(JsonElement data)
    {
        var nsId = GetString(data, "Id");
        if (string.IsNullOrEmpty(nsId) || !_namespaces.TryGetValue(nsId, out var ns))
        {
            return AwsResponseHelpers.ErrorResponseJson("NamespaceNotFound", "Namespace not found", 404);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Namespace"] = ns.ToDict() });
    }

    private ServiceResponse ListNamespaces()
    {
        List<Dictionary<string, object?>> list;
        lock (_lock)
        {
            list = _namespaces.Values.Select(ns => ns.ToDict()).ToList();
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Namespaces"] = list });
    }

    private ServiceResponse UpdateNamespace(JsonElement data)
    {
        var nsId = GetString(data, "Id");
        if (string.IsNullOrEmpty(nsId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "Id is required", 400);
        }

        lock (_lock)
        {
            if (!_namespaces.TryGetValue(nsId, out var ns))
            {
                return AwsResponseHelpers.ErrorResponseJson("NamespaceNotFound", "Namespace not found", 404);
            }

            var nsUpdate = GetObject(data, "Namespace");
            if (nsUpdate.HasValue)
            {
                var desc = GetString(nsUpdate.Value, "Description");
                if (desc is not null)
                {
                    ns.Description = desc;
                }
            }

            var opId = CreateOperation("UPDATE_NAMESPACE", new Dictionary<string, object?> { ["NAMESPACE"] = nsId });
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["OperationId"] = opId });
        }
    }

    // ── Service operations ───────────────────────────────────────────────────────

    private ServiceResponse CreateService(JsonElement data)
    {
        var nsId = GetString(data, "NamespaceId");
        if (string.IsNullOrEmpty(nsId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "NamespaceId is required", 400);
        }

        var name = GetString(data, "Name");
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "Name is required", 400);
        }

        lock (_lock)
        {
            if (!_namespaces.ContainsKey(nsId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NamespaceNotFound", "Namespace not found", 404);
            }

            var svcId = $"srv-{HashHelpers.NewUuid()[..8]}";
            var svc = new SdService
            {
                Id = svcId,
                Arn = ServiceArn(svcId),
                Name = name,
                NamespaceId = nsId,
                Description = GetString(data, "Description"),
                CreateDate = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0,
            };

            // Preserve DnsConfig if present
            var dnsConfig = GetObject(data, "DnsConfig");
            if (dnsConfig.HasValue)
            {
                svc.DnsConfig = JsonElementToDict(dnsConfig.Value);
            }

            var healthCheckConfig = GetObject(data, "HealthCheckConfig");
            if (healthCheckConfig.HasValue)
            {
                svc.HealthCheckConfig = JsonElementToDict(healthCheckConfig.Value);
            }

            var healthCheckCustomConfig = GetObject(data, "HealthCheckCustomConfig");
            if (healthCheckCustomConfig.HasValue)
            {
                svc.HealthCheckCustomConfig = JsonElementToDict(healthCheckCustomConfig.Value);
            }

            _services[svcId] = svc;
            _serviceAttributes[svcId] = new Dictionary<string, string>(StringComparer.Ordinal);
            _instanceHealthStatus[svcId] = new Dictionary<string, string>(StringComparer.Ordinal);
            _instancesRevision[svcId] = 1;

            var tags = GetTags(data);
            if (tags is not null)
            {
                _resourceTags[svc.Arn] = tags;
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Service"] = svc.ToDict() });
        }
    }

    private ServiceResponse DeleteService(JsonElement data)
    {
        var svcId = GetString(data, "Id");
        if (string.IsNullOrEmpty(svcId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "Id is required", 400);
        }

        lock (_lock)
        {
            if (!_services.TryGetValue(svcId, out var svc))
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            _services.TryRemove(svcId, out _);
            _instances.TryRemove(svcId, out _);
            _serviceAttributes.TryRemove(svcId, out _);
            _instanceHealthStatus.TryRemove(svcId, out _);
            _instancesRevision.TryRemove(svcId, out _);
            _resourceTags.TryRemove(svc.Arn, out _);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse DeleteServiceAttributes(JsonElement data)
    {
        var svcId = GetString(data, "ServiceId");
        if (string.IsNullOrEmpty(svcId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "ServiceId is required", 400);
        }

        lock (_lock)
        {
            if (!_services.ContainsKey(svcId))
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            if (!_serviceAttributes.TryGetValue(svcId, out var current))
            {
                current = new Dictionary<string, string>(StringComparer.Ordinal);
                _serviceAttributes[svcId] = current;
            }

            if (data.TryGetProperty("Attributes", out var attrsEl) && attrsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var attrEl in attrsEl.EnumerateArray())
                {
                    var key = attrEl.GetString();
                    if (key is not null)
                    {
                        current.Remove(key);
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse GetService(JsonElement data)
    {
        var svcId = GetString(data, "Id");
        if (string.IsNullOrEmpty(svcId) || !_services.TryGetValue(svcId, out var svc))
        {
            return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Service"] = svc.ToDict() });
    }

    private ServiceResponse ListServices()
    {
        List<Dictionary<string, object?>> list;
        lock (_lock)
        {
            list = _services.Values.Select(s => s.ToDict()).ToList();
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Services"] = list });
    }

    private ServiceResponse UpdateService(JsonElement data)
    {
        var svcId = GetString(data, "Id");
        if (string.IsNullOrEmpty(svcId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "Id is required", 400);
        }

        lock (_lock)
        {
            if (!_services.TryGetValue(svcId, out var svc))
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            var update = GetObject(data, "Service");
            if (update.HasValue)
            {
                var desc = GetString(update.Value, "Description");
                if (desc is not null)
                {
                    svc.Description = desc;
                }

                var dnsConfig = GetObject(update.Value, "DnsConfig");
                if (dnsConfig.HasValue)
                {
                    svc.DnsConfig = JsonElementToDict(dnsConfig.Value);
                }

                var healthCheckConfig = GetObject(update.Value, "HealthCheckConfig");
                if (healthCheckConfig.HasValue)
                {
                    svc.HealthCheckConfig = JsonElementToDict(healthCheckConfig.Value);
                }

                var healthCheckCustomConfig = GetObject(update.Value, "HealthCheckCustomConfig");
                if (healthCheckCustomConfig.HasValue)
                {
                    svc.HealthCheckCustomConfig = JsonElementToDict(healthCheckCustomConfig.Value);
                }
            }

            var opId = CreateOperation("UPDATE_SERVICE", new Dictionary<string, object?> { ["SERVICE"] = svcId });
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["OperationId"] = opId });
        }
    }

    // ── Instance operations ──────────────────────────────────────────────────────

    private ServiceResponse RegisterInstance(JsonElement data)
    {
        var svcId = GetString(data, "ServiceId");
        if (string.IsNullOrEmpty(svcId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "ServiceId is required", 400);
        }

        var instId = GetString(data, "InstanceId");
        if (string.IsNullOrEmpty(instId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "InstanceId is required", 400);
        }

        lock (_lock)
        {
            if (!_services.ContainsKey(svcId))
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            if (!_instances.TryGetValue(svcId, out var svcInstances))
            {
                svcInstances = new Dictionary<string, SdInstance>(StringComparer.Ordinal);
                _instances[svcId] = svcInstances;
            }

            var attributes = new Dictionary<string, string>(StringComparer.Ordinal);
            if (data.TryGetProperty("Attributes", out var attrsEl) && attrsEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in attrsEl.EnumerateObject())
                {
                    attributes[prop.Name] = prop.Value.GetString() ?? "";
                }
            }

            svcInstances[instId] = new SdInstance
            {
                Id = instId,
                Attributes = attributes,
            };

            if (!_instanceHealthStatus.TryGetValue(svcId, out var healthMap))
            {
                healthMap = new Dictionary<string, string>(StringComparer.Ordinal);
                _instanceHealthStatus[svcId] = healthMap;
            }

            healthMap[instId] = "HEALTHY";
            TouchInstancesRevision(svcId);

            var opId = CreateOperation("REGISTER_INSTANCE",
                new Dictionary<string, object?> { ["INSTANCE"] = instId, ["SERVICE"] = svcId });
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["OperationId"] = opId });
        }
    }

    private ServiceResponse DeregisterInstance(JsonElement data)
    {
        var svcId = GetString(data, "ServiceId");
        if (string.IsNullOrEmpty(svcId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "ServiceId is required", 400);
        }

        var instId = GetString(data, "InstanceId");
        if (string.IsNullOrEmpty(instId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "InstanceId is required", 400);
        }

        lock (_lock)
        {
            if (!_services.ContainsKey(svcId))
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            if (_instances.TryGetValue(svcId, out var svcInstances))
            {
                svcInstances.Remove(instId);
            }

            if (_instanceHealthStatus.TryGetValue(svcId, out var healthMap))
            {
                healthMap.Remove(instId);
            }

            TouchInstancesRevision(svcId);

            var opId = CreateOperation("DEREGISTER_INSTANCE",
                new Dictionary<string, object?> { ["INSTANCE"] = instId, ["SERVICE"] = svcId });
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["OperationId"] = opId });
        }
    }

    private ServiceResponse GetInstance(JsonElement data)
    {
        var svcId = GetString(data, "ServiceId");
        var instId = GetString(data, "InstanceId");

        if (svcId is null || instId is null ||
            !_instances.TryGetValue(svcId, out var svcInstances) ||
            !svcInstances.TryGetValue(instId, out var inst))
        {
            return AwsResponseHelpers.ErrorResponseJson("InstanceNotFound", "Instance not found", 404);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Instance"] = inst.ToDict() });
    }

    private ServiceResponse ListInstances(JsonElement data)
    {
        var svcId = GetString(data, "ServiceId");
        if (string.IsNullOrEmpty(svcId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "ServiceId is required", 400);
        }

        lock (_lock)
        {
            if (!_services.ContainsKey(svcId))
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            var instances = _instances.TryGetValue(svcId, out var svcInstances)
                ? svcInstances.Values.Select(i => i.ToDict()).ToList()
                : [];

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Instances"] = instances });
        }
    }

    // ── Discover operations ──────────────────────────────────────────────────────

    private ServiceResponse DiscoverInstances(JsonElement data)
    {
        var nsName = GetString(data, "NamespaceName");
        var svcName = GetString(data, "ServiceName");
        if (string.IsNullOrEmpty(nsName) || string.IsNullOrEmpty(svcName))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidInput", "NamespaceName and ServiceName are required", 400);
        }

        lock (_lock)
        {
            var ns = _namespaces.Values.FirstOrDefault(n => n.Name == nsName);
            if (ns is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NamespaceNotFound", "Namespace not found", 404);
            }

            var svc = _services.Values.FirstOrDefault(s => s.Name == svcName && s.NamespaceId == ns.Id);
            if (svc is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            var requestedHealthStatus = GetString(data, "HealthStatus") ?? "";
            var svcInstances = _instances.TryGetValue(svc.Id, out var inst) ? inst : [];
            _instanceHealthStatus.TryGetValue(svc.Id, out var healthMap);
            healthMap ??= new Dictionary<string, string>(StringComparer.Ordinal);

            var result = new List<Dictionary<string, object?>>();
            foreach (var instance in svcInstances.Values)
            {
                var health = healthMap.GetValueOrDefault(instance.Id, "HEALTHY");
                if (!string.IsNullOrEmpty(requestedHealthStatus) &&
                    requestedHealthStatus != "ALL" &&
                    health != requestedHealthStatus)
                {
                    continue;
                }

                result.Add(new Dictionary<string, object?>
                {
                    ["InstanceId"] = instance.Id,
                    ["NamespaceName"] = nsName,
                    ["ServiceName"] = svcName,
                    ["Attributes"] = instance.Attributes,
                    ["HealthStatus"] = health,
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Instances"] = result });
        }
    }

    private ServiceResponse DiscoverInstancesRevision(JsonElement data)
    {
        var nsName = GetString(data, "NamespaceName");
        var svcName = GetString(data, "ServiceName");
        if (string.IsNullOrEmpty(nsName) || string.IsNullOrEmpty(svcName))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidInput", "NamespaceName and ServiceName are required", 400);
        }

        lock (_lock)
        {
            var ns = _namespaces.Values.FirstOrDefault(n => n.Name == nsName);
            if (ns is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("NamespaceNotFound", "Namespace not found", 404);
            }

            var svc = _services.Values.FirstOrDefault(s => s.Name == svcName && s.NamespaceId == ns.Id);
            if (svc is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            var rev = _instancesRevision.TryGetValue(svc.Id, out var r) ? r : 1;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["InstancesRevision"] = rev });
        }
    }

    // ── Health status operations ─────────────────────────────────────────────────

    private ServiceResponse GetInstancesHealthStatus(JsonElement data)
    {
        var svcId = GetString(data, "ServiceId");
        if (string.IsNullOrEmpty(svcId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "ServiceId is required", 400);
        }

        lock (_lock)
        {
            if (!_services.ContainsKey(svcId))
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            _instanceHealthStatus.TryGetValue(svcId, out var statuses);
            statuses ??= new Dictionary<string, string>(StringComparer.Ordinal);

            // Filter by requested instances if provided
            List<string> filteredInstanceIds;
            if (data.TryGetProperty("Instances", out var instancesEl) && instancesEl.ValueKind == JsonValueKind.Array)
            {
                filteredInstanceIds = [];
                foreach (var el in instancesEl.EnumerateArray())
                {
                    var id = el.GetString();
                    if (id is not null)
                    {
                        filteredInstanceIds.Add(id);
                    }
                }
            }
            else
            {
                filteredInstanceIds = [.. statuses.Keys];
            }

            var filtered = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var iid in filteredInstanceIds)
            {
                filtered[iid] = statuses.GetValueOrDefault(iid, "UNKNOWN");
            }

            var maxResults = GetInt(data, "MaxResults", filtered.Count > 0 ? filtered.Count : 1);
            var nextToken = GetString(data, "NextToken");
            var start = 0;
            if (nextToken is not null && int.TryParse(nextToken, out var parsedStart))
            {
                start = parsedStart;
            }

            var items = filtered.ToList();
            var page = items.GetRange(start, Math.Min(maxResults, items.Count - start));
            var outStatus = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var (key, value) in page)
            {
                outStatus[key] = value;
            }

            var result = new Dictionary<string, object?> { ["Status"] = outStatus };
            if (start + maxResults < items.Count)
            {
                result["NextToken"] = (start + maxResults).ToString();
            }

            return AwsResponseHelpers.JsonResponse(result);
        }
    }

    private ServiceResponse UpdateInstanceCustomHealthStatus(JsonElement data)
    {
        var svcId = GetString(data, "ServiceId");
        var instId = GetString(data, "InstanceId");
        var status = GetString(data, "Status");

        if (string.IsNullOrEmpty(svcId) || string.IsNullOrEmpty(instId) || string.IsNullOrEmpty(status))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidInput", "ServiceId, InstanceId, and Status are required", 400);
        }

        lock (_lock)
        {
            if (!_services.ContainsKey(svcId))
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            if (!_instances.TryGetValue(svcId, out var svcInstances) || !svcInstances.ContainsKey(instId))
            {
                return AwsResponseHelpers.ErrorResponseJson("InstanceNotFound", "Instance not found", 404);
            }

            if (!_instanceHealthStatus.TryGetValue(svcId, out var healthMap))
            {
                healthMap = new Dictionary<string, string>(StringComparer.Ordinal);
                _instanceHealthStatus[svcId] = healthMap;
            }

            healthMap[instId] = status;
            TouchInstancesRevision(svcId);
        }

        // Python returns raw tuple: 200, headers, b""
        return new ServiceResponse(200,
            new Dictionary<string, string> { ["Content-Type"] = "application/x-amz-json-1.0" }, []);
    }

    // ── Operation operations ─────────────────────────────────────────────────────

    private ServiceResponse GetOperation(JsonElement data)
    {
        var opId = GetString(data, "OperationId");
        if (string.IsNullOrEmpty(opId) || !_operations.TryGetValue(opId, out var op))
        {
            return AwsResponseHelpers.ErrorResponseJson("OperationNotFound", "Operation not found", 404);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Operation"] = op.ToDict() });
    }

    private ServiceResponse ListOperations(JsonElement data)
    {
        lock (_lock)
        {
            var ops = _operations.Values.Select(o => o.ToDict()).ToList();

            // Apply filters
            if (data.TryGetProperty("Filters", out var filtersEl) && filtersEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var filterEl in filtersEl.EnumerateArray())
                {
                    var filterName = GetString(filterEl, "Name");
                    if (string.IsNullOrEmpty(filterName))
                    {
                        continue;
                    }

                    var filterValues = new HashSet<string>(StringComparer.Ordinal);
                    if (filterEl.TryGetProperty("Values", out var valuesEl) && valuesEl.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var v in valuesEl.EnumerateArray())
                        {
                            var s = v.GetString();
                            if (s is not null)
                            {
                                filterValues.Add(s);
                            }
                        }
                    }

                    if (filterValues.Count == 0)
                    {
                        continue;
                    }

                    ops = filterName switch
                    {
                        "STATUS" => ops.Where(o =>
                            o.TryGetValue("Status", out var v) && v is string s && filterValues.Contains(s)).ToList(),
                        "TYPE" => ops.Where(o =>
                            o.TryGetValue("Type", out var v) && v is string s && filterValues.Contains(s)).ToList(),
                        "NAMESPACE_ID" => ops.Where(o =>
                            o.TryGetValue("Targets", out var t) &&
                            t is Dictionary<string, object?> targets &&
                            targets.TryGetValue("NAMESPACE", out var ns) &&
                            ns is string s && filterValues.Contains(s)).ToList(),
                        "SERVICE_ID" => ops.Where(o =>
                            o.TryGetValue("Targets", out var t) &&
                            t is Dictionary<string, object?> targets &&
                            targets.TryGetValue("SERVICE", out var svc) &&
                            svc is string s && filterValues.Contains(s)).ToList(),
                        _ => ops,
                    };
                }
            }

            var maxResults = GetInt(data, "MaxResults", ops.Count > 0 ? ops.Count : 1);
            var nextToken = GetString(data, "NextToken");
            var start = 0;
            if (nextToken is not null && int.TryParse(nextToken, out var parsedStart))
            {
                start = parsedStart;
            }

            var page = ops.GetRange(start, Math.Min(maxResults, ops.Count - start));

            var result = new Dictionary<string, object?> { ["Operations"] = page };
            if (start + maxResults < ops.Count)
            {
                result["NextToken"] = (start + maxResults).ToString();
            }

            return AwsResponseHelpers.JsonResponse(result);
        }
    }

    // ── Service attributes operations ────────────────────────────────────────────

    private ServiceResponse GetServiceAttributes(JsonElement data)
    {
        var svcId = GetString(data, "ServiceId");
        if (string.IsNullOrEmpty(svcId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "ServiceId is required", 400);
        }

        lock (_lock)
        {
            if (!_services.ContainsKey(svcId))
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            _serviceAttributes.TryGetValue(svcId, out var attrs);
            attrs ??= new Dictionary<string, string>(StringComparer.Ordinal);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ServiceAttributes"] = new Dictionary<string, object?>
                {
                    ["ServiceArn"] = ServiceArn(svcId),
                    ["ResourceOwner"] = "SELF",
                    ["Attributes"] = attrs,
                },
            });
        }
    }

    private ServiceResponse UpdateServiceAttributes(JsonElement data)
    {
        var svcId = GetString(data, "ServiceId");
        if (string.IsNullOrEmpty(svcId))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "ServiceId is required", 400);
        }

        lock (_lock)
        {
            if (!_services.ContainsKey(svcId))
            {
                return AwsResponseHelpers.ErrorResponseJson("ServiceNotFound", "Service not found", 404);
            }

            if (!_serviceAttributes.TryGetValue(svcId, out var current))
            {
                current = new Dictionary<string, string>(StringComparer.Ordinal);
                _serviceAttributes[svcId] = current;
            }

            if (data.TryGetProperty("Attributes", out var attrsEl) && attrsEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in attrsEl.EnumerateObject())
                {
                    current[prop.Name] = prop.Value.GetString() ?? "";
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // ── Tag operations ───────────────────────────────────────────────────────────

    private ServiceResponse TagResource(JsonElement data)
    {
        var arn = GetString(data, "ResourceARN");
        if (string.IsNullOrEmpty(arn))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "ResourceARN is required", 400);
        }

        lock (_lock)
        {
            if (!_resourceTags.TryGetValue(arn, out var existing))
            {
                existing = [];
                _resourceTags[arn] = existing;
            }

            var existingMap = existing.ToDictionary(t => t.Key, t => t, StringComparer.Ordinal);

            var incoming = GetTags(data);
            if (incoming is not null)
            {
                foreach (var tag in incoming)
                {
                    existingMap[tag.Key] = tag;
                }
            }

            _resourceTags[arn] = existingMap.Values.ToList();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse UntagResource(JsonElement data)
    {
        var arn = GetString(data, "ResourceARN");
        if (string.IsNullOrEmpty(arn))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "ResourceARN is required", 400);
        }

        lock (_lock)
        {
            if (data.TryGetProperty("TagKeys", out var keysEl) && keysEl.ValueKind == JsonValueKind.Array)
            {
                var keysToRemove = new HashSet<string>(StringComparer.Ordinal);
                foreach (var keyEl in keysEl.EnumerateArray())
                {
                    var key = keyEl.GetString();
                    if (key is not null)
                    {
                        keysToRemove.Add(key);
                    }
                }

                if (_resourceTags.TryGetValue(arn, out var existing))
                {
                    _resourceTags[arn] = existing.Where(t => !keysToRemove.Contains(t.Key)).ToList();
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ListTagsForResource(JsonElement data)
    {
        var arn = GetString(data, "ResourceARN");
        if (string.IsNullOrEmpty(arn))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidInput", "ResourceARN is required", 400);
        }

        _resourceTags.TryGetValue(arn, out var tags);
        var tagList = (tags ?? []).Select(t => new Dictionary<string, object?>
        {
            ["Key"] = t.Key,
            ["Value"] = t.Value,
        }).ToList();

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Tags"] = tagList });
    }

    // ── JsonElement conversion helpers ────────────────────────────────────────────

    private static Dictionary<string, object?> JsonElementToDict(JsonElement element)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (element.ValueKind != JsonValueKind.Object)
        {
            return dict;
        }

        foreach (var prop in element.EnumerateObject())
        {
            dict[prop.Name] = JsonElementToObject(prop.Value);
        }

        return dict;
    }

    private static object? JsonElementToObject(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Object => JsonElementToDict(element),
            JsonValueKind.Array => element.EnumerateArray().Select(JsonElementToObject).ToList(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    // ── Data models ──────────────────────────────────────────────────────────────

    private sealed class SdNamespace
    {
        internal string Id { get; init; } = "";
        internal string Arn { get; init; } = "";
        internal string Name { get; init; } = "";
        internal string Type { get; init; } = "";
        internal string? Description { get; set; }
        internal double CreateDate { get; init; }
        internal Dictionary<string, object?>? Properties { get; init; }

        internal Dictionary<string, object?> ToDict()
        {
            return new Dictionary<string, object?>
            {
                ["Id"] = Id,
                ["Arn"] = Arn,
                ["Name"] = Name,
                ["Type"] = Type,
                ["Description"] = Description,
                ["CreateDate"] = CreateDate,
                ["Properties"] = Properties,
            };
        }
    }

    private sealed class SdService
    {
        internal string Id { get; init; } = "";
        internal string Arn { get; init; } = "";
        internal string Name { get; init; } = "";
        internal string NamespaceId { get; init; } = "";
        internal string? Description { get; set; }
        internal double CreateDate { get; init; }
        internal Dictionary<string, object?>? DnsConfig { get; set; }
        internal Dictionary<string, object?>? HealthCheckConfig { get; set; }
        internal Dictionary<string, object?>? HealthCheckCustomConfig { get; set; }

        internal Dictionary<string, object?> ToDict()
        {
            return new Dictionary<string, object?>
            {
                ["Id"] = Id,
                ["Arn"] = Arn,
                ["Name"] = Name,
                ["NamespaceId"] = NamespaceId,
                ["Description"] = Description,
                ["DnsConfig"] = DnsConfig,
                ["HealthCheckConfig"] = HealthCheckConfig,
                ["HealthCheckCustomConfig"] = HealthCheckCustomConfig,
                ["CreateDate"] = CreateDate,
            };
        }
    }

    private sealed class SdInstance
    {
        internal string Id { get; init; } = "";
        internal Dictionary<string, string> Attributes { get; init; } = new(StringComparer.Ordinal);

        internal Dictionary<string, object?> ToDict()
        {
            return new Dictionary<string, object?>
            {
                ["Id"] = Id,
                ["Attributes"] = Attributes,
            };
        }
    }

    private sealed class SdOperation
    {
        internal string Id { get; init; } = "";
        internal string Status { get; init; } = "";
        internal string Type { get; init; } = "";
        internal Dictionary<string, object?> Targets { get; init; } = new();
        internal double CreateDate { get; init; }
        internal double UpdateDate { get; init; }

        internal Dictionary<string, object?> ToDict()
        {
            return new Dictionary<string, object?>
            {
                ["Id"] = Id,
                ["Status"] = Status,
                ["Type"] = Type,
                ["Targets"] = Targets,
                ["CreateDate"] = CreateDate,
                ["UpdateDate"] = UpdateDate,
            };
        }
    }

    private sealed class SdTag
    {
        internal string Key { get; init; } = "";
        internal string Value { get; init; } = "";
    }
}
