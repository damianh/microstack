using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Ecs;

/// <summary>
/// ECS (Elastic Container Service) service handler -- supports JSON protocol via X-Amz-Target.
///
/// Port of ministack/services/ecs.py.
///
/// NO Docker integration -- all in-memory stubs. Tasks are "running" in-memory,
/// not actually executing containers.
///
/// Supports: CreateCluster, DeleteCluster, DescribeClusters, ListClusters,
///           UpdateCluster, UpdateClusterSettings, PutClusterCapacityProviders,
///           RegisterTaskDefinition, DeregisterTaskDefinition, DescribeTaskDefinition,
///           ListTaskDefinitions, ListTaskDefinitionFamilies, DeleteTaskDefinitions,
///           CreateService, DeleteService, DescribeServices, UpdateService, ListServices,
///           ListServicesByNamespace,
///           RunTask, StopTask, DescribeTasks, ListTasks, ExecuteCommand,
///           UpdateTaskProtection, GetTaskProtection,
///           TagResource, UntagResource, ListTagsForResource,
///           CreateCapacityProvider, DeleteCapacityProvider, UpdateCapacityProvider,
///           DescribeCapacityProviders,
///           ListAccountSettings, PutAccountSetting, PutAccountSettingDefault, DeleteAccountSetting,
///           PutAttributes, DeleteAttributes, ListAttributes,
///           DescribeServiceDeployments, ListServiceDeployments, DescribeServiceRevisions,
///           SubmitTaskStateChange, SubmitContainerStateChange, SubmitAttachmentStateChanges,
///           DiscoverPollEndpoint.
/// </summary>
internal sealed class EcsServiceHandler : IServiceHandler
{
    // keyed by cluster name
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _clusters = new();

    // keyed by "family:revision"
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _taskDefs = new();

    // keyed by family name → latest revision number
    private readonly AccountScopedDictionary<string, int> _taskDefLatest = new();

    // keyed by "clusterName/serviceName"
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _services = new();

    // keyed by task ARN
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _tasks = new();

    // keyed by resource ARN → list of tags
    private readonly AccountScopedDictionary<string, List<Dictionary<string, string>>> _tags = new();

    // keyed by setting name
    private readonly AccountScopedDictionary<string, object> _accountSettings = new();

    // keyed by capacity provider name
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _capacityProviders = new();

    // keyed by "targetId:name"
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _attributes = new();

    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "ecs";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
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
                return Task.FromResult(
                    AwsResponseHelpers.ErrorResponseJson("SerializationException", "Invalid JSON", 400));
            }
        }
        else
        {
            data = JsonDocument.Parse("{}"u8.ToArray()).RootElement.Clone();
        }

        ServiceResponse response;
        lock (_lock)
        {
            response = action switch
            {
                // Clusters
                "CreateCluster" => ActCreateCluster(data),
                "DeleteCluster" => ActDeleteCluster(data),
                "DescribeClusters" => ActDescribeClusters(data),
                "ListClusters" => ActListClusters(),
                "UpdateCluster" => ActUpdateCluster(data),
                "UpdateClusterSettings" => ActUpdateClusterSettings(data),
                "PutClusterCapacityProviders" => ActPutClusterCapacityProviders(data),
                // Task Definitions
                "RegisterTaskDefinition" => ActRegisterTaskDefinition(data),
                "DeregisterTaskDefinition" => ActDeregisterTaskDefinition(data),
                "DescribeTaskDefinition" => ActDescribeTaskDefinition(data),
                "ListTaskDefinitions" => ActListTaskDefinitions(data),
                "ListTaskDefinitionFamilies" => ActListTaskDefinitionFamilies(data),
                "DeleteTaskDefinitions" => ActDeleteTaskDefinitions(data),
                // Services
                "CreateService" => ActCreateService(data),
                "DeleteService" => ActDeleteService(data),
                "DescribeServices" => ActDescribeServices(data),
                "UpdateService" => ActUpdateService(data),
                "ListServices" => ActListServices(data),
                "ListServicesByNamespace" => ActListServicesByNamespace(data),
                // Tasks
                "RunTask" => ActRunTask(data),
                "StopTask" => ActStopTask(data),
                "DescribeTasks" => ActDescribeTasks(data),
                "ListTasks" => ActListTasks(data),
                "ExecuteCommand" => ActExecuteCommand(data),
                "UpdateTaskProtection" => ActUpdateTaskProtection(),
                "GetTaskProtection" => ActGetTaskProtection(),
                // Tags
                "TagResource" => ActTagResource(data),
                "UntagResource" => ActUntagResource(data),
                "ListTagsForResource" => ActListTagsForResource(data),
                // Account Settings
                "ListAccountSettings" => ActListAccountSettings(data),
                "PutAccountSetting" => ActPutAccountSetting(data),
                "PutAccountSettingDefault" => ActPutAccountSettingDefault(data),
                "DeleteAccountSetting" => ActDeleteAccountSetting(data),
                // Capacity Providers
                "CreateCapacityProvider" => ActCreateCapacityProvider(data),
                "DeleteCapacityProvider" => ActDeleteCapacityProvider(data),
                "UpdateCapacityProvider" => ActUpdateCapacityProvider(data),
                "DescribeCapacityProviders" => ActDescribeCapacityProviders(data),
                // Attributes
                "PutAttributes" => ActPutAttributes(data),
                "DeleteAttributes" => ActDeleteAttributes(data),
                "ListAttributes" => ActListAttributes(data),
                // Service Deployments (stubs)
                "DescribeServiceDeployments" => ActDescribeServiceDeployments(),
                "ListServiceDeployments" => ActListServiceDeployments(),
                "DescribeServiceRevisions" => ActDescribeServiceRevisions(),
                // Agent stubs
                "SubmitTaskStateChange" => ActSubmitStateChange(),
                "SubmitContainerStateChange" => ActSubmitStateChange(),
                "SubmitAttachmentStateChanges" => ActSubmitStateChange(),
                "DiscoverPollEndpoint" => ActDiscoverPollEndpoint(),
                _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown ECS action: {action}", 400),
            };
        }

        // Normalize ECS timestamps (ISO→epoch) in the response
        response = NormalizeTimestamps(response);

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _clusters.Clear();
            _taskDefs.Clear();
            _taskDefLatest.Clear();
            _services.Clear();
            _tasks.Clear();
            _tags.Clear();
            _accountSettings.Clear();
            _capacityProviders.Clear();
            _attributes.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // -- Timestamp normalization -----------------------------------------------

    private static readonly HashSet<string> TimestampFields = new(StringComparer.Ordinal)
    {
        "createdAt", "startedAt", "stoppedAt", "registeredAt",
        "deregisteredAt", "updatedAt", "lastModified", "runningAt",
        "pullStartedAt", "pullStoppedAt", "executionStoppedAt",
        "stoppingAt", "connectivityAt",
    };

    private static ServiceResponse NormalizeTimestamps(ServiceResponse response)
    {
        if (response.Body.Length == 0)
        {
            return response;
        }

        try
        {
            using var doc = JsonDocument.Parse(response.Body);
            var normalized = NormalizeElement(doc.RootElement, null);
            var bytes = JsonSerializer.SerializeToUtf8Bytes(normalized, JsonOpts);
            return new ServiceResponse(response.StatusCode, response.Headers, bytes);
        }
        catch (JsonException)
        {
            return response;
        }
    }

    private static object? NormalizeElement(JsonElement el, string? fieldName)
    {
        switch (el.ValueKind)
        {
            case JsonValueKind.Object:
            {
                var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var prop in el.EnumerateObject())
                {
                    dict[prop.Name] = NormalizeElement(prop.Value, prop.Name);
                }
                return dict;
            }
            case JsonValueKind.Array:
            {
                var list = new List<object?>();
                foreach (var item in el.EnumerateArray())
                {
                    list.Add(NormalizeElement(item, fieldName));
                }
                return list;
            }
            case JsonValueKind.String when fieldName is not null && TimestampFields.Contains(fieldName):
            {
                var s = el.GetString();
                if (s is not null && DateTimeOffset.TryParse(s, out var dto))
                {
                    return dto.ToUnixTimeMilliseconds() / 1000.0;
                }
                return s;
            }
            case JsonValueKind.String:
                return el.GetString();
            case JsonValueKind.Number:
                return el.TryGetInt64(out var l) ? l : el.GetDouble();
            case JsonValueKind.True:
                return true;
            case JsonValueKind.False:
                return false;
            default:
                return null;
        }
    }

    // -- JSON helpers ----------------------------------------------------------

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

    private static bool GetBool(JsonElement el, string propertyName, bool defaultValue)
    {
        if (!el.TryGetProperty(propertyName, out var prop))
        {
            return defaultValue;
        }

        return prop.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => defaultValue,
        };
    }

    private static List<Dictionary<string, string>> GetTags(JsonElement el)
    {
        var tags = new List<Dictionary<string, string>>();
        if (el.TryGetProperty("tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var tagEl in tagsEl.EnumerateArray())
            {
                var tag = new Dictionary<string, string>(StringComparer.Ordinal);
                if (tagEl.TryGetProperty("key", out var keyProp) && keyProp.ValueKind == JsonValueKind.String)
                {
                    tag["key"] = keyProp.GetString()!;
                }

                if (tagEl.TryGetProperty("value", out var valProp) && valProp.ValueKind == JsonValueKind.String)
                {
                    tag["value"] = valProp.GetString()!;
                }

                if (tag.ContainsKey("key"))
                {
                    tags.Add(tag);
                }
            }
        }

        return tags;
    }

    private static List<string> GetStringList(JsonElement el, string propertyName)
    {
        var list = new List<string>();
        if (el.TryGetProperty(propertyName, out var arr) && arr.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in arr.EnumerateArray())
            {
                var s = item.GetString();
                if (s is not null)
                {
                    list.Add(s);
                }
            }
        }

        return list;
    }

    private static object? JsonElementToObject(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.Object => JsonElementToDict(el),
            JsonValueKind.Array => el.EnumerateArray().Select(JsonElementToObject).ToList(),
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? l : el.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    private static Dictionary<string, object?> JsonElementToDict(JsonElement el)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (el.ValueKind != JsonValueKind.Object)
        {
            return dict;
        }

        foreach (var prop in el.EnumerateObject())
        {
            dict[prop.Name] = JsonElementToObject(prop.Value);
        }

        return dict;
    }

    private static List<object?> JsonElementToList(JsonElement el)
    {
        var list = new List<object?>();
        if (el.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in el.EnumerateArray())
            {
                list.Add(JsonElementToObject(item));
            }
        }

        return list;
    }

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false,
    };

    // -- Resolve helpers -------------------------------------------------------

    private static string ResolveClusterName(string? reference)
    {
        if (string.IsNullOrEmpty(reference))
        {
            return "default";
        }

        if (reference.StartsWith("arn:", StringComparison.Ordinal))
        {
            return reference[(reference.LastIndexOf('/') + 1)..];
        }

        if (reference.Contains('/', StringComparison.Ordinal))
        {
            return reference[(reference.LastIndexOf('/') + 1)..];
        }

        return reference;
    }

    private static string ResolveServiceName(string reference)
    {
        if (reference.StartsWith("arn:", StringComparison.Ordinal))
        {
            return reference[(reference.LastIndexOf('/') + 1)..];
        }

        if (reference.Contains('/', StringComparison.Ordinal))
        {
            return reference[(reference.LastIndexOf('/') + 1)..];
        }

        return reference;
    }

    private string ResolveTdKey(string? reference)
    {
        if (string.IsNullOrEmpty(reference))
        {
            return "";
        }

        if (reference.Contains("task-definition/", StringComparison.Ordinal))
        {
            reference = reference[(reference.IndexOf("task-definition/", StringComparison.Ordinal) + "task-definition/".Length)..];
        }

        if (!reference.Contains(':', StringComparison.Ordinal))
        {
            if (_taskDefLatest.TryGetValue(reference, out var rev))
            {
                return $"{reference}:{rev}";
            }

            return reference;
        }

        return reference;
    }

    private Dictionary<string, object?>? ResolveTask(string reference, string clusterName)
    {
        if (_tasks.TryGetValue(reference, out var task))
        {
            return task;
        }

        var accountId = AccountContext.GetAccountId();
        var clusterArn = $"arn:aws:ecs:{Region}:{accountId}:cluster/{clusterName}";

        foreach (var (arn, t) in _tasks.Items)
        {
            var tClusterArn = t.TryGetValue("clusterArn", out var ca) ? ca as string : null;
            if (tClusterArn != clusterArn)
            {
                continue;
            }

            if (arn.EndsWith($"/{reference}", StringComparison.Ordinal) || arn.EndsWith(reference, StringComparison.Ordinal))
            {
                return t;
            }
        }

        foreach (var (arn, t) in _tasks.Items)
        {
            if (arn.EndsWith($"/{reference}", StringComparison.Ordinal) || arn.EndsWith(reference, StringComparison.Ordinal))
            {
                return t;
            }
        }

        return null;
    }

    private static string ClusterNameFromArn(string? arn)
    {
        if (string.IsNullOrEmpty(arn))
        {
            return "";
        }

        return arn.Contains('/', StringComparison.Ordinal) ? arn[(arn.LastIndexOf('/') + 1)..] : arn;
    }

    private static Dictionary<string, object?> Sanitize(Dictionary<string, object?> obj)
    {
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (key, value) in obj)
        {
            if (key.StartsWith('_'))
            {
                continue;
            }

            result[key] = value switch
            {
                Dictionary<string, object?> nested => Sanitize(nested),
                List<object?> list => list.Select(i => i is Dictionary<string, object?> d ? (object?)Sanitize(d) : i).ToList(),
                _ => value,
            };
        }

        return result;
    }

    private void RecountCluster(string clusterName)
    {
        if (!_clusters.TryGetValue(clusterName, out var cluster))
        {
            return;
        }

        var clusterArn = cluster.TryGetValue("clusterArn", out var ca) ? ca as string : null;
        var running = 0;
        var pending = 0;

        foreach (var t in _tasks.Values)
        {
            var tClusterArn = t.TryGetValue("clusterArn", out var tca) ? tca as string : null;
            if (tClusterArn != clusterArn)
            {
                continue;
            }

            var lastStatus = t.TryGetValue("lastStatus", out var ls) ? ls as string : null;
            if (lastStatus == "RUNNING")
            {
                running++;
            }
            else if (lastStatus == "PENDING")
            {
                pending++;
            }
        }

        cluster["runningTasksCount"] = running;
        cluster["pendingTasksCount"] = pending;

        var activeSvcs = 0;
        foreach (var (key, svc) in _services.Items)
        {
            if (!key.StartsWith($"{clusterName}/", StringComparison.Ordinal))
            {
                continue;
            }

            var status = svc.TryGetValue("status", out var s) ? s as string : null;
            if (status == "ACTIVE")
            {
                activeSvcs++;
            }
        }

        cluster["activeServicesCount"] = activeSvcs;
    }

    private void EnsureCluster(string clusterName)
    {
        if (!_clusters.ContainsKey(clusterName))
        {
            var json = $"{{\"clusterName\":\"{clusterName}\"}}";
            var emptyData = JsonDocument.Parse(json).RootElement.Clone();
            ActCreateCluster(emptyData);
        }
    }

    // -- Cluster actions -------------------------------------------------------

    private ServiceResponse ActCreateCluster(JsonElement data)
    {
        var name = GetString(data, "clusterName") ?? "default";
        if (_clusters.TryGetValue(name, out var existing))
        {
            var existingStatus = existing.TryGetValue("status", out var es) ? es as string : null;
            if (existingStatus == "ACTIVE")
            {
                return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["cluster"] = existing });
            }
        }

        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:ecs:{Region}:{accountId}:cluster/{name}";
        var tags = GetTags(data);

        var settings = data.TryGetProperty("settings", out var settingsEl) && settingsEl.ValueKind == JsonValueKind.Array
            ? JsonElementToList(settingsEl)
            : new List<object?> { new Dictionary<string, object?> { ["name"] = "containerInsights", ["value"] = "disabled" } };

        var capacityProviders = GetStringList(data, "capacityProviders");
        var defaultCpStrategy = data.TryGetProperty("defaultCapacityProviderStrategy", out var cpStratEl) && cpStratEl.ValueKind == JsonValueKind.Array
            ? JsonElementToList(cpStratEl)
            : new List<object?>();

        var cluster = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["clusterArn"] = arn,
            ["clusterName"] = name,
            ["status"] = "ACTIVE",
            ["registeredContainerInstancesCount"] = 0,
            ["runningTasksCount"] = 0,
            ["pendingTasksCount"] = 0,
            ["activeServicesCount"] = 0,
            ["tags"] = tags,
            ["settings"] = settings,
            ["capacityProviders"] = capacityProviders,
            ["defaultCapacityProviderStrategy"] = defaultCpStrategy,
            ["statistics"] = new List<object?>(),
            ["attachments"] = new List<object?>(),
            ["attachmentsStatus"] = "",
        };

        _clusters[name] = cluster;
        if (tags.Count > 0)
        {
            _tags[arn] = new List<Dictionary<string, string>>(tags);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["cluster"] = cluster });
    }

    private ServiceResponse ActDeleteCluster(JsonElement data)
    {
        var name = ResolveClusterName(GetString(data, "cluster") ?? "default");
        if (!_clusters.TryRemove(name, out var cluster))
        {
            return AwsResponseHelpers.ErrorResponseJson("ClusterNotFoundException", "Cluster not found.", 400);
        }

        cluster["status"] = "INACTIVE";
        var clusterArn = cluster.TryGetValue("clusterArn", out var ca) ? ca as string : null;
        if (clusterArn is not null)
        {
            _tags.TryRemove(clusterArn, out _);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["cluster"] = cluster });
    }

    private ServiceResponse ActDescribeClusters(JsonElement data)
    {
        var names = GetStringList(data, "clusters");
        if (names.Count == 0)
        {
            names.Add("default");
        }

        var include = new HashSet<string>(GetStringList(data, "include"), StringComparer.Ordinal);
        var result = new List<object?>();
        var failures = new List<object?>();

        foreach (var reference in names)
        {
            var n = ResolveClusterName(reference);
            if (_clusters.TryGetValue(n, out var c))
            {
                var copy = new Dictionary<string, object?>(c, StringComparer.Ordinal);
                if (include.Contains("TAGS"))
                {
                    var clusterArn = c.TryGetValue("clusterArn", out var ca) ? ca as string : null;
                    if (clusterArn is not null && _tags.TryGetValue(clusterArn, out var tagList))
                    {
                        copy["tags"] = tagList;
                    }
                }

                RecountCluster(n);
                copy["runningTasksCount"] = _clusters[n]["runningTasksCount"];
                copy["pendingTasksCount"] = _clusters[n]["pendingTasksCount"];
                copy["activeServicesCount"] = _clusters[n]["activeServicesCount"];
                result.Add(copy);
            }
            else
            {
                var accountId = AccountContext.GetAccountId();
                var arn = reference.StartsWith("arn:", StringComparison.Ordinal)
                    ? reference
                    : $"arn:aws:ecs:{Region}:{accountId}:cluster/{reference}";
                failures.Add(new Dictionary<string, object?> { ["arn"] = arn, ["reason"] = "MISSING" });
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["clusters"] = result,
            ["failures"] = failures,
        });
    }

    private ServiceResponse ActListClusters()
    {
        var arns = new List<string>();
        foreach (var c in _clusters.Values)
        {
            var status = c.TryGetValue("status", out var s) ? s as string : null;
            if (status == "ACTIVE")
            {
                var arn = c.TryGetValue("clusterArn", out var ca) ? ca as string : null;
                if (arn is not null)
                {
                    arns.Add(arn);
                }
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["clusterArns"] = arns });
    }

    private ServiceResponse ActUpdateCluster(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        if (!_clusters.TryGetValue(clusterName, out var cluster))
        {
            return AwsResponseHelpers.ErrorResponseJson("ClusterNotFoundException", "Cluster not found.", 400);
        }

        if (data.TryGetProperty("configuration", out var configEl))
        {
            cluster["configuration"] = JsonElementToObject(configEl);
        }

        if (data.TryGetProperty("settings", out var settingsEl))
        {
            cluster["settings"] = JsonElementToObject(settingsEl);
        }

        if (data.TryGetProperty("serviceConnectDefaults", out var scdEl))
        {
            cluster["serviceConnectDefaults"] = JsonElementToObject(scdEl);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["cluster"] = cluster });
    }

    private ServiceResponse ActUpdateClusterSettings(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        if (!_clusters.TryGetValue(clusterName, out var cluster))
        {
            return AwsResponseHelpers.ErrorResponseJson("ClusterNotFoundException", "Cluster not found.", 400);
        }

        if (data.TryGetProperty("settings", out var settingsEl))
        {
            cluster["settings"] = JsonElementToObject(settingsEl);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["cluster"] = cluster });
    }

    private ServiceResponse ActPutClusterCapacityProviders(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        if (!_clusters.TryGetValue(clusterName, out var cluster))
        {
            return AwsResponseHelpers.ErrorResponseJson("ClusterNotFoundException", "Cluster not found.", 400);
        }

        cluster["capacityProviders"] = GetStringList(data, "capacityProviders");
        if (data.TryGetProperty("defaultCapacityProviderStrategy", out var stratEl))
        {
            cluster["defaultCapacityProviderStrategy"] = JsonElementToObject(stratEl);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["cluster"] = cluster });
    }

    // -- Task Definition actions -----------------------------------------------

    private ServiceResponse ActRegisterTaskDefinition(JsonElement data)
    {
        var family = GetString(data, "family");
        if (string.IsNullOrEmpty(family))
        {
            return AwsResponseHelpers.ErrorResponseJson("ClientException", "family is required", 400);
        }

        if (!data.TryGetProperty("containerDefinitions", out var cdEl) || cdEl.ValueKind != JsonValueKind.Array || cdEl.GetArrayLength() == 0)
        {
            return AwsResponseHelpers.ErrorResponseJson("ClientException",
                "TaskDefinition must contain at least one container definition.", 400);
        }

        var containerDefs = new List<object?>();
        var idx = 0;
        foreach (var cdefEl in cdEl.EnumerateArray())
        {
            var cName = GetString(cdefEl, "name");
            if (cName is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ClientException",
                    $"Container definition {idx}: name is required.", 400);
            }

            var cImage = GetString(cdefEl, "image");
            if (cImage is null)
            {
                return AwsResponseHelpers.ErrorResponseJson("ClientException",
                    $"Container definition {idx} ({cName}): image is required.", 400);
            }

            var cdef = JsonElementToDict(cdefEl);
            cdef.TryAdd("cpu", (object?)0);
            cdef.TryAdd("memory", (object?)0);
            cdef.TryAdd("memoryReservation", (object?)0);
            cdef.TryAdd("essential", (object?)true);
            cdef.TryAdd("portMappings", new List<object?>());
            cdef.TryAdd("environment", new List<object?>());
            cdef.TryAdd("mountPoints", new List<object?>());
            cdef.TryAdd("volumesFrom", new List<object?>());
            cdef.TryAdd("logConfiguration", (object?)null);
            containerDefs.Add(cdef);
            idx++;
        }

        var rev = (_taskDefLatest.TryGetValue(family, out var prevRev) ? prevRev : 0) + 1;
        _taskDefLatest[family] = rev;

        var tdKey = $"{family}:{rev}";
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:ecs:{Region}:{accountId}:task-definition/{tdKey}";

        var compat = GetStringList(data, "requiresCompatibilities");
        if (compat.Count == 0)
        {
            compat.Add("EC2");
        }

        var networkMode = GetString(data, "networkMode")
            ?? (compat.Contains("FARGATE") ? "awsvpc" : "bridge");

        var compatibilities = new List<string>(compat);
        if (compat.Contains("FARGATE") && !compat.Contains("EC2"))
        {
            compatibilities.Add("EC2");
        }

        var td = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["taskDefinitionArn"] = arn,
            ["family"] = family,
            ["revision"] = rev,
            ["status"] = "ACTIVE",
            ["containerDefinitions"] = containerDefs,
            ["volumes"] = data.TryGetProperty("volumes", out var volEl) ? JsonElementToObject(volEl) : new List<object?>(),
            ["placementConstraints"] = data.TryGetProperty("placementConstraints", out var pcEl) ? JsonElementToObject(pcEl) : new List<object?>(),
            ["networkMode"] = networkMode,
            ["requiresCompatibilities"] = compat,
            ["cpu"] = GetString(data, "cpu") ?? "256",
            ["memory"] = GetString(data, "memory") ?? "512",
            ["executionRoleArn"] = GetString(data, "executionRoleArn") ?? "",
            ["taskRoleArn"] = GetString(data, "taskRoleArn") ?? "",
            ["pidMode"] = GetString(data, "pidMode") ?? "",
            ["ipcMode"] = GetString(data, "ipcMode") ?? "",
            ["proxyConfiguration"] = data.TryGetProperty("proxyConfiguration", out var proxyEl) ? JsonElementToObject(proxyEl) : null,
            ["runtimePlatform"] = data.TryGetProperty("runtimePlatform", out var rpEl) ? JsonElementToObject(rpEl) : null,
            ["ephemeralStorage"] = data.TryGetProperty("ephemeralStorage", out var esEl) ? JsonElementToObject(esEl) : null,
            ["registeredAt"] = TimeHelpers.NowIso(),
            ["registeredBy"] = $"arn:aws:iam::{accountId}:root",
            ["compatibilities"] = compatibilities,
        };

        _taskDefs[tdKey] = td;

        var reqTags = GetTags(data);
        if (reqTags.Count > 0)
        {
            _tags[arn] = new List<Dictionary<string, string>>(reqTags);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["taskDefinition"] = td,
            ["tags"] = reqTags,
        });
    }

    private ServiceResponse ActDeregisterTaskDefinition(JsonElement data)
    {
        var tdRef = GetString(data, "taskDefinition") ?? "";
        var key = ResolveTdKey(tdRef);
        if (!_taskDefs.TryGetValue(key, out var td))
        {
            return AwsResponseHelpers.ErrorResponseJson("ClientException",
                $"Unable to describe task definition: {tdRef}", 400);
        }

        td["status"] = "INACTIVE";
        td["deregisteredAt"] = TimeHelpers.NowIso();
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["taskDefinition"] = td });
    }

    private ServiceResponse ActDescribeTaskDefinition(JsonElement data)
    {
        var tdRef = GetString(data, "taskDefinition") ?? "";
        var key = ResolveTdKey(tdRef);
        if (!_taskDefs.TryGetValue(key, out var td))
        {
            return AwsResponseHelpers.ErrorResponseJson("ClientException",
                $"Unable to describe task definition: {tdRef}", 400);
        }

        var include = new HashSet<string>(GetStringList(data, "include"), StringComparer.Ordinal);
        var resp = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["taskDefinition"] = td,
        };

        if (include.Contains("TAGS"))
        {
            var tdArn = td.TryGetValue("taskDefinitionArn", out var a) ? a as string : null;
            resp["tags"] = tdArn is not null && _tags.TryGetValue(tdArn, out var tagList) ? tagList : new List<Dictionary<string, string>>();
        }
        else
        {
            resp["tags"] = new List<Dictionary<string, string>>();
        }

        return AwsResponseHelpers.JsonResponse(resp);
    }

    private ServiceResponse ActListTaskDefinitions(JsonElement data)
    {
        var familyPrefix = GetString(data, "familyPrefix") ?? "";
        var statusFilter = GetString(data, "status") ?? "ACTIVE";
        var sort = GetString(data, "sort") ?? "ASC";

        var arns = new List<string>();
        foreach (var td in _taskDefs.Values)
        {
            var family = td.TryGetValue("family", out var f) ? f as string : "";
            var status = td.TryGetValue("status", out var s) ? s as string : "";

            if (!string.IsNullOrEmpty(familyPrefix) && family is not null && !family.StartsWith(familyPrefix, StringComparison.Ordinal))
            {
                continue;
            }

            if (status != statusFilter)
            {
                continue;
            }

            var arn = td.TryGetValue("taskDefinitionArn", out var a) ? a as string : null;
            if (arn is not null)
            {
                arns.Add(arn);
            }
        }

        if (sort == "DESC")
        {
            arns.Reverse();
        }

        var maxResults = GetInt(data, "maxResults", 100);
        var nextToken = GetString(data, "nextToken");
        var start = !string.IsNullOrEmpty(nextToken) && int.TryParse(nextToken, out var s2) ? s2 : 0;
        var page = arns.GetRange(start, Math.Min(maxResults, arns.Count - start));

        var resp = new Dictionary<string, object?>(StringComparer.Ordinal) { ["taskDefinitionArns"] = page };
        if (start + maxResults < arns.Count)
        {
            resp["nextToken"] = (start + maxResults).ToString();
        }

        return AwsResponseHelpers.JsonResponse(resp);
    }

    private ServiceResponse ActListTaskDefinitionFamilies(JsonElement data)
    {
        var familyPrefix = GetString(data, "familyPrefix") ?? "";
        var statusFilter = GetString(data, "status") ?? "ACTIVE";

        var families = new HashSet<string>(StringComparer.Ordinal);
        foreach (var td in _taskDefs.Values)
        {
            var status = td.TryGetValue("status", out var s) ? s as string : "";
            if (!string.IsNullOrEmpty(statusFilter) && status != statusFilter)
            {
                continue;
            }

            var family = td.TryGetValue("family", out var f) ? f as string : "";
            if (!string.IsNullOrEmpty(familyPrefix) && family is not null && !family.StartsWith(familyPrefix, StringComparison.Ordinal))
            {
                continue;
            }

            if (family is not null)
            {
                families.Add(family);
            }
        }

        var sorted = families.OrderBy(f => f, StringComparer.Ordinal).ToList();
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["families"] = sorted });
    }

    private ServiceResponse ActDeleteTaskDefinitions(JsonElement data)
    {
        var tdArns = GetStringList(data, "taskDefinitions");
        var failures = new List<object?>();
        var deleted = new List<object?>();

        foreach (var arnRef in tdArns)
        {
            var key = arnRef.Contains('/', StringComparison.Ordinal) ? arnRef[(arnRef.LastIndexOf('/') + 1)..] : arnRef;
            if (_taskDefs.TryGetValue(key, out var td))
            {
                td["status"] = "DELETE_IN_PROGRESS";
                deleted.Add(td);
            }
            else
            {
                failures.Add(new Dictionary<string, object?> { ["arn"] = arnRef, ["reason"] = "TASK_DEFINITION_NOT_FOUND" });
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["taskDefinitions"] = deleted,
            ["failures"] = failures,
        });
    }

    // -- Service actions -------------------------------------------------------

    private static Dictionary<string, object?> MakeDeployment(string taskDefinition, int desiredCount, string status)
    {
        var depId = $"ecs-svc/{HashHelpers.NewUuidNoDashes()[..20]}";
        var now = TimeHelpers.NowIso();
        return new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["id"] = depId,
            ["status"] = status,
            ["taskDefinition"] = taskDefinition,
            ["desiredCount"] = desiredCount,
            ["runningCount"] = status == "PRIMARY" ? desiredCount : 0,
            ["pendingCount"] = 0,
            ["failedTasks"] = 0,
            ["launchType"] = "EC2",
            ["createdAt"] = now,
            ["updatedAt"] = now,
            ["rolloutState"] = status == "PRIMARY" ? "COMPLETED" : "IN_PROGRESS",
            ["rolloutStateReason"] = status == "PRIMARY" ? "ECS deployment completed." : "",
        };
    }

    private ServiceResponse ActCreateService(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        EnsureCluster(clusterName);

        var name = GetString(data, "serviceName");
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ClientException", "serviceName is required", 400);
        }

        var svcKey = $"{clusterName}/{name}";
        if (_services.TryGetValue(svcKey, out var existingSvc))
        {
            var existingStatus = existingSvc.TryGetValue("status", out var es) ? es as string : null;
            if (existingStatus == "ACTIVE")
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidParameterException",
                    "Creation of service was not idempotent.", 400);
            }
        }

        var tdRef = GetString(data, "taskDefinition") ?? "";
        var tdKey = ResolveTdKey(tdRef);
        string tdArn;
        if (_taskDefs.TryGetValue(tdKey, out var tdObj))
        {
            tdArn = tdObj.TryGetValue("taskDefinitionArn", out var a) ? a as string ?? tdRef : tdRef;
        }
        else
        {
            tdArn = tdRef;
        }

        var desired = GetInt(data, "desiredCount", 1);
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:ecs:{Region}:{accountId}:service/{clusterName}/{name}";
        var now = TimeHelpers.NowIso();
        var launchType = GetString(data, "launchType") ?? "EC2";

        var deployment = MakeDeployment(tdArn, desired, "PRIMARY");
        deployment["launchType"] = launchType;

        var tags = GetTags(data);

        var svc = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["serviceArn"] = arn,
            ["serviceName"] = name,
            ["clusterArn"] = _clusters[clusterName]["clusterArn"],
            ["taskDefinition"] = tdArn,
            ["desiredCount"] = desired,
            ["runningCount"] = desired,
            ["pendingCount"] = 0,
            ["status"] = "ACTIVE",
            ["launchType"] = launchType,
            ["platformVersion"] = GetString(data, "platformVersion") ?? "",
            ["platformFamily"] = GetString(data, "platformFamily") ?? "",
            ["networkConfiguration"] = data.TryGetProperty("networkConfiguration", out var ncEl) ? JsonElementToObject(ncEl) : new Dictionary<string, object?>(),
            ["loadBalancers"] = data.TryGetProperty("loadBalancers", out var lbEl) ? JsonElementToObject(lbEl) : new List<object?>(),
            ["serviceRegistries"] = data.TryGetProperty("serviceRegistries", out var srEl) ? JsonElementToObject(srEl) : new List<object?>(),
            ["healthCheckGracePeriodSeconds"] = GetInt(data, "healthCheckGracePeriodSeconds", 0),
            ["schedulingStrategy"] = GetString(data, "schedulingStrategy") ?? "REPLICA",
            ["deploymentController"] = data.TryGetProperty("deploymentController", out var dcEl) ? JsonElementToObject(dcEl) : new Dictionary<string, object?> { ["type"] = "ECS" },
            ["deploymentConfiguration"] = data.TryGetProperty("deploymentConfiguration", out var depCfgEl)
                ? JsonElementToObject(depCfgEl)
                : new Dictionary<string, object?>
                {
                    ["maximumPercent"] = 200,
                    ["minimumHealthyPercent"] = 100,
                    ["deploymentCircuitBreaker"] = new Dictionary<string, object?> { ["enable"] = false, ["rollback"] = false },
                },
            ["deployments"] = new List<object?> { deployment },
            ["events"] = new List<object?>
            {
                new Dictionary<string, object?>
                {
                    ["id"] = HashHelpers.NewUuid(),
                    ["createdAt"] = now,
                    ["message"] = $"(service {name}) has started 1 tasks: (task placeholder).",
                },
            },
            ["roleArn"] = GetString(data, "role") ?? "",
            ["createdAt"] = now,
            ["createdBy"] = $"arn:aws:iam::{accountId}:root",
            ["enableECSManagedTags"] = GetBool(data, "enableECSManagedTags", false),
            ["propagateTags"] = GetString(data, "propagateTags") ?? "NONE",
            ["enableExecuteCommand"] = GetBool(data, "enableExecuteCommand", false),
            ["tags"] = tags,
        };

        _services[svcKey] = svc;
        if (tags.Count > 0)
        {
            _tags[arn] = new List<Dictionary<string, string>>(tags);
        }

        RecountCluster(clusterName);
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["service"] = Sanitize(svc) });
    }

    private ServiceResponse ActDeleteService(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        var serviceRef = GetString(data, "service") ?? "";
        var svcName = ResolveServiceName(serviceRef);
        var svcKey = $"{clusterName}/{svcName}";

        if (!_services.TryGetValue(svcKey, out var svc))
        {
            return AwsResponseHelpers.ErrorResponseJson("ServiceNotFoundException", "Service not found.", 400);
        }

        var force = GetBool(data, "force", false);
        var desiredCount = svc.TryGetValue("desiredCount", out var dc) ? Convert.ToInt32(dc) : 0;
        if (!force && desiredCount > 0)
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidParameterException",
                "The service cannot be stopped while it is scaled above 0.", 400);
        }

        svc["status"] = "DRAINING";
        svc["runningCount"] = 0;
        svc["desiredCount"] = 0;

        var svcArn = svc.TryGetValue("serviceArn", out var sa) ? sa as string : null;
        if (svcArn is not null)
        {
            _tags.TryRemove(svcArn, out _);
        }

        _services.TryRemove(svcKey, out _);
        RecountCluster(clusterName);
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["service"] = Sanitize(svc) });
    }

    private ServiceResponse ActDescribeServices(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        var refs = GetStringList(data, "services");
        var include = new HashSet<string>(GetStringList(data, "include"), StringComparer.Ordinal);

        var result = new List<object?>();
        var failures = new List<object?>();

        foreach (var reference in refs)
        {
            var svcName = ResolveServiceName(reference);
            var svcKey = $"{clusterName}/{svcName}";
            if (_services.TryGetValue(svcKey, out var svc))
            {
                var copy = new Dictionary<string, object?>(svc, StringComparer.Ordinal);
                if (include.Contains("TAGS"))
                {
                    var svcArn = svc.TryGetValue("serviceArn", out var sa) ? sa as string : null;
                    if (svcArn is not null && _tags.TryGetValue(svcArn, out var tagList))
                    {
                        copy["tags"] = tagList;
                    }
                }

                result.Add(Sanitize(copy));
            }
            else
            {
                var accountId = AccountContext.GetAccountId();
                var arn = reference.StartsWith("arn:", StringComparison.Ordinal)
                    ? reference
                    : $"arn:aws:ecs:{Region}:{accountId}:service/{clusterName}/{reference}";
                failures.Add(new Dictionary<string, object?> { ["arn"] = arn, ["reason"] = "MISSING" });
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["services"] = result,
            ["failures"] = failures,
        });
    }

    private ServiceResponse ActUpdateService(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        var serviceRef = GetString(data, "service") ?? "";
        var svcName = ResolveServiceName(serviceRef);
        var svcKey = $"{clusterName}/{svcName}";

        if (!_services.TryGetValue(svcKey, out var svc))
        {
            return AwsResponseHelpers.ErrorResponseJson("ServiceNotFoundException", "Service not found.", 400);
        }

        var changed = false;
        var newTd = GetString(data, "taskDefinition");

        if (newTd is not null)
        {
            var tdKey = ResolveTdKey(newTd);
            string tdArn;
            if (_taskDefs.TryGetValue(tdKey, out var tdObj))
            {
                tdArn = tdObj.TryGetValue("taskDefinitionArn", out var a) ? a as string ?? newTd : newTd;
            }
            else
            {
                tdArn = newTd;
            }

            var currentTd = svc.TryGetValue("taskDefinition", out var ctd) ? ctd as string : null;
            if (tdArn != currentTd)
            {
                var deployments = svc.TryGetValue("deployments", out var deps) ? deps as List<object?> : new List<object?>();
                foreach (var dep in deployments!)
                {
                    if (dep is Dictionary<string, object?> d && d.TryGetValue("status", out var s) && s as string == "PRIMARY")
                    {
                        d["status"] = "ACTIVE";
                    }
                }

                var desiredCount = svc.TryGetValue("desiredCount", out var dcObj) ? Convert.ToInt32(dcObj) : 1;
                var newDep = MakeDeployment(tdArn, desiredCount, "PRIMARY");
                deployments.Insert(0, newDep);
                svc["taskDefinition"] = tdArn;
                changed = true;
            }
        }

        if (data.TryGetProperty("desiredCount", out var newDesiredEl) && newDesiredEl.TryGetInt32(out var newDesired))
        {
            svc["desiredCount"] = newDesired;
            svc["runningCount"] = newDesired;
            var deployments = svc.TryGetValue("deployments", out var deps) ? deps as List<object?> : null;
            if (deployments is { Count: > 0 } && deployments[0] is Dictionary<string, object?> firstDep)
            {
                firstDep["desiredCount"] = newDesired;
                firstDep["runningCount"] = newDesired;
                firstDep["updatedAt"] = TimeHelpers.NowIso();
            }

            changed = true;
        }

        if (data.TryGetProperty("networkConfiguration", out var ncEl))
        {
            svc["networkConfiguration"] = JsonElementToObject(ncEl);
        }

        if (data.TryGetProperty("healthCheckGracePeriodSeconds", out var hcEl) && hcEl.TryGetInt32(out var hcVal))
        {
            svc["healthCheckGracePeriodSeconds"] = hcVal;
        }

        if (data.TryGetProperty("enableExecuteCommand", out var execEl))
        {
            svc["enableExecuteCommand"] = execEl.ValueKind == JsonValueKind.True;
        }

        if (data.TryGetProperty("deploymentConfiguration", out var depCfgEl))
        {
            svc["deploymentConfiguration"] = JsonElementToObject(depCfgEl);
        }

        if (data.TryGetProperty("platformVersion", out var pvEl))
        {
            svc["platformVersion"] = pvEl.GetString();
        }

        if (data.TryGetProperty("loadBalancers", out var lbEl))
        {
            svc["loadBalancers"] = JsonElementToObject(lbEl);
        }

        if (data.TryGetProperty("capacityProviderStrategy", out var cpsEl))
        {
            svc["capacityProviderStrategy"] = JsonElementToObject(cpsEl);
        }

        if (changed)
        {
            var svcNameVal = svc.TryGetValue("serviceName", out var sn) ? sn as string : "";
            var events = svc.TryGetValue("events", out var ev) ? ev as List<object?> : new List<object?>();
            events!.Insert(0, new Dictionary<string, object?>
            {
                ["id"] = HashHelpers.NewUuid(),
                ["createdAt"] = TimeHelpers.NowIso(),
                ["message"] = $"(service {svcNameVal}) has begun draining connections on 1 tasks.",
            });
        }

        RecountCluster(clusterName);
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["service"] = Sanitize(svc) });
    }

    private ServiceResponse ActListServices(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        var launchType = GetString(data, "launchType");
        var scheduling = GetString(data, "schedulingStrategy");

        var arns = new List<string>();
        foreach (var (key, svc) in _services.Items)
        {
            if (!key.StartsWith($"{clusterName}/", StringComparison.Ordinal))
            {
                continue;
            }

            var status = svc.TryGetValue("status", out var s) ? s as string : null;
            if (status != "ACTIVE")
            {
                continue;
            }

            if (launchType is not null)
            {
                var svcLt = svc.TryGetValue("launchType", out var lt) ? lt as string : null;
                if (svcLt != launchType)
                {
                    continue;
                }
            }

            if (scheduling is not null)
            {
                var svcSched = svc.TryGetValue("schedulingStrategy", out var ss) ? ss as string : null;
                if (svcSched != scheduling)
                {
                    continue;
                }
            }

            var arn = svc.TryGetValue("serviceArn", out var sa) ? sa as string : null;
            if (arn is not null)
            {
                arns.Add(arn);
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["serviceArns"] = arns });
    }

    private ServiceResponse ActListServicesByNamespace(JsonElement data)
    {
        var namespaceName = GetString(data, "namespace") ?? "";
        var items = new List<string>();

        foreach (var svc in _services.Values)
        {
            if (!string.IsNullOrEmpty(namespaceName))
            {
                var svcNamespace = svc.TryGetValue("_namespace", out var ns) ? ns as string : "";
                if (svcNamespace != namespaceName)
                {
                    continue;
                }
            }

            var arn = svc.TryGetValue("serviceArn", out var sa) ? sa as string : null;
            if (arn is not null)
            {
                items.Add(arn);
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["serviceArns"] = items });
    }

    // -- Task actions ----------------------------------------------------------

    private ServiceResponse ActRunTask(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        EnsureCluster(clusterName);

        var tdRef = GetString(data, "taskDefinition") ?? "";
        var tdKey = ResolveTdKey(tdRef);
        if (!_taskDefs.TryGetValue(tdKey, out var td))
        {
            return AwsResponseHelpers.ErrorResponseJson("ClientException",
                $"Unable to find task definition: {tdRef}", 400);
        }

        var count = GetInt(data, "count", 1);
        var containerOverrides = new List<Dictionary<string, object?>>();
        if (data.TryGetProperty("overrides", out var overridesEl)
            && overridesEl.TryGetProperty("containerOverrides", out var coEl)
            && coEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var coItem in coEl.EnumerateArray())
            {
                containerOverrides.Add(JsonElementToDict(coItem));
            }
        }

        var launchType = GetString(data, "launchType") ?? "EC2";
        var group = GetString(data, "group") ?? "";
        var startedBy = GetString(data, "startedBy") ?? "";
        var enableExec = GetBool(data, "enableExecuteCommand", false);
        var reqTags = GetTags(data);
        var accountId = AccountContext.GetAccountId();

        var tasks = new List<object?>();
        var failures = new List<object?>();

        for (var i = 0; i < count; i++)
        {
            var taskId = HashHelpers.NewUuid();
            var taskArn = $"arn:aws:ecs:{Region}:{accountId}:task/{clusterName}/{taskId}";
            var now = TimeHelpers.NowIso();

            var containers = BuildTaskContainers(td, containerOverrides, taskArn);

            var overridesObj = data.TryGetProperty("overrides", out var ovEl)
                ? JsonElementToObject(ovEl)
                : new Dictionary<string, object?> { ["containerOverrides"] = new List<object?>(), ["inferenceAcceleratorOverrides"] = new List<object?>() };

            var task = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["taskArn"] = taskArn,
                ["clusterArn"] = _clusters[clusterName]["clusterArn"],
                ["taskDefinitionArn"] = td["taskDefinitionArn"],
                ["containerInstanceArn"] = $"arn:aws:ecs:{Region}:{accountId}:container-instance/{clusterName}/{HashHelpers.NewUuid()}",
                ["overrides"] = overridesObj,
                ["lastStatus"] = "RUNNING",
                ["desiredStatus"] = "RUNNING",
                ["launchType"] = launchType,
                ["cpu"] = td.TryGetValue("cpu", out var cpuVal) ? cpuVal : "256",
                ["memory"] = td.TryGetValue("memory", out var memVal) ? memVal : "512",
                ["platformVersion"] = GetString(data, "platformVersion") ?? "",
                ["platformFamily"] = "",
                ["connectivity"] = "CONNECTED",
                ["connectivityAt"] = now,
                ["pullStartedAt"] = now,
                ["pullStoppedAt"] = now,
                ["createdAt"] = now,
                ["startedAt"] = now,
                ["stoppingAt"] = null,
                ["stoppedAt"] = null,
                ["stoppedReason"] = "",
                ["stopCode"] = "",
                ["group"] = group,
                ["startedBy"] = startedBy,
                ["version"] = 1,
                ["containers"] = containers,
                ["attachments"] = new List<object?>(),
                ["availabilityZone"] = $"{Region}a",
                ["enableExecuteCommand"] = enableExec,
                ["tags"] = reqTags,
                ["healthStatus"] = "UNKNOWN",
                ["ephemeralStorage"] = td.TryGetValue("ephemeralStorage", out var ephVal) ? ephVal : new Dictionary<string, object?> { ["sizeInGiB"] = 20 },
            };

            _tasks[taskArn] = task;
            if (reqTags.Count > 0)
            {
                _tags[taskArn] = new List<Dictionary<string, string>>(reqTags);
            }

            tasks.Add(Sanitize(task));
        }

        RecountCluster(clusterName);
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["tasks"] = tasks,
            ["failures"] = failures,
        });
    }

    private List<object?> BuildTaskContainers(
        Dictionary<string, object?> td,
        List<Dictionary<string, object?>> containerOverrides,
        string taskArn)
    {
        var containers = new List<object?>();
        var containerDefs = td.TryGetValue("containerDefinitions", out var cdVal) ? cdVal as List<object?> : null;
        if (containerDefs is null)
        {
            return containers;
        }

        var accountId = AccountContext.GetAccountId();

        foreach (var cdefObj in containerDefs)
        {
            if (cdefObj is not Dictionary<string, object?> cdef)
            {
                continue;
            }

            var cdefName = cdef.TryGetValue("name", out var n) ? n as string ?? "" : "";

            // Build environment: merge base + overrides
            var envOverride = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var ov in containerOverrides)
            {
                var ovName = ov.TryGetValue("name", out var ovn) ? ovn as string : null;
                if (ovName == cdefName && ov.TryGetValue("environment", out var envList) && envList is List<object?> envListTyped)
                {
                    foreach (var e in envListTyped)
                    {
                        if (e is Dictionary<string, object?> envEntry)
                        {
                            var eName = envEntry.TryGetValue("name", out var en) ? en as string : null;
                            var eValue = envEntry.TryGetValue("value", out var ev) ? ev as string : null;
                            if (eName is not null)
                            {
                                envOverride[eName] = eValue ?? "";
                            }
                        }
                    }
                }
            }

            var cpu = cdef.TryGetValue("cpu", out var cpuObj) ? Convert.ToInt32(cpuObj) : 0;
            var memory = cdef.TryGetValue("memory", out var memObj) && memObj is not null && Convert.ToInt32(memObj) != 0
                ? Convert.ToInt32(memObj)
                : (cdef.TryGetValue("memoryReservation", out var mrObj) ? Convert.ToInt32(mrObj) : 0);

            containers.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["containerArn"] = $"arn:aws:ecs:{Region}:{accountId}:container/{HashHelpers.NewUuid()}",
                ["taskArn"] = taskArn,
                ["name"] = cdefName,
                ["image"] = cdef.TryGetValue("image", out var img) ? img as string ?? "" : "",
                ["lastStatus"] = "RUNNING",
                ["exitCode"] = null,
                ["networkBindings"] = new List<object?>(),
                ["networkInterfaces"] = new List<object?>(),
                ["cpu"] = cpu.ToString(),
                ["memory"] = memory.ToString(),
                ["runtimeId"] = HashHelpers.NewUuid()[..12],
                ["healthStatus"] = "UNKNOWN",
            });
        }

        return containers;
    }

    private ServiceResponse ActStopTask(JsonElement data)
    {
        var taskRef = GetString(data, "task") ?? "";
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        var reason = GetString(data, "reason") ?? "Task stopped by user";

        var task = ResolveTask(taskRef, clusterName);
        if (task is null)
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidParameterException",
                "The referenced task was not found.", 400);
        }

        var lastStatus = task.TryGetValue("lastStatus", out var ls) ? ls as string : null;
        if (lastStatus == "STOPPED")
        {
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["task"] = Sanitize(task) });
        }

        var now = TimeHelpers.NowIso();
        task["lastStatus"] = "STOPPED";
        task["desiredStatus"] = "STOPPED";
        task["stoppingAt"] = now;
        task["stoppedAt"] = now;
        task["stoppedReason"] = reason;
        task["stopCode"] = "UserInitiated";

        if (task.TryGetValue("containers", out var containersObj) && containersObj is List<object?> containersList)
        {
            foreach (var cObj in containersList)
            {
                if (cObj is Dictionary<string, object?> c)
                {
                    c["lastStatus"] = "STOPPED";
                    c["exitCode"] = 0;
                }
            }
        }

        var cname = ClusterNameFromArn(task.TryGetValue("clusterArn", out var ca) ? ca as string : null);
        if (!string.IsNullOrEmpty(cname))
        {
            RecountCluster(cname);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["task"] = Sanitize(task) });
    }

    private ServiceResponse ActDescribeTasks(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        var taskRefs = GetStringList(data, "tasks");
        var include = new HashSet<string>(GetStringList(data, "include"), StringComparer.Ordinal);

        var result = new List<object?>();
        var failures = new List<object?>();

        foreach (var reference in taskRefs)
        {
            var task = ResolveTask(reference, clusterName);
            if (task is not null)
            {
                var t = Sanitize(task);
                if (include.Contains("TAGS"))
                {
                    var tArn = task.TryGetValue("taskArn", out var ta) ? ta as string : null;
                    if (tArn is not null && _tags.TryGetValue(tArn, out var tagList))
                    {
                        t["tags"] = tagList;
                    }
                }

                result.Add(t);
            }
            else
            {
                var accountId = AccountContext.GetAccountId();
                var arn = reference.StartsWith("arn:", StringComparison.Ordinal)
                    ? reference
                    : $"arn:aws:ecs:{Region}:{accountId}:task/{clusterName}/{reference}";
                failures.Add(new Dictionary<string, object?> { ["arn"] = arn, ["reason"] = "MISSING" });
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["tasks"] = result,
            ["failures"] = failures,
        });
    }

    private ServiceResponse ActListTasks(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        var accountId = AccountContext.GetAccountId();
        var clusterArn = $"arn:aws:ecs:{Region}:{accountId}:cluster/{clusterName}";
        var statusFilter = GetString(data, "desiredStatus") ?? "RUNNING";
        var family = GetString(data, "family") ?? "";
        var serviceName = GetString(data, "serviceName") ?? "";
        var startedBy = GetString(data, "startedBy") ?? "";

        var arns = new List<string>();
        foreach (var (arn, t) in _tasks.Items)
        {
            var tClusterArn = t.TryGetValue("clusterArn", out var tca) ? tca as string : null;
            if (tClusterArn != clusterArn)
            {
                continue;
            }

            var desiredStatus = t.TryGetValue("desiredStatus", out var ds) ? ds as string : null;
            if (desiredStatus != statusFilter)
            {
                continue;
            }

            if (!string.IsNullOrEmpty(family))
            {
                var tdArn = t.TryGetValue("taskDefinitionArn", out var tda) ? tda as string ?? "" : "";
                if (!tdArn.Contains($"/{family}:", StringComparison.Ordinal))
                {
                    continue;
                }
            }

            if (!string.IsNullOrEmpty(serviceName))
            {
                var groupVal = t.TryGetValue("group", out var g) ? g as string : null;
                var startedByVal = t.TryGetValue("startedBy", out var sb) ? sb as string : null;
                if (groupVal != $"service:{serviceName}" && startedByVal != serviceName)
                {
                    continue;
                }
            }

            if (!string.IsNullOrEmpty(startedBy))
            {
                var startedByVal = t.TryGetValue("startedBy", out var sb) ? sb as string : null;
                if (startedByVal != startedBy)
                {
                    continue;
                }
            }

            arns.Add(arn);
        }

        var maxResults = GetInt(data, "maxResults", 100);
        var nextToken = GetString(data, "nextToken");
        var start = !string.IsNullOrEmpty(nextToken) && int.TryParse(nextToken, out var s) ? s : 0;
        var page = arns.GetRange(start, Math.Min(maxResults, arns.Count - start));

        var resp = new Dictionary<string, object?>(StringComparer.Ordinal) { ["taskArns"] = page };
        if (start + maxResults < arns.Count)
        {
            resp["nextToken"] = (start + maxResults).ToString();
        }

        return AwsResponseHelpers.JsonResponse(resp);
    }

    private ServiceResponse ActExecuteCommand(JsonElement data)
    {
        var clusterName = ResolveClusterName(GetString(data, "cluster") ?? "default");
        var taskRef = GetString(data, "task") ?? "";
        var task = ResolveTask(taskRef, clusterName);
        if (task is null)
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidParameterException",
                "The referenced task was not found.", 400);
        }

        var containerName = GetString(data, "container") ?? "";
        if (string.IsNullOrEmpty(containerName))
        {
            var containers = task.TryGetValue("containers", out var cObj) ? cObj as List<object?> : null;
            if (containers is { Count: > 0 } && containers[0] is Dictionary<string, object?> first)
            {
                containerName = first.TryGetValue("name", out var n) ? n as string ?? "" : "";
            }
        }

        var containerArn = "";
        if (task.TryGetValue("containers", out var containersObj) && containersObj is List<object?> containersList)
        {
            foreach (var cObj in containersList)
            {
                if (cObj is Dictionary<string, object?> c)
                {
                    var cName = c.TryGetValue("name", out var cn) ? cn as string : null;
                    if (cName == containerName)
                    {
                        containerArn = c.TryGetValue("containerArn", out var ca) ? ca as string ?? "" : "";
                        break;
                    }
                }
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["clusterArn"] = task["clusterArn"],
            ["taskArn"] = task["taskArn"],
            ["containerArn"] = containerArn,
            ["containerName"] = containerName,
            ["interactive"] = GetBool(data, "interactive", true),
            ["session"] = new Dictionary<string, object?>
            {
                ["sessionId"] = HashHelpers.NewUuid(),
                ["streamUrl"] = $"wss://ssmmessages.{Region}.amazonaws.com/v1/data-channel/{HashHelpers.NewUuid()}",
                ["tokenValue"] = HashHelpers.NewUuid(),
            },
        });
    }

    // -- Tag actions -----------------------------------------------------------

    private ServiceResponse ActTagResource(JsonElement data)
    {
        var arn = GetString(data, "resourceArn") ?? "";
        if (string.IsNullOrEmpty(arn))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidParameterException", "resourceArn is required", 400);
        }

        var newTags = GetTags(data);
        if (!_tags.TryGetValue(arn, out var existing))
        {
            existing = [];
        }

        var existingKeys = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < existing.Count; i++)
        {
            if (existing[i].TryGetValue("key", out var k))
            {
                existingKeys[k] = i;
            }
        }

        foreach (var tag in newTags)
        {
            if (tag.TryGetValue("key", out var k) && existingKeys.TryGetValue(k, out var idx))
            {
                existing[idx] = tag;
            }
            else
            {
                existing.Add(tag);
                if (tag.TryGetValue("key", out var k2))
                {
                    existingKeys[k2] = existing.Count - 1;
                }
            }
        }

        _tags[arn] = existing;
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActUntagResource(JsonElement data)
    {
        var arn = GetString(data, "resourceArn") ?? "";
        if (string.IsNullOrEmpty(arn))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidParameterException", "resourceArn is required", 400);
        }

        var keysToRemove = new HashSet<string>(GetStringList(data, "tagKeys"), StringComparer.Ordinal);
        if (!_tags.TryGetValue(arn, out var existing))
        {
            existing = [];
        }

        _tags[arn] = existing.Where(t => !t.TryGetValue("key", out var k) || !keysToRemove.Contains(k)).ToList();
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActListTagsForResource(JsonElement data)
    {
        var arn = GetString(data, "resourceArn") ?? "";
        if (string.IsNullOrEmpty(arn))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidParameterException", "resourceArn is required", 400);
        }

        var tags = _tags.TryGetValue(arn, out var existing) ? existing : new List<Dictionary<string, string>>();
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["tags"] = tags });
    }

    // -- Account Settings actions ----------------------------------------------

    private ServiceResponse ActListAccountSettings(JsonElement data)
    {
        var name = GetString(data, "name") ?? "";
        var allNames = new[]
        {
            "serviceLongArnFormat", "taskLongArnFormat",
            "containerInstanceLongArnFormat", "awsvpcTrunking",
            "containerInsights", "fargateTaskRetirementWaitPeriod",
            "dualStackIPv6", "fargateFIPSMode", "tagResourceAuthorization",
            "guardDutyActivate",
        };

        var accountId = AccountContext.GetAccountId();
        var settings = new List<object?>();
        foreach (var settingName in allNames)
        {
            if (!string.IsNullOrEmpty(name) && settingName != name)
            {
                continue;
            }

            var hasUserSetting = _accountSettings.TryGetValue(settingName, out var settingValue);
            settings.Add(new Dictionary<string, object?>
            {
                ["name"] = settingName,
                ["value"] = hasUserSetting ? settingValue?.ToString() ?? "enabled" : "enabled",
                ["principalArn"] = $"arn:aws:iam::{accountId}:root",
                ["type"] = hasUserSetting ? "user" : "aws",
            });
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["settings"] = settings });
    }

    private ServiceResponse ActPutAccountSetting(JsonElement data)
    {
        var name = GetString(data, "name") ?? "";
        var value = GetString(data, "value") ?? "enabled";
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidParameterException", "name is required", 400);
        }

        var accountId = AccountContext.GetAccountId();
        _accountSettings[name] = value;
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["setting"] = new Dictionary<string, object?>
            {
                ["name"] = name,
                ["value"] = value,
                ["principalArn"] = $"arn:aws:iam::{accountId}:root",
                ["type"] = "user",
            },
        });
    }

    private ServiceResponse ActPutAccountSettingDefault(JsonElement data)
    {
        var name = GetString(data, "name") ?? "";
        var value = GetString(data, "value") ?? "";
        var accountId = AccountContext.GetAccountId();
        var setting = new Dictionary<string, object?>
        {
            ["name"] = name,
            ["value"] = value,
            ["principalArn"] = $"arn:aws:iam::{accountId}:root",
        };

        _accountSettings[name] = setting;
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["setting"] = setting });
    }

    private ServiceResponse ActDeleteAccountSetting(JsonElement data)
    {
        var name = GetString(data, "name") ?? "";
        _accountSettings.TryRemove(name, out _);
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["setting"] = new Dictionary<string, object?> { ["name"] = name, ["value"] = "" },
        });
    }

    // -- Capacity Provider actions ----------------------------------------------

    private ServiceResponse ActCreateCapacityProvider(JsonElement data)
    {
        var name = GetString(data, "name") ?? "";
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidParameterException", "name is required", 400);
        }

        if (_capacityProviders.ContainsKey(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidParameterException",
                $"Capacity provider {name} already exists.", 400);
        }

        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:ecs:{Region}:{accountId}:capacity-provider/{name}";
        var asgProvider = data.TryGetProperty("autoScalingGroupProvider", out var asgEl)
            ? JsonElementToDict(asgEl)
            : new Dictionary<string, object?>();

        var tags = GetTags(data);

        var cp = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["capacityProviderArn"] = arn,
            ["name"] = name,
            ["status"] = "ACTIVE",
            ["autoScalingGroupProvider"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["autoScalingGroupArn"] = asgProvider.TryGetValue("autoScalingGroupArn", out var asgArn) ? asgArn as string ?? "" : "",
                ["managedScaling"] = asgProvider.TryGetValue("managedScaling", out var ms) ? ms : new Dictionary<string, object?>
                {
                    ["status"] = "DISABLED",
                    ["targetCapacity"] = 100,
                    ["minimumScalingStepSize"] = 1,
                    ["maximumScalingStepSize"] = 10000,
                    ["instanceWarmupPeriod"] = 300,
                },
                ["managedTerminationProtection"] = asgProvider.TryGetValue("managedTerminationProtection", out var mtp) ? mtp as string ?? "DISABLED" : "DISABLED",
            },
            ["updateStatus"] = "UPDATE_COMPLETE",
            ["tags"] = tags,
        };

        _capacityProviders[name] = cp;
        if (tags.Count > 0)
        {
            _tags[arn] = new List<Dictionary<string, string>>(tags);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["capacityProvider"] = cp });
    }

    private ServiceResponse ActDeleteCapacityProvider(JsonElement data)
    {
        var name = GetString(data, "capacityProvider") ?? "";
        if (name.StartsWith("arn:", StringComparison.Ordinal))
        {
            name = name[(name.LastIndexOf('/') + 1)..];
        }

        if (!_capacityProviders.TryRemove(name, out var cp))
        {
            return AwsResponseHelpers.ErrorResponseJson("InvalidParameterException",
                $"Capacity provider {name} not found.", 400);
        }

        var cpArn = cp.TryGetValue("capacityProviderArn", out var a) ? a as string : null;
        if (cpArn is not null)
        {
            _tags.TryRemove(cpArn, out _);
        }

        cp["status"] = "INACTIVE";
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["capacityProvider"] = cp });
    }

    private ServiceResponse ActUpdateCapacityProvider(JsonElement data)
    {
        var name = GetString(data, "name") ?? "";
        if (!_capacityProviders.TryGetValue(name, out var cp))
        {
            return AwsResponseHelpers.ErrorResponseJson("ClientException", $"Capacity provider {name} not found", 400);
        }

        if (data.TryGetProperty("autoScalingGroupProvider", out var asgEl))
        {
            var update = JsonElementToDict(asgEl);
            if (cp.TryGetValue("autoScalingGroupProvider", out var existingObj) && existingObj is Dictionary<string, object?> existing)
            {
                foreach (var (k, v) in update)
                {
                    existing[k] = v;
                }
            }
        }

        cp["updateStatus"] = "UPDATE_COMPLETE";
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["capacityProvider"] = cp });
    }

    private ServiceResponse ActDescribeCapacityProviders(JsonElement data)
    {
        var names = GetStringList(data, "capacityProviders");
        var include = GetStringList(data, "include");
        var providers = new List<object?>();

        var accountId = AccountContext.GetAccountId();

        // Built-in defaults
        var defaults = new[]
        {
            ("FARGATE", "ACTIVE"),
            ("FARGATE_SPOT", "ACTIVE"),
        };

        foreach (var (pName, pStatus) in defaults)
        {
            if (names.Count > 0 && !names.Contains(pName))
            {
                continue;
            }

            var cpArn = $"arn:aws:ecs:{Region}:{accountId}:capacity-provider/{pName}";
            var cp = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["capacityProviderArn"] = cpArn,
                ["name"] = pName,
                ["status"] = pStatus,
                ["autoScalingGroupProvider"] = new Dictionary<string, object?>(),
                ["updateStatus"] = "UPDATE_COMPLETE",
            };

            if (include.Contains("TAGS") && _tags.TryGetValue(cpArn, out var tagList))
            {
                cp["tags"] = tagList;
            }

            providers.Add(cp);
        }

        foreach (var (cpName, cpObj) in _capacityProviders.Items)
        {
            if (names.Count > 0 && !names.Contains(cpName))
            {
                continue;
            }

            var entry = new Dictionary<string, object?>(cpObj, StringComparer.Ordinal);
            if (include.Contains("TAGS"))
            {
                var cpArn = cpObj.TryGetValue("capacityProviderArn", out var a) ? a as string : null;
                if (cpArn is not null && _tags.TryGetValue(cpArn, out var tagList))
                {
                    entry["tags"] = tagList;
                }
            }

            providers.Add(entry);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["capacityProviders"] = providers });
    }

    // -- Attributes actions ----------------------------------------------------

    private ServiceResponse ActPutAttributes(JsonElement data)
    {
        var attrs = new List<object?>();
        if (data.TryGetProperty("attributes", out var attrsEl) && attrsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var attrEl in attrsEl.EnumerateArray())
            {
                var attr = JsonElementToDict(attrEl);
                var targetId = attr.TryGetValue("targetId", out var tid) ? tid as string ?? "" : "";
                var attrName = attr.TryGetValue("name", out var n) ? n as string ?? "" : "";
                _attributes[$"{targetId}:{attrName}"] = attr;
                attrs.Add(attr);
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["attributes"] = attrs });
    }

    private ServiceResponse ActDeleteAttributes(JsonElement data)
    {
        var attrs = new List<object?>();
        if (data.TryGetProperty("attributes", out var attrsEl) && attrsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var attrEl in attrsEl.EnumerateArray())
            {
                var attr = JsonElementToDict(attrEl);
                var targetId = attr.TryGetValue("targetId", out var tid) ? tid as string ?? "" : "";
                var attrName = attr.TryGetValue("name", out var n) ? n as string ?? "" : "";
                _attributes.TryRemove($"{targetId}:{attrName}", out _);
                attrs.Add(attr);
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["attributes"] = attrs });
    }

    private ServiceResponse ActListAttributes(JsonElement data)
    {
        var targetType = GetString(data, "targetType") ?? "";
        var attrName = GetString(data, "attributeName") ?? "";

        var results = new List<object?>();
        foreach (var attr in _attributes.Values)
        {
            if (!string.IsNullOrEmpty(targetType))
            {
                var tt = attr.TryGetValue("targetType", out var t) ? t as string : "";
                if (tt != targetType)
                {
                    continue;
                }
            }

            if (!string.IsNullOrEmpty(attrName))
            {
                var an = attr.TryGetValue("name", out var n) ? n as string : "";
                if (an != attrName)
                {
                    continue;
                }
            }

            results.Add(attr);
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["attributes"] = results });
    }

    // -- Stub actions ----------------------------------------------------------

    private static ServiceResponse ActDescribeServiceDeployments()
    {
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["serviceDeployments"] = new List<object?>() });
    }

    private static ServiceResponse ActListServiceDeployments()
    {
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["serviceDeployments"] = new List<object?>() });
    }

    private static ServiceResponse ActDescribeServiceRevisions()
    {
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["serviceRevisions"] = new List<object?>() });
    }

    private static ServiceResponse ActSubmitStateChange()
    {
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["acknowledgment"] = "ACCEPT" });
    }

    private static ServiceResponse ActDiscoverPollEndpoint()
    {
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["endpoint"] = "http://localhost:4566",
            ["telemetryEndpoint"] = "http://localhost:4566",
        });
    }

    private static ServiceResponse ActUpdateTaskProtection()
    {
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["protectedTasks"] = new List<object?>(),
            ["failures"] = new List<object?>(),
        });
    }

    private static ServiceResponse ActGetTaskProtection()
    {
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["protectedTasks"] = new List<object?>(),
            ["failures"] = new List<object?>(),
        });
    }
}
