using System.Text.Json;
using System.Text.RegularExpressions;
using MicroStack.Internal;

namespace MicroStack.Services.S3Files;

/// <summary>
/// S3 Files (S3 Access Grants / S3-backed file systems) service handler — REST/JSON protocol
/// with path-based routing.
///
/// Port of ministack/services/s3files.py.
///
/// Supports:
///   File Systems:    CreateFileSystem, GetFileSystem, ListFileSystems, DeleteFileSystem
///   Mount Targets:   CreateMountTarget, GetMountTarget, ListMountTargets,
///                    DeleteMountTarget, UpdateMountTarget
///   Access Points:   CreateAccessPoint, GetAccessPoint, ListAccessPoints, DeleteAccessPoint
///   Policies:        GetFileSystemPolicy, PutFileSystemPolicy, DeleteFileSystemPolicy
///   Sync:            GetSynchronizationConfiguration, PutSynchronizationConfiguration
///   Tags:            TagResource, UntagResource, ListTagsForResource
/// </summary>
internal sealed partial class S3FilesServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // ── State ────────────────────────────────────────────────────────────────
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _fileSystems = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _mountTargets = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _accessPoints = new();
    private readonly AccountScopedDictionary<string, string> _policies = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _syncConfigs = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, object?>>> _tags = new();

    // ── Regex patterns ───────────────────────────────────────────────────────
    [GeneratedRegex(@"^/file-systems/([^/]+)$")]
    private static partial Regex FileSystemIdRegex();

    [GeneratedRegex(@"^/file-systems/([^/]+)/policy$")]
    private static partial Regex FileSystemPolicyRegex();

    [GeneratedRegex(@"^/file-systems/([^/]+)/synchronization-configuration$")]
    private static partial Regex SyncConfigRegex();

    [GeneratedRegex(@"^/mount-targets/([^/]+)$")]
    private static partial Regex MountTargetIdRegex();

    [GeneratedRegex(@"^/access-points/([^/]+)$")]
    private static partial Regex AccessPointIdRegex();

    [GeneratedRegex(@"^/tags/(.+)$")]
    private static partial Regex TagsRegex();

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "s3files";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        JsonElement body;
        if (request.Body.Length > 0)
        {
            try
            {
                using var doc = JsonDocument.Parse(request.Body);
                body = doc.RootElement.Clone();
            }
            catch (JsonException)
            {
                body = JsonDocument.Parse("{}").RootElement.Clone();
            }
        }
        else
        {
            body = JsonDocument.Parse("{}").RootElement.Clone();
        }

        var method = request.Method;
        var path = request.Path;

        // Flatten query params
        var queryParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in request.QueryParams)
        {
            if (v.Length > 0) queryParams[k] = v[0];
        }

        ServiceResponse response;
        lock (_lock)
        {
            response = DispatchRequest(method, path, body, queryParams, request.QueryParams);
        }

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _fileSystems.Clear();
            _mountTargets.Clear();
            _accessPoints.Clear();
            _policies.Clear();
            _syncConfigs.Clear();
            _tags.Clear();
        }
    }

    public JsonElement? GetState() => null;

    public void RestoreState(JsonElement state) { }

    // ═══════════════════════════════════════════════════════════════════════════
    // Request router
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse DispatchRequest(
        string method,
        string path,
        JsonElement body,
        Dictionary<string, string> query,
        IReadOnlyDictionary<string, string[]> rawQuery)
    {
        // POST /file-systems
        if (path == "/file-systems")
        {
            if (method == "POST") return CreateFileSystem(body);
            if (method == "GET") return ListFileSystems();
        }

        // GET|DELETE /file-systems/{id}
        var fsMatch = FileSystemIdRegex().Match(path);
        if (fsMatch.Success)
        {
            var fsId = fsMatch.Groups[1].Value;
            if (method == "GET") return GetFileSystem(fsId);
            if (method == "DELETE") return DeleteFileSystem(fsId);
        }

        // GET|PUT|DELETE /file-systems/{id}/policy
        var fpMatch = FileSystemPolicyRegex().Match(path);
        if (fpMatch.Success)
        {
            var fsId = fpMatch.Groups[1].Value;
            if (method == "GET") return GetFileSystemPolicy(fsId);
            if (method == "PUT") return PutFileSystemPolicy(fsId, body);
            if (method == "DELETE") return DeleteFileSystemPolicy(fsId);
        }

        // GET|PUT /file-systems/{id}/synchronization-configuration
        var scMatch = SyncConfigRegex().Match(path);
        if (scMatch.Success)
        {
            var fsId = scMatch.Groups[1].Value;
            if (method == "GET") return GetSyncConfig(fsId);
            if (method == "PUT") return PutSyncConfig(fsId, body);
        }

        // POST /mount-targets
        if (path == "/mount-targets")
        {
            if (method == "POST") return CreateMountTarget(body);
            if (method == "GET") return ListMountTargets(query);
        }

        // GET|PUT|DELETE /mount-targets/{id}
        var mtMatch = MountTargetIdRegex().Match(path);
        if (mtMatch.Success)
        {
            var mtId = mtMatch.Groups[1].Value;
            if (method == "GET") return GetMountTarget(mtId);
            if (method == "PUT") return UpdateMountTarget(mtId, body);
            if (method == "DELETE") return DeleteMountTarget(mtId);
        }

        // POST /access-points
        if (path == "/access-points")
        {
            if (method == "POST") return CreateAccessPoint(body);
            if (method == "GET") return ListAccessPoints();
        }

        // GET|DELETE /access-points/{id}
        var apMatch = AccessPointIdRegex().Match(path);
        if (apMatch.Success)
        {
            var apId = apMatch.Groups[1].Value;
            if (method == "GET") return GetAccessPoint(apId);
            if (method == "DELETE") return DeleteAccessPoint(apId);
        }

        // POST|DELETE|GET /tags/{arn}
        var tagMatch = TagsRegex().Match(path);
        if (tagMatch.Success)
        {
            var resourceArn = Uri.UnescapeDataString(tagMatch.Groups[1].Value);
            if (method == "POST") return TagResource(resourceArn, body);
            if (method == "DELETE")
            {
                var tagKeys = rawQuery.TryGetValue("tagKeys", out var tagKeysArr) && tagKeysArr.Length > 0
                    ? tagKeysArr.ToList()
                    : [];
                return UntagResource(resourceArn, tagKeys);
            }
            if (method == "GET") return ListTags(resourceArn);
        }

        return S3FilesError(400, "InvalidRequest", $"Unknown S3 Files route: {method} {path}");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // File Systems
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse CreateFileSystem(JsonElement body)
    {
        var fsId = $"fs-{HashHelpers.NewUuidNoDashes()[..17]}";
        var bucketName = GetStr(body, "BucketName") ?? "";
        var arn = $"arn:aws:s3files:{Region}:{AccountContext.GetAccountId()}:file-system/{fsId}";
        var now = NowIso();

        var fs = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["FileSystemId"] = fsId,
            ["FileSystemArn"] = arn,
            ["BucketName"] = bucketName,
            ["LifeCycleState"] = "available",
            ["CreationTime"] = now,
            ["OwnerId"] = AccountContext.GetAccountId(),
        };

        _fileSystems[fsId] = fs;
        return S3FilesJson(201, fs);
    }

    private ServiceResponse GetFileSystem(string fsId)
    {
        if (!_fileSystems.TryGetValue(fsId, out var fs))
            return S3FilesError(404, "FileSystemNotFound", $"File system {fsId} not found");
        return S3FilesJson(200, fs);
    }

    private ServiceResponse ListFileSystems()
    {
        var items = _fileSystems.Values.ToList();
        return S3FilesJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["FileSystems"] = items,
        });
    }

    private ServiceResponse DeleteFileSystem(string fsId)
    {
        if (!_fileSystems.ContainsKey(fsId))
            return S3FilesError(404, "FileSystemNotFound", $"File system {fsId} not found");
        _fileSystems.TryRemove(fsId, out _);
        _policies.TryRemove(fsId, out _);
        _syncConfigs.TryRemove(fsId, out _);
        return S3FilesEmpty(204);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Mount Targets
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse CreateMountTarget(JsonElement body)
    {
        var mtId = $"fsmt-{HashHelpers.NewUuidNoDashes()[..17]}";
        var fsId = GetStr(body, "FileSystemId") ?? "";
        var subnetId = GetStr(body, "SubnetId") ?? "";

        var mt = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["MountTargetId"] = mtId,
            ["FileSystemId"] = fsId,
            ["SubnetId"] = subnetId,
            ["LifeCycleState"] = "available",
            ["IpAddress"] = GetStr(body, "IpAddress") ?? "10.0.0.1",
            ["VpcId"] = GetStr(body, "VpcId") ?? "vpc-00000001",
            ["AvailabilityZone"] = GetStr(body, "AvailabilityZone") ?? $"{Region}a",
        };

        _mountTargets[mtId] = mt;
        return S3FilesJson(201, mt);
    }

    private ServiceResponse GetMountTarget(string mtId)
    {
        if (!_mountTargets.TryGetValue(mtId, out var mt))
            return S3FilesError(404, "MountTargetNotFound", $"Mount target {mtId} not found");
        return S3FilesJson(200, mt);
    }

    private ServiceResponse ListMountTargets(Dictionary<string, string> query)
    {
        var items = _mountTargets.Values.ToList();
        if (query.TryGetValue("FileSystemId", out var fsId) && !string.IsNullOrEmpty(fsId))
        {
            items = items.Where(mt => (string?)mt["FileSystemId"] == fsId).ToList();
        }
        return S3FilesJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["MountTargets"] = items,
        });
    }

    private ServiceResponse UpdateMountTarget(string mtId, JsonElement body)
    {
        if (!_mountTargets.TryGetValue(mtId, out var mt))
            return S3FilesError(404, "MountTargetNotFound", $"Mount target {mtId} not found");

        foreach (var key in new[] { "SubnetId", "IpAddress", "SecurityGroups" })
        {
            if (body.TryGetProperty(key, out var val))
            {
                mt[key] = val.ValueKind == JsonValueKind.String ? val.GetString() : JsonElementToObject(val);
            }
        }

        return S3FilesJson(200, mt);
    }

    private ServiceResponse DeleteMountTarget(string mtId)
    {
        if (!_mountTargets.ContainsKey(mtId))
            return S3FilesError(404, "MountTargetNotFound", $"Mount target {mtId} not found");
        _mountTargets.TryRemove(mtId, out _);
        return S3FilesEmpty(204);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Access Points
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse CreateAccessPoint(JsonElement body)
    {
        var apId = $"fsap-{HashHelpers.NewUuidNoDashes()[..17]}";
        var fsId = GetStr(body, "FileSystemId") ?? "";
        var arn = $"arn:aws:s3files:{Region}:{AccountContext.GetAccountId()}:access-point/{apId}";

        var ap = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["AccessPointId"] = apId,
            ["AccessPointArn"] = arn,
            ["FileSystemId"] = fsId,
            ["Name"] = GetStr(body, "Name") ?? "",
            ["LifeCycleState"] = "available",
        };

        _accessPoints[apId] = ap;
        return S3FilesJson(201, ap);
    }

    private ServiceResponse GetAccessPoint(string apId)
    {
        if (!_accessPoints.TryGetValue(apId, out var ap))
            return S3FilesError(404, "AccessPointNotFound", $"Access point {apId} not found");
        return S3FilesJson(200, ap);
    }

    private ServiceResponse ListAccessPoints()
    {
        var items = _accessPoints.Values.ToList();
        return S3FilesJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["AccessPoints"] = items,
        });
    }

    private ServiceResponse DeleteAccessPoint(string apId)
    {
        if (!_accessPoints.ContainsKey(apId))
            return S3FilesError(404, "AccessPointNotFound", $"Access point {apId} not found");
        _accessPoints.TryRemove(apId, out _);
        return S3FilesEmpty(204);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Policies
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse GetFileSystemPolicy(string fsId)
    {
        if (!_policies.TryGetValue(fsId, out var policy))
            return S3FilesError(404, "PolicyNotFound", $"No policy for file system {fsId}");
        return S3FilesJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["FileSystemId"] = fsId,
            ["Policy"] = policy,
        });
    }

    private ServiceResponse PutFileSystemPolicy(string fsId, JsonElement body)
    {
        if (!_fileSystems.ContainsKey(fsId))
            return S3FilesError(404, "FileSystemNotFound", $"File system {fsId} not found");
        _policies[fsId] = GetStr(body, "Policy") ?? "";
        return S3FilesJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["FileSystemId"] = fsId,
            ["Policy"] = _policies[fsId],
        });
    }

    private ServiceResponse DeleteFileSystemPolicy(string fsId)
    {
        _policies.TryRemove(fsId, out _);
        return S3FilesEmpty(204);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Synchronization Configuration
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse GetSyncConfig(string fsId)
    {
        var config = _syncConfigs.TryGetValue(fsId, out var sc)
            ? sc
            : new Dictionary<string, object?>(StringComparer.Ordinal);
        return S3FilesJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["FileSystemId"] = fsId,
            ["SynchronizationConfiguration"] = config,
        });
    }

    private ServiceResponse PutSyncConfig(string fsId, JsonElement body)
    {
        if (!_fileSystems.ContainsKey(fsId))
            return S3FilesError(404, "FileSystemNotFound", $"File system {fsId} not found");

        var config = body.TryGetProperty("SynchronizationConfiguration", out var scEl)
            ? JsonElementToDict(scEl)
            : JsonElementToDict(body);
        _syncConfigs[fsId] = config;
        return S3FilesJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["FileSystemId"] = fsId,
            ["SynchronizationConfiguration"] = config,
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tags
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse TagResource(string resourceArn, JsonElement body)
    {
        var tagList = _tags.TryGetValue(resourceArn, out var existing)
            ? existing
            : [];

        if (body.TryGetProperty("Tags", out var tagsArr) && tagsArr.ValueKind == JsonValueKind.Array)
        {
            foreach (var tag in tagsArr.EnumerateArray())
            {
                var key = GetStr(tag, "Key") ?? "";
                var value = GetStr(tag, "Value") ?? "";
                var existingTag = tagList.FirstOrDefault(t => (string?)t["Key"] == key);
                if (existingTag is not null)
                {
                    existingTag["Value"] = value;
                }
                else
                {
                    tagList.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["Key"] = key,
                        ["Value"] = value,
                    });
                }
            }
        }

        _tags[resourceArn] = tagList;
        return S3FilesJson(200, new Dictionary<string, object?>());
    }

    private ServiceResponse UntagResource(string resourceArn, List<string> tagKeys)
    {
        if (_tags.TryGetValue(resourceArn, out var tagList))
        {
            var keySet = new HashSet<string>(tagKeys, StringComparer.Ordinal);
            _tags[resourceArn] = tagList.Where(t => t["Key"] is not string k || !keySet.Contains(k)).ToList();
        }
        return S3FilesJson(200, new Dictionary<string, object?>());
    }

    private ServiceResponse ListTags(string resourceArn)
    {
        var tagList = _tags.TryGetValue(resourceArn, out var existing) ? existing : [];
        return S3FilesJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Tags"] = tagList,
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private static string NowIso()
        => DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

    private static string? GetStr(JsonElement el, string propertyName)
        => el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;

    private static object? JsonElementToObject(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Object => element.EnumerateObject()
                .ToDictionary(p => p.Name, p => JsonElementToObject(p.Value), StringComparer.Ordinal),
            JsonValueKind.Array => element.EnumerateArray().Select(JsonElementToObject).ToList(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    private static Dictionary<string, object?> JsonElementToDict(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object)
            return new Dictionary<string, object?>(StringComparer.Ordinal);
        return element.EnumerateObject()
            .ToDictionary(p => p.Name, p => JsonElementToObject(p.Value), StringComparer.Ordinal);
    }

    private static readonly Dictionary<string, string> JsonResponseHeaders = new()
        { ["Content-Type"] = "application/json" };

    private static ServiceResponse S3FilesJson(int status, object data)
    {
        if (status == 204)
            return new ServiceResponse(204, new Dictionary<string, string>(), []);
        var json = System.Text.Encoding.UTF8.GetBytes(DictionaryObjectJsonConverter.SerializeValue(data));
        return new ServiceResponse(status, JsonResponseHeaders, json);
    }

    private static ServiceResponse S3FilesEmpty(int status)
        => new(status, new Dictionary<string, string>(), []);

    private static ServiceResponse S3FilesError(int status, string code, string message)
    {
        var data = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ErrorCode"] = code,
            ["Message"] = message,
            ["error"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["code"] = code,
            },
        };
        var json = DictionaryObjectJsonConverter.SerializeObject(data);
        var headers = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/json",
            ["x-amzn-errortype"] = code,
        };
        return new ServiceResponse(status, headers, json);
    }
}
