using System.Text.Json;
using System.Text.RegularExpressions;
using MicroStack.Internal;

namespace MicroStack.Services.Efs;

/// <summary>
/// EFS (Elastic File System) service handler — REST/JSON protocol with path-based routing.
///
/// Port of ministack/services/efs.py.
///
/// Supports:
///   File Systems:   CreateFileSystem, DescribeFileSystems, DeleteFileSystem, UpdateFileSystem
///   Mount Targets:  CreateMountTarget, DescribeMountTargets, DeleteMountTarget,
///                   DescribeMountTargetSecurityGroups, ModifyMountTargetSecurityGroups
///   Access Points:  CreateAccessPoint, DescribeAccessPoints, DeleteAccessPoint
///   Tags:           TagResource, UntagResource, ListTagsForResource
///   Lifecycle:      PutLifecycleConfiguration, DescribeLifecycleConfiguration
///   Backup Policy:  PutBackupPolicy, DescribeBackupPolicy
///   Account:        DescribeAccountPreferences, PutAccountPreferences
///   File System Policy: PutFileSystemPolicy, DescribeFileSystemPolicy
/// </summary>
internal sealed partial class EfsServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // ── State ────────────────────────────────────────────────────────────────
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _fileSystems = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _mountTargets = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _accessPoints = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, object?>>> _lifecycleConfigs = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _backupPolicies = new();
    private readonly AccountScopedDictionary<string, string> _fileSystemPolicies = new();

    private static int _counter;

    // ── Regex patterns ───────────────────────────────────────────────────────
    [GeneratedRegex(@"^/file-systems/([^/]+)$")]
    private static partial Regex FileSystemIdRegex();

    [GeneratedRegex(@"^/mount-targets/([^/]+)$")]
    private static partial Regex MountTargetIdRegex();

    [GeneratedRegex(@"^/mount-targets/([^/]+)/security-groups$")]
    private static partial Regex MountTargetSecGroupsRegex();

    [GeneratedRegex(@"^/access-points/([^/]+)$")]
    private static partial Regex AccessPointIdRegex();

    [GeneratedRegex(@"^/resource-tags/(.+)$")]
    private static partial Regex ResourceTagsRegex();

    [GeneratedRegex(@"^/file-systems/([^/]+)/lifecycle-configuration$")]
    private static partial Regex LifecycleConfigRegex();

    [GeneratedRegex(@"^/file-systems/([^/]+)/backup-policy$")]
    private static partial Regex BackupPolicyRegex();

    [GeneratedRegex(@"^/file-systems/([^/]+)/policy$")]
    private static partial Regex FileSystemPolicyRegex();

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "elasticfilesystem";

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

        // Strip EFS API version prefix
        if (path.StartsWith("/2015-02-01", StringComparison.Ordinal))
            path = path["/2015-02-01".Length..];

        // Flatten query params (keep full arrays for tagKeys)
        var query = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in request.QueryParams)
        {
            if (v.Length > 0) query[k] = v[0];
        }

        ServiceResponse response;
        lock (_lock)
        {
            response = DispatchRequest(method, path, body, query, request.QueryParams);
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
            _lifecycleConfigs.Clear();
            _backupPolicies.Clear();
            _fileSystemPolicies.Clear();
        }
    }

    public JsonElement? GetState() => null;

    public void RestoreState(JsonElement state) { }

    // ═══════════════════════════════════════════════════════════════════════════
    // Request router
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse DispatchRequest(string method, string path, JsonElement body, Dictionary<string, string> query, IReadOnlyDictionary<string, string[]> rawQuery)
    {
        // File Systems
        if (path == "/file-systems")
        {
            if (method == "POST") return CreateFileSystem(body);
            if (method == "GET") return DescribeFileSystems(query);
        }

        var fsMatch = FileSystemIdRegex().Match(path);
        if (fsMatch.Success)
        {
            var fsId = fsMatch.Groups[1].Value;
            if (method == "DELETE") return DeleteFileSystem(fsId);
            if (method == "PUT") return UpdateFileSystem(fsId, body);
        }

        // Mount Targets
        if (path == "/mount-targets")
        {
            if (method == "POST") return CreateMountTarget(body);
            if (method == "GET") return DescribeMountTargets(query);
        }

        var mtMatch = MountTargetIdRegex().Match(path);
        if (mtMatch.Success)
        {
            var mtId = mtMatch.Groups[1].Value;
            if (method == "DELETE") return DeleteMountTarget(mtId);
        }

        var mtSgMatch = MountTargetSecGroupsRegex().Match(path);
        if (mtSgMatch.Success)
        {
            var mtId = mtSgMatch.Groups[1].Value;
            if (method == "GET") return DescribeMountTargetSecurityGroups(mtId);
            if (method == "PUT") return ModifyMountTargetSecurityGroups(mtId, body);
        }

        // Access Points
        if (path == "/access-points")
        {
            if (method == "POST") return CreateAccessPoint(body);
            if (method == "GET") return DescribeAccessPoints(query);
        }

        var apMatch = AccessPointIdRegex().Match(path);
        if (apMatch.Success)
        {
            var apId = apMatch.Groups[1].Value;
            if (method == "DELETE") return DeleteAccessPoint(apId);
        }

        // Tags
        var tagMatch = ResourceTagsRegex().Match(path);
        if (tagMatch.Success)
        {
            var resourceId = Uri.UnescapeDataString(tagMatch.Groups[1].Value);
            if (method == "GET") return ListTagsForResource(resourceId);
            if (method == "POST") return TagResource(resourceId, body);
            if (method == "DELETE")
            {
                var keys = rawQuery.TryGetValue("tagKeys", out var tagKeysArr) && tagKeysArr.Length > 0
                    ? tagKeysArr.ToList()
                    : GetStringArray(body, "TagKeys");
                return UntagResource(resourceId, keys);
            }
        }

        // Lifecycle configuration
        var lcMatch = LifecycleConfigRegex().Match(path);
        if (lcMatch.Success)
        {
            var fsId = lcMatch.Groups[1].Value;
            if (method == "PUT") return PutLifecycleConfiguration(fsId, body);
            if (method == "GET") return DescribeLifecycleConfiguration(fsId);
        }

        // Backup policy
        var bpMatch = BackupPolicyRegex().Match(path);
        if (bpMatch.Success)
        {
            var fsId = bpMatch.Groups[1].Value;
            if (method == "PUT") return PutBackupPolicy(fsId, body);
            if (method == "GET") return DescribeBackupPolicy(fsId);
        }

        // File system policy
        var fpMatch = FileSystemPolicyRegex().Match(path);
        if (fpMatch.Success)
        {
            var fsId = fpMatch.Groups[1].Value;
            if (method == "PUT") return PutFileSystemPolicy(fsId, body);
            if (method == "GET") return DescribeFileSystemPolicy(fsId);
        }

        // Account preferences
        if (path == "/account-preferences")
        {
            if (method == "GET") return DescribeAccountPreferences();
            if (method == "PUT") return PutAccountPreferences(body);
        }

        return EfsError(400, "InvalidAction", $"Unknown EFS path: {method} {path}");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // File Systems
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse CreateFileSystem(JsonElement body)
    {
        var perfMode = GetStr(body, "PerformanceMode") ?? "generalPurpose";
        var throughputMode = GetStr(body, "ThroughputMode") ?? "bursting";
        var encrypted = body.TryGetProperty("Encrypted", out var encEl) && encEl.ValueKind == JsonValueKind.True;
        var kmsKeyId = GetStr(body, "KmsKeyId") ?? "";
        var tags = GetTagsList(body);
        var creationToken = GetStr(body, "CreationToken") ?? GenerateFsId();

        // Idempotency — same CreationToken returns existing FS
        foreach (var existingFs in _fileSystems.Values)
        {
            if ((string?)existingFs["CreationToken"] == creationToken)
                return EfsJson(200, existingFs);
        }

        var fsId = GenerateFsId();
        var arn = $"arn:aws:elasticfilesystem:{Region}:{AccountContext.GetAccountId()}:file-system/{fsId}";
        var now = NowIso();

        var nameTag = tags.FirstOrDefault(t => (string?)t["Key"] == "Name");
        var nameValue = nameTag is not null ? (string?)nameTag["Value"] ?? "" : "";

        var record = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["FileSystemId"] = fsId,
            ["FileSystemArn"] = arn,
            ["CreationToken"] = creationToken,
            ["CreationTime"] = now,
            ["LifeCycleState"] = "available",
            ["NumberOfMountTargets"] = 0,
            ["SizeInBytes"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Value"] = 0,
                ["Timestamp"] = now,
                ["ValueInIA"] = 0,
                ["ValueInStandard"] = 0,
            },
            ["PerformanceMode"] = perfMode,
            ["ThroughputMode"] = throughputMode,
            ["Encrypted"] = encrypted,
            ["KmsKeyId"] = kmsKeyId,
            ["Tags"] = tags,
            ["OwnerId"] = AccountContext.GetAccountId(),
            ["Name"] = nameValue,
        };

        if (body.TryGetProperty("ProvisionedThroughputInMibps", out var ptEl) && ptEl.TryGetDouble(out var ptVal))
        {
            record["ProvisionedThroughputInMibps"] = ptVal;
        }

        _fileSystems[fsId] = record;
        return EfsJson(201, record);
    }

    private ServiceResponse DescribeFileSystems(Dictionary<string, string> query)
    {
        query.TryGetValue("FileSystemId", out var fsId);
        query.TryGetValue("CreationToken", out var creationToken);
        var maxItems = query.TryGetValue("MaxItems", out var maxStr) && int.TryParse(maxStr, out var m) ? m : 100;

        var results = new List<Dictionary<string, object?>>();
        foreach (var fs in _fileSystems.Values)
        {
            if (fsId is not null && (string?)fs["FileSystemId"] != fsId)
                continue;
            if (creationToken is not null && (string?)fs["CreationToken"] != creationToken)
                continue;
            results.Add(fs);
        }

        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["FileSystems"] = results.Take(maxItems).ToList(),
        });
    }

    private ServiceResponse DeleteFileSystem(string fsId)
    {
        if (!_fileSystems.TryGetValue(fsId, out var fs))
            return EfsError(404, "FileSystemNotFound", $"File system '{fsId}' does not exist.");

        var mountCount = fs["NumberOfMountTargets"] is int c ? c : 0;
        if (mountCount > 0)
            return EfsError(400, "FileSystemInUse", $"File system '{fsId}' has mount targets and cannot be deleted.");

        _fileSystems.TryRemove(fsId, out _);
        return EfsEmpty(204);
    }

    private ServiceResponse UpdateFileSystem(string fsId, JsonElement body)
    {
        if (!_fileSystems.TryGetValue(fsId, out var fs))
            return EfsError(404, "FileSystemNotFound", $"File system '{fsId}' does not exist.");

        if (body.TryGetProperty("ThroughputMode", out var tmEl))
            fs["ThroughputMode"] = tmEl.GetString();
        if (body.TryGetProperty("ProvisionedThroughputInMibps", out var ptEl) && ptEl.TryGetDouble(out var ptVal))
            fs["ProvisionedThroughputInMibps"] = ptVal;

        return EfsJson(202, fs);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Mount Targets
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse CreateMountTarget(JsonElement body)
    {
        var fsId = GetStr(body, "FileSystemId") ?? "";
        var subnetId = GetStr(body, "SubnetId") ?? "";
        var ipAddress = GetStr(body, "IpAddress") ?? $"10.0.{Random.Shared.Next(256)}.{Random.Shared.Next(1, 255)}";
        var securityGroups = GetStringArray(body, "SecurityGroups");

        if (!_fileSystems.TryGetValue(fsId, out var fs))
            return EfsError(404, "FileSystemNotFound", $"File system '{fsId}' does not exist.");

        var mtId = GenerateMtId();
        var arn = $"arn:aws:elasticfilesystem:{Region}:{AccountContext.GetAccountId()}:file-system/{fsId}/mount-target/{mtId}";

        var record = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["MountTargetId"] = mtId,
            ["FileSystemId"] = fsId,
            ["SubnetId"] = subnetId,
            ["AvailabilityZoneId"] = $"use1-az{Random.Shared.Next(1, 4)}",
            ["AvailabilityZoneName"] = $"{Region}a",
            ["VpcId"] = "vpc-00000001",
            ["LifeCycleState"] = "available",
            ["IpAddress"] = ipAddress,
            ["NetworkInterfaceId"] = $"eni-{HashHelpers.NewUuidNoDashes()[..17]}",
            ["OwnerId"] = AccountContext.GetAccountId(),
            ["MountTargetArn"] = arn,
            ["SecurityGroups"] = securityGroups,
        };
        _mountTargets[mtId] = record;
        fs["NumberOfMountTargets"] = (fs["NumberOfMountTargets"] is int n ? n : 0) + 1;

        return EfsJson(200, MtResponse(record));
    }

    private ServiceResponse DescribeMountTargets(Dictionary<string, string> query)
    {
        query.TryGetValue("FileSystemId", out var fsId);
        query.TryGetValue("MountTargetId", out var mtId);
        var maxItems = query.TryGetValue("MaxItems", out var maxStr) && int.TryParse(maxStr, out var m) ? m : 100;

        var results = new List<Dictionary<string, object?>>();
        foreach (var mt in _mountTargets.Values)
        {
            if (fsId is not null && (string?)mt["FileSystemId"] != fsId)
                continue;
            if (mtId is not null && (string?)mt["MountTargetId"] != mtId)
                continue;
            results.Add(MtResponse(mt));
        }

        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["MountTargets"] = results.Take(maxItems).ToList(),
        });
    }

    private ServiceResponse DeleteMountTarget(string mtId)
    {
        if (!_mountTargets.TryGetValue(mtId, out var mt))
            return EfsError(404, "MountTargetNotFound", $"Mount target '{mtId}' does not exist.");

        var fsId = (string?)mt["FileSystemId"];
        if (fsId is not null && _fileSystems.TryGetValue(fsId, out var fs))
        {
            var current = fs["NumberOfMountTargets"] is int n ? n : 1;
            fs["NumberOfMountTargets"] = Math.Max(0, current - 1);
        }

        _mountTargets.TryRemove(mtId, out _);
        return EfsEmpty(204);
    }

    private ServiceResponse DescribeMountTargetSecurityGroups(string mtId)
    {
        if (!_mountTargets.TryGetValue(mtId, out var mt))
            return EfsError(404, "MountTargetNotFound", $"Mount target '{mtId}' does not exist.");
        var sgs = mt["SecurityGroups"] as List<string> ?? [];
        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["SecurityGroups"] = sgs,
        });
    }

    private ServiceResponse ModifyMountTargetSecurityGroups(string mtId, JsonElement body)
    {
        if (!_mountTargets.TryGetValue(mtId, out var mt))
            return EfsError(404, "MountTargetNotFound", $"Mount target '{mtId}' does not exist.");
        mt["SecurityGroups"] = GetStringArray(body, "SecurityGroups");
        return EfsEmpty(204);
    }

    private static Dictionary<string, object?> MtResponse(Dictionary<string, object?> mt)
    {
        return mt.Where(kv => kv.Key != "SecurityGroups")
                 .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Access Points
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse CreateAccessPoint(JsonElement body)
    {
        var fsId = GetStr(body, "FileSystemId") ?? "";
        if (!_fileSystems.ContainsKey(fsId))
            return EfsError(404, "FileSystemNotFound", $"File system '{fsId}' does not exist.");

        var apId = GenerateApId();
        var arn = $"arn:aws:elasticfilesystem:{Region}:{AccountContext.GetAccountId()}:access-point/{apId}";
        var tags = GetTagsList(body);
        var nameTag = tags.FirstOrDefault(t => (string?)t["Key"] == "Name");
        var nameValue = nameTag is not null ? (string?)nameTag["Value"] ?? "" : "";

        var record = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["AccessPointId"] = apId,
            ["AccessPointArn"] = arn,
            ["FileSystemId"] = fsId,
            ["LifeCycleState"] = "available",
            ["ClientToken"] = GetStr(body, "ClientToken") ?? apId,
            ["PosixUser"] = body.TryGetProperty("PosixUser", out var puEl) ? JsonElementToObject(puEl) : new Dictionary<string, object?>(),
            ["RootDirectory"] = body.TryGetProperty("RootDirectory", out var rdEl) ? JsonElementToObject(rdEl) : new Dictionary<string, object?> { ["Path"] = "/" },
            ["Tags"] = tags,
            ["OwnerId"] = AccountContext.GetAccountId(),
            ["Name"] = nameValue,
        };
        _accessPoints[apId] = record;
        return EfsJson(200, record);
    }

    private ServiceResponse DescribeAccessPoints(Dictionary<string, string> query)
    {
        query.TryGetValue("FileSystemId", out var fsId);
        query.TryGetValue("AccessPointId", out var apId);
        var maxResults = query.TryGetValue("MaxResults", out var maxStr) && int.TryParse(maxStr, out var m) ? m : 100;

        var results = new List<Dictionary<string, object?>>();
        foreach (var ap in _accessPoints.Values)
        {
            if (fsId is not null && (string?)ap["FileSystemId"] != fsId)
                continue;
            if (apId is not null && (string?)ap["AccessPointId"] != apId)
                continue;
            results.Add(ap);
        }

        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["AccessPoints"] = results.Take(maxResults).ToList(),
        });
    }

    private ServiceResponse DeleteAccessPoint(string apId)
    {
        if (!_accessPoints.ContainsKey(apId))
            return EfsError(404, "AccessPointNotFound", $"Access point '{apId}' does not exist.");
        _accessPoints.TryRemove(apId, out _);
        return EfsEmpty(204);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tags
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse TagResource(string resourceId, JsonElement body)
    {
        var resource = FindResource(resourceId);
        if (resource is null)
            return EfsError(404, "ResourceNotFound", $"Resource '{resourceId}' does not exist.");

        var newTags = GetTagsList(body);
        if (resource["Tags"] is not List<Dictionary<string, object?>> tagList)
        {
            tagList = [];
            resource["Tags"] = tagList;
        }

        var existingMap = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < tagList.Count; i++)
        {
            if (tagList[i]["Key"] is string k) existingMap[k] = i;
        }

        foreach (var tag in newTags)
        {
            if (tag["Key"] is string tagKey && existingMap.TryGetValue(tagKey, out var idx))
                tagList[idx] = tag;
            else
                tagList.Add(tag);
        }

        return EfsJson(200, new Dictionary<string, object?>());
    }

    private ServiceResponse UntagResource(string resourceId, List<string> keys)
    {
        var resource = FindResource(resourceId);
        if (resource is null)
            return EfsError(404, "ResourceNotFound", $"Resource '{resourceId}' does not exist.");

        var keySet = new HashSet<string>(keys, StringComparer.Ordinal);
        if (resource["Tags"] is List<Dictionary<string, object?>> tagList)
        {
            resource["Tags"] = tagList.Where(t => t["Key"] is not string k || !keySet.Contains(k)).ToList();
        }

        return EfsJson(200, new Dictionary<string, object?>());
    }

    private ServiceResponse ListTagsForResource(string resourceId)
    {
        var resource = FindResource(resourceId);
        if (resource is null)
            return EfsError(404, "ResourceNotFound", $"Resource '{resourceId}' does not exist.");

        var tags = resource["Tags"] as List<Dictionary<string, object?>> ?? [];
        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Tags"] = tags,
        });
    }

    private Dictionary<string, object?>? FindResource(string resourceId)
    {
        if (_fileSystems.TryGetValue(resourceId, out var fs)) return fs;
        if (_accessPoints.TryGetValue(resourceId, out var ap)) return ap;

        // Search by ARN
        foreach (var v in _fileSystems.Values)
        {
            if ((string?)v["FileSystemArn"] == resourceId) return v;
        }
        foreach (var v in _accessPoints.Values)
        {
            if ((string?)v["AccessPointArn"] == resourceId) return v;
        }
        return null;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Lifecycle / Backup / Policy / Account
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse PutLifecycleConfiguration(string fsId, JsonElement body)
    {
        if (!_fileSystems.ContainsKey(fsId))
            return EfsError(404, "FileSystemNotFound", $"File system '{fsId}' does not exist.");

        var policies = new List<Dictionary<string, object?>>();
        if (body.TryGetProperty("LifecyclePolicies", out var polArr) && polArr.ValueKind == JsonValueKind.Array)
        {
            foreach (var p in polArr.EnumerateArray())
            {
                var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var prop in p.EnumerateObject())
                {
                    dict[prop.Name] = prop.Value.ValueKind == JsonValueKind.String ? prop.Value.GetString() : JsonElementToObject(prop.Value);
                }
                policies.Add(dict);
            }
        }
        _lifecycleConfigs[fsId] = policies;
        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["LifecyclePolicies"] = policies,
        });
    }

    private ServiceResponse DescribeLifecycleConfiguration(string fsId)
    {
        if (!_fileSystems.ContainsKey(fsId))
            return EfsError(404, "FileSystemNotFound", $"File system '{fsId}' does not exist.");

        var policies = _lifecycleConfigs.TryGetValue(fsId, out var lc) ? lc : [];
        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["LifecyclePolicies"] = policies,
        });
    }

    private ServiceResponse PutBackupPolicy(string fsId, JsonElement body)
    {
        if (!_fileSystems.ContainsKey(fsId))
            return EfsError(404, "FileSystemNotFound", $"File system '{fsId}' does not exist.");

        var policy = body.TryGetProperty("BackupPolicy", out var bpEl)
            ? JsonElementToDict(bpEl)
            : new Dictionary<string, object?>(StringComparer.Ordinal) { ["Status"] = "DISABLED" };
        _backupPolicies[fsId] = policy;
        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["BackupPolicy"] = policy,
        });
    }

    private ServiceResponse DescribeBackupPolicy(string fsId)
    {
        if (!_fileSystems.ContainsKey(fsId))
            return EfsError(404, "FileSystemNotFound", $"File system '{fsId}' does not exist.");

        var policy = _backupPolicies.TryGetValue(fsId, out var bp)
            ? bp
            : new Dictionary<string, object?>(StringComparer.Ordinal) { ["Status"] = "DISABLED" };
        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["BackupPolicy"] = policy,
        });
    }

    private ServiceResponse PutFileSystemPolicy(string fsId, JsonElement body)
    {
        if (!_fileSystems.ContainsKey(fsId))
            return EfsError(404, "FileSystemNotFound", $"File system '{fsId}' does not exist.");

        var policyStr = GetStr(body, "Policy") ?? body.ToString();
        _fileSystemPolicies[fsId] = policyStr;
        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["FileSystemId"] = fsId,
            ["Policy"] = policyStr,
        });
    }

    private ServiceResponse DescribeFileSystemPolicy(string fsId)
    {
        if (!_fileSystems.ContainsKey(fsId))
            return EfsError(404, "FileSystemNotFound", $"File system '{fsId}' does not exist.");

        if (!_fileSystemPolicies.TryGetValue(fsId, out var policy))
            return EfsError(404, "PolicyNotFound", $"File system '{fsId}' does not have a file system policy.");

        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["FileSystemId"] = fsId,
            ["Policy"] = policy,
        });
    }

    private static ServiceResponse DescribeAccountPreferences()
    {
        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ResourceIdPreference"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["ResourceIdType"] = "LONG_ID",
                ["Resources"] = new List<string> { "FILE_SYSTEM", "MOUNT_TARGET" },
            },
        });
    }

    private static ServiceResponse PutAccountPreferences(JsonElement body)
    {
        var ridType = GetStr(body, "ResourceIdType") ?? "LONG_ID";
        return EfsJson(200, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ResourceIdPreference"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["ResourceIdType"] = ridType,
                ["Resources"] = new List<string> { "FILE_SYSTEM", "MOUNT_TARGET" },
            },
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private static string GenerateFsId()
        => $"fs-{Interlocked.Increment(ref _counter):x17}";

    private static string GenerateMtId()
        => $"fsmt-{Interlocked.Increment(ref _counter):x17}";

    private static string GenerateApId()
        => $"fsap-{Interlocked.Increment(ref _counter):x17}";

    private static string NowIso()
        => DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.000Z");

    private static string? GetStr(JsonElement el, string propertyName)
        => el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;

    private static List<string> GetStringArray(JsonElement el, string propertyName)
    {
        var result = new List<string>();
        if (el.TryGetProperty(propertyName, out var arr) && arr.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in arr.EnumerateArray())
            {
                var s = item.GetString();
                if (s is not null) result.Add(s);
            }
        }
        return result;
    }

    private static List<Dictionary<string, object?>> GetTagsList(JsonElement body)
    {
        var result = new List<Dictionary<string, object?>>();
        if (body.TryGetProperty("Tags", out var arr) && arr.ValueKind == JsonValueKind.Array)
        {
            foreach (var tag in arr.EnumerateArray())
            {
                var d = new Dictionary<string, object?>(StringComparer.Ordinal);
                if (tag.TryGetProperty("Key", out var k)) d["Key"] = k.GetString();
                if (tag.TryGetProperty("Value", out var v)) d["Value"] = v.GetString();
                result.Add(d);
            }
        }
        return result;
    }

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

    private static ServiceResponse EfsJson(int status, object data)
    {
        if (status == 204)
            return new ServiceResponse(204, new Dictionary<string, string>(), []);
        var json = System.Text.Encoding.UTF8.GetBytes(DictionaryObjectJsonConverter.SerializeValue(data));
        return new ServiceResponse(status, JsonResponseHeaders, json);
    }

    private static ServiceResponse EfsEmpty(int status)
        => new(status, new Dictionary<string, string>(), []);

    private static ServiceResponse EfsError(int status, string code, string message)
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
