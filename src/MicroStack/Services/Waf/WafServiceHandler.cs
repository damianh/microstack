using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Waf;

/// <summary>
/// WAF v2 service handler — supports JSON protocol via X-Amz-Target: AWSWAF_20190729.
///
/// Port of ministack/services/waf.py.
///
/// Supports: CreateWebACL, GetWebACL, UpdateWebACL, DeleteWebACL, ListWebACLs,
///           AssociateWebACL, DisassociateWebACL, GetWebACLForResource, ListResourcesForWebACL,
///           CreateIPSet, GetIPSet, UpdateIPSet, DeleteIPSet, ListIPSets,
///           CreateRuleGroup, GetRuleGroup, UpdateRuleGroup, DeleteRuleGroup, ListRuleGroups,
///           TagResource, UntagResource, ListTagsForResource,
///           CheckCapacity, DescribeManagedRuleGroup.
/// </summary>
internal sealed class WafServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // ── State ────────────────────────────────────────────────────────────────
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _webAcls = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _ipSets = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _ruleGroups = new();
    private readonly AccountScopedDictionary<string, string> _associations = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, string>>> _wafTags = new();

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "wafv2";

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
            data = JsonDocument.Parse("{}").RootElement.Clone();
        }

        ServiceResponse response;
        lock (_lock)
        {
            response = action switch
            {
                "CreateWebACL"             => ActCreateWebAcl(data),
                "GetWebACL"                => ActGetWebAcl(data),
                "UpdateWebACL"             => ActUpdateWebAcl(data),
                "DeleteWebACL"             => ActDeleteWebAcl(data),
                "ListWebACLs"              => ActListWebAcls(data),
                "AssociateWebACL"          => ActAssociateWebAcl(data),
                "DisassociateWebACL"       => ActDisassociateWebAcl(data),
                "GetWebACLForResource"     => ActGetWebAclForResource(data),
                "ListResourcesForWebACL"   => ActListResourcesForWebAcl(data),
                "CreateIPSet"              => ActCreateIpSet(data),
                "GetIPSet"                 => ActGetIpSet(data),
                "UpdateIPSet"              => ActUpdateIpSet(data),
                "DeleteIPSet"              => ActDeleteIpSet(data),
                "ListIPSets"               => ActListIpSets(data),
                "CreateRuleGroup"          => ActCreateRuleGroup(data),
                "GetRuleGroup"             => ActGetRuleGroup(data),
                "UpdateRuleGroup"          => ActUpdateRuleGroup(data),
                "DeleteRuleGroup"          => ActDeleteRuleGroup(data),
                "ListRuleGroups"           => ActListRuleGroups(data),
                "TagResource"              => ActTagResource(data),
                "UntagResource"            => ActUntagResource(data),
                "ListTagsForResource"      => ActListTagsForResource(data),
                "CheckCapacity"            => ActCheckCapacity(),
                "DescribeManagedRuleGroup" => ActDescribeManagedRuleGroup(data),
                _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown WAF action: {action}", 400),
            };
        }

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _webAcls.Clear();
            _ipSets.Clear();
            _ruleGroups.Clear();
            _associations.Clear();
            _wafTags.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // ═══════════════════════════════════════════════════════════════════════════
    // WebACL
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse ActCreateWebAcl(JsonElement data)
    {
        var name = GetStr(data, "Name") ?? "";
        if (name.Length == 0)
            return WafError("WAFInvalidParameterException", "Name is required");

        var uid = HashHelpers.NewUuid();
        var lockToken = HashHelpers.NewUuid();
        var arn = AclArn(name, uid);
        var description = GetStr(data, "Description") ?? "";

        _webAcls[uid] = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ARN"] = arn,
            ["Id"] = uid,
            ["Name"] = name,
            ["Description"] = description,
            ["DefaultAction"] = GetJsonElement(data, "DefaultAction"),
            ["Rules"] = GetJsonElement(data, "Rules"),
            ["VisibilityConfig"] = GetJsonElement(data, "VisibilityConfig"),
            ["Capacity"] = 0,
            ["LockToken"] = lockToken,
            ["Scope"] = GetStr(data, "Scope") ?? "REGIONAL",
        };

        if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            _wafTags[arn] = ParseTagsList(tagsEl);
        }

        return AwsResponseHelpers.JsonResponse(new
        {
            Summary = new
            {
                ARN = arn,
                Id = uid,
                Name = name,
                Description = description,
                LockToken = lockToken,
            },
        });
    }

    private ServiceResponse ActGetWebAcl(JsonElement data)
    {
        var uid = GetStr(data, "Id") ?? "";
        if (!_webAcls.TryGetValue(uid, out var acl))
            return WafError("WAFNonexistentItemException", $"WebACL {uid} not found");

        var aclBody = acl
            .Where(kv => kv.Key != "LockToken")
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["WebACL"] = aclBody,
            ["LockToken"] = acl["LockToken"],
        });
    }

    private ServiceResponse ActUpdateWebAcl(JsonElement data)
    {
        var uid = GetStr(data, "Id") ?? "";
        if (!_webAcls.TryGetValue(uid, out var acl))
            return WafError("WAFNonexistentItemException", $"WebACL {uid} not found");

        if (data.TryGetProperty("Rules", out _))
            acl["Rules"] = GetJsonElement(data, "Rules");
        if (data.TryGetProperty("DefaultAction", out _))
            acl["DefaultAction"] = GetJsonElement(data, "DefaultAction");
        if (data.TryGetProperty("VisibilityConfig", out _))
            acl["VisibilityConfig"] = GetJsonElement(data, "VisibilityConfig");
        acl["LockToken"] = HashHelpers.NewUuid();

        return AwsResponseHelpers.JsonResponse(new { NextLockToken = (string)acl["LockToken"]! });
    }

    private ServiceResponse ActDeleteWebAcl(JsonElement data)
    {
        var uid = GetStr(data, "Id") ?? "";
        if (!_webAcls.TryGetValue(uid, out var acl))
            return WafError("WAFNonexistentItemException", $"WebACL {uid} not found");

        var arn = (string)acl["ARN"]!;
        _webAcls.TryRemove(uid, out _);
        _wafTags.TryRemove(arn, out _);
        return AwsResponseHelpers.JsonResponse(new { });
    }

    private ServiceResponse ActListWebAcls(JsonElement data)
    {
        var scope = GetStr(data, "Scope") ?? "REGIONAL";
        var acls = _webAcls.Values
            .Where(a => (string?)a["Scope"] == scope)
            .Select(a => new
            {
                ARN = (string?)a["ARN"],
                Id = (string?)a["Id"],
                Name = (string?)a["Name"],
                Description = (string?)a["Description"] ?? "",
                LockToken = (string?)a["LockToken"],
            })
            .ToList();
        return AwsResponseHelpers.JsonResponse(new { WebACLs = acls, NextMarker = (string?)null });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Association
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse ActAssociateWebAcl(JsonElement data)
    {
        var webAclArn = GetStr(data, "WebACLArn") ?? "";
        var resourceArn = GetStr(data, "ResourceArn") ?? "";
        _associations[resourceArn] = webAclArn;
        return AwsResponseHelpers.JsonResponse(new { });
    }

    private ServiceResponse ActDisassociateWebAcl(JsonElement data)
    {
        var resourceArn = GetStr(data, "ResourceArn") ?? "";
        _associations.TryRemove(resourceArn, out _);
        return AwsResponseHelpers.JsonResponse(new { });
    }

    private ServiceResponse ActGetWebAclForResource(JsonElement data)
    {
        var resourceArn = GetStr(data, "ResourceArn") ?? "";
        if (!_associations.TryGetValue(resourceArn, out var webAclArn))
            return WafError("WAFNonexistentItemException", $"No WebACL associated with {resourceArn}");

        foreach (var acl in _webAcls.Values)
        {
            if ((string?)acl["ARN"] == webAclArn)
            {
                var aclBody = acl
                    .Where(kv => kv.Key != "LockToken")
                    .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
                return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["WebACL"] = aclBody,
                });
            }
        }

        return WafError("WAFNonexistentItemException", $"WebACL {webAclArn} not found");
    }

    private ServiceResponse ActListResourcesForWebAcl(JsonElement data)
    {
        var webAclArn = GetStr(data, "WebACLArn") ?? "";
        var arns = _associations
            .Where(kv => kv.Value == webAclArn)
            .Select(kv => kv.Key)
            .ToList();
        return AwsResponseHelpers.JsonResponse(new { ResourceArns = arns });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // IPSet
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse ActCreateIpSet(JsonElement data)
    {
        var name = GetStr(data, "Name") ?? "";
        var uid = HashHelpers.NewUuid();
        var lockToken = HashHelpers.NewUuid();
        var arn = IpSetArn(name, uid);

        _ipSets[uid] = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ARN"] = arn,
            ["Id"] = uid,
            ["Name"] = name,
            ["Description"] = GetStr(data, "Description") ?? "",
            ["IPAddressVersion"] = GetStr(data, "IPAddressVersion") ?? "IPV4",
            ["Addresses"] = GetJsonElement(data, "Addresses"),
            ["LockToken"] = lockToken,
            ["Scope"] = GetStr(data, "Scope") ?? "REGIONAL",
        };

        if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            _wafTags[arn] = ParseTagsList(tagsEl);
        }

        return AwsResponseHelpers.JsonResponse(new
        {
            Summary = new { ARN = arn, Id = uid, Name = name, LockToken = lockToken },
        });
    }

    private ServiceResponse ActGetIpSet(JsonElement data)
    {
        var uid = GetStr(data, "Id") ?? "";
        if (!_ipSets.TryGetValue(uid, out var ipset))
            return WafError("WAFNonexistentItemException", $"IPSet {uid} not found");

        var body = ipset
            .Where(kv => kv.Key != "LockToken")
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["IPSet"] = body,
            ["LockToken"] = ipset["LockToken"],
        });
    }

    private ServiceResponse ActUpdateIpSet(JsonElement data)
    {
        var uid = GetStr(data, "Id") ?? "";
        if (!_ipSets.TryGetValue(uid, out var ipset))
            return WafError("WAFNonexistentItemException", $"IPSet {uid} not found");

        if (data.TryGetProperty("Addresses", out _))
            ipset["Addresses"] = GetJsonElement(data, "Addresses");
        ipset["LockToken"] = HashHelpers.NewUuid();
        return AwsResponseHelpers.JsonResponse(new { NextLockToken = (string)ipset["LockToken"]! });
    }

    private ServiceResponse ActDeleteIpSet(JsonElement data)
    {
        var uid = GetStr(data, "Id") ?? "";
        if (!_ipSets.TryGetValue(uid, out var ipset))
            return WafError("WAFNonexistentItemException", $"IPSet {uid} not found");

        var arn = (string)ipset["ARN"]!;
        _ipSets.TryRemove(uid, out _);
        _wafTags.TryRemove(arn, out _);
        return AwsResponseHelpers.JsonResponse(new { });
    }

    private ServiceResponse ActListIpSets(JsonElement data)
    {
        var scope = GetStr(data, "Scope") ?? "REGIONAL";
        var sets = _ipSets.Values
            .Where(s => (string?)s["Scope"] == scope)
            .Select(s => new
            {
                ARN = (string?)s["ARN"],
                Id = (string?)s["Id"],
                Name = (string?)s["Name"],
                Description = (string?)s["Description"] ?? "",
                LockToken = (string?)s["LockToken"],
            })
            .ToList();
        return AwsResponseHelpers.JsonResponse(new { IPSets = sets, NextMarker = (string?)null });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // RuleGroup
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse ActCreateRuleGroup(JsonElement data)
    {
        var name = GetStr(data, "Name") ?? "";
        var uid = HashHelpers.NewUuid();
        var lockToken = HashHelpers.NewUuid();
        var arn = RuleGroupArn(name, uid);

        _ruleGroups[uid] = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ARN"] = arn,
            ["Id"] = uid,
            ["Name"] = name,
            ["Description"] = GetStr(data, "Description") ?? "",
            ["Capacity"] = data.TryGetProperty("Capacity", out var capEl) && capEl.TryGetInt64(out var capVal) ? capVal : 0L,
            ["Rules"] = GetJsonElement(data, "Rules"),
            ["VisibilityConfig"] = GetJsonElement(data, "VisibilityConfig"),
            ["LockToken"] = lockToken,
            ["Scope"] = GetStr(data, "Scope") ?? "REGIONAL",
        };

        if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            _wafTags[arn] = ParseTagsList(tagsEl);
        }

        return AwsResponseHelpers.JsonResponse(new
        {
            Summary = new { ARN = arn, Id = uid, Name = name, LockToken = lockToken },
        });
    }

    private ServiceResponse ActGetRuleGroup(JsonElement data)
    {
        var uid = GetStr(data, "Id") ?? "";
        if (!_ruleGroups.TryGetValue(uid, out var rg))
            return WafError("WAFNonexistentItemException", $"RuleGroup {uid} not found");

        var body = rg
            .Where(kv => kv.Key != "LockToken")
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["RuleGroup"] = body,
            ["LockToken"] = rg["LockToken"],
        });
    }

    private ServiceResponse ActUpdateRuleGroup(JsonElement data)
    {
        var uid = GetStr(data, "Id") ?? "";
        if (!_ruleGroups.TryGetValue(uid, out var rg))
            return WafError("WAFNonexistentItemException", $"RuleGroup {uid} not found");

        if (data.TryGetProperty("Rules", out _))
            rg["Rules"] = GetJsonElement(data, "Rules");
        if (data.TryGetProperty("VisibilityConfig", out _))
            rg["VisibilityConfig"] = GetJsonElement(data, "VisibilityConfig");
        rg["LockToken"] = HashHelpers.NewUuid();
        return AwsResponseHelpers.JsonResponse(new { NextLockToken = (string)rg["LockToken"]! });
    }

    private ServiceResponse ActDeleteRuleGroup(JsonElement data)
    {
        var uid = GetStr(data, "Id") ?? "";
        if (!_ruleGroups.TryGetValue(uid, out var rg))
            return WafError("WAFNonexistentItemException", $"RuleGroup {uid} not found");

        var arn = (string)rg["ARN"]!;
        _ruleGroups.TryRemove(uid, out _);
        _wafTags.TryRemove(arn, out _);
        return AwsResponseHelpers.JsonResponse(new { });
    }

    private ServiceResponse ActListRuleGroups(JsonElement data)
    {
        var scope = GetStr(data, "Scope") ?? "REGIONAL";
        var groups = _ruleGroups.Values
            .Where(r => (string?)r["Scope"] == scope)
            .Select(r => new
            {
                ARN = (string?)r["ARN"],
                Id = (string?)r["Id"],
                Name = (string?)r["Name"],
                Description = (string?)r["Description"] ?? "",
                LockToken = (string?)r["LockToken"],
            })
            .ToList();
        return AwsResponseHelpers.JsonResponse(new { RuleGroups = groups, NextMarker = (string?)null });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tags
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse ActTagResource(JsonElement data)
    {
        var arn = GetStr(data, "ResourceARN") ?? "";
        if (!_wafTags.TryGetValue(arn, out var existing))
        {
            existing = [];
            _wafTags[arn] = existing;
        }
        var existingMap = existing.ToDictionary(t => t["Key"], StringComparer.Ordinal);
        if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var tag in tagsEl.EnumerateArray())
            {
                var key = GetStr(tag, "Key") ?? "";
                var val = GetStr(tag, "Value") ?? "";
                existingMap[key] = new Dictionary<string, string> { ["Key"] = key, ["Value"] = val };
            }
        }
        _wafTags[arn] = existingMap.Values.ToList();
        return AwsResponseHelpers.JsonResponse(new { });
    }

    private ServiceResponse ActUntagResource(JsonElement data)
    {
        var arn = GetStr(data, "ResourceARN") ?? "";
        if (_wafTags.TryGetValue(arn, out var existing))
        {
            var removeKeys = new HashSet<string>(StringComparer.Ordinal);
            if (data.TryGetProperty("TagKeys", out var keysEl) && keysEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var k in keysEl.EnumerateArray())
                {
                    var kStr = k.GetString();
                    if (kStr is not null) removeKeys.Add(kStr);
                }
            }
            _wafTags[arn] = existing.Where(t => !removeKeys.Contains(t["Key"])).ToList();
        }
        return AwsResponseHelpers.JsonResponse(new { });
    }

    private ServiceResponse ActListTagsForResource(JsonElement data)
    {
        var arn = GetStr(data, "ResourceARN") ?? "";
        var tags = _wafTags.TryGetValue(arn, out var t) ? t : [];
        return AwsResponseHelpers.JsonResponse(new
        {
            TagInfoForResource = new
            {
                ResourceARN = arn,
                TagList = tags,
            },
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Misc
    // ═══════════════════════════════════════════════════════════════════════════

    private static ServiceResponse ActCheckCapacity()
    {
        return AwsResponseHelpers.JsonResponse(new { Capacity = 1 });
    }

    private static ServiceResponse ActDescribeManagedRuleGroup(JsonElement data)
    {
        var vendorName = GetStr(data, "VendorName") ?? "AWS";
        var name = GetStr(data, "Name") ?? "";
        return AwsResponseHelpers.JsonResponse(new
        {
            VersionName = "Version_1.0",
            SnsTopicArn = "",
            Capacity = 700,
            Rules = Array.Empty<object>(),
            LabelNamespace = $"awswaf:managed:{vendorName}:{name}:",
            AvailableLabels = Array.Empty<object>(),
            ConsumedLabels = Array.Empty<object>(),
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private static string AclArn(string name, string uid)
        => $"arn:aws:wafv2:{Region}:{AccountContext.GetAccountId()}:regional/webacl/{name}/{uid}";

    private static string IpSetArn(string name, string uid)
        => $"arn:aws:wafv2:{Region}:{AccountContext.GetAccountId()}:regional/ipset/{name}/{uid}";

    private static string RuleGroupArn(string name, string uid)
        => $"arn:aws:wafv2:{Region}:{AccountContext.GetAccountId()}:regional/rulegroup/{name}/{uid}";

    private static ServiceResponse WafError(string code, string message)
        => AwsResponseHelpers.ErrorResponseJson(code, message, 400);

    private static string? GetStr(JsonElement el, string propertyName)
        => el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;

    private static object? GetJsonElement(JsonElement parent, string propertyName)
    {
        if (!parent.TryGetProperty(propertyName, out var el))
            return null;
        return JsonElementToObject(el);
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

    private static List<Dictionary<string, string>> ParseTagsList(JsonElement tagsEl)
    {
        var result = new List<Dictionary<string, string>>();
        foreach (var tag in tagsEl.EnumerateArray())
        {
            var key = GetStr(tag, "Key") ?? "";
            var val = GetStr(tag, "Value") ?? "";
            result.Add(new Dictionary<string, string> { ["Key"] = key, ["Value"] = val });
        }
        return result;
    }
}
