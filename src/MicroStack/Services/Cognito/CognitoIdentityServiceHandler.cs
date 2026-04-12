using System.Security.Cryptography;
using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Cognito;

/// <summary>
/// Cognito Identity (Identity Pools) service handler.
/// X-Amz-Target: AWSCognitoIdentityService.*
///
/// Port of ministack/services/cognito.py (Identity Pool portion).
/// </summary>
internal sealed class CognitoIdentityServiceHandler : IServiceHandler
{
    private readonly CognitoIdpServiceHandler _idp;
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // ── Identity Pools state ───────────────────────────────────────────────
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _identityPools = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _identityTags = new();

    internal CognitoIdentityServiceHandler(CognitoIdpServiceHandler idpHandler)
    {
        _idp = idpHandler;
    }

    // ── IServiceHandler ────────────────────────────────────────────────────
    public string ServiceName => "cognito-identity";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var target = request.GetHeader("x-amz-target") ?? "";

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

        if (!target.StartsWith("AWSCognitoIdentityService.", StringComparison.Ordinal))
        {
            return Task.FromResult(
                AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown Cognito Identity target: {target}", 400));
        }

        var action = target[(target.LastIndexOf('.') + 1)..];
        var response = DispatchIdentity(action, data);
        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _identityPools.Clear();
            _identityTags.Clear();
        }
    }

    public object? GetState() => null;
    public void RestoreState(object state) { }

    // ── Identity Dispatcher ────────────────────────────────────────────────
    private ServiceResponse DispatchIdentity(string action, JsonElement data)
    {
        return action switch
        {
            "CreateIdentityPool" => ActCreateIdentityPool(data),
            "DeleteIdentityPool" => ActDeleteIdentityPool(data),
            "DescribeIdentityPool" => ActDescribeIdentityPool(data),
            "ListIdentityPools" => ActListIdentityPools(data),
            "UpdateIdentityPool" => ActUpdateIdentityPool(data),
            "GetId" => ActGetId(data),
            "GetCredentialsForIdentity" => ActGetCredentialsForIdentity(data),
            "GetOpenIdToken" => ActGetOpenIdToken(data),
            "SetIdentityPoolRoles" => ActSetIdentityPoolRoles(data),
            "GetIdentityPoolRoles" => ActGetIdentityPoolRoles(data),
            "ListIdentities" => ActListIdentities(data),
            "DescribeIdentity" => ActDescribeIdentity(data),
            "MergeDeveloperIdentities" => ActMergeDeveloperIdentities(data),
            "UnlinkDeveloperIdentity" => ActUnlinkDeveloperIdentity(),
            "UnlinkIdentity" => ActUnlinkIdentity(),
            "TagResource" => ActTagResource(data),
            "UntagResource" => ActUntagResource(data),
            "ListTagsForResource" => ActListTagsForResource(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown Cognito Identity action: {action}", 400),
        };
    }

    // ═══════════════════════════════════════════════════════════════════════
    // IDENTITY POOL CRUD
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActCreateIdentityPool(JsonElement data)
    {
        var name = data.GetStringOrDefault("IdentityPoolName");
        if (string.IsNullOrEmpty(name))
        {
            return Error("InvalidParameterException", "IdentityPoolName is required.");
        }

        lock (_lock)
        {
            var iid = GenerateIdentityPoolId();
            var pool = new Dictionary<string, object?>
            {
                ["IdentityPoolId"] = iid,
                ["IdentityPoolName"] = name,
                ["AllowUnauthenticatedIdentities"] = data.GetBoolOrDefault("AllowUnauthenticatedIdentities"),
                ["AllowClassicFlow"] = data.GetBoolOrDefault("AllowClassicFlow"),
                ["SupportedLoginProviders"] = data.GetPropertyOrNull("SupportedLoginProviders")?.ToStringDictionary()
                    ?? new Dictionary<string, string>(),
                ["DeveloperProviderName"] = data.GetStringOrDefault("DeveloperProviderName"),
                ["OpenIdConnectProviderARNs"] = data.GetPropertyOrNull("OpenIdConnectProviderARNs")?.ToStringList()
                    ?? new List<string>(),
                ["CognitoIdentityProviders"] = data.GetPropertyOrNull("CognitoIdentityProviders")?.ToObjectList()
                    ?? new List<object?>(),
                ["SamlProviderARNs"] = data.GetPropertyOrNull("SamlProviderARNs")?.ToStringList()
                    ?? new List<string>(),
                ["IdentityPoolTags"] = data.GetPropertyOrNull("IdentityPoolTags")?.ToStringDictionary()
                    ?? new Dictionary<string, string>(),
                // Internal state
                ["_roles"] = new Dictionary<string, string>(),
                ["_identities"] = new Dictionary<string, Dictionary<string, object?>>(),
            };
            _identityPools[iid] = pool;
            return Json(IdentityPoolOut(pool));
        }
    }

    private ServiceResponse ActDeleteIdentityPool(JsonElement data)
    {
        var iid = data.GetStringOrDefault("IdentityPoolId");
        lock (_lock)
        {
            if (!_identityPools.ContainsKey(iid))
            {
                return Error("ResourceNotFoundException", $"Identity pool {iid} not found.");
            }
            _identityPools.TryRemove(iid, out _);
            _identityTags.TryRemove(iid, out _);
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDescribeIdentityPool(JsonElement data)
    {
        var iid = data.GetStringOrDefault("IdentityPoolId");
        if (!_identityPools.TryGetValue(iid, out var pool))
        {
            return Error("ResourceNotFoundException", $"Identity pool {iid} not found.");
        }
        return Json(IdentityPoolOut(pool));
    }

    private ServiceResponse ActListIdentityPools(JsonElement data)
    {
        var maxResults = Math.Min(data.GetIntOrDefault("MaxResults", 60), 60);
        var nextToken = data.GetStringOrDefault("NextToken");
        var pools = _identityPools.Values
            .OrderBy(p => (string)p["IdentityPoolId"]!)
            .ToList();
        var start = string.IsNullOrEmpty(nextToken) ? 0 : int.Parse(nextToken);
        var page = pools.Skip(start).Take(maxResults).ToList();
        var resp = new Dictionary<string, object?>
        {
            ["IdentityPools"] = page.Select(p => new Dictionary<string, object?>
            {
                ["IdentityPoolId"] = p["IdentityPoolId"],
                ["IdentityPoolName"] = p["IdentityPoolName"],
            }).ToList<object?>(),
        };
        if (start + maxResults < pools.Count)
        {
            resp["NextToken"] = (start + maxResults).ToString();
        }
        return Json(resp);
    }

    private ServiceResponse ActUpdateIdentityPool(JsonElement data)
    {
        var iid = data.GetStringOrDefault("IdentityPoolId");
        lock (_lock)
        {
            if (!_identityPools.TryGetValue(iid, out var pool))
            {
                return Error("ResourceNotFoundException", $"Identity pool {iid} not found.");
            }
            var updatable = new[]
            {
                "IdentityPoolName", "AllowUnauthenticatedIdentities", "AllowClassicFlow",
                "SupportedLoginProviders", "DeveloperProviderName", "OpenIdConnectProviderARNs",
                "CognitoIdentityProviders", "SamlProviderARNs", "IdentityPoolTags",
            };
            foreach (var key in updatable)
            {
                var prop = data.GetPropertyOrNull(key);
                if (prop is not null)
                {
                    pool[key] = prop.Value.ToObject();
                }
            }
            return Json(IdentityPoolOut(pool));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // IDENTITY OPERATIONS
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActGetId(JsonElement data)
    {
        var iid = data.GetStringOrDefault("IdentityPoolId");
        lock (_lock)
        {
            if (!_identityPools.TryGetValue(iid, out var pool))
            {
                return Error("ResourceNotFoundException", $"Identity pool {iid} not found.");
            }
            var identityId = GenerateIdentityId();
            var identities = GetIdentities(pool);
            var now = NowEpoch();
            identities[identityId] = new Dictionary<string, object?>
            {
                ["IdentityId"] = identityId,
                ["Logins"] = data.GetPropertyOrNull("Logins")?.ToStringDictionary() ?? new Dictionary<string, string>(),
                ["CreationDate"] = now,
                ["LastModifiedDate"] = now,
            };
            return Json(new Dictionary<string, object?> { ["IdentityId"] = identityId });
        }
    }

    private ServiceResponse ActGetCredentialsForIdentity(JsonElement data)
    {
        var identityId = data.GetStringOrDefault("IdentityId", HashHelpers.NewUuid());
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var accessKeyId = "ASIA" + Guid.NewGuid().ToString("N")[..16].ToUpperInvariant();
        var secretKey = Convert.ToBase64String(RandomNumberGenerator.GetBytes(30));
        var sessionToken = Convert.ToBase64String(RandomNumberGenerator.GetBytes(64));
        return Json(new Dictionary<string, object?>
        {
            ["IdentityId"] = identityId,
            ["Credentials"] = new Dictionary<string, object?>
            {
                ["AccessKeyId"] = accessKeyId,
                ["SecretKey"] = secretKey,
                ["SessionToken"] = sessionToken,
                ["Expiration"] = now + 3600,
            },
        });
    }

    private ServiceResponse ActGetOpenIdToken(JsonElement data)
    {
        var identityId = data.GetStringOrDefault("IdentityId", HashHelpers.NewUuid());
        // Find pool containing this identity
        var poolId = "";
        foreach (var (iid, pool) in _identityPools.Items)
        {
            var identities = GetIdentities(pool);
            if (identities.ContainsKey(identityId))
            {
                poolId = iid;
                break;
            }
        }
        var token = _idp.GenerateJwt(identityId, poolId, "", "id");
        return Json(new Dictionary<string, object?> { ["IdentityId"] = identityId, ["Token"] = token });
    }

    private ServiceResponse ActSetIdentityPoolRoles(JsonElement data)
    {
        var iid = data.GetStringOrDefault("IdentityPoolId");
        lock (_lock)
        {
            if (!_identityPools.TryGetValue(iid, out var pool))
            {
                return Error("ResourceNotFoundException", $"Identity pool {iid} not found.");
            }
            pool["_roles"] = data.GetPropertyOrNull("Roles")?.ToStringDictionary() ?? new Dictionary<string, string>();
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetIdentityPoolRoles(JsonElement data)
    {
        var iid = data.GetStringOrDefault("IdentityPoolId");
        if (!_identityPools.TryGetValue(iid, out var pool))
        {
            return Error("ResourceNotFoundException", $"Identity pool {iid} not found.");
        }
        return Json(new Dictionary<string, object?>
        {
            ["IdentityPoolId"] = iid,
            ["Roles"] = pool["_roles"],
            ["RoleMappings"] = new Dictionary<string, object?>(),
        });
    }

    private ServiceResponse ActListIdentities(JsonElement data)
    {
        var iid = data.GetStringOrDefault("IdentityPoolId");
        if (!_identityPools.TryGetValue(iid, out var pool))
        {
            return Error("ResourceNotFoundException", $"Identity pool {iid} not found.");
        }
        var maxResults = Math.Min(data.GetIntOrDefault("MaxResults", 60), 60);
        var identities = GetIdentities(pool);
        var page = identities.Values.Take(maxResults).ToList();
        return Json(new Dictionary<string, object?>
        {
            ["IdentityPoolId"] = iid,
            ["Identities"] = page.Select(i => new Dictionary<string, object?>
            {
                ["IdentityId"] = i["IdentityId"],
                ["Logins"] = i.TryGetValue("Logins", out var logins) && logins is Dictionary<string, string> d
                    ? d.Keys.ToList()
                    : new List<string>(),
                ["CreationDate"] = i["CreationDate"],
                ["LastModifiedDate"] = i["LastModifiedDate"],
            }).ToList<object?>(),
        });
    }

    private ServiceResponse ActDescribeIdentity(JsonElement data)
    {
        var identityId = data.GetStringOrDefault("IdentityId");
        foreach (var pool in _identityPools.Values)
        {
            var identities = GetIdentities(pool);
            if (identities.TryGetValue(identityId, out var identity))
            {
                return Json(new Dictionary<string, object?>
                {
                    ["IdentityId"] = identityId,
                    ["Logins"] = identity.TryGetValue("Logins", out var logins) && logins is Dictionary<string, string> d
                        ? d.Keys.ToList()
                        : new List<string>(),
                    ["CreationDate"] = identity["CreationDate"],
                    ["LastModifiedDate"] = identity["LastModifiedDate"],
                });
            }
        }
        return Error("ResourceNotFoundException", $"Identity {identityId} not found.");
    }

    private ServiceResponse ActMergeDeveloperIdentities(JsonElement data)
    {
        var poolId = data.GetStringOrDefault("IdentityPoolId");
        return Json(new Dictionary<string, object?> { ["IdentityId"] = GenerateIdentityId() });
    }

    private static ServiceResponse ActUnlinkDeveloperIdentity() =>
        Json(new Dictionary<string, object?>());

    private static ServiceResponse ActUnlinkIdentity() =>
        Json(new Dictionary<string, object?>());

    // ═══════════════════════════════════════════════════════════════════════
    // TAGS
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActTagResource(JsonElement data)
    {
        var arn = data.GetStringOrDefault("ResourceArn");
        var tags = data.GetPropertyOrNull("Tags")?.ToStringDictionary() ?? new Dictionary<string, string>();
        var iid = arn.Contains('/') ? arn[(arn.LastIndexOf('/') + 1)..] : arn;
        lock (_lock)
        {
            if (!_identityTags.TryGetValue(iid, out var existing))
            {
                existing = new Dictionary<string, string>();
                _identityTags[iid] = existing;
            }
            foreach (var (k, v) in tags)
            {
                existing[k] = v;
            }
        }
        return Json(new Dictionary<string, object?>());
    }

    private ServiceResponse ActUntagResource(JsonElement data)
    {
        var arn = data.GetStringOrDefault("ResourceArn");
        var tagKeys = data.GetPropertyOrNull("TagKeys")?.ToStringList() ?? [];
        var iid = arn.Contains('/') ? arn[(arn.LastIndexOf('/') + 1)..] : arn;
        lock (_lock)
        {
            if (_identityTags.TryGetValue(iid, out var existing))
            {
                foreach (var k in tagKeys)
                {
                    existing.Remove(k);
                }
            }
        }
        return Json(new Dictionary<string, object?>());
    }

    private ServiceResponse ActListTagsForResource(JsonElement data)
    {
        var arn = data.GetStringOrDefault("ResourceArn");
        var iid = arn.Contains('/') ? arn[(arn.LastIndexOf('/') + 1)..] : arn;
        var tags = _identityTags.TryGetValue(iid, out var existing) ? existing : new Dictionary<string, string>();
        return Json(new Dictionary<string, object?> { ["Tags"] = tags });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // HELPERS
    // ═══════════════════════════════════════════════════════════════════════

    private static Dictionary<string, Dictionary<string, object?>> GetIdentities(Dictionary<string, object?> pool) =>
        (Dictionary<string, Dictionary<string, object?>>)pool["_identities"]!;

    private static Dictionary<string, object?> IdentityPoolOut(Dictionary<string, object?> pool)
    {
        var result = new Dictionary<string, object?>();
        foreach (var (k, v) in pool)
        {
            if (!k.StartsWith('_'))
            {
                result[k] = v;
            }
        }
        return result;
    }

    private static string GenerateIdentityPoolId() =>
        $"{Region}:{HashHelpers.NewUuid()}";

    private static string GenerateIdentityId() =>
        $"{Region}:{HashHelpers.NewUuid()}";

    private static double NowEpoch() =>
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;

    private static ServiceResponse Json(Dictionary<string, object?> data) =>
        AwsResponseHelpers.JsonResponse(data);

    private static ServiceResponse Error(string code, string message) =>
        AwsResponseHelpers.ErrorResponseJson(code, message, 400);
}
