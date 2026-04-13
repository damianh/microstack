using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.SecretsManager;

/// <summary>
/// Secrets Manager service handler -- supports JSON protocol via X-Amz-Target.
///
/// Port of ministack/services/secretsmanager.py.
///
/// Supports: CreateSecret, GetSecretValue, BatchGetSecretValue, ListSecrets,
///           DeleteSecret, RestoreSecret, UpdateSecret, DescribeSecret,
///           PutSecretValue, UpdateSecretVersionStage,
///           TagResource, UntagResource, ListSecretVersionIds,
///           RotateSecret, GetRandomPassword, ReplicateSecretToRegions,
///           PutResourcePolicy, GetResourcePolicy, DeleteResourcePolicy,
///           ValidateResourcePolicy.
/// </summary>
internal sealed class SecretsManagerServiceHandler : IServiceHandler
{
    private readonly AccountScopedDictionary<string, SmSecret> _secrets = new(); // keyed by Name
    private readonly AccountScopedDictionary<string, string> _resourcePolicies = new(); // keyed by ARN
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // Characters used by GetRandomPassword
    private const string LowercaseChars = "abcdefghijklmnopqrstuvwxyz";
    private const string UppercaseChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private const string DigitChars = "0123456789";
    private const string PunctuationChars = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "secretsmanager";

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

        var response = action switch
        {
            "CreateSecret" => ActCreateSecret(data),
            "GetSecretValue" => ActGetSecretValue(data),
            "BatchGetSecretValue" => ActBatchGetSecretValue(data),
            "ListSecrets" => ActListSecrets(data),
            "DeleteSecret" => ActDeleteSecret(data),
            "RestoreSecret" => ActRestoreSecret(data),
            "UpdateSecret" => ActUpdateSecret(data),
            "DescribeSecret" => ActDescribeSecret(data),
            "PutSecretValue" => ActPutSecretValue(data),
            "UpdateSecretVersionStage" => ActUpdateSecretVersionStage(data),
            "TagResource" => ActTagResource(data),
            "UntagResource" => ActUntagResource(data),
            "ListSecretVersionIds" => ActListSecretVersionIds(data),
            "RotateSecret" => ActRotateSecret(data),
            "GetRandomPassword" => ActGetRandomPassword(data),
            "ReplicateSecretToRegions" => ActReplicateSecretToRegions(data),
            "PutResourcePolicy" => ActPutResourcePolicy(data),
            "GetResourcePolicy" => ActGetResourcePolicy(data),
            "DeleteResourcePolicy" => ActDeleteResourcePolicy(data),
            "ValidateResourcePolicy" => ActValidateResourcePolicy(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidRequestException", $"Unknown action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _secrets.Clear();
            _resourcePolicies.Clear();
        }
    }

    public JsonElement? GetState()
    {
        lock (_lock)
        {
            var secrets = _secrets.ToRaw()
                .Select(kv => new SmSecretsEntry(kv.Key.AccountId, kv.Key.Key, kv.Value))
                .ToList();
            var policies = _resourcePolicies.ToRaw()
                .Select(kv => new SmPolicyEntry(kv.Key.AccountId, kv.Key.Key, kv.Value))
                .ToList();
            var state = new SecretsManagerState(secrets, policies);
            return JsonSerializer.SerializeToElement(state, MicroStackJsonContext.Default.SecretsManagerState);
        }
    }

    public void RestoreState(JsonElement state)
    {
        var restored = JsonSerializer.Deserialize(state, MicroStackJsonContext.Default.SecretsManagerState);
        if (restored is null) return;
        lock (_lock)
        {
            _secrets.FromRaw(restored.Secrets.Select(e =>
                new KeyValuePair<(string, string), SmSecret>((e.AccountId, e.Key), e.Value)));
            _resourcePolicies.FromRaw(restored.Policies.Select(e =>
                new KeyValuePair<(string, string), string>((e.AccountId, e.Key), e.Value)));
        }
    }

    // -- Helpers ---------------------------------------------------------------

    private (string? Key, SmSecret? Secret) Resolve(JsonElement data, string propertyName)
    {
        var secretId = GetString(data, propertyName);
        return ResolveById(secretId);
    }

    private (string? Key, SmSecret? Secret) ResolveById(string? secretId)
    {
        if (string.IsNullOrEmpty(secretId))
        {
            return (null, null);
        }

        if (_secrets.TryGetValue(secretId, out var byName))
        {
            return (secretId, byName);
        }

        foreach (var (key, secret) in _secrets.Items)
        {
            if (string.Equals(secret.Arn, secretId, StringComparison.Ordinal))
            {
                return (key, secret);
            }
        }

        return (null, null);
    }

    private static (string? VersionId, SmSecretVersion? Version) FindStageVersion(SmSecret secret, string stage)
    {
        foreach (var (vid, ver) in secret.Versions)
        {
            if (ver.Stages.Contains(stage))
            {
                return (vid, ver);
            }
        }

        return (null, null);
    }

    private static void ApplyCurrentPromotion(SmSecret secret, string newVid)
    {
        string? oldCurrVid = null;
        string? oldPrevVid = null;

        foreach (var (vid, ver) in secret.Versions)
        {
            if (string.Equals(vid, newVid, StringComparison.Ordinal))
            {
                continue;
            }

            if (ver.Stages.Contains("AWSCURRENT"))
            {
                oldCurrVid = vid;
            }

            if (ver.Stages.Contains("AWSPREVIOUS"))
            {
                oldPrevVid = vid;
            }
        }

        // Remove AWSPREVIOUS from old previous; prune if stageless
        if (oldPrevVid is not null && secret.Versions.TryGetValue(oldPrevVid, out var oldPrevVer))
        {
            oldPrevVer.Stages.Remove("AWSPREVIOUS");
            if (oldPrevVer.Stages.Count == 0)
            {
                secret.Versions.Remove(oldPrevVid);
            }
        }

        // Demote old AWSCURRENT to AWSPREVIOUS
        if (oldCurrVid is not null && secret.Versions.TryGetValue(oldCurrVid, out var oldCurrVer))
        {
            oldCurrVer.Stages.Remove("AWSCURRENT");
            if (!oldCurrVer.Stages.Contains("AWSPREVIOUS"))
            {
                oldCurrVer.Stages.Add("AWSPREVIOUS");
            }
        }

        // Promote new version
        if (secret.Versions.TryGetValue(newVid, out var newVer))
        {
            if (!newVer.Stages.Contains("AWSCURRENT"))
            {
                newVer.Stages.Add("AWSCURRENT");
            }

            newVer.Stages.Remove("AWSPENDING");
        }
    }

    private static Dictionary<string, List<string>> VidToStages(SmSecret secret)
    {
        var result = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var (vid, ver) in secret.Versions)
        {
            if (ver.Stages.Count > 0)
            {
                result[vid] = new List<string>(ver.Stages);
            }
        }

        return result;
    }

    private static bool RemoveStage(SmSecret secret, string versionId, string stage)
    {
        if (!secret.Versions.TryGetValue(versionId, out var ver))
        {
            return false;
        }

        return ver.Stages.Remove(stage);
    }

    private static void RemoveStageEverywhere(SmSecret secret, string stage, string? exceptVersionId)
    {
        foreach (var vid in secret.Versions.Keys)
        {
            if (string.Equals(vid, exceptVersionId, StringComparison.Ordinal))
            {
                continue;
            }

            RemoveStage(secret, vid, stage);
        }
    }

    private static void RemoveStageEverywhere(SmSecret secret, string stage)
    {
        RemoveStageEverywhere(secret, stage, null);
    }

    private static void AddStage(SmSecret secret, string versionId, string stage)
    {
        if (secret.Versions.TryGetValue(versionId, out var ver) && !ver.Stages.Contains(stage))
        {
            ver.Stages.Add(stage);
        }
    }

    // -- JSON property access helpers ------------------------------------------

    private static string? GetString(JsonElement el, string propertyName)
    {
        return el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;
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

    private static int GetInt(JsonElement el, string propertyName, int defaultValue)
    {
        if (!el.TryGetProperty(propertyName, out var prop))
        {
            return defaultValue;
        }

        return prop.TryGetInt32(out var val) ? val : defaultValue;
    }

    private static int? GetNullableInt(JsonElement el, string propertyName)
    {
        if (!el.TryGetProperty(propertyName, out var prop))
        {
            return null;
        }

        return prop.TryGetInt32(out var val) ? val : null;
    }

    // -- Actions ---------------------------------------------------------------

    private ServiceResponse ActCreateSecret(JsonElement data)
    {
        var name = GetString(data, "Name");
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException", "Name is required.", 400);
        }

        lock (_lock)
        {
            if (_secrets.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceExistsException",
                    $"The operation failed because the secret {name} already exists.", 400);
            }

            var accountId = AccountContext.GetAccountId();
            var arn = $"arn:aws:secretsmanager:{Region}:{accountId}:secret:{name}-{HashHelpers.NewUuid()[..6]}";
            var vid = HashHelpers.NewUuid();
            var now = TimeHelpers.NowEpoch();

            var secret = new SmSecret
            {
                Arn = arn,
                Name = name,
                Description = GetString(data, "Description") ?? "",
                Tags = ParseTags(data),
                CreatedDate = now,
                LastChangedDate = now,
                RotationLambdaArn = GetString(data, "RotationLambdaARN"),
                KmsKeyId = GetString(data, "KmsKeyId"),
            };

            if (data.TryGetProperty("RotationRules", out var rotRulesEl))
            {
                secret.RotationRules = ParseRotationRules(rotRulesEl);
            }

            secret.Versions[vid] = new SmSecretVersion
            {
                SecretString = GetString(data, "SecretString"),
                SecretBinary = GetString(data, "SecretBinary"),
                CreatedDate = now,
                Stages = ["AWSCURRENT"],
            };

            _secrets[name] = secret;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ARN"] = arn,
                ["Name"] = name,
                ["VersionId"] = vid,
            });
        }
    }

    private ServiceResponse ActGetSecretValue(JsonElement data)
    {
        lock (_lock)
        {
            return GetSecretValueInternal(data);
        }
    }

    /// <summary>
    /// Internal get-secret-value logic. Must be called while holding <see cref="_lock"/>.
    /// </summary>
    private ServiceResponse GetSecretValueInternal(JsonElement data)
    {
        var (_, secret) = Resolve(data, "SecretId");
        if (secret is null)
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "ResourceNotFoundException",
                "Secrets Manager can't find the specified secret.", 400);
        }

        if (secret.DeletedDate.HasValue)
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidRequestException",
                "You can't perform this operation on the secret because it was marked for deletion.", 400);
        }

        secret.LastAccessedDate = TimeHelpers.NowEpoch();

        var reqVid = GetString(data, "VersionId");
        var reqStage = GetString(data, "VersionStage") ?? "AWSCURRENT";

        string? vid;
        SmSecretVersion? ver;

        if (reqVid is not null)
        {
            if (!secret.Versions.TryGetValue(reqVid, out ver))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"Secrets Manager can't find the specified secret version: {reqVid}.", 400);
            }

            vid = reqVid;
        }
        else
        {
            (vid, ver) = FindStageVersion(secret, reqStage);
            if (ver is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"Secrets Manager can't find the specified secret value for staging label: {reqStage}.", 400);
            }
        }

        var result = new Dictionary<string, object?>
        {
            ["ARN"] = secret.Arn,
            ["Name"] = secret.Name,
            ["VersionId"] = vid,
            ["VersionStages"] = new List<string>(ver.Stages),
            ["CreatedDate"] = ver.CreatedDate,
        };

        if (ver.SecretString is not null)
        {
            result["SecretString"] = ver.SecretString;
        }

        if (ver.SecretBinary is not null)
        {
            result["SecretBinary"] = ver.SecretBinary;
        }

        return AwsResponseHelpers.JsonResponse(result);
    }

    private ServiceResponse ActBatchGetSecretValue(JsonElement data)
    {
        lock (_lock)
        {
            var secretIds = new List<string>();
            if (data.TryGetProperty("SecretIdList", out var listEl) && listEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in listEl.EnumerateArray())
                {
                    var s = item.GetString();
                    if (s is not null)
                    {
                        secretIds.Add(s);
                    }
                }
            }

            List<string> targets;
            if (secretIds.Count > 0)
            {
                targets = secretIds;
            }
            else
            {
                targets = [];
                foreach (var (name, secret) in _secrets.Items)
                {
                    if (!secret.DeletedDate.HasValue)
                    {
                        targets.Add(name);
                    }
                }

                targets.Sort(StringComparer.Ordinal);
            }

            var results = new List<Dictionary<string, object?>>();
            var errors = new List<Dictionary<string, object?>>();

            foreach (var sid in targets)
            {
                // Build a JsonElement for the internal call
                var innerJson = JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, string> { ["SecretId"] = sid }, MicroStackJsonContext.Default.DictionaryStringString);
                using var innerDoc = JsonDocument.Parse(innerJson);
                var innerData = innerDoc.RootElement.Clone();

                var resp = GetSecretValueInternal(innerData);
                using var respDoc = JsonDocument.Parse(resp.Body);
                var parsed = respDoc.RootElement;

                if (resp.StatusCode >= 400)
                {
                    errors.Add(new Dictionary<string, object?>
                    {
                        ["SecretId"] = sid,
                        ["ErrorCode"] = parsed.TryGetProperty("__type", out var typeProp) ? typeProp.GetString() : "UnknownError",
                        ["Message"] = parsed.TryGetProperty("message", out var msgProp) ? msgProp.GetString() : "",
                    });
                }
                else
                {
                    var entry = JsonElementToDict(parsed);
                    results.Add(entry);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["SecretValues"] = results,
                ["Errors"] = errors,
            });
        }
    }

    private ServiceResponse ActListSecrets(JsonElement data)
    {
        lock (_lock)
        {
            var maxResults = Math.Min(GetInt(data, "MaxResults", 100), 100);
            var nextToken = GetString(data, "NextToken");

            // Build filtered name list (non-deleted only)
            var names = new List<string>();
            foreach (var (name, secret) in _secrets.Items)
            {
                if (!secret.DeletedDate.HasValue)
                {
                    names.Add(name);
                }
            }

            names.Sort(StringComparer.Ordinal);

            // Apply filters
            if (data.TryGetProperty("Filters", out var filtersEl) && filtersEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var filter in filtersEl.EnumerateArray())
                {
                    var filterKey = GetString(filter, "Key") ?? "";
                    var filterValues = new List<string>();
                    if (filter.TryGetProperty("Values", out var valuesEl) && valuesEl.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var v in valuesEl.EnumerateArray())
                        {
                            var s = v.GetString();
                            if (s is not null)
                            {
                                filterValues.Add(s.ToLowerInvariant());
                            }
                        }
                    }

                    names = filterKey switch
                    {
                        "name" => names.FindAll(n =>
                            filterValues.Exists(v => n.ToLowerInvariant().Contains(v, StringComparison.Ordinal))),
                        "tag-key" => names.FindAll(n =>
                            _secrets.TryGetValue(n, out var s) &&
                            s.Tags.Exists(t => filterValues.Contains(t.Key.ToLowerInvariant()))),
                        "tag-value" => names.FindAll(n =>
                            _secrets.TryGetValue(n, out var s) &&
                            s.Tags.Exists(t => filterValues.Contains(t.Value.ToLowerInvariant()))),
                        "description" => names.FindAll(n =>
                            _secrets.TryGetValue(n, out var s) &&
                            filterValues.Exists(v => s.Description.ToLowerInvariant().Contains(v, StringComparison.Ordinal))),
                        _ => names,
                    };
                }
            }

            // Pagination
            var start = 0;
            if (!string.IsNullOrEmpty(nextToken))
            {
                try
                {
                    start = int.Parse(Encoding.UTF8.GetString(Convert.FromBase64String(nextToken)));
                }
                catch
                {
                    // Ignore invalid tokens
                }
            }

            var page = names.GetRange(start, Math.Min(maxResults, names.Count - start));
            var secretList = new List<Dictionary<string, object?>>();
            foreach (var n in page)
            {
                if (!_secrets.TryGetValue(n, out var s))
                {
                    continue;
                }

                secretList.Add(new Dictionary<string, object?>
                {
                    ["ARN"] = s.Arn,
                    ["Name"] = s.Name,
                    ["Description"] = s.Description,
                    ["CreatedDate"] = s.CreatedDate,
                    ["LastChangedDate"] = s.LastChangedDate,
                    ["LastAccessedDate"] = s.LastAccessedDate,
                    ["Tags"] = s.Tags.ConvertAll(t => new Dictionary<string, string> { ["Key"] = t.Key, ["Value"] = t.Value }),
                    ["SecretVersionsToStages"] = VidToStages(s),
                    ["RotationEnabled"] = s.RotationEnabled,
                });
            }

            var resp = new Dictionary<string, object?>
            {
                ["SecretList"] = secretList,
            };

            var end = start + maxResults;
            if (end < names.Count)
            {
                resp["NextToken"] = Convert.ToBase64String(Encoding.UTF8.GetBytes(end.ToString()));
            }

            return AwsResponseHelpers.JsonResponse(resp);
        }
    }

    private ServiceResponse ActDeleteSecret(JsonElement data)
    {
        lock (_lock)
        {
            var (key, secret) = Resolve(data, "SecretId");
            if (secret is null || key is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            if (secret.DeletedDate.HasValue)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidRequestException",
                    "You can't perform this operation on the secret because it was already scheduled for deletion.", 400);
            }

            var force = GetBool(data, "ForceDeleteWithoutRecovery", false);
            var window = GetNullableInt(data, "RecoveryWindowInDays");

            if (force && window.HasValue)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidParameterException",
                    "You can't use ForceDeleteWithoutRecovery in conjunction with RecoveryWindowInDays.", 400);
            }

            var effectiveWindow = window ?? 30;
            if (!force && (effectiveWindow < 7 || effectiveWindow > 30))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidParameterException",
                    "RecoveryWindowInDays value must be between 7 and 30 days (inclusive).", 400);
            }

            var now = TimeHelpers.NowEpoch();
            var deletionDate = force ? now : now + effectiveWindow * 86400;

            if (force)
            {
                var arn = secret.Arn;
                var sname = secret.Name;
                _secrets.TryRemove(key, out _);
                return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
                {
                    ["ARN"] = arn,
                    ["Name"] = sname,
                    ["DeletionDate"] = deletionDate,
                });
            }

            secret.DeletedDate = deletionDate;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["Name"] = secret.Name,
                ["DeletionDate"] = deletionDate,
            });
        }
    }

    private ServiceResponse ActRestoreSecret(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            if (!secret.DeletedDate.HasValue)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidRequestException",
                    "Secret is not scheduled for deletion.", 400);
            }

            secret.DeletedDate = null;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["Name"] = secret.Name,
            });
        }
    }

    private ServiceResponse ActUpdateSecret(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            if (secret.DeletedDate.HasValue)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidRequestException",
                    "You can't perform this operation on the secret because it was marked for deletion.", 400);
            }

            if (data.TryGetProperty("Description", out var descEl))
            {
                secret.Description = descEl.GetString() ?? "";
            }

            if (data.TryGetProperty("KmsKeyId", out var kmsEl))
            {
                secret.KmsKeyId = kmsEl.GetString();
            }

            var hasNewValue = data.TryGetProperty("SecretString", out _) || data.TryGetProperty("SecretBinary", out _);
            if (!hasNewValue)
            {
                secret.LastChangedDate = TimeHelpers.NowEpoch();
                return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
                {
                    ["ARN"] = secret.Arn,
                    ["Name"] = secret.Name,
                });
            }

            var vid = HashHelpers.NewUuid();
            var now = TimeHelpers.NowEpoch();
            secret.Versions[vid] = new SmSecretVersion
            {
                SecretString = GetString(data, "SecretString"),
                SecretBinary = GetString(data, "SecretBinary"),
                CreatedDate = now,
                Stages = [],
            };

            ApplyCurrentPromotion(secret, vid);
            secret.LastChangedDate = now;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["Name"] = secret.Name,
                ["VersionId"] = vid,
            });
        }
    }

    private ServiceResponse ActDescribeSecret(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            var result = new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["Name"] = secret.Name,
                ["Description"] = secret.Description,
                ["CreatedDate"] = secret.CreatedDate,
                ["LastChangedDate"] = secret.LastChangedDate,
                ["LastAccessedDate"] = secret.LastAccessedDate,
                ["Tags"] = secret.Tags.ConvertAll(t => new Dictionary<string, string> { ["Key"] = t.Key, ["Value"] = t.Value }),
                ["VersionIdsToStages"] = VidToStages(secret),
                ["RotationEnabled"] = secret.RotationEnabled,
            };

            if (secret.DeletedDate.HasValue)
            {
                result["DeletedDate"] = secret.DeletedDate.Value;
            }

            if (secret.KmsKeyId is not null)
            {
                result["KmsKeyId"] = secret.KmsKeyId;
            }

            if (secret.RotationLambdaArn is not null)
            {
                result["RotationLambdaARN"] = secret.RotationLambdaArn;
            }

            if (secret.RotationRules is not null)
            {
                var rules = new Dictionary<string, object?>();
                if (secret.RotationRules.AutomaticallyAfterDays.HasValue)
                {
                    rules["AutomaticallyAfterDays"] = secret.RotationRules.AutomaticallyAfterDays.Value;
                }

                result["RotationRules"] = rules;
            }

            if (secret.ReplicationStatus.Count > 0)
            {
                result["ReplicationStatus"] = secret.ReplicationStatus.ConvertAll(r =>
                    new Dictionary<string, string>
                    {
                        ["Region"] = r.Region,
                        ["Status"] = r.Status,
                        ["StatusMessage"] = r.StatusMessage,
                    });
            }

            return AwsResponseHelpers.JsonResponse(result);
        }
    }

    private ServiceResponse ActPutSecretValue(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            if (secret.DeletedDate.HasValue)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidRequestException",
                    "You can't perform this operation on the secret because it was marked for deletion.", 400);
            }

            var vid = GetString(data, "ClientRequestToken") ?? HashHelpers.NewUuid();
            var stages = new List<string>();
            if (data.TryGetProperty("VersionStages", out var stagesEl) && stagesEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in stagesEl.EnumerateArray())
                {
                    var s = item.GetString();
                    if (s is not null)
                    {
                        stages.Add(s);
                    }
                }
            }

            if (stages.Count == 0)
            {
                stages.Add("AWSCURRENT");
            }

            var now = TimeHelpers.NowEpoch();

            secret.Versions[vid] = new SmSecretVersion
            {
                SecretString = GetString(data, "SecretString"),
                SecretBinary = GetString(data, "SecretBinary"),
                CreatedDate = now,
                Stages = [],
            };

            if (stages.Contains("AWSCURRENT"))
            {
                ApplyCurrentPromotion(secret, vid);
            }
            else
            {
                secret.Versions[vid].Stages = new List<string>(stages);
            }

            secret.LastChangedDate = now;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["Name"] = secret.Name,
                ["VersionId"] = vid,
                ["VersionStages"] = new List<string>(secret.Versions[vid].Stages),
            });
        }
    }

    private ServiceResponse ActUpdateSecretVersionStage(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            if (secret.DeletedDate.HasValue)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidRequestException",
                    "You can't perform this operation on the secret because it was marked for deletion.", 400);
            }

            var versionStage = GetString(data, "VersionStage");
            var moveToVid = GetString(data, "MoveToVersionId");
            var removeFromVid = GetString(data, "RemoveFromVersionId");

            if (string.IsNullOrEmpty(versionStage))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidParameterException", "VersionStage is required.", 400);
            }

            if (string.IsNullOrEmpty(moveToVid) && string.IsNullOrEmpty(removeFromVid))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidParameterException",
                    "You must specify MoveToVersionId or RemoveFromVersionId.", 400);
            }

            // Validate version IDs exist
            foreach (var versionId in new[] { moveToVid, removeFromVid })
            {
                if (versionId is not null && !secret.Versions.ContainsKey(versionId))
                {
                    return AwsResponseHelpers.ErrorResponseJson(
                        "ResourceNotFoundException",
                        $"Secrets Manager can't find the specified secret version: {versionId}.", 400);
                }
            }

            var (currentVid, _) = FindStageVersion(secret, versionStage);

            if (moveToVid is not null)
            {
                if (currentVid is not null && !string.Equals(currentVid, moveToVid, StringComparison.Ordinal))
                {
                    if (removeFromVid is null)
                    {
                        return AwsResponseHelpers.ErrorResponseJson(
                            "InvalidParameterException",
                            $"The staging label {versionStage} is currently attached to version {currentVid}. "
                            + "You must specify RemoveFromVersionId to move it.",
                            400);
                    }

                    if (!string.Equals(removeFromVid, currentVid, StringComparison.Ordinal))
                    {
                        return AwsResponseHelpers.ErrorResponseJson(
                            "InvalidParameterException",
                            $"The staging label {versionStage} is currently attached to version {currentVid}, "
                            + $"not version {removeFromVid}.",
                            400);
                    }
                }
                else if (removeFromVid is not null
                         && !string.Equals(removeFromVid, currentVid, StringComparison.Ordinal)
                         && !string.Equals(removeFromVid, moveToVid, StringComparison.Ordinal))
                {
                    return AwsResponseHelpers.ErrorResponseJson(
                        "InvalidParameterException",
                        $"The staging label {versionStage} is not attached to version {removeFromVid}.",
                        400);
                }
            }

            if (removeFromVid is not null && moveToVid is null
                && !string.Equals(currentVid, removeFromVid, StringComparison.Ordinal))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidParameterException",
                    $"The staging label {versionStage} is not attached to version {removeFromVid}.",
                    400);
            }

            var oldCurrentVid = string.Equals(versionStage, "AWSCURRENT", StringComparison.Ordinal) ? currentVid : null;

            if (removeFromVid is not null && !string.Equals(removeFromVid, moveToVid, StringComparison.Ordinal))
            {
                RemoveStage(secret, removeFromVid, versionStage);
            }

            if (moveToVid is not null)
            {
                RemoveStageEverywhere(secret, versionStage, moveToVid);
                AddStage(secret, moveToVid, versionStage);

                if (oldCurrentVid is not null && !string.Equals(oldCurrentVid, moveToVid, StringComparison.Ordinal))
                {
                    RemoveStageEverywhere(secret, "AWSPREVIOUS");
                    AddStage(secret, oldCurrentVid, "AWSPREVIOUS");
                }
            }

            secret.LastChangedDate = TimeHelpers.NowEpoch();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["Name"] = secret.Name,
            });
        }
    }

    private ServiceResponse ActTagResource(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            var existing = new Dictionary<string, SmTag>(StringComparer.Ordinal);
            foreach (var t in secret.Tags)
            {
                existing[t.Key] = t;
            }

            if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var tagEl in tagsEl.EnumerateArray())
                {
                    var key = GetString(tagEl, "Key");
                    var value = GetString(tagEl, "Value") ?? "";
                    if (key is not null)
                    {
                        existing[key] = new SmTag { Key = key, Value = value };
                    }
                }
            }

            secret.Tags = [.. existing.Values];
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActUntagResource(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            var keysToRemove = new HashSet<string>(StringComparer.Ordinal);
            if (data.TryGetProperty("TagKeys", out var keysEl) && keysEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in keysEl.EnumerateArray())
                {
                    var s = item.GetString();
                    if (s is not null)
                    {
                        keysToRemove.Add(s);
                    }
                }
            }

            secret.Tags = secret.Tags.FindAll(t => !keysToRemove.Contains(t.Key));
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListSecretVersionIds(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            var maxResults = Math.Min(GetInt(data, "MaxResults", 100), 100);
            var nextToken = GetString(data, "NextToken");

            var allVids = new List<string>(secret.Versions.Keys);
            allVids.Sort(StringComparer.Ordinal);

            var start = 0;
            if (!string.IsNullOrEmpty(nextToken))
            {
                try
                {
                    start = int.Parse(Encoding.UTF8.GetString(Convert.FromBase64String(nextToken)));
                }
                catch
                {
                    // Ignore invalid tokens
                }
            }

            var page = allVids.GetRange(start, Math.Min(maxResults, allVids.Count - start));
            var versions = new List<Dictionary<string, object?>>();
            foreach (var vid in page)
            {
                var ver = secret.Versions[vid];
                versions.Add(new Dictionary<string, object?>
                {
                    ["VersionId"] = vid,
                    ["VersionStages"] = new List<string>(ver.Stages),
                    ["CreatedDate"] = ver.CreatedDate,
                });
            }

            var resp = new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["Name"] = secret.Name,
                ["Versions"] = versions,
            };

            var end = start + maxResults;
            if (end < allVids.Count)
            {
                resp["NextToken"] = Convert.ToBase64String(Encoding.UTF8.GetBytes(end.ToString()));
            }

            return AwsResponseHelpers.JsonResponse(resp);
        }
    }

    private ServiceResponse ActRotateSecret(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            if (secret.DeletedDate.HasValue)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidRequestException",
                    "You can't perform this operation on the secret because it was marked for deletion.", 400);
            }

            var lambdaArn = GetString(data, "RotationLambdaARN") ?? secret.RotationLambdaArn;
            if (data.TryGetProperty("RotationRules", out var rotRulesEl))
            {
                secret.RotationRules = ParseRotationRules(rotRulesEl);
            }

            if (lambdaArn is not null)
            {
                secret.RotationLambdaArn = lambdaArn;
            }

            secret.RotationEnabled = true;

            var vid = GetString(data, "ClientRequestToken") ?? HashHelpers.NewUuid();
            var now = TimeHelpers.NowEpoch();

            var (_, currVer) = FindStageVersion(secret, "AWSCURRENT");
            secret.Versions[vid] = new SmSecretVersion
            {
                SecretString = currVer?.SecretString,
                SecretBinary = currVer?.SecretBinary,
                CreatedDate = now,
                Stages = ["AWSPENDING"],
            };

            ApplyCurrentPromotion(secret, vid);
            secret.LastChangedDate = now;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["Name"] = secret.Name,
                ["VersionId"] = vid,
            });
        }
    }

    private static ServiceResponse ActGetRandomPassword(JsonElement data)
    {
        var length = GetInt(data, "PasswordLength", 32);
        if (length < 1 || length > 4096)
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException", "PasswordLength must be between 1 and 4096.", 400);
        }

        var excludeCharsStr = GetString(data, "ExcludeCharacters") ?? "";
        var excludeChars = new HashSet<char>(excludeCharsStr);
        var excludeNumbers = GetBool(data, "ExcludeNumbers", false);
        var excludePunctuation = GetBool(data, "ExcludePunctuation", false);
        var excludeUpper = GetBool(data, "ExcludeUppercase", false);
        var excludeLower = GetBool(data, "ExcludeLowercase", false);
        var includeSpace = GetBool(data, "IncludeSpace", false);
        var requireEach = GetBool(data, "RequireEachIncludedType", true);

        var pools = new List<List<char>>();
        var allChars = new List<char>();

        void AddPool(string chars)
        {
            var filtered = new List<char>();
            foreach (var c in chars)
            {
                if (!excludeChars.Contains(c))
                {
                    filtered.Add(c);
                }
            }

            if (filtered.Count > 0)
            {
                pools.Add(filtered);
                allChars.AddRange(filtered);
            }
        }

        if (!excludeLower)
        {
            AddPool(LowercaseChars);
        }

        if (!excludeUpper)
        {
            AddPool(UppercaseChars);
        }

        if (!excludeNumbers)
        {
            AddPool(DigitChars);
        }

        if (!excludePunctuation)
        {
            AddPool(PunctuationChars);
        }

        if (includeSpace)
        {
            AddPool(" ");
        }

        if (allChars.Count == 0)
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException", "No characters available to generate password.", 400);
        }

        if (requireEach && pools.Count > length)
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException", "PasswordLength too short to include required character types.", 400);
        }

        var pw = new char[length];
        if (requireEach)
        {
            // One from each pool first
            for (var i = 0; i < pools.Count; i++)
            {
                pw[i] = pools[i][RandomNumberGenerator.GetInt32(pools[i].Count)];
            }

            // Fill remaining with any char
            for (var i = pools.Count; i < length; i++)
            {
                pw[i] = allChars[RandomNumberGenerator.GetInt32(allChars.Count)];
            }

            // Shuffle using Fisher-Yates
            for (var i = pw.Length - 1; i > 0; i--)
            {
                var j = RandomNumberGenerator.GetInt32(i + 1);
                (pw[i], pw[j]) = (pw[j], pw[i]);
            }
        }
        else
        {
            for (var i = 0; i < length; i++)
            {
                pw[i] = allChars[RandomNumberGenerator.GetInt32(allChars.Count)];
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["RandomPassword"] = new string(pw),
        });
    }

    private ServiceResponse ActReplicateSecretToRegions(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            if (data.TryGetProperty("AddReplicaRegions", out var regionsEl) && regionsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var regionEl in regionsEl.EnumerateArray())
                {
                    var region = GetString(regionEl, "Region");
                    if (region is not null)
                    {
                        secret.ReplicationStatus.Add(new SmReplicationStatus
                        {
                            Region = region,
                            Status = "InSync",
                            StatusMessage = "Replication succeeded (stub).",
                        });
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["ReplicationStatus"] = secret.ReplicationStatus.ConvertAll(r =>
                    new Dictionary<string, string>
                    {
                        ["Region"] = r.Region,
                        ["Status"] = r.Status,
                        ["StatusMessage"] = r.StatusMessage,
                    }),
            });
        }
    }

    // -- Resource Policies -----------------------------------------------------

    private ServiceResponse ActPutResourcePolicy(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            var policy = GetString(data, "ResourcePolicy") ?? "{}";
            _resourcePolicies[secret.Arn] = policy;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["Name"] = secret.Name,
            });
        }
    }

    private ServiceResponse ActGetResourcePolicy(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            var result = new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["Name"] = secret.Name,
            };

            if (_resourcePolicies.TryGetValue(secret.Arn, out var policy))
            {
                result["ResourcePolicy"] = policy;
            }

            return AwsResponseHelpers.JsonResponse(result);
        }
    }

    private ServiceResponse ActDeleteResourcePolicy(JsonElement data)
    {
        lock (_lock)
        {
            var (_, secret) = Resolve(data, "SecretId");
            if (secret is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    "Secrets Manager can't find the specified secret.", 400);
            }

            _resourcePolicies.TryRemove(secret.Arn, out _);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ARN"] = secret.Arn,
                ["Name"] = secret.Name,
            });
        }
    }

    private static ServiceResponse ActValidateResourcePolicy(JsonElement data)
    {
        _ = data; // unused but matches action signature
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["PolicyValidationPassed"] = true,
            ["ValidationErrors"] = Array.Empty<object>(),
        });
    }

    // -- Tag / RotationRules parsing helpers ------------------------------------

    private static List<SmTag> ParseTags(JsonElement data)
    {
        var tags = new List<SmTag>();
        if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var tagEl in tagsEl.EnumerateArray())
            {
                var key = GetString(tagEl, "Key");
                var value = GetString(tagEl, "Value") ?? "";
                if (key is not null)
                {
                    tags.Add(new SmTag { Key = key, Value = value });
                }
            }
        }

        return tags;
    }

    private static SmRotationRules? ParseRotationRules(JsonElement el)
    {
        if (el.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        var rules = new SmRotationRules();
        if (el.TryGetProperty("AutomaticallyAfterDays", out var daysEl) && daysEl.TryGetInt32(out var days))
        {
            rules.AutomaticallyAfterDays = days;
        }

        return rules;
    }

    /// <summary>Convert a JsonElement (object) into a Dictionary for serialization round-tripping.</summary>
    private static Dictionary<string, object?> JsonElementToDict(JsonElement element)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (element.ValueKind != JsonValueKind.Object)
        {
            return dict;
        }

        foreach (var prop in element.EnumerateObject())
        {
            dict[prop.Name] = prop.Value.ValueKind switch
            {
                JsonValueKind.String => prop.Value.GetString(),
                JsonValueKind.Number => prop.Value.GetDouble(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                JsonValueKind.Array => JsonElementToArray(prop.Value),
                JsonValueKind.Object => JsonElementToDict(prop.Value),
                _ => prop.Value.GetRawText(),
            };
        }

        return dict;
    }

    private static List<object?> JsonElementToArray(JsonElement element)
    {
        var list = new List<object?>();
        foreach (var item in element.EnumerateArray())
        {
            list.Add(item.ValueKind switch
            {
                JsonValueKind.String => item.GetString(),
                JsonValueKind.Number => item.GetDouble(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                JsonValueKind.Array => JsonElementToArray(item),
                JsonValueKind.Object => JsonElementToDict(item),
                _ => item.GetRawText(),
            });
        }

        return list;
    }
}

// -- Data models ---------------------------------------------------------------

internal sealed class SmSecret
{
    public required string Arn { get; set; }
    public required string Name { get; set; }
    public string Description { get; set; } = "";
    public List<SmTag> Tags { get; set; } = [];
    public double CreatedDate { get; set; }
    public double LastChangedDate { get; set; }
    public double? LastAccessedDate { get; set; }
    public double? DeletedDate { get; set; }
    public bool RotationEnabled { get; set; }
    public string? RotationLambdaArn { get; set; }
    public SmRotationRules? RotationRules { get; set; }
    public string? KmsKeyId { get; set; }
    public List<SmReplicationStatus> ReplicationStatus { get; set; } = [];
    public Dictionary<string, SmSecretVersion> Versions { get; set; } = new(StringComparer.Ordinal);
}

internal sealed class SmSecretVersion
{
    public string? SecretString { get; set; }
    public string? SecretBinary { get; set; } // base64-encoded
    public double CreatedDate { get; set; }
    public List<string> Stages { get; set; } = [];
}

internal sealed class SmTag
{
    public required string Key { get; set; }
    public required string Value { get; set; }
}

internal sealed class SmRotationRules
{
    public int? AutomaticallyAfterDays { get; set; }
}

internal sealed class SmReplicationStatus
{
    public required string Region { get; set; }
    public required string Status { get; set; }
    public required string StatusMessage { get; set; }
}

// Persistence state records for SecretsManagerServiceHandler
internal sealed record SmSecretsEntry(string AccountId, string Key, SmSecret Value);
internal sealed record SmPolicyEntry(string AccountId, string Key, string Value);
internal sealed record SecretsManagerState(List<SmSecretsEntry> Secrets, List<SmPolicyEntry> Policies);
