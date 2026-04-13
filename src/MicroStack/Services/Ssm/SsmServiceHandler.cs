using System.Text;
using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Ssm;

/// <summary>
/// SSM Parameter Store service handler -- supports JSON protocol via X-Amz-Target.
///
/// Port of ministack/services/ssm.py.
///
/// Supports: PutParameter, GetParameter, GetParameters, GetParametersByPath,
///           DeleteParameter, DeleteParameters, DescribeParameters,
///           GetParameterHistory, LabelParameterVersion,
///           AddTagsToResource, RemoveTagsFromResource, ListTagsForResource.
/// </summary>
internal sealed class SsmServiceHandler : IServiceHandler
{
    private readonly AccountScopedDictionary<string, SsmParameter> _parameters = new();
    private readonly AccountScopedDictionary<string, List<SsmHistoryEntry>> _parameterHistory = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _tags = new();
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    private const int DefaultPageSize = 10;

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "ssm";

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
            "PutParameter" => ActPutParameter(data),
            "GetParameter" => ActGetParameter(data),
            "GetParameters" => ActGetParameters(data),
            "GetParametersByPath" => ActGetParametersByPath(data),
            "DeleteParameter" => ActDeleteParameter(data),
            "DeleteParameters" => ActDeleteParameters(data),
            "DescribeParameters" => ActDescribeParameters(data),
            "GetParameterHistory" => ActGetParameterHistory(data),
            "LabelParameterVersion" => ActLabelParameterVersion(data),
            "AddTagsToResource" => ActAddTagsToResource(data),
            "RemoveTagsFromResource" => ActRemoveTagsFromResource(data),
            "ListTagsForResource" => ActListTagsForResource(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _parameters.Clear();
            _parameterHistory.Clear();
            _tags.Clear();
        }
    }

    public JsonElement? GetState()
    {
        lock (_lock)
        {
            var parameters = _parameters.ToRaw()
                .Select(kv => new SsmParameterEntry(kv.Key.AccountId, kv.Key.Key, kv.Value))
                .ToList();
            var history = _parameterHistory.ToRaw()
                .Select(kv => new SsmHistoryEntry2(kv.Key.AccountId, kv.Key.Key, kv.Value))
                .ToList();
            var tags = _tags.ToRaw()
                .Select(kv => new SsmTagEntry(kv.Key.AccountId, kv.Key.Key, kv.Value))
                .ToList();
            var state = new SsmState(parameters, history, tags);
            return JsonSerializer.SerializeToElement(state, MicroStackJsonContext.Default.SsmState);
        }
    }

    public void RestoreState(JsonElement state)
    {
        var restored = JsonSerializer.Deserialize(state, MicroStackJsonContext.Default.SsmState);
        if (restored is null) return;
        lock (_lock)
        {
            _parameters.FromRaw(restored.Parameters.Select(e =>
                new KeyValuePair<(string, string), SsmParameter>((e.AccountId, e.Key), e.Value)));
            _parameterHistory.FromRaw(restored.History.Select(e =>
                new KeyValuePair<(string, string), List<SsmHistoryEntry>>((e.AccountId, e.Key), e.Value)));
            _tags.FromRaw(restored.Tags.Select(e =>
                new KeyValuePair<(string, string), Dictionary<string, string>>((e.AccountId, e.Key), e.Value)));
        }
    }

    // -- Helpers ---------------------------------------------------------------

    private static string ParamArn(string name)
    {
        return $"arn:aws:ssm:{Region}:{AccountContext.GetAccountId()}:parameter{name}";
    }

    private static string EncodeNextToken(int index)
    {
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(index.ToString()));
    }

    private static int DecodeNextToken(string token)
    {
        try
        {
            return int.Parse(Encoding.UTF8.GetString(Convert.FromBase64String(token)));
        }
        catch
        {
            return 0;
        }
    }

    private static Dictionary<string, object?> ParamOut(SsmParameter param, bool withDecryption)
    {
        string value;
        if (withDecryption || !string.Equals(param.Type, "SecureString", StringComparison.Ordinal))
        {
            value = param.OriginalValue;
        }
        else
        {
            value = param.Value;
        }

        return new Dictionary<string, object?>
        {
            ["Name"] = param.Name,
            ["Type"] = param.Type,
            ["Value"] = value,
            ["Version"] = param.Version,
            ["ARN"] = param.Arn,
            ["LastModifiedDate"] = param.LastModifiedDate,
            ["DataType"] = param.DataType,
        };
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

    private static List<string> GetStringList(JsonElement el, string propertyName)
    {
        var result = new List<string>();
        if (el.TryGetProperty(propertyName, out var arr) && arr.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in arr.EnumerateArray())
            {
                var s = item.GetString();
                if (s is not null)
                {
                    result.Add(s);
                }
            }
        }

        return result;
    }

    // -- Actions ---------------------------------------------------------------

    private ServiceResponse ActPutParameter(JsonElement data)
    {
        var name = GetString(data, "Name");
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "ValidationException", "Name is required", 400);
        }

        var paramType = GetString(data, "Type") ?? "String";
        var value = GetString(data, "Value") ?? "";
        var overwrite = GetBool(data, "Overwrite", false);

        lock (_lock)
        {
            if (_parameters.ContainsKey(name) && !overwrite)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ParameterAlreadyExists",
                    "The parameter already exists. To overwrite this value, set the overwrite option in the request to true.",
                    400);
            }

            var version = _parameters.TryGetValue(name, out var existing)
                ? existing.Version + 1
                : 1;
            var arn = ParamArn(name);
            var now = TimeHelpers.NowEpoch();

            string storedValue;
            string keyId;
            if (string.Equals(paramType, "SecureString", StringComparison.Ordinal))
            {
                keyId = GetString(data, "KeyId") ?? "alias/aws/ssm";
                storedValue = "ENCRYPTED:" + Convert.ToBase64String(Encoding.UTF8.GetBytes(value));
            }
            else
            {
                keyId = "";
                storedValue = value;
            }

            var existingDescription = _parameters.TryGetValue(name, out var prev)
                ? prev.Description
                : "";

            var record = new SsmParameter
            {
                Name = name,
                Value = storedValue,
                OriginalValue = value,
                Type = paramType,
                KeyId = keyId,
                Version = version,
                Arn = arn,
                LastModifiedDate = now,
                DataType = GetString(data, "DataType") ?? "text",
                Description = GetString(data, "Description") ?? existingDescription,
                Tier = GetString(data, "Tier") ?? "Standard",
                AllowedPattern = GetString(data, "AllowedPattern") ?? "",
                Policies = [],
                Labels = [],
            };

            _parameters[name] = record;

            var historyEntry = new SsmHistoryEntry
            {
                Name = name,
                Value = storedValue,
                OriginalValue = value,
                Type = paramType,
                KeyId = keyId,
                Version = version,
                LastModifiedDate = now,
                LastModifiedUser = $"arn:aws:iam::{AccountContext.GetAccountId()}:root",
                Description = record.Description,
                AllowedPattern = record.AllowedPattern,
                Tier = record.Tier,
                Policies = [],
                DataType = record.DataType,
                Labels = [],
            };

            if (!_parameterHistory.TryGetValue(name, out var history))
            {
                history = [];
                _parameterHistory[name] = history;
            }

            history.Add(historyEntry);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Version"] = version,
                ["Tier"] = record.Tier,
            });
        }
    }

    private ServiceResponse ActGetParameter(JsonElement data)
    {
        var name = GetString(data, "Name");

        lock (_lock)
        {
            if (name is null || !_parameters.TryGetValue(name, out var param))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ParameterNotFound", $"Parameter {name} not found", 400);
            }

            var withDecryption = GetBool(data, "WithDecryption", false);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Parameter"] = ParamOut(param, withDecryption),
            });
        }
    }

    private ServiceResponse ActGetParameters(JsonElement data)
    {
        var names = GetStringList(data, "Names");
        var withDecryption = GetBool(data, "WithDecryption", false);

        lock (_lock)
        {
            var parameters = new List<Dictionary<string, object?>>();
            var invalid = new List<string>();

            foreach (var name in names)
            {
                if (_parameters.TryGetValue(name, out var param))
                {
                    parameters.Add(ParamOut(param, withDecryption));
                }
                else
                {
                    invalid.Add(name);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Parameters"] = parameters,
                ["InvalidParameters"] = invalid,
            });
        }
    }

    private ServiceResponse ActGetParametersByPath(JsonElement data)
    {
        var path = GetString(data, "Path") ?? "/";
        var recursive = GetBool(data, "Recursive", false);
        var withDecryption = GetBool(data, "WithDecryption", false);
        var maxResults = GetInt(data, "MaxResults", DefaultPageSize);
        var nextToken = GetString(data, "NextToken");

        var pathPrefix = path.EndsWith('/') ? path : path + "/";

        lock (_lock)
        {
            var allResults = new List<SsmParameter>();
            var sortedNames = _parameters.Keys.OrderBy(n => n, StringComparer.Ordinal).ToList();

            foreach (var name in sortedNames)
            {
                if (string.Equals(name, path, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!name.StartsWith(pathPrefix, StringComparison.Ordinal)
                    && !(name.StartsWith(path, StringComparison.Ordinal) && path == "/"))
                {
                    continue;
                }

                bool matches;
                if (path == "/")
                {
                    matches = true;
                }
                else if (recursive)
                {
                    matches = true;
                }
                else
                {
                    var suffix = name[pathPrefix.Length..];
                    matches = !suffix.Contains('/');
                }

                if (matches && _parameters.TryGetValue(name, out var param))
                {
                    allResults.Add(param);
                }
            }

            var start = 0;
            if (!string.IsNullOrEmpty(nextToken))
            {
                start = DecodeNextToken(nextToken);
            }

            var page = allResults.GetRange(start, Math.Min(maxResults, allResults.Count - start));
            var output = page.ConvertAll(p => ParamOut(p, withDecryption));

            var resp = new Dictionary<string, object?>
            {
                ["Parameters"] = output,
            };

            if (start + maxResults < allResults.Count)
            {
                resp["NextToken"] = EncodeNextToken(start + maxResults);
            }

            return AwsResponseHelpers.JsonResponse(resp);
        }
    }

    private ServiceResponse ActDeleteParameter(JsonElement data)
    {
        var name = GetString(data, "Name");

        lock (_lock)
        {
            if (name is null || !_parameters.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ParameterNotFound", $"Parameter {name} not found", 400);
            }

            _parameters.TryRemove(name, out _);
            _parameterHistory.TryRemove(name, out _);
            var arn = ParamArn(name);
            _tags.TryRemove(arn, out _);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDeleteParameters(JsonElement data)
    {
        var names = GetStringList(data, "Names");

        lock (_lock)
        {
            var deleted = new List<string>();
            var invalid = new List<string>();

            foreach (var name in names)
            {
                if (_parameters.ContainsKey(name))
                {
                    _parameters.TryRemove(name, out _);
                    _parameterHistory.TryRemove(name, out _);
                    _tags.TryRemove(ParamArn(name), out _);
                    deleted.Add(name);
                }
                else
                {
                    invalid.Add(name);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["DeletedParameters"] = deleted,
                ["InvalidParameters"] = invalid,
            });
        }
    }

    private ServiceResponse ActDescribeParameters(JsonElement data)
    {
        var maxResults = GetInt(data, "MaxResults", DefaultPageSize);
        var nextToken = GetString(data, "NextToken");

        lock (_lock)
        {
            var candidates = new List<SsmParameter>(_parameters.Values);

            // Apply ParameterFilters
            if (data.TryGetProperty("ParameterFilters", out var filtersEl) && filtersEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var f in filtersEl.EnumerateArray())
                {
                    var key = GetString(f, "Key") ?? "";
                    var option = GetString(f, "Option") ?? "Equals";
                    var values = GetStringList(f, "Values");
                    candidates = candidates.FindAll(p => ApplyFilter(p, key, option, values));
                }
            }

            // Apply legacy Filters
            if (data.TryGetProperty("Filters", out var stringFiltersEl) && stringFiltersEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var f in stringFiltersEl.EnumerateArray())
                {
                    var key = GetString(f, "Key") ?? "";
                    var values = GetStringList(f, "Values");
                    if (string.Equals(key, "Name", StringComparison.Ordinal) && values.Count > 0)
                    {
                        candidates = candidates.FindAll(p => values.Contains(p.Name));
                    }
                    else if (string.Equals(key, "Type", StringComparison.Ordinal) && values.Count > 0)
                    {
                        candidates = candidates.FindAll(p => values.Contains(p.Type));
                    }
                }
            }

            candidates.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.Ordinal));

            var start = 0;
            if (!string.IsNullOrEmpty(nextToken))
            {
                start = DecodeNextToken(nextToken);
            }

            var page = candidates.GetRange(start, Math.Min(maxResults, candidates.Count - start));
            var results = new List<Dictionary<string, object?>>();

            foreach (var param in page)
            {
                var desc = new Dictionary<string, object?>
                {
                    ["Name"] = param.Name,
                    ["Type"] = param.Type,
                    ["Version"] = param.Version,
                    ["LastModifiedDate"] = param.LastModifiedDate,
                    ["LastModifiedUser"] = $"arn:aws:iam::{AccountContext.GetAccountId()}:root",
                    ["ARN"] = param.Arn,
                    ["DataType"] = param.DataType,
                    ["Description"] = param.Description,
                    ["Tier"] = param.Tier,
                    ["AllowedPattern"] = param.AllowedPattern,
                };

                if (param.Policies.Count > 0)
                {
                    desc["Policies"] = param.Policies;
                }

                results.Add(desc);
            }

            var resp = new Dictionary<string, object?>
            {
                ["Parameters"] = results,
            };

            if (start + maxResults < candidates.Count)
            {
                resp["NextToken"] = EncodeNextToken(start + maxResults);
            }

            return AwsResponseHelpers.JsonResponse(resp);
        }
    }

    private static bool ApplyFilter(SsmParameter param, string key, string option, List<string> values)
    {
        if (values.Count == 0)
        {
            return true;
        }

        if (string.Equals(key, "Name", StringComparison.Ordinal))
        {
            var target = param.Name;
            if (string.Equals(option, "Equals", StringComparison.Ordinal))
            {
                return values.Contains(target);
            }

            if (string.Equals(option, "Contains", StringComparison.Ordinal))
            {
                return values.Exists(v => target.Contains(v, StringComparison.Ordinal));
            }

            if (string.Equals(option, "BeginsWith", StringComparison.Ordinal))
            {
                return values.Exists(v => target.StartsWith(v, StringComparison.Ordinal));
            }
        }
        else if (string.Equals(key, "Type", StringComparison.Ordinal))
        {
            return values.Contains(param.Type);
        }
        else if (string.Equals(key, "KeyId", StringComparison.Ordinal))
        {
            return values.Contains(param.KeyId);
        }
        else if (string.Equals(key, "Path", StringComparison.Ordinal))
        {
            var name = param.Name;
            foreach (var v in values)
            {
                var prefix = v.EndsWith('/') ? v : v + "/";
                if (name.StartsWith(prefix, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }
        else if (string.Equals(key, "DataType", StringComparison.Ordinal))
        {
            return values.Contains(param.DataType);
        }
        else if (string.Equals(key, "Tier", StringComparison.Ordinal))
        {
            return values.Contains(param.Tier);
        }
        else if (string.Equals(key, "Label", StringComparison.Ordinal))
        {
            return values.Exists(v => param.Labels.Contains(v));
        }

        return true;
    }

    private ServiceResponse ActGetParameterHistory(JsonElement data)
    {
        var name = GetString(data, "Name");
        var withDecryption = GetBool(data, "WithDecryption", false);
        var maxResults = GetInt(data, "MaxResults", 50);
        var nextToken = GetString(data, "NextToken");

        lock (_lock)
        {
            if (name is null || !_parameterHistory.TryGetValue(name, out var history))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ParameterNotFound", $"Parameter {name} not found", 400);
            }

            var start = 0;
            if (!string.IsNullOrEmpty(nextToken))
            {
                start = DecodeNextToken(nextToken);
            }

            var page = history.GetRange(start, Math.Min(maxResults, history.Count - start));
            var results = new List<Dictionary<string, object?>>();

            foreach (var entry in page)
            {
                string value;
                if (withDecryption || !string.Equals(entry.Type, "SecureString", StringComparison.Ordinal))
                {
                    value = entry.OriginalValue;
                }
                else
                {
                    value = entry.Value;
                }

                results.Add(new Dictionary<string, object?>
                {
                    ["Name"] = entry.Name,
                    ["Type"] = entry.Type,
                    ["Version"] = entry.Version,
                    ["LastModifiedDate"] = entry.LastModifiedDate,
                    ["LastModifiedUser"] = entry.LastModifiedUser,
                    ["Description"] = entry.Description,
                    ["DataType"] = entry.DataType,
                    ["Tier"] = entry.Tier,
                    ["Labels"] = entry.Labels,
                    ["Policies"] = entry.Policies,
                    ["Value"] = value,
                });
            }

            var resp = new Dictionary<string, object?>
            {
                ["Parameters"] = results,
            };

            if (start + maxResults < history.Count)
            {
                resp["NextToken"] = EncodeNextToken(start + maxResults);
            }

            return AwsResponseHelpers.JsonResponse(resp);
        }
    }

    private ServiceResponse ActLabelParameterVersion(JsonElement data)
    {
        var name = GetString(data, "Name");
        var versionParam = GetNullableInt(data, "ParameterVersion");
        var labels = GetStringList(data, "Labels");

        lock (_lock)
        {
            if (name is null || !_parameterHistory.TryGetValue(name, out var history))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ParameterNotFound", $"Parameter {name} not found", 400);
            }

            var version = versionParam ?? _parameters[name].Version;

            SsmHistoryEntry? target = null;
            foreach (var entry in history)
            {
                if (entry.Version == version)
                {
                    target = entry;
                    break;
                }
            }

            if (target is null)
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ParameterVersionNotFound",
                    $"Version {version} of parameter {name} not found",
                    400);
            }

            var invalidLabels = new List<string>();
            foreach (var label in labels)
            {
                if (label.Length > 100
                    || label.StartsWith("aws:", StringComparison.Ordinal)
                    || label.StartsWith("ssm:", StringComparison.Ordinal))
                {
                    invalidLabels.Add(label);
                    continue;
                }

                // Remove label from any other version
                foreach (var entry in history)
                {
                    if (entry.Version != version && entry.Labels.Contains(label))
                    {
                        entry.Labels.Remove(label);
                    }
                }

                if (!target.Labels.Contains(label))
                {
                    target.Labels.Add(label);
                }
            }

            // Sync labels to current parameter version
            if (version == _parameters[name].Version)
            {
                _parameters[name].Labels = new List<string>(target.Labels);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["InvalidLabels"] = invalidLabels,
                ["ParameterVersion"] = version,
            });
        }
    }

    private ServiceResponse ActAddTagsToResource(JsonElement data)
    {
        var resourceType = GetString(data, "ResourceType") ?? "Parameter";
        var resourceId = GetString(data, "ResourceId") ?? "";

        string arn;
        if (string.Equals(resourceType, "Parameter", StringComparison.Ordinal))
        {
            if (!resourceId.StartsWith('/'))
            {
                resourceId = "/" + resourceId;
            }

            arn = ParamArn(resourceId);
        }
        else
        {
            arn = resourceId;
        }

        var newTags = new List<(string Key, string Value)>();
        if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var tagEl in tagsEl.EnumerateArray())
            {
                var key = GetString(tagEl, "Key");
                var value = GetString(tagEl, "Value") ?? "";
                if (key is not null)
                {
                    newTags.Add((key, value));
                }
            }
        }

        lock (_lock)
        {
            if (!_tags.TryGetValue(arn, out var tagDict))
            {
                tagDict = new Dictionary<string, string>(StringComparer.Ordinal);
                _tags[arn] = tagDict;
            }

            foreach (var (key, value) in newTags)
            {
                tagDict[key] = value;
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActRemoveTagsFromResource(JsonElement data)
    {
        var resourceType = GetString(data, "ResourceType") ?? "Parameter";
        var resourceId = GetString(data, "ResourceId") ?? "";
        var tagKeys = GetStringList(data, "TagKeys");

        string arn;
        if (string.Equals(resourceType, "Parameter", StringComparison.Ordinal))
        {
            if (!resourceId.StartsWith('/'))
            {
                resourceId = "/" + resourceId;
            }

            arn = ParamArn(resourceId);
        }
        else
        {
            arn = resourceId;
        }

        lock (_lock)
        {
            if (_tags.TryGetValue(arn, out var tagDict))
            {
                foreach (var key in tagKeys)
                {
                    tagDict.Remove(key);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListTagsForResource(JsonElement data)
    {
        var resourceType = GetString(data, "ResourceType") ?? "Parameter";
        var resourceId = GetString(data, "ResourceId") ?? "";

        string arn;
        if (string.Equals(resourceType, "Parameter", StringComparison.Ordinal))
        {
            if (!resourceId.StartsWith('/'))
            {
                resourceId = "/" + resourceId;
            }

            arn = ParamArn(resourceId);
        }
        else
        {
            arn = resourceId;
        }

        lock (_lock)
        {
            Dictionary<string, string> tagDict;
            if (!_tags.TryGetValue(arn, out tagDict!))
            {
                tagDict = new Dictionary<string, string>(StringComparer.Ordinal);
            }

            var tagList = new List<Dictionary<string, string>>();
            foreach (var (key, value) in tagDict)
            {
                tagList.Add(new Dictionary<string, string>
                {
                    ["Key"] = key,
                    ["Value"] = value,
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["TagList"] = tagList,
            });
        }
    }
}

// -- Data models ---------------------------------------------------------------

internal sealed class SsmParameter
{
    public required string Name { get; set; }
    public required string Value { get; set; }
    public required string OriginalValue { get; set; }
    public required string Type { get; set; }
    public string KeyId { get; set; } = "";
    public int Version { get; set; }
    public required string Arn { get; set; }
    public double LastModifiedDate { get; set; }
    public string DataType { get; set; } = "text";
    public string Description { get; set; } = "";
    public string Tier { get; set; } = "Standard";
    public string AllowedPattern { get; set; } = "";
    public List<string> Policies { get; set; } = [];
    public List<string> Labels { get; set; } = [];
}

internal sealed class SsmHistoryEntry
{
    public required string Name { get; set; }
    public required string Value { get; set; }
    public required string OriginalValue { get; set; }
    public required string Type { get; set; }
    public string KeyId { get; set; } = "";
    public int Version { get; set; }
    public double LastModifiedDate { get; set; }
    public required string LastModifiedUser { get; set; }
    public string Description { get; set; } = "";
    public string AllowedPattern { get; set; } = "";
    public string Tier { get; set; } = "Standard";
    public List<string> Policies { get; set; } = [];
    public string DataType { get; set; } = "text";
    public List<string> Labels { get; set; } = [];
}

// Persistence state records for SsmServiceHandler
internal sealed record SsmParameterEntry(string AccountId, string Key, SsmParameter Value);
internal sealed record SsmHistoryEntry2(string AccountId, string Key, List<SsmHistoryEntry> Value);
internal sealed record SsmTagEntry(string AccountId, string Key, Dictionary<string, string> Value);
internal sealed record SsmState(List<SsmParameterEntry> Parameters, List<SsmHistoryEntry2> History, List<SsmTagEntry> Tags);
