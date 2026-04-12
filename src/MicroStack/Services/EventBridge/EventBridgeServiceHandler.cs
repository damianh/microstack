using System.Text.Json;
using System.Text.RegularExpressions;
using MicroStack.Internal;

namespace MicroStack.Services.EventBridge;

/// <summary>
/// EventBridge service handler -- supports JSON protocol via X-Amz-Target.
///
/// Port of ministack/services/eventbridge.py.
///
/// Supports: CreateEventBus, UpdateEventBus, DeleteEventBus, ListEventBuses, DescribeEventBus,
///           PutRule, DeleteRule, ListRules, DescribeRule, EnableRule, DisableRule,
///           PutTargets, RemoveTargets, ListTargetsByRule,
///           PutEvents, TagResource, UntagResource, ListTagsForResource,
///           CreateArchive, DeleteArchive, DescribeArchive, UpdateArchive, ListArchives,
///           StartReplay, DescribeReplay, ListReplays, CancelReplay,
///           PutPermission, RemovePermission,
///           CreateConnection, DescribeConnection, DeleteConnection, ListConnections,
///           UpdateConnection, DeauthorizeConnection,
///           CreateApiDestination, DescribeApiDestination, DeleteApiDestination,
///           ListApiDestinations, UpdateApiDestination,
///           CreateEndpoint, DeleteEndpoint, DescribeEndpoint, ListEndpoints, UpdateEndpoint,
///           ActivateEventSource, DeactivateEventSource, DescribeEventSource,
///           CreatePartnerEventSource, DeletePartnerEventSource, DescribePartnerEventSource,
///           ListPartnerEventSources, ListPartnerEventSourceAccounts,
///           ListEventSources, PutPartnerEvents, ListRuleNamesByTarget, TestEventPattern.
/// </summary>
internal sealed partial class EventBridgeServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private static readonly string Region =
        Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1";

    // In-memory state
    private Dictionary<string, Dictionary<string, object?>> _eventBuses = CreateDefaultBuses();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _rules = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, object?>>> _targets = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _tags = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _archives = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _eventBusPolicies = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _connections = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _apiDestinations = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _replays = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _endpoints = new();

    private static Dictionary<string, Dictionary<string, object?>> CreateDefaultBuses()
    {
        var now = TimeHelpers.NowEpoch();
        return new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal)
        {
            ["default"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = "default",
                ["Arn"] = $"arn:aws:events:{Region}:{AccountContext.GetAccountId()}:event-bus/default",
                ["CreationTime"] = now,
                ["LastModifiedTime"] = now,
            },
        };
    }

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "events";

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
            "CreateEventBus" => ActCreateEventBus(data),
            "UpdateEventBus" => ActUpdateEventBus(data),
            "DeleteEventBus" => ActDeleteEventBus(data),
            "ListEventBuses" => ActListEventBuses(data),
            "DescribeEventBus" => ActDescribeEventBus(data),
            "PutRule" => ActPutRule(data),
            "DeleteRule" => ActDeleteRule(data),
            "ListRules" => ActListRules(data),
            "DescribeRule" => ActDescribeRule(data),
            "EnableRule" => ActEnableRule(data),
            "DisableRule" => ActDisableRule(data),
            "PutTargets" => ActPutTargets(data),
            "RemoveTargets" => ActRemoveTargets(data),
            "ListTargetsByRule" => ActListTargetsByRule(data),
            "ListRuleNamesByTarget" => ActListRuleNamesByTarget(data),
            "TestEventPattern" => ActTestEventPattern(data),
            "PutEvents" => ActPutEvents(data),
            "TagResource" => ActTagResource(data),
            "UntagResource" => ActUntagResource(data),
            "ListTagsForResource" => ActListTagsForResource(data),
            "CreateArchive" => ActCreateArchive(data),
            "DeleteArchive" => ActDeleteArchive(data),
            "DescribeArchive" => ActDescribeArchive(data),
            "UpdateArchive" => ActUpdateArchive(data),
            "ListArchives" => ActListArchives(data),
            "StartReplay" => ActStartReplay(data),
            "DescribeReplay" => ActDescribeReplay(data),
            "ListReplays" => ActListReplays(data),
            "CancelReplay" => ActCancelReplay(data),
            "PutPermission" => ActPutPermission(data),
            "RemovePermission" => ActRemovePermission(data),
            "CreateConnection" => ActCreateConnection(data),
            "DescribeConnection" => ActDescribeConnection(data),
            "DeleteConnection" => ActDeleteConnection(data),
            "ListConnections" => ActListConnections(data),
            "UpdateConnection" => ActUpdateConnection(data),
            "DeauthorizeConnection" => ActDeauthorizeConnection(data),
            "CreateApiDestination" => ActCreateApiDestination(data),
            "DescribeApiDestination" => ActDescribeApiDestination(data),
            "DeleteApiDestination" => ActDeleteApiDestination(data),
            "ListApiDestinations" => ActListApiDestinations(data),
            "UpdateApiDestination" => ActUpdateApiDestination(data),
            "CreateEndpoint" => ActCreateEndpoint(data),
            "DeleteEndpoint" => ActDeleteEndpoint(data),
            "DescribeEndpoint" => ActDescribeEndpoint(data),
            "ListEndpoints" => ActListEndpoints(data),
            "UpdateEndpoint" => ActUpdateEndpoint(data),
            "ActivateEventSource" => ActActivateEventSource(data),
            "DeactivateEventSource" => ActDeactivateEventSource(data),
            "DescribeEventSource" => ActDescribeEventSource(data),
            "CreatePartnerEventSource" => ActCreatePartnerEventSource(data),
            "DeletePartnerEventSource" => ActDeletePartnerEventSource(data),
            "DescribePartnerEventSource" => ActDescribePartnerEventSource(data),
            "ListPartnerEventSources" => ActListPartnerEventSources(data),
            "ListPartnerEventSourceAccounts" => ActListPartnerEventSourceAccounts(data),
            "ListEventSources" => ActListEventSources(data),
            "PutPartnerEvents" => ActPutPartnerEvents(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _rules.Clear();
            _targets.Clear();
            _tags.Clear();
            _archives.Clear();
            _eventBusPolicies.Clear();
            _connections.Clear();
            _apiDestinations.Clear();
            _replays.Clear();
            _endpoints.Clear();
            _eventBuses = CreateDefaultBuses();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

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

    private static double GetDouble(JsonElement el, string propertyName, double defaultValue)
    {
        if (!el.TryGetProperty(propertyName, out var prop))
        {
            return defaultValue;
        }

        return prop.TryGetDouble(out var val) ? val : defaultValue;
    }

    private static List<Dictionary<string, string>> GetTags(JsonElement data)
    {
        var tags = new List<Dictionary<string, string>>();
        if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var tagEl in tagsEl.EnumerateArray())
            {
                var key = GetString(tagEl, "Key");
                var value = GetString(tagEl, "Value") ?? "";
                if (key is not null)
                {
                    tags.Add(new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["Key"] = key,
                        ["Value"] = value,
                    });
                }
            }
        }

        return tags;
    }

    private static JsonElement GetJsonElement(JsonElement el, string propertyName)
    {
        if (el.TryGetProperty(propertyName, out var prop))
        {
            return prop.Clone();
        }

        return default;
    }

    private static string RuleKey(string ruleName, string busName) => $"{busName}|{ruleName}";

    private static string RuleArn(string ruleName, string busName)
    {
        var accountId = AccountContext.GetAccountId();
        if (busName == "default")
        {
            return $"arn:aws:events:{Region}:{accountId}:rule/{ruleName}";
        }

        return $"arn:aws:events:{Region}:{accountId}:rule/{busName}/{ruleName}";
    }

    [GeneratedRegex(@"^rate\(\d+\s+(minute|minutes|hour|hours|day|days)\)$")]
    private static partial Regex RatePatternRegex();

    [GeneratedRegex(@"^cron\(.+\)$")]
    private static partial Regex CronPatternRegex();

    private static bool ValidateScheduleExpression(string expr)
    {
        if (string.IsNullOrEmpty(expr))
        {
            return true;
        }

        return RatePatternRegex().IsMatch(expr) || CronPatternRegex().IsMatch(expr);
    }

    // -- Event Buses -----------------------------------------------------------

    private ServiceResponse ActCreateEventBus(JsonElement data)
    {
        var name = GetString(data, "Name");
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Name is required", 400);
        }

        lock (_lock)
        {
            if (_eventBuses.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceAlreadyExistsException", $"Event bus {name} already exists", 400);
            }

            var accountId = AccountContext.GetAccountId();
            var arn = $"arn:aws:events:{Region}:{accountId}:event-bus/{name}";
            var description = GetString(data, "Description") ?? "";
            var now = TimeHelpers.NowEpoch();

            _eventBuses[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["Arn"] = arn,
                ["Description"] = description,
                ["CreationTime"] = now,
                ["LastModifiedTime"] = now,
            };

            var tags = GetTags(data);
            if (tags.Count > 0)
            {
                _tags[arn] = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (var t in tags)
                {
                    _tags[arn][t["Key"]] = t["Value"];
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["EventBusArn"] = arn,
            });
        }
    }

    private ServiceResponse ActDeleteEventBus(JsonElement data)
    {
        var name = GetString(data, "Name");
        if (name == "default")
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "ValidationException", "Cannot delete the default event bus", 400);
        }

        lock (_lock)
        {
            if (_eventBuses.TryGetValue(name ?? "", out var bus))
            {
                _eventBuses.Remove(name!);
                if (bus.TryGetValue("Arn", out var arnObj) && arnObj is string arn)
                {
                    _tags.TryRemove(arn, out _);
                }

                var keysToDelete = new List<string>();
                foreach (var (key, rule) in _rules.Items)
                {
                    if (rule.TryGetValue("EventBusName", out var busNameObj) &&
                        busNameObj is string busName &&
                        busName == name)
                    {
                        keysToDelete.Add(key);
                    }
                }

                foreach (var key in keysToDelete)
                {
                    _rules.TryRemove(key, out _);
                    _targets.TryRemove(key, out _);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListEventBuses(JsonElement data)
    {
        var prefix = GetString(data, "NamePrefix") ?? "";
        lock (_lock)
        {
            var buses = new List<Dictionary<string, object?>>();
            foreach (var (n, b) in _eventBuses)
            {
                if (!string.IsNullOrEmpty(prefix) && !n.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                var policy = _eventBusPolicies.TryGetValue(n, out var p) ? p : null;
                buses.Add(new Dictionary<string, object?>
                {
                    ["Name"] = b["Name"],
                    ["Arn"] = b["Arn"],
                    ["Description"] = b.GetValueOrDefault("Description", ""),
                    ["CreationTime"] = b["CreationTime"],
                    ["LastModifiedTime"] = b.GetValueOrDefault("LastModifiedTime", b["CreationTime"]),
                    ["Policy"] = policy is not null ? JsonSerializer.Serialize(policy) : "",
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["EventBuses"] = buses,
            });
        }
    }

    private ServiceResponse ActDescribeEventBus(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "default";
        lock (_lock)
        {
            if (!_eventBuses.TryGetValue(name, out var bus))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Event bus {name} not found", 400);
            }

            var policy = _eventBusPolicies.TryGetValue(name, out var p) ? p : null;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Name"] = bus["Name"],
                ["Arn"] = bus["Arn"],
                ["Description"] = bus.GetValueOrDefault("Description", ""),
                ["CreationTime"] = bus["CreationTime"],
                ["LastModifiedTime"] = bus.GetValueOrDefault("LastModifiedTime", bus["CreationTime"]),
                ["Policy"] = policy is not null ? JsonSerializer.Serialize(policy) : "",
            });
        }
    }

    private ServiceResponse ActUpdateEventBus(JsonElement data)
    {
        var name = GetString(data, "Name");
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Name is required", 400);
        }

        lock (_lock)
        {
            if (!_eventBuses.TryGetValue(name, out var bus))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Event bus {name} not found", 400);
            }

            var now = TimeHelpers.NowEpoch();
            if (data.TryGetProperty("Description", out _))
            {
                bus["Description"] = GetString(data, "Description") ?? "";
            }

            bus["LastModifiedTime"] = now;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["EventBusArn"] = bus["Arn"],
                ["LastModifiedTime"] = bus["LastModifiedTime"],
            });
        }
    }

    // -- Rules -----------------------------------------------------------------

    private ServiceResponse ActPutRule(JsonElement data)
    {
        var name = GetString(data, "Name");
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Name is required", 400);
        }

        var busName = GetString(data, "EventBusName") ?? "default";

        lock (_lock)
        {
            if (!_eventBuses.ContainsKey(busName))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Event bus {busName} does not exist.", 400);
            }

            var schedule = GetString(data, "ScheduleExpression") ?? "";
            if (!string.IsNullOrEmpty(schedule) && !ValidateScheduleExpression(schedule))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ValidationException", "Parameter ScheduleExpression is not valid.", 400);
            }

            var eventPattern = GetString(data, "EventPattern") ?? "";
            if (!string.IsNullOrEmpty(eventPattern))
            {
                try
                {
                    JsonDocument.Parse(eventPattern).Dispose();
                }
                catch (JsonException)
                {
                    return AwsResponseHelpers.ErrorResponseJson(
                        "InvalidEventPatternException", "Event pattern is not valid JSON", 400);
                }
            }

            var arn = RuleArn(name, busName);
            var key = RuleKey(name, busName);

            _rules.TryGetValue(key, out var existing);
            var existingState = existing?.GetValueOrDefault("State")?.ToString() ?? "ENABLED";
            var existingDesc = existing?.GetValueOrDefault("Description")?.ToString() ?? "";
            var existingRole = existing?.GetValueOrDefault("RoleArn")?.ToString() ?? "";
            var existingCreation = existing?.GetValueOrDefault("CreationTime") ?? TimeHelpers.NowEpoch();

            _rules[key] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["Arn"] = arn,
                ["EventBusName"] = busName,
                ["ScheduleExpression"] = schedule,
                ["EventPattern"] = eventPattern,
                ["State"] = GetString(data, "State") ?? existingState,
                ["Description"] = GetString(data, "Description") ?? existingDesc,
                ["RoleArn"] = GetString(data, "RoleArn") ?? existingRole,
                ["CreationTime"] = existingCreation,
            };

            var tags = GetTags(data);
            if (tags.Count > 0)
            {
                _tags[arn] = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (var t in tags)
                {
                    _tags[arn][t["Key"]] = t["Value"];
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["RuleArn"] = arn,
            });
        }
    }

    private ServiceResponse ActDeleteRule(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        var busName = GetString(data, "EventBusName") ?? "default";
        var key = RuleKey(name, busName);

        lock (_lock)
        {
            if (_rules.TryRemove(key, out var rule))
            {
                _targets.TryRemove(key, out _);
                if (rule.TryGetValue("Arn", out var arnObj) && arnObj is string arn)
                {
                    _tags.TryRemove(arn, out _);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListRules(JsonElement data)
    {
        var prefix = GetString(data, "NamePrefix") ?? "";
        var busName = GetString(data, "EventBusName") ?? "default";

        lock (_lock)
        {
            var rules = new List<Dictionary<string, object?>>();
            foreach (var (_, r) in _rules.Items)
            {
                var ruleBus = r.GetValueOrDefault("EventBusName")?.ToString() ?? "default";
                if (ruleBus != busName)
                {
                    continue;
                }

                var ruleName = r["Name"]?.ToString() ?? "";
                if (!string.IsNullOrEmpty(prefix) && !ruleName.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                rules.Add(BuildRuleOutput(r));
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Rules"] = rules,
            });
        }
    }

    private ServiceResponse ActDescribeRule(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        var busName = GetString(data, "EventBusName") ?? "default";
        var key = RuleKey(name, busName);

        lock (_lock)
        {
            if (!_rules.TryGetValue(key, out var rule))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Rule {name} does not exist.", 400);
            }

            return AwsResponseHelpers.JsonResponse(BuildRuleOutput(rule));
        }
    }

    private ServiceResponse ActEnableRule(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        var busName = GetString(data, "EventBusName") ?? "default";
        var key = RuleKey(name, busName);

        lock (_lock)
        {
            if (_rules.TryGetValue(key, out var rule))
            {
                rule["State"] = "ENABLED";
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDisableRule(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        var busName = GetString(data, "EventBusName") ?? "default";
        var key = RuleKey(name, busName);

        lock (_lock)
        {
            if (_rules.TryGetValue(key, out var rule))
            {
                rule["State"] = "DISABLED";
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private static Dictionary<string, object?> BuildRuleOutput(Dictionary<string, object?> rule)
    {
        var output = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Name"] = rule["Name"],
            ["Arn"] = rule["Arn"],
            ["EventBusName"] = rule["EventBusName"],
            ["State"] = rule["State"],
        };

        var schedule = rule.GetValueOrDefault("ScheduleExpression")?.ToString();
        if (!string.IsNullOrEmpty(schedule))
        {
            output["ScheduleExpression"] = schedule;
        }

        var eventPattern = rule.GetValueOrDefault("EventPattern")?.ToString();
        if (!string.IsNullOrEmpty(eventPattern))
        {
            output["EventPattern"] = eventPattern;
        }

        var desc = rule.GetValueOrDefault("Description")?.ToString();
        if (!string.IsNullOrEmpty(desc))
        {
            output["Description"] = desc;
        }

        var roleArn = rule.GetValueOrDefault("RoleArn")?.ToString();
        if (!string.IsNullOrEmpty(roleArn))
        {
            output["RoleArn"] = roleArn;
        }

        return output;
    }

    // -- Targets ---------------------------------------------------------------

    private ServiceResponse ActPutTargets(JsonElement data)
    {
        var ruleName = GetString(data, "Rule") ?? "";
        var busName = GetString(data, "EventBusName") ?? "default";
        var key = RuleKey(ruleName, busName);

        lock (_lock)
        {
            if (!_rules.ContainsKey(key))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Rule {ruleName} does not exist.", 400);
            }

            if (!_targets.TryGetValue(key, out var targetList))
            {
                targetList = [];
                _targets[key] = targetList;
            }

            if (data.TryGetProperty("Targets", out var targetsEl) && targetsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var targetEl in targetsEl.EnumerateArray())
                {
                    var targetDict = JsonElementToDict(targetEl);
                    var targetId = targetDict.GetValueOrDefault("Id")?.ToString() ?? "";

                    // Remove existing target with same ID
                    targetList.RemoveAll(t => t.GetValueOrDefault("Id")?.ToString() == targetId);
                    targetList.Add(targetDict);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["FailedEntryCount"] = 0,
                ["FailedEntries"] = Array.Empty<object>(),
            });
        }
    }

    private ServiceResponse ActRemoveTargets(JsonElement data)
    {
        var ruleName = GetString(data, "Rule") ?? "";
        var busName = GetString(data, "EventBusName") ?? "default";
        var key = RuleKey(ruleName, busName);
        var ids = new HashSet<string>(StringComparer.Ordinal);

        if (data.TryGetProperty("Ids", out var idsEl) && idsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var idEl in idsEl.EnumerateArray())
            {
                var id = idEl.GetString();
                if (id is not null)
                {
                    ids.Add(id);
                }
            }
        }

        lock (_lock)
        {
            if (_targets.TryGetValue(key, out var targetList))
            {
                targetList.RemoveAll(t =>
                    ids.Contains(t.GetValueOrDefault("Id")?.ToString() ?? ""));
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["FailedEntryCount"] = 0,
                ["FailedEntries"] = Array.Empty<object>(),
            });
        }
    }

    private ServiceResponse ActListTargetsByRule(JsonElement data)
    {
        var ruleName = GetString(data, "Rule") ?? "";
        var busName = GetString(data, "EventBusName") ?? "default";
        var key = RuleKey(ruleName, busName);

        lock (_lock)
        {
            var targets = _targets.TryGetValue(key, out var list) ? list : [];
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Targets"] = targets,
            });
        }
    }

    private ServiceResponse ActListRuleNamesByTarget(JsonElement data)
    {
        var targetArn = GetString(data, "TargetArn") ?? "";
        if (string.IsNullOrEmpty(targetArn))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "TargetArn is required", 400);
        }

        var busFilter = GetString(data, "EventBusName") ?? "";
        var limit = GetInt(data, "Limit", 100);
        if (limit < 1 || limit > 100)
        {
            limit = 100;
        }

        var nextToken = GetString(data, "NextToken") ?? "";

        lock (_lock)
        {
            var matched = new SortedSet<string>(StringComparer.Ordinal);
            foreach (var (key, tlist) in _targets.Items)
            {
                var parts = key.Split('|', 2);
                var busName = parts.Length > 1 ? parts[0] : "default";
                if (!string.IsNullOrEmpty(busFilter) && busName != busFilter)
                {
                    continue;
                }

                var hasTarget = false;
                foreach (var t in tlist)
                {
                    if (t.GetValueOrDefault("Arn")?.ToString() == targetArn)
                    {
                        hasTarget = true;
                        break;
                    }
                }

                if (!hasTarget)
                {
                    continue;
                }

                if (_rules.TryGetValue(key, out var rule))
                {
                    matched.Add(rule["Name"]?.ToString() ?? "");
                }
            }

            var sortedList = matched.ToList();
            var start = 0;
            if (!string.IsNullOrEmpty(nextToken) && int.TryParse(nextToken, out var parsedStart))
            {
                start = parsedStart;
            }

            var page = sortedList.GetRange(start, Math.Min(limit, sortedList.Count - start));
            var resp = new Dictionary<string, object?>
            {
                ["RuleNames"] = page,
            };

            if (start + limit < sortedList.Count)
            {
                resp["NextToken"] = (start + limit).ToString();
            }

            return AwsResponseHelpers.JsonResponse(resp);
        }
    }

    // -- TestEventPattern ------------------------------------------------------

    private ServiceResponse ActTestEventPattern(JsonElement data)
    {
        var eventStr = GetString(data, "Event") ?? "";
        var patternStr = GetString(data, "EventPattern") ?? "";

        if (string.IsNullOrEmpty(eventStr))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Event is required", 400);
        }

        if (string.IsNullOrEmpty(patternStr))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "EventPattern is required", 400);
        }

        Dictionary<string, object?>? eventObj;
        try
        {
            using var doc = JsonDocument.Parse(eventStr);
            eventObj = JsonElementToDict(doc.RootElement);
        }
        catch (JsonException)
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidEventPatternException", "Event is not valid JSON", 400);
        }

        if (eventObj is null)
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidEventPatternException", "Event must be a JSON object", 400);
        }

        var synthetic = EventFromTestPayload(eventObj);
        var matched = MatchesPattern(patternStr, synthetic);
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["Result"] = matched,
        });
    }

    private static Dictionary<string, object?> EventFromTestPayload(Dictionary<string, object?> eventObj)
    {
        var detail = eventObj.GetValueOrDefault("detail") ?? eventObj.GetValueOrDefault("Detail");
        string detailStr;
        if (detail is Dictionary<string, object?> detailDict)
        {
            detailStr = JsonSerializer.Serialize(detailDict);
        }
        else if (detail is null)
        {
            detailStr = "{}";
        }
        else
        {
            detailStr = detail.ToString() ?? "{}";
        }

        return new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Source"] = eventObj.GetValueOrDefault("source") ?? eventObj.GetValueOrDefault("Source") ?? "",
            ["DetailType"] = eventObj.GetValueOrDefault("detail-type") ?? eventObj.GetValueOrDefault("DetailType") ?? "",
            ["Detail"] = detailStr,
            ["Account"] = eventObj.GetValueOrDefault("account") ?? eventObj.GetValueOrDefault("Account") ?? AccountContext.GetAccountId(),
            ["Region"] = eventObj.GetValueOrDefault("region") ?? eventObj.GetValueOrDefault("Region") ?? Region,
            ["Resources"] = eventObj.GetValueOrDefault("resources") ?? eventObj.GetValueOrDefault("Resources") ?? new List<object?>(),
        };
    }

    // -- PutEvents -------------------------------------------------------------

    private ServiceResponse ActPutEvents(JsonElement data)
    {
        lock (_lock)
        {
            var results = new List<Dictionary<string, object?>>();
            if (data.TryGetProperty("Entries", out var entriesEl) && entriesEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var entryEl in entriesEl.EnumerateArray())
                {
                    var eventId = HashHelpers.NewUuid();
                    var busNameRaw = GetString(entryEl, "EventBusName") ?? "default";
                    var busName = NormalizeBusName(busNameRaw);

                    var eventRecord = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["EventId"] = eventId,
                        ["Source"] = GetString(entryEl, "Source") ?? "",
                        ["DetailType"] = GetString(entryEl, "DetailType") ?? "",
                        ["Detail"] = GetString(entryEl, "Detail") ?? "{}",
                        ["EventBusName"] = busName,
                        ["Time"] = TimeHelpers.NowEpoch(),
                        ["Account"] = AccountContext.GetAccountId(),
                        ["Region"] = Region,
                    };

                    // Read Resources array
                    if (entryEl.TryGetProperty("Resources", out var resourcesEl) && resourcesEl.ValueKind == JsonValueKind.Array)
                    {
                        var resources = new List<object?>();
                        foreach (var r in resourcesEl.EnumerateArray())
                        {
                            resources.Add(r.GetString());
                        }

                        eventRecord["Resources"] = resources;
                    }
                    else
                    {
                        eventRecord["Resources"] = new List<object?>();
                    }

                    results.Add(new Dictionary<string, object?> { ["EventId"] = eventId });

                    // Dispatch event (in-process, no SQS/Lambda integration in C# port)
                    DispatchEvent(eventRecord);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["FailedEntryCount"] = 0,
                ["Entries"] = results,
            });
        }
    }

    private static string NormalizeBusName(string name)
    {
        if (!string.IsNullOrEmpty(name) && name.StartsWith("arn:", StringComparison.Ordinal))
        {
            var idx = name.LastIndexOf('/');
            if (idx >= 0)
            {
                return name[(idx + 1)..];
            }
        }

        return name;
    }

    private void DispatchEvent(Dictionary<string, object?> eventRecord)
    {
        var busName = eventRecord.GetValueOrDefault("EventBusName")?.ToString() ?? "default";

        foreach (var (key, rule) in _rules.Items)
        {
            var ruleBus = rule.GetValueOrDefault("EventBusName")?.ToString() ?? "default";
            if (ruleBus != busName)
            {
                continue;
            }

            var state = rule.GetValueOrDefault("State")?.ToString();
            if (state != "ENABLED")
            {
                continue;
            }

            var pattern = rule.GetValueOrDefault("EventPattern")?.ToString();
            if (string.IsNullOrEmpty(pattern))
            {
                continue;
            }

            if (MatchesPattern(pattern, eventRecord))
            {
                // For now, dispatch is a no-op in the C# port.
                // The Python version dispatches to Lambda/SQS/SNS.
            }
        }
    }

    // -- Pattern matching ------------------------------------------------------

    private static bool MatchesPattern(string patternStr, Dictionary<string, object?> eventRecord)
    {
        Dictionary<string, object?>? pattern;
        try
        {
            using var doc = JsonDocument.Parse(patternStr);
            pattern = JsonElementToDict(doc.RootElement);
        }
        catch (JsonException)
        {
            return false;
        }

        if (pattern is null)
        {
            return false;
        }

        if (pattern.TryGetValue("source", out var sourcePatternObj) && sourcePatternObj is not null)
        {
            if (!MatchesField(eventRecord.GetValueOrDefault("Source")?.ToString() ?? "", sourcePatternObj))
            {
                return false;
            }
        }

        if (pattern.TryGetValue("detail-type", out var dtPatternObj) && dtPatternObj is not null)
        {
            if (!MatchesField(eventRecord.GetValueOrDefault("DetailType")?.ToString() ?? "", dtPatternObj))
            {
                return false;
            }
        }

        if (pattern.TryGetValue("detail", out var detailPatternObj) && detailPatternObj is not null)
        {
            var detailRaw = eventRecord.GetValueOrDefault("Detail");
            Dictionary<string, object?>? detail;
            if (detailRaw is string detailStr)
            {
                try
                {
                    using var detDoc = JsonDocument.Parse(detailStr);
                    detail = JsonElementToDict(detDoc.RootElement);
                }
                catch
                {
                    detail = new Dictionary<string, object?>();
                }
            }
            else if (detailRaw is Dictionary<string, object?> d)
            {
                detail = d;
            }
            else
            {
                detail = new Dictionary<string, object?>();
            }

            if (detailPatternObj is Dictionary<string, object?> detailPattern)
            {
                if (!MatchesDetail(detail, detailPattern))
                {
                    return false;
                }
            }
        }

        if (pattern.TryGetValue("account", out var accountPatternObj) && accountPatternObj is not null)
        {
            if (!MatchesField(eventRecord.GetValueOrDefault("Account")?.ToString() ?? AccountContext.GetAccountId(), accountPatternObj))
            {
                return false;
            }
        }

        if (pattern.TryGetValue("region", out var regionPatternObj) && regionPatternObj is not null)
        {
            if (!MatchesField(eventRecord.GetValueOrDefault("Region")?.ToString() ?? Region, regionPatternObj))
            {
                return false;
            }
        }

        return true;
    }

    private static bool MatchesField(string value, object? patternValues)
    {
        if (patternValues is List<object?> list)
        {
            foreach (var item in list)
            {
                if (item is Dictionary<string, object?> filterRule)
                {
                    if (MatchesContentFilter(value, filterRule))
                    {
                        return true;
                    }
                }
                else if (item?.ToString() == value)
                {
                    return true;
                }
            }

            return false;
        }

        return patternValues?.ToString() == value;
    }

    private static bool MatchesDetail(Dictionary<string, object?>? detail, Dictionary<string, object?> pattern)
    {
        if (detail is null)
        {
            return true;
        }

        foreach (var (key, expected) in pattern)
        {
            var actual = detail.GetValueOrDefault(key);

            if (expected is List<object?> expectedList)
            {
                if (actual is null)
                {
                    return false;
                }

                var matched = false;
                foreach (var item in expectedList)
                {
                    if (item is Dictionary<string, object?> filterRule)
                    {
                        if (MatchesContentFilter(actual, filterRule))
                        {
                            matched = true;
                        }
                    }
                    else if (actual.ToString() == item?.ToString())
                    {
                        matched = true;
                    }
                }

                if (!matched)
                {
                    return false;
                }
            }
            else if (expected is Dictionary<string, object?> expectedDict)
            {
                if (actual is not Dictionary<string, object?> actualDict)
                {
                    return false;
                }

                if (!MatchesDetail(actualDict, expectedDict))
                {
                    return false;
                }
            }
        }

        return true;
    }

    private static bool MatchesContentFilter(object? value, Dictionary<string, object?> filterRule)
    {
        if (filterRule.TryGetValue("wildcard", out var wildcardObj) && wildcardObj is string wildcardPattern)
        {
            var strValue = value?.ToString();
            if (strValue is null)
            {
                return false;
            }

            // Convert glob-style wildcard to regex
            var regexPattern = "^" + Regex.Escape(wildcardPattern).Replace("\\*", ".*").Replace("\\?", ".") + "$";
            return Regex.IsMatch(strValue, regexPattern);
        }

        if (filterRule.TryGetValue("prefix", out var prefixObj) && prefixObj is string prefix)
        {
            return value is string strVal && strVal.StartsWith(prefix, StringComparison.Ordinal);
        }

        if (filterRule.TryGetValue("suffix", out var suffixObj) && suffixObj is string suffix)
        {
            return value is string strVal && strVal.EndsWith(suffix, StringComparison.Ordinal);
        }

        if (filterRule.TryGetValue("anything-but", out var anythingButObj))
        {
            if (anythingButObj is List<object?> excluded)
            {
                var strValue = value?.ToString();
                return !excluded.Any(e => e?.ToString() == strValue);
            }

            return value?.ToString() != anythingButObj?.ToString();
        }

        if (filterRule.TryGetValue("numeric", out var numericObj) && numericObj is List<object?> ops)
        {
            if (!double.TryParse(value?.ToString(), out var num))
            {
                return false;
            }

            var i = 0;
            while (i < ops.Count - 1)
            {
                var op = ops[i]?.ToString();
                if (!double.TryParse(ops[i + 1]?.ToString(), out var threshold))
                {
                    return false;
                }

                var result = op switch
                {
                    ">" => num > threshold,
                    ">=" => num >= threshold,
                    "<" => num < threshold,
                    "<=" => num <= threshold,
                    "=" => Math.Abs(num - threshold) < double.Epsilon,
                    _ => true,
                };

                if (!result)
                {
                    return false;
                }

                i += 2;
            }

            return true;
        }

        if (filterRule.TryGetValue("exists", out var existsObj))
        {
            var existsVal = existsObj is bool b ? b : existsObj?.ToString() == "True";
            return existsVal == (value is not null);
        }

        return false;
    }

    // -- Tags ------------------------------------------------------------------

    private ServiceResponse ActTagResource(JsonElement data)
    {
        var arn = GetString(data, "ResourceARN") ?? "";
        lock (_lock)
        {
            if (!_tags.TryGetValue(arn, out var tagDict))
            {
                tagDict = new Dictionary<string, string>(StringComparer.Ordinal);
                _tags[arn] = tagDict;
            }

            if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var tagEl in tagsEl.EnumerateArray())
                {
                    var key = GetString(tagEl, "Key");
                    var value = GetString(tagEl, "Value") ?? "";
                    if (key is not null)
                    {
                        tagDict[key] = value;
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActUntagResource(JsonElement data)
    {
        var arn = GetString(data, "ResourceARN") ?? "";
        lock (_lock)
        {
            if (_tags.TryGetValue(arn, out var tagDict))
            {
                if (data.TryGetProperty("TagKeys", out var keysEl) && keysEl.ValueKind == JsonValueKind.Array)
                {
                    foreach (var keyEl in keysEl.EnumerateArray())
                    {
                        var key = keyEl.GetString();
                        if (key is not null)
                        {
                            tagDict.Remove(key);
                        }
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListTagsForResource(JsonElement data)
    {
        var arn = GetString(data, "ResourceARN") ?? "";
        lock (_lock)
        {
            var tagDict = _tags.TryGetValue(arn, out var d) ? d : new Dictionary<string, string>();
            var tagList = tagDict.Select(kv => new Dictionary<string, string>
            {
                ["Key"] = kv.Key,
                ["Value"] = kv.Value,
            }).ToList();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Tags"] = tagList,
            });
        }
    }

    // -- Archives --------------------------------------------------------------

    private ServiceResponse ActCreateArchive(JsonElement data)
    {
        var name = GetString(data, "ArchiveName") ?? "";
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "ArchiveName is required", 400);
        }

        lock (_lock)
        {
            if (_archives.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceAlreadyExistsException", $"Archive {name} already exists", 400);
            }

            var arn = $"arn:aws:events:{Region}:{AccountContext.GetAccountId()}:archive/{name}";
            var now = TimeHelpers.NowEpoch();
            _archives[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["ArchiveName"] = name,
                ["ArchiveArn"] = arn,
                ["EventSourceArn"] = GetString(data, "EventSourceArn") ?? "",
                ["Description"] = GetString(data, "Description") ?? "",
                ["EventPattern"] = GetString(data, "EventPattern") ?? "",
                ["RetentionDays"] = GetInt(data, "RetentionDays", 0),
                ["State"] = "ENABLED",
                ["CreationTime"] = now,
                ["EventCount"] = 0,
                ["SizeBytes"] = 0,
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ArchiveArn"] = arn,
                ["State"] = "ENABLED",
                ["CreationTime"] = now,
            });
        }
    }

    private ServiceResponse ActDeleteArchive(JsonElement data)
    {
        var name = GetString(data, "ArchiveName") ?? "";
        lock (_lock)
        {
            if (!_archives.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Archive {name} does not exist.", 400);
            }

            _archives.TryRemove(name, out _);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDescribeArchive(JsonElement data)
    {
        var name = GetString(data, "ArchiveName") ?? "";
        lock (_lock)
        {
            if (!_archives.TryGetValue(name, out var archive))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Archive {name} does not exist.", 400);
            }

            return AwsResponseHelpers.JsonResponse(archive);
        }
    }

    private ServiceResponse ActUpdateArchive(JsonElement data)
    {
        var name = GetString(data, "ArchiveName") ?? "";
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "ArchiveName is required", 400);
        }

        lock (_lock)
        {
            if (!_archives.TryGetValue(name, out var archive))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Archive {name} does not exist.", 400);
            }

            if (data.TryGetProperty("Description", out _))
            {
                archive["Description"] = GetString(data, "Description") ?? "";
            }

            if (data.TryGetProperty("EventPattern", out _))
            {
                var ep = GetString(data, "EventPattern") ?? "";
                if (!string.IsNullOrEmpty(ep))
                {
                    try
                    {
                        JsonDocument.Parse(ep).Dispose();
                    }
                    catch (JsonException)
                    {
                        return AwsResponseHelpers.ErrorResponseJson(
                            "InvalidEventPatternException", "Event pattern is not valid JSON", 400);
                    }
                }

                archive["EventPattern"] = ep;
            }

            if (data.TryGetProperty("RetentionDays", out _))
            {
                archive["RetentionDays"] = GetInt(data, "RetentionDays", 0);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ArchiveArn"] = archive["ArchiveArn"],
                ["State"] = archive.GetValueOrDefault("State", "ENABLED"),
                ["CreationTime"] = archive["CreationTime"],
            });
        }
    }

    private ServiceResponse ActListArchives(JsonElement data)
    {
        var prefix = GetString(data, "NamePrefix") ?? "";
        var sourceArn = GetString(data, "EventSourceArn") ?? "";
        var state = GetString(data, "State") ?? "";

        lock (_lock)
        {
            var results = new List<Dictionary<string, object?>>();
            foreach (var (name, archive) in _archives.Items)
            {
                if (!string.IsNullOrEmpty(prefix) && !name.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(sourceArn) && archive.GetValueOrDefault("EventSourceArn")?.ToString() != sourceArn)
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(state) && archive.GetValueOrDefault("State")?.ToString() != state)
                {
                    continue;
                }

                results.Add(archive);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Archives"] = results,
            });
        }
    }

    // -- Replays ---------------------------------------------------------------

    private ServiceResponse ActStartReplay(JsonElement data)
    {
        var name = GetString(data, "ReplayName") ?? "";
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "ReplayName is required", 400);
        }

        lock (_lock)
        {
            if (_replays.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceAlreadyExistsException", $"Replay {name} already exists", 400);
            }

            // Parse Destination
            Dictionary<string, object?>? dest = null;
            if (data.TryGetProperty("Destination", out var destEl) && destEl.ValueKind == JsonValueKind.Object)
            {
                dest = JsonElementToDict(destEl);
            }

            if (dest is null || !dest.ContainsKey("Arn"))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ValidationException", "Destination.Arn is required", 400);
            }

            var arn = $"arn:aws:events:{Region}:{AccountContext.GetAccountId()}:replay/{name}";
            var now = TimeHelpers.NowEpoch();

            _replays[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["ReplayName"] = name,
                ["ReplayArn"] = arn,
                ["Description"] = GetString(data, "Description") ?? "",
                ["EventSourceArn"] = GetString(data, "EventSourceArn") ?? "",
                ["EventStartTime"] = GetDouble(data, "EventStartTime", now),
                ["EventEndTime"] = GetDouble(data, "EventEndTime", now),
                ["Destination"] = dest,
                ["State"] = "RUNNING",
                ["ReplayStartTime"] = now,
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ReplayArn"] = arn,
                ["State"] = "RUNNING",
            });
        }
    }

    private ServiceResponse ActDescribeReplay(JsonElement data)
    {
        var name = GetString(data, "ReplayName") ?? "";
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "ReplayName is required", 400);
        }

        lock (_lock)
        {
            if (!_replays.TryGetValue(name, out var replay))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Replay {name} does not exist.", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>(replay));
        }
    }

    private ServiceResponse ActListReplays(JsonElement data)
    {
        var prefix = GetString(data, "NamePrefix") ?? "";
        var stateF = GetString(data, "State") ?? "";
        var sourceF = GetString(data, "EventSourceArn") ?? "";

        lock (_lock)
        {
            var results = new List<Dictionary<string, object?>>();
            foreach (var n in _replays.Items.Select(x => x.Key).OrderBy(x => x, StringComparer.Ordinal))
            {
                if (!_replays.TryGetValue(n, out var rep))
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(prefix) && !n.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(stateF) && rep.GetValueOrDefault("State")?.ToString() != stateF)
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(sourceF) && rep.GetValueOrDefault("EventSourceArn")?.ToString() != sourceF)
                {
                    continue;
                }

                results.Add(new Dictionary<string, object?>
                {
                    ["ReplayName"] = rep["ReplayName"],
                    ["ReplayArn"] = rep["ReplayArn"],
                    ["State"] = rep["State"],
                    ["EventSourceArn"] = rep.GetValueOrDefault("EventSourceArn", ""),
                    ["ReplayStartTime"] = rep.GetValueOrDefault("ReplayStartTime", ""),
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Replays"] = results,
            });
        }
    }

    private ServiceResponse ActCancelReplay(JsonElement data)
    {
        var name = GetString(data, "ReplayName") ?? "";
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "ReplayName is required", 400);
        }

        lock (_lock)
        {
            if (!_replays.TryGetValue(name, out var rep))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Replay {name} does not exist.", 400);
            }

            var state = rep.GetValueOrDefault("State")?.ToString();
            if (state == "COMPLETED")
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ValidationException", "Replay is already completed", 400);
            }

            if (state == "CANCELLED")
            {
                return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
                {
                    ["ReplayArn"] = rep["ReplayArn"],
                    ["State"] = "CANCELLED",
                });
            }

            rep["State"] = "CANCELLED";
            rep["ReplayEndTime"] = TimeHelpers.NowEpoch();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ReplayArn"] = rep["ReplayArn"],
                ["State"] = "CANCELLED",
            });
        }
    }

    // -- Permissions -----------------------------------------------------------

    private ServiceResponse ActPutPermission(JsonElement data)
    {
        var busName = GetString(data, "EventBusName") ?? "default";
        var statementId = GetString(data, "StatementId") ?? HashHelpers.NewUuid();

        lock (_lock)
        {
            if (!_eventBusPolicies.TryGetValue(busName, out var policy))
            {
                policy = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["Version"] = "2012-10-17",
                    ["Statement"] = new List<Dictionary<string, object?>>(),
                };
                _eventBusPolicies[busName] = policy;
            }

            var statements = (List<Dictionary<string, object?>>)policy["Statement"]!;
            statements.RemoveAll(s => s.GetValueOrDefault("Sid")?.ToString() == statementId);

            var statement = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Sid"] = statementId,
                ["Effect"] = "Allow",
                ["Principal"] = GetString(data, "Principal") ?? "*",
                ["Action"] = GetString(data, "Action") ?? "events:PutEvents",
                ["Resource"] = $"arn:aws:events:{Region}:{AccountContext.GetAccountId()}:event-bus/{busName}",
            };

            statements.Add(statement);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActRemovePermission(JsonElement data)
    {
        var busName = GetString(data, "EventBusName") ?? "default";
        var statementId = GetString(data, "StatementId");
        var removeAll = GetBool(data, "RemoveAllPermissions", false);

        lock (_lock)
        {
            if (removeAll)
            {
                _eventBusPolicies.TryRemove(busName, out _);
                return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
            }

            if (_eventBusPolicies.TryGetValue(busName, out var policy))
            {
                var statements = (List<Dictionary<string, object?>>)policy["Statement"]!;
                statements.RemoveAll(s => s.GetValueOrDefault("Sid")?.ToString() == statementId);
                if (statements.Count == 0)
                {
                    _eventBusPolicies.TryRemove(busName, out _);
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Connections -----------------------------------------------------------

    private ServiceResponse ActCreateConnection(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Name is required", 400);
        }

        lock (_lock)
        {
            if (_connections.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceAlreadyExistsException", $"Connection {name} already exists", 400);
            }

            var arn = $"arn:aws:events:{Region}:{AccountContext.GetAccountId()}:connection/{name}";
            var now = TimeHelpers.NowEpoch();
            _connections[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["ConnectionArn"] = arn,
                ["ConnectionState"] = "AUTHORIZED",
                ["AuthorizationType"] = GetString(data, "AuthorizationType") ?? "",
                ["AuthParameters"] = data.TryGetProperty("AuthParameters", out var ap) ? JsonElementToDict(ap) : new Dictionary<string, object?>(),
                ["Description"] = GetString(data, "Description") ?? "",
                ["CreationTime"] = now,
                ["LastModifiedTime"] = now,
                ["LastAuthorizedTime"] = now,
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ConnectionArn"] = arn,
                ["ConnectionState"] = "AUTHORIZED",
                ["CreationTime"] = now,
            });
        }
    }

    private ServiceResponse ActDescribeConnection(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        lock (_lock)
        {
            if (!_connections.TryGetValue(name, out var conn))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Connection {name} does not exist.", 400);
            }

            return AwsResponseHelpers.JsonResponse(conn);
        }
    }

    private ServiceResponse ActDeleteConnection(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        lock (_lock)
        {
            if (!_connections.TryRemove(name, out var conn))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Connection {name} does not exist.", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ConnectionArn"] = conn["ConnectionArn"],
                ["ConnectionState"] = "DELETING",
                ["LastModifiedTime"] = TimeHelpers.NowEpoch(),
            });
        }
    }

    private ServiceResponse ActListConnections(JsonElement data)
    {
        var prefix = GetString(data, "NamePrefix") ?? "";
        var state = GetString(data, "ConnectionState") ?? "";

        lock (_lock)
        {
            var results = new List<Dictionary<string, object?>>();
            foreach (var name in _connections.Items.Select(x => x.Key).OrderBy(x => x, StringComparer.Ordinal))
            {
                if (!_connections.TryGetValue(name, out var conn))
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(prefix) && !name.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(state) && conn.GetValueOrDefault("ConnectionState")?.ToString() != state)
                {
                    continue;
                }

                results.Add(new Dictionary<string, object?>
                {
                    ["Name"] = conn["Name"],
                    ["ConnectionArn"] = conn["ConnectionArn"],
                    ["ConnectionState"] = conn["ConnectionState"],
                    ["AuthorizationType"] = conn["AuthorizationType"],
                    ["CreationTime"] = conn["CreationTime"],
                    ["LastModifiedTime"] = conn["LastModifiedTime"],
                    ["LastAuthorizedTime"] = conn.GetValueOrDefault("LastAuthorizedTime", ""),
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Connections"] = results,
            });
        }
    }

    private ServiceResponse ActUpdateConnection(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        lock (_lock)
        {
            if (!_connections.TryGetValue(name, out var conn))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Connection {name} does not exist.", 400);
            }

            var now = TimeHelpers.NowEpoch();
            if (data.TryGetProperty("AuthorizationType", out _))
            {
                conn["AuthorizationType"] = GetString(data, "AuthorizationType");
            }

            if (data.TryGetProperty("AuthParameters", out var ap))
            {
                conn["AuthParameters"] = JsonElementToDict(ap);
            }

            if (data.TryGetProperty("Description", out _))
            {
                conn["Description"] = GetString(data, "Description");
            }

            conn["LastModifiedTime"] = now;
            conn["ConnectionState"] = "AUTHORIZED";
            conn["LastAuthorizedTime"] = now;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ConnectionArn"] = conn["ConnectionArn"],
                ["ConnectionState"] = conn["ConnectionState"],
                ["LastModifiedTime"] = now,
            });
        }
    }

    private ServiceResponse ActDeauthorizeConnection(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Name is required", 400);
        }

        lock (_lock)
        {
            if (!_connections.TryGetValue(name, out var conn))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Connection {name} does not exist.", 400);
            }

            var now = TimeHelpers.NowEpoch();
            conn["ConnectionState"] = "DEAUTHORIZED";
            conn["LastModifiedTime"] = now;
            conn.Remove("LastAuthorizedTime");

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ConnectionArn"] = conn["ConnectionArn"],
                ["ConnectionState"] = conn["ConnectionState"],
                ["LastModifiedTime"] = now,
            });
        }
    }

    // -- API Destinations ------------------------------------------------------

    private ServiceResponse ActCreateApiDestination(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Name is required", 400);
        }

        lock (_lock)
        {
            if (_apiDestinations.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceAlreadyExistsException", $"ApiDestination {name} already exists", 400);
            }

            var arn = $"arn:aws:events:{Region}:{AccountContext.GetAccountId()}:api-destination/{name}";
            var now = TimeHelpers.NowEpoch();
            _apiDestinations[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["ApiDestinationArn"] = arn,
                ["ApiDestinationState"] = "ACTIVE",
                ["ConnectionArn"] = GetString(data, "ConnectionArn") ?? "",
                ["InvocationEndpoint"] = GetString(data, "InvocationEndpoint") ?? "",
                ["HttpMethod"] = GetString(data, "HttpMethod") ?? "",
                ["InvocationRateLimitPerSecond"] = GetInt(data, "InvocationRateLimitPerSecond", 300),
                ["Description"] = GetString(data, "Description") ?? "",
                ["CreationTime"] = now,
                ["LastModifiedTime"] = now,
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ApiDestinationArn"] = arn,
                ["ApiDestinationState"] = "ACTIVE",
                ["CreationTime"] = now,
                ["LastModifiedTime"] = now,
            });
        }
    }

    private ServiceResponse ActDescribeApiDestination(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        lock (_lock)
        {
            if (!_apiDestinations.TryGetValue(name, out var dest))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"ApiDestination {name} does not exist.", 400);
            }

            return AwsResponseHelpers.JsonResponse(dest);
        }
    }

    private ServiceResponse ActDeleteApiDestination(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        lock (_lock)
        {
            if (!_apiDestinations.TryRemove(name, out _))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"ApiDestination {name} does not exist.", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListApiDestinations(JsonElement data)
    {
        var prefix = GetString(data, "NamePrefix") ?? "";
        var connArn = GetString(data, "ConnectionArn") ?? "";

        lock (_lock)
        {
            var results = new List<Dictionary<string, object?>>();
            foreach (var name in _apiDestinations.Items.Select(x => x.Key).OrderBy(x => x, StringComparer.Ordinal))
            {
                if (!_apiDestinations.TryGetValue(name, out var dest))
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(prefix) && !name.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(connArn) && dest.GetValueOrDefault("ConnectionArn")?.ToString() != connArn)
                {
                    continue;
                }

                results.Add(new Dictionary<string, object?>
                {
                    ["Name"] = dest["Name"],
                    ["ApiDestinationArn"] = dest["ApiDestinationArn"],
                    ["ApiDestinationState"] = dest["ApiDestinationState"],
                    ["ConnectionArn"] = dest["ConnectionArn"],
                    ["InvocationEndpoint"] = dest["InvocationEndpoint"],
                    ["HttpMethod"] = dest["HttpMethod"],
                    ["CreationTime"] = dest["CreationTime"],
                    ["LastModifiedTime"] = dest["LastModifiedTime"],
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ApiDestinations"] = results,
            });
        }
    }

    private ServiceResponse ActUpdateApiDestination(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        lock (_lock)
        {
            if (!_apiDestinations.TryGetValue(name, out var dest))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"ApiDestination {name} does not exist.", 400);
            }

            var now = TimeHelpers.NowEpoch();
            foreach (var key in new[] { "ConnectionArn", "InvocationEndpoint", "HttpMethod", "Description" })
            {
                if (data.TryGetProperty(key, out _))
                {
                    dest[key] = GetString(data, key);
                }
            }

            if (data.TryGetProperty("InvocationRateLimitPerSecond", out _))
            {
                dest["InvocationRateLimitPerSecond"] = GetInt(data, "InvocationRateLimitPerSecond", 300);
            }

            dest["LastModifiedTime"] = now;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ApiDestinationArn"] = dest["ApiDestinationArn"],
                ["ApiDestinationState"] = dest["ApiDestinationState"],
                ["LastModifiedTime"] = now,
            });
        }
    }

    // -- Endpoints -------------------------------------------------------------

    private ServiceResponse ActCreateEndpoint(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Name is required", 400);
        }

        lock (_lock)
        {
            if (_endpoints.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceAlreadyExistsException", $"Endpoint {name} already exists", 400);
            }

            var arn = $"arn:aws:events:{Region}:{AccountContext.GetAccountId()}:endpoint/{name}";
            var now = TimeHelpers.NowEpoch();
            _endpoints[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["Description"] = GetString(data, "Description") ?? "",
                ["RoutingConfig"] = data.TryGetProperty("RoutingConfig", out var rc) ? JsonElementToDict(rc) : new Dictionary<string, object?>(),
                ["ReplicationConfig"] = data.TryGetProperty("ReplicationConfig", out var repCfg) ? JsonElementToDict(repCfg) : new Dictionary<string, object?>(),
                ["EventBuses"] = data.TryGetProperty("EventBuses", out var eb) ? JsonElementToList(eb) : new List<object?>(),
                ["RoleArn"] = GetString(data, "RoleArn") ?? "",
                ["Arn"] = arn,
                ["EndpointUrl"] = $"https://{name}.global-events.{Region}.amazonaws.com",
                ["State"] = "ACTIVE",
                ["CreationTime"] = now,
                ["LastModifiedTime"] = now,
            };

            var ep = _endpoints[name];
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Name"] = ep["Name"],
                ["Arn"] = ep["Arn"],
                ["RoutingConfig"] = ep["RoutingConfig"],
                ["ReplicationConfig"] = ep["ReplicationConfig"],
                ["EventBuses"] = ep["EventBuses"],
                ["RoleArn"] = ep["RoleArn"],
                ["State"] = ep["State"],
            });
        }
    }

    private ServiceResponse ActDeleteEndpoint(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        lock (_lock)
        {
            if (!_endpoints.TryRemove(name, out _))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Endpoint {name} does not exist.", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDescribeEndpoint(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        lock (_lock)
        {
            if (!_endpoints.TryGetValue(name, out var ep))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Endpoint {name} does not exist.", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Name"] = ep["Name"],
                ["Description"] = ep.GetValueOrDefault("Description", ""),
                ["Arn"] = ep["Arn"],
                ["RoutingConfig"] = ep.GetValueOrDefault("RoutingConfig"),
                ["ReplicationConfig"] = ep.GetValueOrDefault("ReplicationConfig"),
                ["EventBuses"] = ep.GetValueOrDefault("EventBuses"),
                ["RoleArn"] = ep.GetValueOrDefault("RoleArn", ""),
                ["EndpointId"] = ep["Name"],
                ["EndpointUrl"] = ep["EndpointUrl"],
                ["State"] = ep["State"],
                ["StateReason"] = "",
                ["CreationTime"] = ep["CreationTime"],
                ["LastModifiedTime"] = ep.GetValueOrDefault("LastModifiedTime", ep["CreationTime"]),
            });
        }
    }

    private ServiceResponse ActListEndpoints(JsonElement data)
    {
        var prefix = GetString(data, "NamePrefix") ?? "";

        lock (_lock)
        {
            var results = new List<Dictionary<string, object?>>();
            foreach (var n in _endpoints.Items.Select(x => x.Key).OrderBy(x => x, StringComparer.Ordinal))
            {
                if (!_endpoints.TryGetValue(n, out var ep))
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(prefix) && !n.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                results.Add(new Dictionary<string, object?>
                {
                    ["Name"] = ep["Name"],
                    ["Arn"] = ep["Arn"],
                    ["EndpointUrl"] = ep["EndpointUrl"],
                    ["State"] = ep["State"],
                    ["CreationTime"] = ep["CreationTime"],
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Endpoints"] = results,
            });
        }
    }

    private ServiceResponse ActUpdateEndpoint(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        lock (_lock)
        {
            if (!_endpoints.TryGetValue(name, out var ep))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Endpoint {name} does not exist.", 400);
            }

            var now = TimeHelpers.NowEpoch();
            if (data.TryGetProperty("Description", out _))
            {
                ep["Description"] = GetString(data, "Description");
            }

            if (data.TryGetProperty("RoutingConfig", out var rc))
            {
                ep["RoutingConfig"] = JsonElementToDict(rc);
            }

            if (data.TryGetProperty("ReplicationConfig", out var repCfg))
            {
                ep["ReplicationConfig"] = JsonElementToDict(repCfg);
            }

            if (data.TryGetProperty("EventBuses", out var eb))
            {
                ep["EventBuses"] = JsonElementToList(eb);
            }

            if (data.TryGetProperty("RoleArn", out _))
            {
                ep["RoleArn"] = GetString(data, "RoleArn");
            }

            ep["LastModifiedTime"] = now;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Name"] = ep["Name"],
                ["Arn"] = ep["Arn"],
                ["RoutingConfig"] = ep["RoutingConfig"],
                ["ReplicationConfig"] = ep["ReplicationConfig"],
                ["EventBuses"] = ep["EventBuses"],
                ["RoleArn"] = ep["RoleArn"],
                ["EndpointId"] = ep["Name"],
                ["EndpointUrl"] = ep["EndpointUrl"],
                ["State"] = ep["State"],
            });
        }
    }

    // -- Event sources (stubs) -------------------------------------------------

    private static ServiceResponse ActActivateEventSource(JsonElement data)
    {
        _ = GetString(data, "Name");
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private static ServiceResponse ActDeactivateEventSource(JsonElement data)
    {
        _ = GetString(data, "Name");
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private static ServiceResponse ActDescribeEventSource(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["Name"] = name,
            ["State"] = "ENABLED",
            ["Arn"] = !string.IsNullOrEmpty(name) ? $"arn:aws:events:{Region}::event-source/{name}" : "",
        });
    }

    // -- Partner event sources -------------------------------------------------
    private readonly Dictionary<string, Dictionary<string, string>> _partnerEventSources = new(StringComparer.Ordinal);

    private static string PartnerKey(string account, string name) => $"{account}|{name}";

    private ServiceResponse ActCreatePartnerEventSource(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        var account = GetString(data, "Account") ?? "";
        if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(account))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Name and Account are required", 400);
        }

        var pk = PartnerKey(account, name);
        lock (_lock)
        {
            if (_partnerEventSources.ContainsKey(pk))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceAlreadyExistsException", "Partner event source already exists", 400);
            }

            var arn = $"arn:aws:events:{Region}:{account}:event-source/{name}";
            _partnerEventSources[pk] = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["Name"] = name,
                ["Account"] = account,
                ["EventSourceArn"] = arn,
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["EventSourceArn"] = arn,
            });
        }
    }

    private ServiceResponse ActDeletePartnerEventSource(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        var account = GetString(data, "Account") ?? "";
        var pk = PartnerKey(account, name);

        lock (_lock)
        {
            if (!_partnerEventSources.ContainsKey(pk))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", "Partner event source does not exist.", 400);
            }

            _partnerEventSources.Remove(pk);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDescribePartnerEventSource(JsonElement data)
    {
        var name = GetString(data, "Name") ?? "";
        lock (_lock)
        {
            foreach (var (_, rec) in _partnerEventSources)
            {
                if (rec["Name"] == name)
                {
                    return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
                    {
                        ["Name"] = rec["Name"],
                        ["Arn"] = rec["EventSourceArn"],
                        ["State"] = "ACTIVE",
                    });
                }
            }

            return AwsResponseHelpers.ErrorResponseJson(
                "ResourceNotFoundException", $"Partner event source {name} does not exist.", 400);
        }
    }

    private ServiceResponse ActListPartnerEventSources(JsonElement data)
    {
        var prefix = GetString(data, "NamePrefix") ?? "";
        lock (_lock)
        {
            var results = new List<Dictionary<string, object?>>();
            foreach (var (_, rec) in _partnerEventSources)
            {
                if (!string.IsNullOrEmpty(prefix) && !rec["Name"].StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                results.Add(new Dictionary<string, object?>
                {
                    ["Name"] = rec["Name"],
                    ["Arn"] = rec["EventSourceArn"],
                    ["State"] = "ACTIVE",
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["PartnerEventSources"] = results,
            });
        }
    }

    private static ServiceResponse ActListPartnerEventSourceAccounts(JsonElement data)
    {
        _ = GetString(data, "EventSourceName");
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["PartnerEventSourceAccounts"] = Array.Empty<object>(),
            ["NextToken"] = "",
        });
    }

    private static ServiceResponse ActListEventSources(JsonElement data)
    {
        _ = GetString(data, "NamePrefix");
        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["EventSources"] = Array.Empty<object>(),
        });
    }

    private static ServiceResponse ActPutPartnerEvents(JsonElement data)
    {
        var count = 0;
        if (data.TryGetProperty("Entries", out var entriesEl) && entriesEl.ValueKind == JsonValueKind.Array)
        {
            count = entriesEl.GetArrayLength();
        }

        var results = new List<Dictionary<string, object?>>();
        for (var i = 0; i < count; i++)
        {
            results.Add(new Dictionary<string, object?> { ["EventId"] = HashHelpers.NewUuid() });
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["FailedEntryCount"] = 0,
            ["Entries"] = results,
        });
    }

    // -- JSON conversion helpers -----------------------------------------------

    private static Dictionary<string, object?> JsonElementToDict(JsonElement element)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (element.ValueKind != JsonValueKind.Object)
        {
            return dict;
        }

        foreach (var prop in element.EnumerateObject())
        {
            dict[prop.Name] = JsonElementToValue(prop.Value);
        }

        return dict;
    }

    private static List<object?> JsonElementToList(JsonElement element)
    {
        var list = new List<object?>();
        if (element.ValueKind != JsonValueKind.Array)
        {
            return list;
        }

        foreach (var item in element.EnumerateArray())
        {
            list.Add(JsonElementToValue(item));
        }

        return list;
    }

    private static object? JsonElementToValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Array => JsonElementToList(element),
            JsonValueKind.Object => JsonElementToDict(element),
            _ => element.GetRawText(),
        };
    }
}
