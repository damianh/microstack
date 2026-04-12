using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using MicroStack.Internal;
using MicroStack.Services.Lambda;

namespace MicroStack.Services.StepFunctions;

/// <summary>
/// Step Functions service handler -- supports JSON protocol via X-Amz-Target (AWSStepFunctions).
///
/// Port of ministack/services/stepfunctions.py.
///
/// Supports: CreateStateMachine, DeleteStateMachine, DescribeStateMachine,
///           UpdateStateMachine, ListStateMachines,
///           StartExecution, StartSyncExecution, StopExecution,
///           DescribeExecution, DescribeStateMachineForExecution, ListExecutions,
///           GetExecutionHistory,
///           SendTaskSuccess, SendTaskFailure, SendTaskHeartbeat,
///           CreateActivity, DeleteActivity, DescribeActivity, ListActivities,
///           GetActivityTask,
///           TagResource, UntagResource, ListTagsForResource,
///           TestState.
///
/// ASL state types: Pass, Task, Choice, Wait, Succeed, Fail, Parallel, Map.
/// </summary>
internal sealed partial class StepFunctionsServiceHandler : IServiceHandler
{
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _stateMachines = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _executions = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, object?>>> _tags = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _activities = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, object?>>> _activityTasks = new();
    private readonly ConcurrentDictionary<string, TaskTokenInfo> _taskTokens = new();
    private Dictionary<string, object?> _sfnMockConfig = new();

    private readonly LambdaServiceHandler _lambda;
    private readonly ServiceRegistry _registry;

    private static string Region => MicroStackOptions.Instance.Region;

    private static readonly HashSet<string> TimestampResponseFields =
    [
        "creationDate", "redriveDate", "startDate", "stopDate", "timestamp", "updateDate"
    ];

    internal StepFunctionsServiceHandler(LambdaServiceHandler lambda, ServiceRegistry registry)
    {
        _lambda = lambda;
        _registry = registry;
    }

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "states";

    public async Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var target = request.GetHeader("x-amz-target") ?? "";
        var action = target.Contains('.', StringComparison.Ordinal)
            ? target[(target.LastIndexOf('.') + 1)..]
            : "";

        Dictionary<string, object?> data;
        if (request.Body.Length > 0)
        {
            try
            {
                data = ParseJsonToDict(request.Body);
            }
            catch (JsonException)
            {
                return AwsResponseHelpers.ErrorResponseJson("SerializationException", "Invalid JSON", 400);
            }
        }
        else
        {
            data = new Dictionary<string, object?>();
        }

        ServiceResponse response;
        if (action == "GetActivityTask")
        {
            response = await ActGetActivityTaskAsync(data);
        }
        else
        {
            response = action switch
            {
                "CreateStateMachine" => ActCreateStateMachine(data),
                "DeleteStateMachine" => ActDeleteStateMachine(data),
                "DescribeStateMachine" => ActDescribeStateMachine(data),
                "UpdateStateMachine" => ActUpdateStateMachine(data),
                "ListStateMachines" => ActListStateMachines(data),
                "StartExecution" => ActStartExecution(data),
                "StopExecution" => ActStopExecution(data),
                "DescribeExecution" => ActDescribeExecution(data),
                "ListExecutions" => ActListExecutions(data),
                "GetExecutionHistory" => ActGetExecutionHistory(data),
                "StartSyncExecution" => ActStartSyncExecution(data),
                "DescribeStateMachineForExecution" => ActDescribeStateMachineForExecution(data),
                "SendTaskSuccess" => ActSendTaskSuccess(data),
                "SendTaskFailure" => ActSendTaskFailure(data),
                "SendTaskHeartbeat" => ActSendTaskHeartbeat(data),
                "TagResource" => ActTagResource(data),
                "UntagResource" => ActUntagResource(data),
                "ListTagsForResource" => ActListTagsForResource(data),
                "CreateActivity" => ActCreateActivity(data),
                "DeleteActivity" => ActDeleteActivity(data),
                "DescribeActivity" => ActDescribeActivity(data),
                "ListActivities" => ActListActivities(data),
                "TestState" => ActTestState(data),
                _ => ErrorResponse("InvalidAction", $"Unknown action: {action}"),
            };
        }

        return FinalizeResponse(response);
    }

    public void Reset()
    {
        _stateMachines.Clear();
        _executions.Clear();
        _tags.Clear();
        _activities.Clear();
        _activityTasks.Clear();
        _taskTokens.Clear();
        _sfnMockConfig = new Dictionary<string, object?>();
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    internal void SetMockConfig(Dictionary<string, object?> config) => _sfnMockConfig = config;
    // -- Helpers ---------------------------------------------------------------

    private static ServiceResponse JsonResp(Dictionary<string, object?> data)
        => AwsResponseHelpers.JsonResponse(data);

    private static ServiceResponse ErrorResponse(string code, string message, int status = 400)
        => AwsResponseHelpers.ErrorResponseJson(code, message, status);

    private static string? GetString(Dictionary<string, object?> data, string key)
        => data.TryGetValue(key, out var v) ? v?.ToString() : null;

    private static int GetInt(Dictionary<string, object?> data, string key, int defaultValue)
        => data.TryGetValue(key, out var v) && v is IConvertible c ? Convert.ToInt32(c) : defaultValue;

    private static bool GetBool(Dictionary<string, object?> data, string key, bool defaultValue)
        => data.TryGetValue(key, out var v) && v is bool b ? b : defaultValue;

    private static Dictionary<string, object?> GetDict(Dictionary<string, object?> data, string key)
        => data.TryGetValue(key, out var v) && v is Dictionary<string, object?> d ? d : new();

    private static List<object?> GetList(Dictionary<string, object?> data, string key)
        => data.TryGetValue(key, out var v) && v is List<object?> l ? l : new();

    private static string NowIso() => TimeHelpers.NowIso();

    /// <summary>
    /// Serializes state output, unwrapping internal wrapper keys so that
    /// Map/Parallel results emit JSON arrays and scalar Pass results emit primitives.
    /// </summary>
    private static string SerializeOutput(Dictionary<string, object?> data)
        => JsonSerializer.Serialize(UnwrapForSerialization(data));

    /// <summary>
    /// Recursively unwraps __list__ and __scalar__ wrapper dictionaries
    /// so that the JSON output matches AWS Step Functions semantics.
    /// </summary>
    private static object? UnwrapForSerialization(object? value)
    {
        if (value is Dictionary<string, object?> dict)
        {
            if (dict.Count == 1)
            {
                if (dict.TryGetValue("__list__", out var listVal))
                {
                    return UnwrapForSerialization(listVal);
                }

                if (dict.TryGetValue("__scalar__", out var scalarVal))
                {
                    return scalarVal;
                }
            }

            var result = new Dictionary<string, object?>(dict.Count);
            foreach (var kv in dict)
            {
                result[kv.Key] = UnwrapForSerialization(kv.Value);
            }

            return result;
        }

        if (value is List<object?> list)
        {
            var result = new List<object?>(list.Count);
            foreach (var item in list)
            {
                result.Add(UnwrapForSerialization(item));
            }

            return result;
        }

        return value;
    }

    private static void AddEvent(Dictionary<string, object?> execution, string eventType, Dictionary<string, object?>? details = null)
    {
        var events = GetList(execution, "events");
        var evt = new Dictionary<string, object?>
        {
            ["id"] = events.Count + 1,
            ["type"] = eventType,
            ["timestamp"] = NowIso(),
        };
        if (details is not null)
        {
            foreach (var kv in details)
            {
                evt[kv.Key] = kv.Value;
            }
        }

        events.Add(evt);
    }

    private static string? NextOrEnd(Dictionary<string, object?> stateDef)
    {
        if (GetBool(stateDef, "End", false))
        {
            return null;
        }

        return GetString(stateDef, "Next");
    }

    // -- JSON parsing helpers ---------------------------------------------------

    private static Dictionary<string, object?> ParseJsonToDict(byte[] body)
    {
        using var doc = JsonDocument.Parse(body);
        return JsonElementToDict(doc.RootElement);
    }

    private static object? JsonElementToObject(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Object => JsonElementToDict(element),
            JsonValueKind.Array => JsonElementToList(element),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => ParseNumber(element),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    private static object ParseNumber(JsonElement element)
    {
        var raw = element.GetRawText();
        if (raw.Contains('.'))
        {
            return element.GetDouble();
        }

        if (element.TryGetInt64(out var l))
        {
            return l;
        }

        return element.GetDouble();
    }

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

    private static List<object?> JsonElementToList(JsonElement element)
    {
        var list = new List<object?>();
        foreach (var item in element.EnumerateArray())
        {
            list.Add(JsonElementToObject(item));
        }

        return list;
    }

    // -- Timestamp normalization ------------------------------------------------

    private static object? TimestampResponseValue(object? value)
    {
        if (value is not string s)
        {
            return value;
        }

        if (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dto))
        {
            return dto.ToUnixTimeMilliseconds() / 1000.0;
        }

        return value;
    }

    private static object? NormalizeTimestampResponse(object? payload, string? fieldName = null)
    {
        if (payload is Dictionary<string, object?> dict)
        {
            var result = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var kv in dict)
            {
                result[kv.Key] = NormalizeTimestampResponse(kv.Value, kv.Key);
            }

            return result;
        }

        if (payload is List<object?> list)
        {
            var result = new List<object?>(list.Count);
            foreach (var item in list)
            {
                result.Add(NormalizeTimestampResponse(item, fieldName));
            }

            return result;
        }

        if (fieldName is not null && TimestampResponseFields.Contains(fieldName))
        {
            return TimestampResponseValue(payload);
        }

        return payload;
    }

    private static ServiceResponse FinalizeResponse(ServiceResponse response)
    {
        if (response.Body.Length == 0)
        {
            return response;
        }

        Dictionary<string, object?> payload;
        try
        {
            payload = ParseJsonToDict(response.Body);
        }
        catch (JsonException)
        {
            return response;
        }

        var normalized = NormalizeTimestampResponse(payload);
        if (normalized is not Dictionary<string, object?> normalizedDict)
        {
            return response;
        }

        return AwsResponseHelpers.JsonResponse(normalizedDict, response.StatusCode);
    }

    // -- Mock config ------------------------------------------------------------

    private Dictionary<string, object?>? GetMockResponse(string smName, string testCase, string stateName, int attempt)
    {
        var smCfg = GetDict(_sfnMockConfig, "StateMachines");
        var smEntry = GetDict(smCfg, smName);
        if (string.IsNullOrEmpty(testCase) || smEntry.Count == 0)
        {
            return null;
        }

        var tc = GetDict(smEntry, "TestCases");
        var tcEntry = GetDict(tc, testCase);
        var responseName = GetString(tcEntry, stateName);
        if (string.IsNullOrEmpty(responseName))
        {
            return null;
        }

        var mocked = GetDict(_sfnMockConfig, "MockedResponses");
        var mockedEntry = GetDict(mocked, responseName);
        if (mockedEntry.Count == 0)
        {
            return null;
        }

        var strAttempt = attempt.ToString(CultureInfo.InvariantCulture);
        if (mockedEntry.TryGetValue(strAttempt, out var exactMatch) && exactMatch is Dictionary<string, object?> exactDict)
        {
            return exactDict;
        }

        foreach (var kv in mockedEntry)
        {
            if (kv.Key.Contains('-') && kv.Value is Dictionary<string, object?> rangeDict)
            {
                var parts = kv.Key.Split('-', 2);
                if (parts.Length == 2
                    && int.TryParse(parts[0], out var lo)
                    && int.TryParse(parts[1], out var hi)
                    && attempt >= lo && attempt <= hi)
                {
                    return rangeDict;
                }
            }
        }

        return null;
    }
    // -- State Machine CRUD -----------------------------------------------------

    private ServiceResponse ActCreateStateMachine(Dictionary<string, object?> data)
    {
        var name = GetString(data, "name");
        if (string.IsNullOrEmpty(name))
        {
            return ErrorResponse("ValidationException", "name is required");
        }

        var arn = $"arn:aws:states:{Region}:{AccountContext.GetAccountId()}:stateMachine:{name}";
        if (_stateMachines.ContainsKey(arn))
        {
            return ErrorResponse("StateMachineAlreadyExists", $"State machine {name} already exists");
        }

        var ts = NowIso();
        var roleArn = GetString(data, "roleArn")
            ?? $"arn:aws:iam::{AccountContext.GetAccountId()}:role/StepFunctionsRole";
        var smType = GetString(data, "type") ?? "STANDARD";

        var loggingConfig = data.TryGetValue("loggingConfiguration", out var lc) && lc is Dictionary<string, object?> lcDict
            ? lcDict
            : new Dictionary<string, object?> { ["level"] = "OFF", ["includeExecutionData"] = false };

        _stateMachines[arn] = new Dictionary<string, object?>
        {
            ["stateMachineArn"] = arn,
            ["name"] = name,
            ["definition"] = GetString(data, "definition") ?? "{}",
            ["roleArn"] = roleArn,
            ["type"] = smType,
            ["creationDate"] = ts,
            ["status"] = "ACTIVE",
            ["loggingConfiguration"] = loggingConfig,
        };

        var tags = GetList(data, "tags");
        if (tags.Count > 0)
        {
            var tagList = new List<Dictionary<string, object?>>();
            foreach (var t in tags)
            {
                if (t is Dictionary<string, object?> td)
                {
                    tagList.Add(new Dictionary<string, object?>(td));
                }
            }

            _tags[arn] = tagList;
        }

        return JsonResp(new Dictionary<string, object?> { ["stateMachineArn"] = arn, ["creationDate"] = ts });
    }

    private ServiceResponse ActDeleteStateMachine(Dictionary<string, object?> data)
    {
        var arn = GetString(data, "stateMachineArn");
        if (arn is null || !_stateMachines.ContainsKey(arn))
        {
            return ErrorResponse("StateMachineDoesNotExist", $"State machine {arn} not found");
        }

        _stateMachines.TryRemove(arn, out _);
        _tags.TryRemove(arn, out _);
        var stale = new List<string>();
        foreach (var kv in _executions.Items)
        {
            if (string.Equals(GetString(kv.Value, "stateMachineArn"), arn, StringComparison.Ordinal))
            {
                stale.Add(kv.Key);
            }
        }

        foreach (var k in stale)
        {
            _executions.TryRemove(k, out _);
        }

        return JsonResp(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDescribeStateMachine(Dictionary<string, object?> data)
    {
        var arn = GetString(data, "stateMachineArn");
        if (arn is null || !_stateMachines.TryGetValue(arn, out var sm))
        {
            return ErrorResponse("StateMachineDoesNotExist", $"State machine {arn} not found");
        }

        return JsonResp(new Dictionary<string, object?>(sm));
    }

    private ServiceResponse ActUpdateStateMachine(Dictionary<string, object?> data)
    {
        var arn = GetString(data, "stateMachineArn");
        if (arn is null || !_stateMachines.TryGetValue(arn, out var sm))
        {
            return ErrorResponse("StateMachineDoesNotExist", $"State machine {arn} not found");
        }

        if (data.ContainsKey("definition"))
        {
            sm["definition"] = GetString(data, "definition");
        }

        if (data.ContainsKey("roleArn"))
        {
            sm["roleArn"] = GetString(data, "roleArn");
        }

        if (data.TryGetValue("loggingConfiguration", out var lc2))
        {
            sm["loggingConfiguration"] = lc2;
        }

        return JsonResp(new Dictionary<string, object?> { ["updateDate"] = NowIso() });
    }

    private ServiceResponse ActListStateMachines(Dictionary<string, object?> data)
    {
        var allMachines = new List<Dictionary<string, object?>>();
        foreach (var sm in _stateMachines.Values)
        {
            allMachines.Add(new Dictionary<string, object?>
            {
                ["stateMachineArn"] = GetString(sm, "stateMachineArn"),
                ["name"] = GetString(sm, "name"),
                ["type"] = GetString(sm, "type"),
                ["creationDate"] = GetString(sm, "creationDate"),
            });
        }

        var maxResults = GetInt(data, "maxResults", 1000);
        var nextToken = GetString(data, "nextToken");
        var start = 0;
        if (!string.IsNullOrEmpty(nextToken) && int.TryParse(nextToken, out var parsed))
        {
            start = parsed;
        }

        var end = Math.Min(start + maxResults, allMachines.Count);
        var page = allMachines.GetRange(start, end - start);
        var resp = new Dictionary<string, object?> { ["stateMachines"] = page };
        if (end < allMachines.Count)
        {
            resp["nextToken"] = end.ToString(CultureInfo.InvariantCulture);
        }

        return JsonResp(resp);
    }
    // -- Execution lifecycle -----------------------------------------------------

    private ServiceResponse ActStartExecution(Dictionary<string, object?> data)
    {
        var smArnRaw = GetString(data, "stateMachineArn") ?? "";
        var testCase = "";
        if (smArnRaw.Contains('#'))
        {
            var idx = smArnRaw.LastIndexOf('#');
            testCase = smArnRaw[(idx + 1)..];
            smArnRaw = smArnRaw[..idx];
        }

        var smArn = smArnRaw;
        if (!_stateMachines.TryGetValue(smArn, out var sm))
        {
            return ErrorResponse("StateMachineDoesNotExist", $"State machine {smArn} not found");
        }

        var name = GetString(data, "name") ?? HashHelpers.NewUuid();
        var smName = GetString(sm, "name") ?? "";
        var execArn = $"arn:aws:states:{Region}:{AccountContext.GetAccountId()}:execution:{smName}:{name}";
        var startDate = NowIso();
        var inputStr = GetString(data, "input") ?? "{}";

        var execution = new Dictionary<string, object?>
        {
            ["executionArn"] = execArn,
            ["stateMachineArn"] = smArn,
            ["name"] = name,
            ["status"] = "RUNNING",
            ["startDate"] = startDate,
            ["stopDate"] = null,
            ["input"] = inputStr,
            ["inputDetails"] = new Dictionary<string, object?> { ["included"] = true },
            ["output"] = null,
            ["outputDetails"] = new Dictionary<string, object?> { ["included"] = true },
            ["testCase"] = testCase,
            ["mockAttempts"] = new Dictionary<string, object?>(),
            ["events"] = new List<object?>
            {
                new Dictionary<string, object?>
                {
                    ["id"] = 1,
                    ["type"] = "ExecutionStarted",
                    ["timestamp"] = startDate,
                    ["executionStartedEventDetails"] = new Dictionary<string, object?>
                    {
                        ["input"] = inputStr,
                        ["roleArn"] = GetString(sm, "roleArn"),
                    },
                },
            },
        };

        _executions[execArn] = execution;

        var thread = new Thread(() => RunExecution(execArn)) { IsBackground = true };
        thread.Start();

        return JsonResp(new Dictionary<string, object?> { ["executionArn"] = execArn, ["startDate"] = startDate });
    }

    private ServiceResponse ActStopExecution(Dictionary<string, object?> data)
    {
        var execArn = GetString(data, "executionArn");
        if (execArn is null || !_executions.TryGetValue(execArn, out var execution))
        {
            return ErrorResponse("ExecutionDoesNotExist", $"Execution {execArn} not found");
        }

        if (!string.Equals(GetString(execution, "status"), "RUNNING", StringComparison.Ordinal))
        {
            return ErrorResponse("ValidationException", "Execution is not running");
        }

        var stopDate = NowIso();
        execution["status"] = "ABORTED";
        execution["stopDate"] = stopDate;
        AddEvent(execution, "ExecutionAborted", new Dictionary<string, object?>
        {
            ["executionAbortedEventDetails"] = new Dictionary<string, object?>
            {
                ["error"] = GetString(data, "error") ?? "",
                ["cause"] = GetString(data, "cause") ?? "",
            },
        });

        return JsonResp(new Dictionary<string, object?> { ["stopDate"] = stopDate });
    }

    private ServiceResponse ActDescribeExecution(Dictionary<string, object?> data)
    {
        var execArn = GetString(data, "executionArn");
        if (execArn is null || !_executions.TryGetValue(execArn, out var execution))
        {
            return ErrorResponse("ExecutionDoesNotExist", $"Execution {execArn} not found");
        }

        return JsonResp(new Dictionary<string, object?>
        {
            ["executionArn"] = GetString(execution, "executionArn"),
            ["stateMachineArn"] = GetString(execution, "stateMachineArn"),
            ["name"] = GetString(execution, "name"),
            ["status"] = GetString(execution, "status"),
            ["startDate"] = GetString(execution, "startDate"),
            ["stopDate"] = execution.TryGetValue("stopDate", out var sd) ? sd : null,
            ["input"] = GetString(execution, "input"),
            ["inputDetails"] = execution.TryGetValue("inputDetails", out var id2) ? id2 : new Dictionary<string, object?> { ["included"] = true },
            ["output"] = execution.TryGetValue("output", out var o) ? o : null,
            ["outputDetails"] = execution.TryGetValue("outputDetails", out var od) ? od : new Dictionary<string, object?> { ["included"] = true },
        });
    }

    private ServiceResponse ActListExecutions(Dictionary<string, object?> data)
    {
        var smArn = GetString(data, "stateMachineArn");
        var statusFilter = GetString(data, "statusFilter");
        var allExecs = new List<Dictionary<string, object?>>();

        foreach (var ex in _executions.Values)
        {
            if (smArn is not null && !string.Equals(GetString(ex, "stateMachineArn"), smArn, StringComparison.Ordinal))
            {
                continue;
            }

            if (statusFilter is not null && !string.Equals(GetString(ex, "status"), statusFilter, StringComparison.Ordinal))
            {
                continue;
            }

            allExecs.Add(new Dictionary<string, object?>
            {
                ["executionArn"] = GetString(ex, "executionArn"),
                ["stateMachineArn"] = GetString(ex, "stateMachineArn"),
                ["name"] = GetString(ex, "name"),
                ["status"] = GetString(ex, "status"),
                ["startDate"] = GetString(ex, "startDate"),
                ["stopDate"] = ex.TryGetValue("stopDate", out var sd2) ? sd2 : null,
            });
        }

        var maxResults = GetInt(data, "maxResults", 1000);
        var nextToken = GetString(data, "nextToken");
        var start = 0;
        if (!string.IsNullOrEmpty(nextToken) && int.TryParse(nextToken, out var parsed2))
        {
            start = parsed2;
        }

        var end = Math.Min(start + maxResults, allExecs.Count);
        var page = allExecs.GetRange(start, end - start);
        var resp = new Dictionary<string, object?> { ["executions"] = page };
        if (end < allExecs.Count)
        {
            resp["nextToken"] = end.ToString(CultureInfo.InvariantCulture);
        }

        return JsonResp(resp);
    }

    private ServiceResponse ActGetExecutionHistory(Dictionary<string, object?> data)
    {
        var execArn = GetString(data, "executionArn");
        if (execArn is null || !_executions.TryGetValue(execArn, out var execution))
        {
            return ErrorResponse("ExecutionDoesNotExist", $"Execution {execArn} not found");
        }

        var events = GetList(execution, "events");
        var eventsCopy = new List<object?>(events);
        if (GetBool(data, "reverseOrder", false))
        {
            eventsCopy.Reverse();
        }

        var maxResults = GetInt(data, "maxResults", 1000);
        if (eventsCopy.Count > maxResults)
        {
            eventsCopy = eventsCopy.GetRange(0, maxResults);
        }

        return JsonResp(new Dictionary<string, object?> { ["events"] = eventsCopy });
    }

    private ServiceResponse ActStartSyncExecution(Dictionary<string, object?> data)
    {
        var smArnRaw = GetString(data, "stateMachineArn") ?? "";
        var testCase = "";
        if (smArnRaw.Contains('#'))
        {
            var idx = smArnRaw.LastIndexOf('#');
            testCase = smArnRaw[(idx + 1)..];
            smArnRaw = smArnRaw[..idx];
        }

        var smArn = smArnRaw;
        if (!_stateMachines.TryGetValue(smArn, out var sm))
        {
            return ErrorResponse("StateMachineDoesNotExist", $"State machine {smArn} not found");
        }

        var name = GetString(data, "name") ?? HashHelpers.NewUuid();
        var smName = GetString(sm, "name") ?? "";
        var execArn = $"arn:aws:states:{Region}:{AccountContext.GetAccountId()}:execution:{smName}:{name}";
        var startDate = NowIso();
        var inputStr = GetString(data, "input") ?? "{}";

        var execution = new Dictionary<string, object?>
        {
            ["executionArn"] = execArn,
            ["stateMachineArn"] = smArn,
            ["name"] = name,
            ["status"] = "RUNNING",
            ["startDate"] = startDate,
            ["stopDate"] = null,
            ["input"] = inputStr,
            ["inputDetails"] = new Dictionary<string, object?> { ["included"] = true },
            ["output"] = null,
            ["outputDetails"] = new Dictionary<string, object?> { ["included"] = true },
            ["testCase"] = testCase,
            ["mockAttempts"] = new Dictionary<string, object?>(),
            ["events"] = new List<object?>
            {
                new Dictionary<string, object?>
                {
                    ["id"] = 1,
                    ["type"] = "ExecutionStarted",
                    ["timestamp"] = startDate,
                    ["executionStartedEventDetails"] = new Dictionary<string, object?>
                    {
                        ["input"] = inputStr,
                        ["roleArn"] = GetString(sm, "roleArn"),
                    },
                },
            },
        };

        _executions[execArn] = execution;

        // Run synchronously
        RunExecution(execArn);

        var resp = new Dictionary<string, object?>
        {
            ["executionArn"] = execArn,
            ["stateMachineArn"] = smArn,
            ["name"] = name,
            ["startDate"] = startDate,
            ["stopDate"] = execution.TryGetValue("stopDate", out var sd3) && sd3 is not null ? sd3 : NowIso(),
            ["status"] = GetString(execution, "status"),
            ["input"] = inputStr,
            ["inputDetails"] = new Dictionary<string, object?> { ["included"] = true },
            ["output"] = GetString(execution, "output") ?? "{}",
            ["outputDetails"] = new Dictionary<string, object?> { ["included"] = true },
        };

        if (string.Equals(GetString(execution, "status"), "FAILED", StringComparison.Ordinal))
        {
            var events2 = GetList(execution, "events");
            for (var i = events2.Count - 1; i >= 0; i--)
            {
                if (events2[i] is Dictionary<string, object?> evt
                    && string.Equals(GetString(evt, "type"), "ExecutionFailed", StringComparison.Ordinal))
                {
                    var details = GetDict(evt, "executionFailedEventDetails");
                    resp["error"] = GetString(details, "error") ?? "";
                    resp["cause"] = GetString(details, "cause") ?? "";
                    break;
                }
            }
        }

        return JsonResp(resp);
    }

    private ServiceResponse ActDescribeStateMachineForExecution(Dictionary<string, object?> data)
    {
        var execArn = GetString(data, "executionArn");
        if (execArn is null || !_executions.TryGetValue(execArn, out var execution))
        {
            return ErrorResponse("ExecutionDoesNotExist", $"Execution {execArn} not found");
        }

        var smArn = GetString(execution, "stateMachineArn");
        if (smArn is null || !_stateMachines.TryGetValue(smArn, out var sm))
        {
            return ErrorResponse("StateMachineDoesNotExist", $"State machine {smArn} not found");
        }

        return JsonResp(new Dictionary<string, object?>
        {
            ["stateMachineArn"] = GetString(sm, "stateMachineArn"),
            ["name"] = GetString(sm, "name"),
            ["definition"] = GetString(sm, "definition"),
            ["roleArn"] = GetString(sm, "roleArn"),
            ["updateDate"] = GetString(sm, "creationDate") ?? NowIso(),
        });
    }
    // -- Callback pattern -------------------------------------------------------

    private ServiceResponse ActSendTaskSuccess(Dictionary<string, object?> data)
    {
        var token = GetString(data, "taskToken");
        var output = GetString(data, "output") ?? "{}";
        if (token is null || !_taskTokens.TryGetValue(token, out var info))
        {
            return ErrorResponse("TaskDoesNotExist", "Task token not found");
        }

        info.Result = output;
        info.Event.Set();
        return JsonResp(new Dictionary<string, object?>());
    }

    private ServiceResponse ActSendTaskFailure(Dictionary<string, object?> data)
    {
        var token = GetString(data, "taskToken");
        if (token is null || !_taskTokens.TryGetValue(token, out var info))
        {
            return ErrorResponse("TaskDoesNotExist", "Task token not found");
        }

        info.Error = new Dictionary<string, object?>
        {
            ["Error"] = GetString(data, "error") ?? "TaskFailed",
            ["Cause"] = GetString(data, "cause") ?? "",
        };
        info.Event.Set();
        return JsonResp(new Dictionary<string, object?>());
    }

    private ServiceResponse ActSendTaskHeartbeat(Dictionary<string, object?> data)
    {
        var token = GetString(data, "taskToken");
        if (token is null || !_taskTokens.TryGetValue(token, out var info))
        {
            return ErrorResponse("TaskDoesNotExist", "Task token not found");
        }

        info.Heartbeat = NowIso();
        return JsonResp(new Dictionary<string, object?>());
    }

    // -- Activity CRUD ----------------------------------------------------------

    private ServiceResponse ActCreateActivity(Dictionary<string, object?> data)
    {
        var name = GetString(data, "name");
        if (string.IsNullOrEmpty(name))
        {
            return ErrorResponse("ValidationException", "name is required");
        }

        var arn = $"arn:aws:states:{Region}:{AccountContext.GetAccountId()}:activity:{name}";
        if (_activities.ContainsKey(arn))
        {
            return ErrorResponse("ActivityAlreadyExists", $"Activity already exists: {arn}");
        }

        var ts = NowIso();
        _activities[arn] = new Dictionary<string, object?>
        {
            ["activityArn"] = arn,
            ["name"] = name,
            ["creationDate"] = ts,
        };
        _activityTasks[arn] = [];

        var tags = GetList(data, "tags");
        if (tags.Count > 0)
        {
            var tagList = new List<Dictionary<string, object?>>();
            foreach (var t in tags)
            {
                if (t is Dictionary<string, object?> td)
                {
                    tagList.Add(new Dictionary<string, object?>(td));
                }
            }

            _tags[arn] = tagList;
        }

        return JsonResp(new Dictionary<string, object?> { ["activityArn"] = arn, ["creationDate"] = ts });
    }

    private ServiceResponse ActDeleteActivity(Dictionary<string, object?> data)
    {
        var arn = GetString(data, "activityArn");
        if (arn is null || !_activities.ContainsKey(arn))
        {
            return ErrorResponse("ActivityDoesNotExist", $"Activity {arn} not found");
        }

        _activities.TryRemove(arn, out _);
        _activityTasks.TryRemove(arn, out _);
        _tags.TryRemove(arn, out _);
        return JsonResp(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDescribeActivity(Dictionary<string, object?> data)
    {
        var arn = GetString(data, "activityArn");
        if (arn is null || !_activities.TryGetValue(arn, out var act))
        {
            return ErrorResponse("ActivityDoesNotExist", $"Activity {arn} not found");
        }

        return JsonResp(new Dictionary<string, object?>(act));
    }

    private ServiceResponse ActListActivities(Dictionary<string, object?> data)
    {
        _ = data;
        var acts = new List<Dictionary<string, object?>>();
        foreach (var a in _activities.Values)
        {
            acts.Add(new Dictionary<string, object?>
            {
                ["activityArn"] = GetString(a, "activityArn"),
                ["name"] = GetString(a, "name"),
                ["creationDate"] = GetString(a, "creationDate"),
            });
        }

        return JsonResp(new Dictionary<string, object?> { ["activities"] = acts });
    }

    private async Task<ServiceResponse> ActGetActivityTaskAsync(Dictionary<string, object?> data)
    {
        var arn = GetString(data, "activityArn");
        if (arn is null || !_activities.ContainsKey(arn))
        {
            return ErrorResponse("ActivityDoesNotExist", $"Activity {arn} not found");
        }

        var deadline = Environment.TickCount64 + 60_000;
        while (Environment.TickCount64 < deadline)
        {
            if (_activityTasks.TryGetValue(arn, out var queue) && queue.Count > 0)
            {
                Dictionary<string, object?> task;
                lock (queue)
                {
                    if (queue.Count == 0)
                    {
                        goto wait;
                    }

                    task = queue[0];
                    queue.RemoveAt(0);
                }

                return JsonResp(new Dictionary<string, object?>
                {
                    ["taskToken"] = GetString(task, "taskToken"),
                    ["input"] = GetString(task, "input"),
                });
            }

            wait:
            await Task.Delay(500);
        }

        return JsonResp(new Dictionary<string, object?>());
    }

    // -- Tagging ----------------------------------------------------------------

    private ServiceResponse ActTagResource(Dictionary<string, object?> data)
    {
        var arn = GetString(data, "resourceArn") ?? "";
        var newTags = GetList(data, "tags");
        if (!_tags.TryGetValue(arn, out var existing))
        {
            existing = [];
            _tags[arn] = existing;
        }

        var existingMap = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < existing.Count; i++)
        {
            var k = GetString(existing[i], "key");
            if (k is not null)
            {
                existingMap[k] = i;
            }
        }

        foreach (var tag in newTags)
        {
            if (tag is not Dictionary<string, object?> td)
            {
                continue;
            }

            var key = GetString(td, "key");
            if (key is null)
            {
                continue;
            }

            if (existingMap.TryGetValue(key, out var idx))
            {
                existing[idx] = new Dictionary<string, object?>(td);
            }
            else
            {
                existing.Add(new Dictionary<string, object?>(td));
                existingMap[key] = existing.Count - 1;
            }
        }

        return JsonResp(new Dictionary<string, object?>());
    }

    private ServiceResponse ActUntagResource(Dictionary<string, object?> data)
    {
        var arn = GetString(data, "resourceArn") ?? "";
        var keysToRemove = new HashSet<string>(StringComparer.Ordinal);
        foreach (var k in GetList(data, "tagKeys"))
        {
            if (k is string s)
            {
                keysToRemove.Add(s);
            }
        }

        if (_tags.TryGetValue(arn, out var existing))
        {
            _tags[arn] = existing.FindAll(t => !keysToRemove.Contains(GetString(t, "key") ?? ""));
        }

        return JsonResp(new Dictionary<string, object?>());
    }

    private ServiceResponse ActListTagsForResource(Dictionary<string, object?> data)
    {
        var arn = GetString(data, "resourceArn") ?? "";
        _tags.TryGetValue(arn, out var existing);
        return JsonResp(new Dictionary<string, object?> { ["tags"] = existing ?? new List<Dictionary<string, object?>>() });
    }
    // -- ASL Execution Engine ---------------------------------------------------

    private void RunExecution(string execArn)
    {
        if (!_executions.TryGetValue(execArn, out var execution))
        {
            return;
        }

        Thread.Sleep(150);

        var smArn = GetString(execution, "stateMachineArn");
        if (smArn is null || !_stateMachines.TryGetValue(smArn, out var sm))
        {
            FailExecution(execution, "StateMachineDeleted", "State machine no longer exists");
            return;
        }

        Dictionary<string, object?> definition;
        try
        {
            var defStr = GetString(sm, "definition") ?? "{}";
            definition = ParseJsonToDict(Encoding.UTF8.GetBytes(defStr));
        }
        catch (JsonException)
        {
            FailExecution(execution, "InvalidDefinition", "Could not parse state machine definition");
            return;
        }

        var allStates = GetDict(definition, "States");
        var currentName = GetString(definition, "StartAt");
        if (string.IsNullOrEmpty(currentName) || !allStates.ContainsKey(currentName))
        {
            FailExecution(execution, "InvalidDefinition", $"StartAt state '{currentName}' not found");
            return;
        }

        Dictionary<string, object?> currentInput;
        try
        {
            currentInput = ParseJsonToDict(Encoding.UTF8.GetBytes(GetString(execution, "input") ?? "{}"));
        }
        catch (JsonException)
        {
            currentInput = new Dictionary<string, object?>();
        }

        var ctx = new Dictionary<string, object?>
        {
            ["Execution"] = new Dictionary<string, object?>
            {
                ["Id"] = execArn,
                ["Input"] = new Dictionary<string, object?>(currentInput),
                ["Name"] = GetString(execution, "name"),
                ["StartTime"] = GetString(execution, "startDate"),
            },
            ["StateMachine"] = new Dictionary<string, object?>
            {
                ["Id"] = GetString(execution, "stateMachineArn"),
                ["Name"] = GetString(sm, "name"),
            },
        };

        try
        {
            while (currentName is not null && string.Equals(GetString(execution, "status"), "RUNNING", StringComparison.Ordinal))
            {
                var stateDef = GetDict(allStates, currentName);
                if (stateDef.Count == 0)
                {
                    throw new ExecutionError("States.Runtime", $"State '{currentName}' not found in definition");
                }

                ctx["State"] = new Dictionary<string, object?> { ["Name"] = currentName, ["EnteredTime"] = NowIso() };
                var stateType = GetString(stateDef, "Type") ?? "";

                AddEvent(execution, $"{stateType}StateEntered", new Dictionary<string, object?>
                {
                    ["stateEnteredEventDetails"] = new Dictionary<string, object?>
                    {
                        ["name"] = currentName,
                        ["input"] = SerializeOutput(currentInput),
                    },
                });

                if (stateType == "Succeed")
                {
                    currentInput = ApplyInputPath(stateDef, currentInput);
                    currentInput = ApplyOutputPath(stateDef, currentInput);
                    AddEvent(execution, "SucceedStateExited", new Dictionary<string, object?>
                    {
                        ["stateExitedEventDetails"] = new Dictionary<string, object?>
                        {
                            ["name"] = currentName,
                            ["output"] = SerializeOutput(currentInput),
                        },
                    });
                    currentName = null;
                    continue;
                }

                if (stateType == "Fail")
                {
                    throw new ExecutionError(
                        GetString(stateDef, "Error") ?? "States.Fail",
                        GetString(stateDef, "Cause") ?? "");
                }

                string? nextName;
                switch (stateType)
                {
                    case "Pass":
                        (currentInput, nextName) = ExecutePass(stateDef, currentInput);
                        break;
                    case "Task":
                        (currentInput, nextName) = ExecuteTask(stateDef, currentInput, execution, ctx);
                        break;
                    case "Choice":
                        (currentInput, nextName) = ExecuteChoice(stateDef, currentInput);
                        break;
                    case "Wait":
                        (currentInput, nextName) = ExecuteWait(stateDef, currentInput, execution);
                        break;
                    case "Parallel":
                        (currentInput, nextName) = ExecuteParallel(stateDef, currentInput, execution, ctx);
                        break;
                    case "Map":
                        (currentInput, nextName) = ExecuteMap(stateDef, currentInput, execution, ctx);
                        break;
                    default:
                        throw new ExecutionError("States.Runtime", $"Unknown state type: {stateType}");
                }

                AddEvent(execution, $"{stateType}StateExited", new Dictionary<string, object?>
                {
                    ["stateExitedEventDetails"] = new Dictionary<string, object?>
                    {
                        ["name"] = currentName,
                        ["output"] = SerializeOutput(currentInput),
                    },
                });
                currentName = nextName;
            }

            if (string.Equals(GetString(execution, "status"), "RUNNING", StringComparison.Ordinal))
            {
                var outputJson = SerializeOutput(currentInput);
                execution["status"] = "SUCCEEDED";
                execution["output"] = outputJson;
                execution["stopDate"] = NowIso();
                AddEvent(execution, "ExecutionSucceeded", new Dictionary<string, object?>
                {
                    ["executionSucceededEventDetails"] = new Dictionary<string, object?> { ["output"] = outputJson },
                });
            }
        }
        catch (ExecutionError err)
        {
            FailExecution(execution, err.ErrorCode, err.Cause);
        }
        catch (Exception exc)
        {
            FailExecution(execution, "States.Runtime", exc.Message);
        }
    }

    private static void FailExecution(Dictionary<string, object?> execution, string error, string cause)
    {
        execution["status"] = "FAILED";
        execution["output"] = JsonSerializer.Serialize(new Dictionary<string, object?> { ["Error"] = error, ["Cause"] = cause });
        execution["stopDate"] = NowIso();
        AddEvent(execution, "ExecutionFailed", new Dictionary<string, object?>
        {
            ["executionFailedEventDetails"] = new Dictionary<string, object?> { ["error"] = error, ["cause"] = cause },
        });
    }
    // -- Pass state -------------------------------------------------------------

    private static (Dictionary<string, object?> Output, string? Next) ExecutePass(
        Dictionary<string, object?> stateDef, Dictionary<string, object?> rawInput)
    {
        var effective = ApplyInputPath(stateDef, rawInput);
        effective = ApplyParameters(stateDef, effective);

        Dictionary<string, object?> result;
        if (stateDef.TryGetValue("Result", out var resultVal))
        {
            result = resultVal is Dictionary<string, object?> rd
                ? new Dictionary<string, object?>(rd)
                : new Dictionary<string, object?> { ["__scalar__"] = resultVal };
        }
        else
        {
            result = new Dictionary<string, object?>(effective);
        }

        result = ApplyResultSelector(stateDef, result);
        var output = ApplyResultPath(stateDef, rawInput, result);
        output = ApplyOutputPath(stateDef, output);
        return (output, NextOrEnd(stateDef));
    }

    // -- Task state (with Retry/Catch) ------------------------------------------

    private (Dictionary<string, object?> Output, string? Next) ExecuteTask(
        Dictionary<string, object?> stateDef, Dictionary<string, object?> rawInput,
        Dictionary<string, object?> execution, Dictionary<string, object?> ctx)
    {
        var resource = GetString(stateDef, "Resource") ?? "";
        var isCallback = resource.Contains(".waitForTaskToken", StringComparison.Ordinal);

        // SFN mock config
        if (_sfnMockConfig.Count > 0)
        {
            var testCase = GetString(execution, "testCase") ?? "";
            var smName = GetString(GetDict(ctx, "StateMachine"), "Name") ?? "";
            var stateName = GetString(GetDict(ctx, "State"), "Name") ?? "";
            var attempts = GetDict(execution, "mockAttempts");
            var attempt = GetInt(attempts, stateName, 0);
            var mock = GetMockResponse(smName, testCase, stateName, attempt);
            if (mock is not null)
            {
                attempts[stateName] = (long)(attempt + 1);
                if (mock.TryGetValue("Throw", out var throwVal) && throwVal is Dictionary<string, object?> throwDict)
                {
                    throw new ExecutionError(
                        GetString(throwDict, "Error") ?? "MockError",
                        GetString(throwDict, "Cause") ?? "Mocked failure");
                }

                var mockResult = GetDict(mock, "Return");
                var mockResultSel = ApplyResultSelector(stateDef, mockResult);
                var mockOutput = ApplyResultPath(stateDef, rawInput, mockResultSel);
                mockOutput = ApplyOutputPath(stateDef, mockOutput);
                return (mockOutput, NextOrEnd(stateDef));
            }
        }

        if (isCallback)
        {
            ctx["Task"] = new Dictionary<string, object?> { ["Token"] = HashHelpers.NewUuid() };
        }

        var effective = ApplyInputPath(stateDef, rawInput);
        effective = ApplyParameters(stateDef, effective, ctx);

        var retriers = GetList(stateDef, "Retry");
        var catchers = GetList(stateDef, "Catch");
        var retryCounts = new Dictionary<int, int>();
        ExecutionError? lastError = null;

        while (true)
        {
            try
            {
                AddEvent(execution, "TaskScheduled", new Dictionary<string, object?>
                {
                    ["taskScheduledEventDetails"] = new Dictionary<string, object?>
                    {
                        ["resourceType"] = resource.Contains("lambda", StringComparison.OrdinalIgnoreCase) ? "lambda" : "states",
                        ["resource"] = resource,
                    },
                });

                Dictionary<string, object?> taskResult;
                if (isCallback)
                {
                    var token = GetString(GetDict(ctx, "Task"), "Token") ?? HashHelpers.NewUuid();
                    taskResult = InvokeWithCallback(resource, effective, token, stateDef);
                }
                else
                {
                    taskResult = InvokeResource(resource, effective);
                }

                AddEvent(execution, "TaskSucceeded", new Dictionary<string, object?>
                {
                    ["taskSucceededEventDetails"] = new Dictionary<string, object?>
                    {
                        ["output"] = SerializeOutput(taskResult),
                        ["resource"] = resource,
                    },
                });

                var result2 = ApplyResultSelector(stateDef, taskResult);
                var output2 = ApplyResultPath(stateDef, rawInput, result2);
                output2 = ApplyOutputPath(stateDef, output2);
                return (output2, NextOrEnd(stateDef));
            }
            catch (ExecutionError err)
            {
                lastError = err;
                AddEvent(execution, "TaskFailed", new Dictionary<string, object?>
                {
                    ["taskFailedEventDetails"] = new Dictionary<string, object?>
                    {
                        ["error"] = err.ErrorCode,
                        ["cause"] = err.Cause,
                        ["resource"] = resource,
                    },
                });

                var (retrier, retrierIdx) = FindMatchingRetrier(retriers, err.ErrorCode, retryCounts);
                if (retrier is not null && retrierIdx >= 0)
                {
                    var count = retryCounts.GetValueOrDefault(retrierIdx, 0);
                    var interval = GetInt(retrier, "IntervalSeconds", 1);
                    var backoffRate = 2.0;
                    if (retrier.TryGetValue("BackoffRate", out var br) && br is IConvertible brConv)
                    {
                        backoffRate = Convert.ToDouble(brConv);
                    }

                    var sleepSec = interval * Math.Pow(backoffRate, count);
                    Thread.Sleep(Math.Min((int)(sleepSec * 1000), 60000));
                    retryCounts[retrierIdx] = count + 1;
                    continue;
                }

                break;
            }
        }

        if (lastError is not null)
        {
            var catcher = FindMatchingCatcher(catchers, lastError.ErrorCode);
            if (catcher is not null)
            {
                var errorOutput = new Dictionary<string, object?>
                {
                    ["Error"] = lastError.ErrorCode,
                    ["Cause"] = lastError.Cause,
                };
                var catchResultPath = GetString(catcher, "ResultPath") ?? "$";
                var catchOutput = ApplyResultPathRaw(catchResultPath, rawInput, errorOutput);
                return (catchOutput, GetString(catcher, "Next"));
            }

            throw lastError;
        }

        throw new ExecutionError("States.Runtime", "Task failed with no error captured");
    }
    // -- Choice state -----------------------------------------------------------

    private static (Dictionary<string, object?> Output, string? Next) ExecuteChoice(
        Dictionary<string, object?> stateDef, Dictionary<string, object?> rawInput)
    {
        var effective = ApplyInputPath(stateDef, rawInput);
        var choices = GetList(stateDef, "Choices");

        foreach (var choice in choices)
        {
            if (choice is Dictionary<string, object?> rule && EvaluateRule(rule, effective))
            {
                return (ApplyOutputPath(stateDef, effective), GetString(rule, "Next"));
            }
        }

        var defaultState = GetString(stateDef, "Default");
        if (defaultState is not null)
        {
            return (ApplyOutputPath(stateDef, effective), defaultState);
        }

        throw new ExecutionError("States.NoChoiceMatched", "No choice rule matched and no Default");
    }

    private static bool EvaluateRule(Dictionary<string, object?> rule, Dictionary<string, object?> data)
    {
        if (rule.TryGetValue("And", out var andVal) && andVal is List<object?> andList)
        {
            return andList.All(r => r is Dictionary<string, object?> rd && EvaluateRule(rd, data));
        }

        if (rule.TryGetValue("Or", out var orVal) && orVal is List<object?> orList)
        {
            return orList.Any(r => r is Dictionary<string, object?> rd && EvaluateRule(rd, data));
        }

        if (rule.TryGetValue("Not", out var notVal) && notVal is Dictionary<string, object?> notRule)
        {
            return !EvaluateRule(notRule, data);
        }

        var variable = GetString(rule, "Variable");
        if (string.IsNullOrEmpty(variable))
        {
            return false;
        }

        var value = ResolvePath(variable, data);

        // Type checks
        if (rule.TryGetValue("IsPresent", out var isPresent))
        {
            return (value is not null) == (isPresent is true);
        }

        if (rule.TryGetValue("IsNull", out var isNull))
        {
            return (value is null) == (isNull is true);
        }

        if (rule.TryGetValue("IsNumeric", out var isNumeric))
        {
            return IsNum(value) == (isNumeric is true);
        }

        if (rule.TryGetValue("IsString", out var isString))
        {
            return (value is string) == (isString is true);
        }

        if (rule.TryGetValue("IsBoolean", out var isBoolean))
        {
            return (value is bool) == (isBoolean is true);
        }

        if (rule.TryGetValue("IsTimestamp", out var isTimestamp))
        {
            return IsTimestamp(value) == (isTimestamp is true);
        }

        // String comparisons
        if (rule.TryGetValue("StringEquals", out var se))
        {
            return value is string sv && string.Equals(sv, se?.ToString(), StringComparison.Ordinal);
        }

        if (rule.TryGetValue("StringEqualsPath", out var sep))
        {
            return value is string sv2 && string.Equals(sv2, ResolvePath(sep?.ToString() ?? "", data)?.ToString(), StringComparison.Ordinal);
        }

        if (rule.TryGetValue("StringLessThan", out var slt))
        {
            return value is string sv3 && string.Compare(sv3, slt?.ToString(), StringComparison.Ordinal) < 0;
        }

        if (rule.TryGetValue("StringGreaterThan", out var sgt))
        {
            return value is string sv4 && string.Compare(sv4, sgt?.ToString(), StringComparison.Ordinal) > 0;
        }

        if (rule.TryGetValue("StringLessThanEquals", out var slte))
        {
            return value is string sv5 && string.Compare(sv5, slte?.ToString(), StringComparison.Ordinal) <= 0;
        }

        if (rule.TryGetValue("StringGreaterThanEquals", out var sgte))
        {
            return value is string sv6 && string.Compare(sv6, sgte?.ToString(), StringComparison.Ordinal) >= 0;
        }

        if (rule.TryGetValue("StringMatches", out var sm2) && sm2 is string pattern)
        {
            if (value is not string sv7)
            {
                return false;
            }

            var regexPattern = "^" + Regex.Escape(pattern).Replace("\\*", ".*") + "$";
            return Regex.IsMatch(sv7, regexPattern);
        }

        // Numeric comparisons
        if (rule.TryGetValue("NumericEquals", out var ne))
        {
            return IsNum(value) && ToDouble(value) == ToDouble(ne);
        }

        if (rule.TryGetValue("NumericEqualsPath", out var nep))
        {
            var resolved = ResolvePath(nep?.ToString() ?? "", data);
            return IsNum(value) && ToDouble(value) == ToDouble(resolved);
        }

        if (rule.TryGetValue("NumericLessThan", out var nlt))
        {
            return IsNum(value) && ToDouble(value) < ToDouble(nlt);
        }

        if (rule.TryGetValue("NumericGreaterThan", out var ngt))
        {
            return IsNum(value) && ToDouble(value) > ToDouble(ngt);
        }

        if (rule.TryGetValue("NumericLessThanEquals", out var nlte))
        {
            return IsNum(value) && ToDouble(value) <= ToDouble(nlte);
        }

        if (rule.TryGetValue("NumericGreaterThanEquals", out var ngte))
        {
            return IsNum(value) && ToDouble(value) >= ToDouble(ngte);
        }

        // Boolean comparisons
        if (rule.TryGetValue("BooleanEquals", out var be))
        {
            return value is bool bv && be is bool be2 && bv == be2;
        }

        if (rule.TryGetValue("BooleanEqualsPath", out var bep))
        {
            var resolved2 = ResolvePath(bep?.ToString() ?? "", data);
            return value is bool bv2 && resolved2 is bool bv3 && bv2 == bv3;
        }

        // Timestamp comparisons
        foreach (var (op, cmpFn) in new (string, Func<DateTimeOffset, DateTimeOffset, bool>)[]
        {
            ("TimestampEquals", (a, b) => a == b),
            ("TimestampLessThan", (a, b) => a < b),
            ("TimestampGreaterThan", (a, b) => a > b),
            ("TimestampLessThanEquals", (a, b) => a <= b),
            ("TimestampGreaterThanEquals", (a, b) => a >= b),
        })
        {
            if (rule.TryGetValue(op, out var tsVal))
            {
                var a = ParseTs(value);
                var b = ParseTs(tsVal);
                return a.HasValue && b.HasValue && cmpFn(a.Value, b.Value);
            }
        }

        return false;
    }

    // -- Wait state -------------------------------------------------------------

    private static (Dictionary<string, object?> Output, string? Next) ExecuteWait(
        Dictionary<string, object?> stateDef, Dictionary<string, object?> rawInput,
        Dictionary<string, object?> execution)
    {
        var effective = ApplyInputPath(stateDef, rawInput);

        if (stateDef.TryGetValue("Seconds", out var secVal) && secVal is IConvertible secConv)
        {
            var secs = Convert.ToInt32(secConv);
            SleepWithAbortCheck(secs * 1000, execution);
        }
        else if (stateDef.TryGetValue("Timestamp", out var tsVal) && tsVal is string tsStr)
        {
            SleepUntil(tsStr, execution);
        }
        else if (stateDef.TryGetValue("SecondsPath", out var spVal) && spVal is string sp)
        {
            var resolved = ResolvePath(sp, effective);
            if (resolved is IConvertible rc)
            {
                var secs2 = Convert.ToDouble(rc);
                if (secs2 > 0)
                {
                    SleepWithAbortCheck((int)(secs2 * 1000), execution);
                }
            }
        }
        else if (stateDef.TryGetValue("TimestampPath", out var tpVal) && tpVal is string tp)
        {
            var resolved2 = ResolvePath(tp, effective);
            if (resolved2 is string tsStr2)
            {
                SleepUntil(tsStr2, execution);
            }
        }

        var output = ApplyOutputPath(stateDef, effective);
        return (output, NextOrEnd(stateDef));
    }

    private static void SleepUntil(string isoTs, Dictionary<string, object?> execution)
    {
        if (!DateTimeOffset.TryParse(isoTs, CultureInfo.InvariantCulture, DateTimeStyles.None, out var target))
        {
            return;
        }

        var delta = (target - DateTimeOffset.UtcNow).TotalMilliseconds;
        if (delta > 0)
        {
            SleepWithAbortCheck((int)Math.Min(delta, int.MaxValue), execution);
        }
    }

    private static void SleepWithAbortCheck(int totalMs, Dictionary<string, object?> execution)
    {
        var remaining = totalMs;
        while (remaining > 0)
        {
            if (!string.Equals(GetString(execution, "status"), "RUNNING", StringComparison.Ordinal))
            {
                return;
            }

            var slice = Math.Min(remaining, 500);
            Thread.Sleep(slice);
            remaining -= slice;
        }
    }
    // -- Parallel state ---------------------------------------------------------

    private (Dictionary<string, object?> Output, string? Next) ExecuteParallel(
        Dictionary<string, object?> stateDef, Dictionary<string, object?> rawInput,
        Dictionary<string, object?> execution, Dictionary<string, object?> ctx)
    {
        var effective = ApplyInputPath(stateDef, rawInput);
        effective = ApplyParameters(stateDef, effective, ctx);

        var branches = GetList(stateDef, "Branches");
        var results = new object?[branches.Count];
        var errors = new Exception?[branches.Count];

        var threads = new Thread[branches.Count];
        for (var i = 0; i < branches.Count; i++)
        {
            var idx = i;
            var branch = branches[idx] is Dictionary<string, object?> bd ? bd : new Dictionary<string, object?>();
            threads[idx] = new Thread(() =>
            {
                try
                {
                    results[idx] = RunSubMachine(
                        GetDict(branch, "States"),
                        GetString(branch, "StartAt"),
                        new Dictionary<string, object?>(effective),
                        execution, ctx);
                }
                catch (Exception exc)
                {
                    errors[idx] = exc;
                }
            })
            { IsBackground = true };
        }

        foreach (var t in threads)
        {
            t.Start();
        }

        foreach (var t in threads)
        {
            t.Join();
        }

        foreach (var err in errors)
        {
            if (err is ExecutionError ee)
            {
                throw ee;
            }

            if (err is not null)
            {
                throw new ExecutionError("States.BranchFailed", err.Message);
            }
        }

        var resultList = new List<object?>(results);
        var resultDict = ApplyResultSelectorForList(stateDef, resultList);
        var output = ApplyResultPath(stateDef, rawInput, resultDict);
        output = ApplyOutputPath(stateDef, output);
        return (output, NextOrEnd(stateDef));
    }

    // -- Map state --------------------------------------------------------------

    private (Dictionary<string, object?> Output, string? Next) ExecuteMap(
        Dictionary<string, object?> stateDef, Dictionary<string, object?> rawInput,
        Dictionary<string, object?> execution, Dictionary<string, object?> ctx)
    {
        var effective = ApplyInputPath(stateDef, rawInput);
        effective = ApplyParameters(stateDef, effective, ctx);

        var itemsPath = GetString(stateDef, "ItemsPath") ?? "$";
        var itemsObj = ResolvePath(itemsPath, effective);
        List<object?> items;
        if (itemsObj is List<object?> il)
        {
            items = il;
        }
        else
        {
            items = [itemsObj];
        }

        var iterator = stateDef.TryGetValue("Iterator", out var it) && it is Dictionary<string, object?> itd
            ? itd
            : (stateDef.TryGetValue("ItemProcessor", out var ip) && ip is Dictionary<string, object?> ipd
                ? ipd
                : new Dictionary<string, object?>());

        var iterStates = GetDict(iterator, "States");
        var iterStart = GetString(iterator, "StartAt");
        var maxConc = GetInt(stateDef, "MaxConcurrency", 0);

        var results = new object?[items.Count];
        var errors = new Exception?[items.Count];

        var workers = maxConc > 0 ? maxConc : Math.Max(items.Count, 1);
        using var semaphore = new SemaphoreSlim(workers);
        var doneEvent = new CountdownEvent(items.Count > 0 ? items.Count : 1);
        if (items.Count == 0)
        {
            doneEvent.Signal();
        }

        for (var i = 0; i < items.Count; i++)
        {
            var idx = i;
            var item = items[idx];
            semaphore.Wait();
            ThreadPool.QueueUserWorkItem(_ =>
            {
                try
                {
                    var itemCtx = DeepCopyDict(ctx);
                    itemCtx["Map"] = new Dictionary<string, object?>
                    {
                        ["Item"] = new Dictionary<string, object?>
                        {
                            ["Index"] = (long)idx,
                            ["Value"] = item,
                        },
                    };

                    Dictionary<string, object?>? itemParams = null;
                    if (stateDef.TryGetValue("ItemSelector", out var isel) && isel is Dictionary<string, object?> iseld)
                    {
                        itemParams = iseld;
                    }
                    else if (stateDef.TryGetValue("Parameters", out var psel) && psel is Dictionary<string, object?> pseld)
                    {
                        itemParams = pseld;
                    }

                    Dictionary<string, object?> itemInput;
                    if (itemParams is not null)
                    {
                        itemInput = ResolveParamsObj(itemParams, effective, itemCtx);
                    }
                    else
                    {
                        itemInput = item is Dictionary<string, object?> id3 ? id3 : new Dictionary<string, object?> { ["value"] = item };
                    }

                    results[idx] = RunSubMachine(iterStates, iterStart, itemInput, execution, itemCtx);
                }
                catch (Exception exc)
                {
                    errors[idx] = exc;
                }
                finally
                {
                    semaphore.Release();
                    doneEvent.Signal();
                }
            });
        }

        doneEvent.Wait();

        foreach (var err in errors)
        {
            if (err is ExecutionError ee2)
            {
                throw ee2;
            }

            if (err is not null)
            {
                throw new ExecutionError("States.MapFailed", err.Message);
            }
        }

        var resultList2 = new List<object?>(results);
        var resultDict2 = ApplyResultSelectorForList(stateDef, resultList2);
        var output2 = ApplyResultPath(stateDef, rawInput, resultDict2);
        output2 = ApplyOutputPath(stateDef, output2);
        return (output2, NextOrEnd(stateDef));
    }

    // -- Sub-machine runner -----------------------------------------------------

    private Dictionary<string, object?> RunSubMachine(
        Dictionary<string, object?> states, string? startAt,
        Dictionary<string, object?> inputData,
        Dictionary<string, object?> execution, Dictionary<string, object?> ctx)
    {
        var currentName = startAt;
        var currentInput = DeepCopyDict(inputData);

        while (currentName is not null)
        {
            var stateDef = GetDict(states, currentName);
            if (stateDef.Count == 0)
            {
                throw new ExecutionError("States.Runtime", $"State '{currentName}' not found");
            }

            var stateType = GetString(stateDef, "Type") ?? "";
            ctx["State"] = new Dictionary<string, object?> { ["Name"] = currentName, ["EnteredTime"] = NowIso() };

            if (stateType == "Succeed")
            {
                return ApplyOutputPath(stateDef, ApplyInputPath(stateDef, currentInput));
            }

            if (stateType == "Fail")
            {
                throw new ExecutionError(
                    GetString(stateDef, "Error") ?? "States.Fail",
                    GetString(stateDef, "Cause") ?? "");
            }

            string? nextName;
            switch (stateType)
            {
                case "Pass":
                    (currentInput, nextName) = ExecutePass(stateDef, currentInput);
                    break;
                case "Task":
                    (currentInput, nextName) = ExecuteTask(stateDef, currentInput, execution, ctx);
                    break;
                case "Choice":
                    (currentInput, nextName) = ExecuteChoice(stateDef, currentInput);
                    break;
                case "Wait":
                    (currentInput, nextName) = ExecuteWait(stateDef, currentInput, execution);
                    break;
                case "Parallel":
                    (currentInput, nextName) = ExecuteParallel(stateDef, currentInput, execution, ctx);
                    break;
                case "Map":
                    (currentInput, nextName) = ExecuteMap(stateDef, currentInput, execution, ctx);
                    break;
                default:
                    throw new ExecutionError("States.Runtime", $"Unknown state type: {stateType}");
            }

            currentName = nextName;
        }

        return currentInput;
    }
    // -- Path / Parameter processing --------------------------------------------

    private static Dictionary<string, object?> ApplyInputPath(Dictionary<string, object?> stateDef, Dictionary<string, object?> data)
    {
        if (!stateDef.TryGetValue("InputPath", out var ipVal))
        {
            return data;
        }

        if (ipVal is null)
        {
            return new Dictionary<string, object?>();
        }

        var ip = ipVal.ToString() ?? "$";
        var resolved = ResolvePath(ip, data);
        return resolved is Dictionary<string, object?> rd ? rd : data;
    }

    private static Dictionary<string, object?> ApplyOutputPath(Dictionary<string, object?> stateDef, Dictionary<string, object?> data)
    {
        if (!stateDef.TryGetValue("OutputPath", out var opVal))
        {
            return data;
        }

        if (opVal is null)
        {
            return new Dictionary<string, object?>();
        }

        var op = opVal.ToString() ?? "$";
        var resolved = ResolvePath(op, data);
        return resolved is Dictionary<string, object?> rd ? rd : data;
    }

    private static Dictionary<string, object?> ApplyParameters(
        Dictionary<string, object?> stateDef, Dictionary<string, object?> data,
        Dictionary<string, object?>? ctx = null)
    {
        if (!stateDef.TryGetValue("Parameters", out var paramsVal) || paramsVal is not Dictionary<string, object?> paramsDict)
        {
            return data;
        }

        return ResolveParamsObj(paramsDict, data, ctx);
    }

    private static Dictionary<string, object?> ApplyResultSelector(
        Dictionary<string, object?> stateDef, Dictionary<string, object?> data)
    {
        if (!stateDef.TryGetValue("ResultSelector", out var selVal) || selVal is not Dictionary<string, object?> selDict)
        {
            return data;
        }

        return ResolveParamsObj(selDict, data);
    }

    private static Dictionary<string, object?> ApplyResultSelectorForList(
        Dictionary<string, object?> stateDef, List<object?> data)
    {
        // For Parallel/Map: if ResultSelector is present, apply it; otherwise wrap list
        if (stateDef.TryGetValue("ResultSelector", out var selVal) && selVal is Dictionary<string, object?> selDict)
        {
            var wrapper = new Dictionary<string, object?> { ["result"] = data };
            return ResolveParamsObj(selDict, wrapper);
        }

        // No ResultSelector: pass through as a dict wrapping the list for ResultPath
        return new Dictionary<string, object?> { ["__list__"] = data };
    }

    private static Dictionary<string, object?> ApplyResultPath(
        Dictionary<string, object?> stateDef, Dictionary<string, object?> original, Dictionary<string, object?> result)
    {
        var resultPath = "$";
        if (stateDef.TryGetValue("ResultPath", out var rpVal))
        {
            if (rpVal is null)
            {
                return DeepCopyDict(original);
            }

            resultPath = rpVal.ToString() ?? "$";
        }

        // Unwrap list/scalar wrappers from Parallel/Map/Pass
        object? actualResult = result;
        if (result.Count == 1)
        {
            if (result.TryGetValue("__list__", out var listVal))
            {
                actualResult = listVal;
            }
            else if (result.TryGetValue("__scalar__", out var scalarVal))
            {
                actualResult = scalarVal;
            }
        }

        return ApplyResultPathRaw(resultPath, original, actualResult);
    }

    private static Dictionary<string, object?> ApplyResultPathRaw(
        string resultPath, Dictionary<string, object?> original, object? result)
    {
        if (resultPath == "$")
        {
            if (result is Dictionary<string, object?> rd)
            {
                return rd;
            }

            // Preserve list wrapper so serialization emits a JSON array
            if (result is List<object?>)
            {
                return new Dictionary<string, object?> { ["__list__"] = result };
            }

            return new Dictionary<string, object?> { ["__scalar__"] = result };
        }

        var output = DeepCopyDict(original);
        var path = resultPath.TrimStart('$', '.');
        var parts = path.Split('.');
        Dictionary<string, object?> cur = output;
        for (var i = 0; i < parts.Length - 1; i++)
        {
            var p = parts[i];
            if (string.IsNullOrEmpty(p))
            {
                continue;
            }

            if (!cur.TryGetValue(p, out var next) || next is not Dictionary<string, object?> nextDict)
            {
                nextDict = new Dictionary<string, object?>();
                cur[p] = nextDict;
            }

            cur = nextDict;
        }

        var lastPart = parts[^1];
        if (!string.IsNullOrEmpty(lastPart))
        {
            cur[lastPart] = result;
        }

        return output;
    }

    [GeneratedRegex(@"(\w+)\[(\d+)]")]
    private static partial Regex ArrayIndexRegex();

    private static object? ResolvePath(string path, object? data)
    {
        if (path == "$" || string.IsNullOrEmpty(path))
        {
            return data;
        }

        if (!path.StartsWith('$'))
        {
            return data;
        }

        var parts = path.StartsWith("$.") ? path[2..].Split('.') : Array.Empty<string>();
        var cur = data;
        foreach (var part in parts)
        {
            if (string.IsNullOrEmpty(part))
            {
                continue;
            }

            var match = ArrayIndexRegex().Match(part);
            if (match.Success)
            {
                var field = match.Groups[1].Value;
                var idx = int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture);
                if (cur is Dictionary<string, object?> d && d.TryGetValue(field, out var fieldVal))
                {
                    cur = fieldVal;
                    if (cur is List<object?> list && idx < list.Count)
                    {
                        cur = list[idx];
                    }
                    else
                    {
                        return null;
                    }
                }
                else
                {
                    return null;
                }
            }
            else if (cur is Dictionary<string, object?> d2 && d2.TryGetValue(part, out var val))
            {
                cur = val;
            }
            else
            {
                return null;
            }
        }

        return cur;
    }

    private static object? ResolveCtxPath(string path, Dictionary<string, object?> ctx)
    {
        if (!path.StartsWith("$$."))
        {
            return null;
        }

        var parts = path[3..].Split('.');
        object? cur = ctx;
        foreach (var p in parts)
        {
            if (cur is Dictionary<string, object?> d && d.TryGetValue(p, out var val))
            {
                cur = val;
            }
            else
            {
                return null;
            }
        }

        return cur;
    }

    private static Dictionary<string, object?> ResolveParamsObj(
        Dictionary<string, object?> template, Dictionary<string, object?> data,
        Dictionary<string, object?>? ctx = null)
    {
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var kv in template)
        {
            if (kv.Key.EndsWith(".$", StringComparison.Ordinal))
            {
                var realKey = kv.Key[..^2];
                if (kv.Value is string strVal)
                {
                    if (strVal.StartsWith("States.", StringComparison.Ordinal))
                    {
                        result[realKey] = EvaluateIntrinsic(strVal, data, ctx);
                    }
                    else if (strVal.StartsWith("$$.", StringComparison.Ordinal))
                    {
                        result[realKey] = ResolveCtxPath(strVal, ctx ?? new Dictionary<string, object?>());
                    }
                    else
                    {
                        result[realKey] = ResolvePath(strVal, data);
                    }
                }
                else
                {
                    result[realKey] = kv.Value;
                }
            }
            else if (kv.Value is Dictionary<string, object?> nestedDict)
            {
                result[kv.Key] = ResolveParamsObj(nestedDict, data, ctx);
            }
            else if (kv.Value is List<object?> nestedList)
            {
                var newList = new List<object?>();
                foreach (var item in nestedList)
                {
                    if (item is Dictionary<string, object?> itemDict)
                    {
                        newList.Add(ResolveParamsObj(itemDict, data, ctx));
                    }
                    else
                    {
                        newList.Add(item);
                    }
                }

                result[kv.Key] = newList;
            }
            else
            {
                result[kv.Key] = kv.Value;
            }
        }

        return result;
    }
    // -- Intrinsic functions ----------------------------------------------------

    private static object? EvaluateIntrinsic(string expression, Dictionary<string, object?> data, Dictionary<string, object?>? ctx)
    {
        var (node, _) = ParseIntrinsicCall(expression, 0);
        return ExecIntrinsic(node, data, ctx);
    }

    private static ((string Kind, string Name, List<(string Kind, object? Value)> Args) Node, int Pos) ParseIntrinsicCall(string s, int pos)
    {
        var paren = s.IndexOf('(', pos);
        var name = s[pos..paren].Trim();
        var (args, end) = ParseIntrinsicArgs(s, paren + 1);
        return (("call", name, args), end);
    }

    private static (List<(string Kind, object? Value)> Args, int Pos) ParseIntrinsicArgs(string s, int pos)
    {
        var args = new List<(string Kind, object? Value)>();
        pos = SkipWs(s, pos);
        if (pos < s.Length && s[pos] == ')')
        {
            return (args, pos + 1);
        }

        while (pos < s.Length)
        {
            pos = SkipWs(s, pos);
            if (pos >= s.Length)
            {
                break;
            }

            var ch = s[pos];

            if (s[pos..].StartsWith("States.", StringComparison.Ordinal))
            {
                var (node, end2) = ParseIntrinsicCall(s, pos);
                args.Add(("call", node));
                pos = end2;
            }
            else if (ch == '\'')
            {
                var end3 = s.IndexOf('\'', pos + 1);
                args.Add(("str", s[(pos + 1)..end3]));
                pos = end3 + 1;
            }
            else if (ch == '$')
            {
                var end4 = pos;
                while (end4 < s.Length && s[end4] != ',' && s[end4] != ')')
                {
                    end4++;
                }

                args.Add(("path", s[pos..end4].Trim()));
                pos = end4;
            }
            else if (ch is >= '0' and <= '9' or '-')
            {
                var end5 = pos + 1;
                while (end5 < s.Length && s[end5] != ',' && s[end5] != ')')
                {
                    end5++;
                }

                var tok = s[pos..end5].Trim();
                if (tok.Contains('.'))
                {
                    args.Add(("num", double.Parse(tok, CultureInfo.InvariantCulture)));
                }
                else
                {
                    args.Add(("num", long.Parse(tok, CultureInfo.InvariantCulture)));
                }

                pos = end5;
            }
            else if (s[pos..].StartsWith("true", StringComparison.Ordinal))
            {
                args.Add(("bool", true));
                pos += 4;
            }
            else if (s[pos..].StartsWith("false", StringComparison.Ordinal))
            {
                args.Add(("bool", false));
                pos += 5;
            }
            else if (s[pos..].StartsWith("null", StringComparison.Ordinal))
            {
                args.Add(("null", null));
                pos += 4;
            }
            else
            {
                pos++;
                continue;
            }

            pos = SkipWs(s, pos);
            if (pos < s.Length && s[pos] == ',')
            {
                pos++;
            }
            else if (pos < s.Length && s[pos] == ')')
            {
                return (args, pos + 1);
            }
        }

        return (args, pos);
    }

    private static int SkipWs(string s, int pos)
    {
        while (pos < s.Length && char.IsWhiteSpace(s[pos]))
        {
            pos++;
        }

        return pos;
    }

    private static object? EvalIntrinsicArg((string Kind, object? Value) arg, Dictionary<string, object?> data, Dictionary<string, object?>? ctx)
    {
        return arg.Kind switch
        {
            "str" => arg.Value,
            "num" => arg.Value,
            "bool" => arg.Value,
            "null" => null,
            "path" when arg.Value is string pathStr =>
                pathStr.StartsWith("$$.", StringComparison.Ordinal)
                    ? ResolveCtxPath(pathStr, ctx ?? new Dictionary<string, object?>())
                    : ResolvePath(pathStr, data),
            "call" when arg.Value is ValueTuple<string, string, List<(string, object?)>> node => ExecIntrinsic(node, data, ctx),
            _ => null,
        };
    }

    private static object? ExecIntrinsic(
        (string Kind, string Name, List<(string Kind, object? Value)> Args) node,
        Dictionary<string, object?> data, Dictionary<string, object?>? ctx)
    {
        var args = node.Args.Select(a => EvalIntrinsicArg(a, data, ctx)).ToList();

        return node.Name switch
        {
            "States.StringToJson" => args.Count > 0 && args[0] is string s
                ? ParseJsonToDict(Encoding.UTF8.GetBytes(s)).Count > 0
                    ? (object)ParseJsonToDict(Encoding.UTF8.GetBytes(s))
                    : JsonElementToObject(JsonDocument.Parse(s).RootElement.Clone())
                : null,
            "States.JsonToString" => args.Count > 0 ? JsonSerializer.Serialize(args[0]) : null,
            "States.JsonMerge" => MergeJsonObjects(args),
            "States.Format" => FormatIntrinsic(args),
            "States.ArrayGetItem" => ArrayGetItem(args),
            "States.Array" => new List<object?>(args),
            "States.ArrayLength" => args.Count > 0 && args[0] is List<object?> l ? (long)l.Count : 0L,
            _ => throw new ExecutionError("States.Runtime", $"Unsupported intrinsic function: {node.Name}"),
        };
    }

    private static object? MergeJsonObjects(List<object?> args)
    {
        if (args.Count < 2)
        {
            return args.Count > 0 ? args[0] : new Dictionary<string, object?>();
        }

        var merged = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (args[0] is Dictionary<string, object?> d1)
        {
            foreach (var kv in d1)
            {
                merged[kv.Key] = kv.Value;
            }
        }

        if (args[1] is Dictionary<string, object?> d2)
        {
            foreach (var kv in d2)
            {
                merged[kv.Key] = kv.Value;
            }
        }

        return merged;
    }

    private static string FormatIntrinsic(List<object?> args)
    {
        if (args.Count == 0)
        {
            return "";
        }

        var template = args[0]?.ToString() ?? "";
        var parts = template.Split("{}");
        var sb = new StringBuilder();
        for (var i = 0; i < parts.Length; i++)
        {
            sb.Append(parts[i]);
            if (i < parts.Length - 1 && i < args.Count - 1)
            {
                var val = args[i + 1];
                sb.Append(val is string s ? s : val?.ToString() ?? "");
            }
        }

        return sb.ToString();
    }

    private static object? ArrayGetItem(List<object?> args)
    {
        if (args.Count < 2 || args[0] is not List<object?> arr)
        {
            return null;
        }

        var idx = args[1] is IConvertible c ? Convert.ToInt32(c) : 0;
        return idx >= 0 && idx < arr.Count ? arr[idx] : null;
    }
    // -- Resource invocation ----------------------------------------------------

    private Dictionary<string, object?> InvokeResource(string resource, Dictionary<string, object?> inputData)
    {
        if (resource.Contains("states:::lambda:invoke", StringComparison.OrdinalIgnoreCase))
        {
            var funcName = GetString(inputData, "FunctionName") ?? "";
            var payload = inputData.TryGetValue("Payload", out var p) && p is Dictionary<string, object?> pd
                ? pd : inputData;
            if (funcName.Contains(":function:", StringComparison.Ordinal))
            {
                funcName = funcName.Split(":function:")[^1].Split(':')[0];
            }

            var lambdaResult = CallLambda(funcName, payload);
            return new Dictionary<string, object?> { ["StatusCode"] = 200L, ["Payload"] = lambdaResult };
        }

        var directFuncName = ExtractLambdaName(resource);
        if (directFuncName is not null)
        {
            return CallLambda(directFuncName, inputData);
        }

        // Activity resource
        if (resource.Contains(":activity:", StringComparison.Ordinal))
        {
            return InvokeActivity(resource, inputData);
        }

        // Nested execution
        if (resource.StartsWith("arn:aws:states:::states:startExecution.sync", StringComparison.Ordinal))
        {
            return InvokeNestedStartExecution(resource, inputData);
        }

        // Service integration dispatch
        var clean = resource.Replace(".sync", "").Replace(".waitForTaskToken", "");

        if (clean.StartsWith("arn:aws:states:::sqs:sendMessage", StringComparison.Ordinal))
        {
            return InvokeSqsSendMessage(inputData);
        }

        if (clean.StartsWith("arn:aws:states:::sns:publish", StringComparison.Ordinal))
        {
            return InvokeSnsPublish(inputData);
        }

        if (clean.StartsWith("arn:aws:states:::dynamodb:", StringComparison.Ordinal))
        {
            var opName = clean.Split(":::dynamodb:")[^1];
            return InvokeDynamoDb(opName, inputData);
        }

        // Generic aws-sdk:* integration
        if (resource.Contains("aws-sdk:", StringComparison.Ordinal))
        {
            return InvokeAwsSdkIntegration(resource, inputData);
        }

        return inputData;
    }

    private static string? ExtractLambdaName(string resource)
    {
        if (string.IsNullOrEmpty(resource))
        {
            return null;
        }

        if (resource.Contains(":function:", StringComparison.Ordinal))
        {
            return resource.Split(":function:")[^1].Split(':')[0];
        }

        return null;
    }

    private Dictionary<string, object?> CallLambda(string funcName, Dictionary<string, object?> eventData)
    {
        var eventBytes = JsonSerializer.SerializeToUtf8Bytes(eventData);
        var (success, responsePayload, error) = _lambda.InvokeForApiGateway(funcName, eventBytes);

        if (!success)
        {
            throw new ExecutionError("Lambda.ResourceNotFoundException", error ?? $"Function not found: {funcName}");
        }

        if (responsePayload is null || responsePayload.Length == 0)
        {
            return new Dictionary<string, object?>();
        }

        try
        {
            using var doc = JsonDocument.Parse(responsePayload);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Object)
            {
                var result = JsonElementToDict(root);
                // Check for Lambda error
                if (result.TryGetValue("errorType", out var et))
                {
                    throw new ExecutionError(
                        et?.ToString() ?? "Lambda.Unknown",
                        GetString(result, "errorMessage") ?? "");
                }

                return result;
            }

            // Non-object response: wrap
            return new Dictionary<string, object?> { ["value"] = JsonElementToObject(root) };
        }
        catch (JsonException)
        {
            return new Dictionary<string, object?> { ["value"] = Encoding.UTF8.GetString(responsePayload) };
        }
    }

    private Dictionary<string, object?> InvokeActivity(string resource, Dictionary<string, object?> inputData)
    {
        var arn = resource;
        if (!_activities.ContainsKey(arn))
        {
            throw new ExecutionError("ActivityDoesNotExist", $"Activity {arn} not found");
        }

        var token = HashHelpers.NewUuid();
        var evt = new ManualResetEventSlim(false);
        var info = new TaskTokenInfo { Event = evt };
        _taskTokens[token] = info;

        if (_activityTasks.TryGetValue(arn, out var queue))
        {
            lock (queue)
            {
                queue.Add(new Dictionary<string, object?>
                {
                    ["taskToken"] = token,
                    ["input"] = JsonSerializer.Serialize(inputData),
                });
            }
        }

        if (!evt.Wait(TimeSpan.FromSeconds(99999)))
        {
            _taskTokens.TryRemove(token, out _);
            throw new ExecutionError("States.Timeout", "Activity task timed out waiting for worker");
        }

        _taskTokens.TryRemove(token, out var info2);
        info2 ??= info;

        if (info2.Error is not null)
        {
            throw new ExecutionError(
                GetString(info2.Error, "Error") ?? "TaskFailed",
                GetString(info2.Error, "Cause") ?? "");
        }

        var resultRaw = info2.Result ?? "{}";
        try
        {
            return ParseJsonToDict(Encoding.UTF8.GetBytes(resultRaw));
        }
        catch (JsonException)
        {
            return new Dictionary<string, object?> { ["value"] = resultRaw };
        }
    }

    private Dictionary<string, object?> InvokeWithCallback(
        string resource, Dictionary<string, object?> inputData, string token,
        Dictionary<string, object?> stateDef)
    {
        var evt = new ManualResetEventSlim(false);
        _taskTokens[token] = new TaskTokenInfo { Event = evt };

        var cleanResource = resource.Replace(".waitForTaskToken", "");
        var funcName = ExtractLambdaName(cleanResource);
        if (funcName is null && cleanResource.Contains("states:::lambda:invoke", StringComparison.OrdinalIgnoreCase))
        {
            funcName = GetString(inputData, "FunctionName") ?? "";
            if (funcName.Contains(":function:", StringComparison.Ordinal))
            {
                funcName = funcName.Split(":function:")[^1].Split(':')[0];
            }
        }

        if (funcName is not null)
        {
            try
            {
                CallLambda(funcName, inputData);
            }
            catch (ExecutionError)
            {
                // Ignore
            }
        }

        var timeout = GetInt(stateDef, "TimeoutSeconds", 99999);
        if (!evt.Wait(TimeSpan.FromSeconds(timeout)))
        {
            _taskTokens.TryRemove(token, out _);
            throw new ExecutionError("States.Timeout", "Task timed out waiting for callback");
        }

        _taskTokens.TryRemove(token, out var info3);
        if (info3?.Error is not null)
        {
            throw new ExecutionError(
                GetString(info3.Error, "Error") ?? "TaskFailed",
                GetString(info3.Error, "Cause") ?? "");
        }

        var resultRaw2 = info3?.Result ?? "{}";
        try
        {
            return ParseJsonToDict(Encoding.UTF8.GetBytes(resultRaw2));
        }
        catch (JsonException)
        {
            return new Dictionary<string, object?> { ["value"] = resultRaw2 };
        }
    }
    // -- Service integrations ---------------------------------------------------

    private Dictionary<string, object?> InvokeSqsSendMessage(Dictionary<string, object?> inputData)
    {
        return DispatchToService("sqs", "AmazonSQS.SendMessage", inputData);
    }

    private Dictionary<string, object?> InvokeSnsPublish(Dictionary<string, object?> inputData)
    {
        return DispatchToService("sns", "SNS.Publish", inputData);
    }

    private Dictionary<string, object?> InvokeDynamoDb(string opName, Dictionary<string, object?> inputData)
    {
        var actionMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["putItem"] = "DynamoDB_20120810.PutItem",
            ["getItem"] = "DynamoDB_20120810.GetItem",
            ["deleteItem"] = "DynamoDB_20120810.DeleteItem",
            ["updateItem"] = "DynamoDB_20120810.UpdateItem",
        };

        if (!actionMap.TryGetValue(opName, out var target))
        {
            throw new ExecutionError("States.Runtime", $"Unsupported DynamoDB operation: {opName}");
        }

        return DispatchToService("dynamodb", target, inputData);
    }

    private Dictionary<string, object?> DispatchToService(string serviceName, string target, Dictionary<string, object?> inputData)
    {
        var handler = _registry.Resolve(serviceName);
        if (handler is null)
        {
            throw new ExecutionError("States.Runtime", $"Service '{serviceName}' is not available");
        }

        var headers = new Dictionary<string, string>
        {
            ["x-amz-target"] = target,
            ["content-type"] = "application/x-amz-json-1.0",
            ["host"] = $"{serviceName}.{Region}.amazonaws.com",
            ["authorization"] = $"AWS4-HMAC-SHA256 Credential=test/20260101/{Region}/{serviceName}/aws4_request",
        };

        var bodyBytes = JsonSerializer.SerializeToUtf8Bytes(inputData);
        var request = new ServiceRequest("POST", "/", headers, bodyBytes, new Dictionary<string, string[]>());
        var response = handler.HandleAsync(request).GetAwaiter().GetResult();
        var decoded = ParseJsonToDict(response.Body);

        if (response.StatusCode >= 400)
        {
            var errorType = GetString(decoded, "__type") ?? $"{serviceName}.ServiceException";
            var errorMsg = GetString(decoded, "message") ?? "";
            throw new ExecutionError(errorType, errorMsg);
        }

        return decoded;
    }

    private Dictionary<string, object?> InvokeNestedStartExecution(string resource, Dictionary<string, object?> inputData)
    {
        var smArn = GetString(inputData, "StateMachineArn") ?? GetString(inputData, "stateMachineArn");
        if (string.IsNullOrEmpty(smArn))
        {
            throw new ExecutionError("ValidationException", "StateMachineArn is required");
        }

        var nestedInput = inputData.TryGetValue("Input", out var ni) ? ni : (inputData.TryGetValue("input", out var ni2) ? ni2 : null);
        string inputStr;
        if (nestedInput is string s)
        {
            inputStr = s;
        }
        else
        {
            inputStr = nestedInput is not null ? JsonSerializer.Serialize(nestedInput) : "{}";
        }

        var request = new Dictionary<string, object?>
        {
            ["stateMachineArn"] = smArn,
            ["input"] = inputStr,
        };
        var name = GetString(inputData, "Name") ?? GetString(inputData, "name");
        if (name is not null)
        {
            request["name"] = name;
        }

        var resp = ActStartSyncExecution(request);
        var payload = ParseJsonToDict(resp.Body);

        if (resp.StatusCode >= 400)
        {
            throw new ExecutionError(
                GetString(payload, "__type") ?? "States.Runtime",
                GetString(payload, "message") ?? "Nested execution failed to start");
        }

        var status = GetString(payload, "status");
        if (!string.Equals(status, "SUCCEEDED", StringComparison.Ordinal))
        {
            var (error, cause) = NestedExecutionFailure(payload);
            throw new ExecutionError(error, cause);
        }

        object? outputValue = GetString(payload, "output") ?? "{}";
        if (resource.EndsWith(".sync:2", StringComparison.Ordinal) && outputValue is string outStr)
        {
            try
            {
                outputValue = ParseJsonToDict(Encoding.UTF8.GetBytes(outStr));
            }
            catch (JsonException)
            {
                // Keep as string
            }
        }

        return new Dictionary<string, object?>
        {
            ["ExecutionArn"] = GetString(payload, "executionArn"),
            ["Input"] = GetString(payload, "input") ?? "{}",
            ["InputDetails"] = payload.TryGetValue("inputDetails", out var idet) ? idet : new Dictionary<string, object?> { ["included"] = true },
            ["Name"] = GetString(payload, "name"),
            ["Output"] = outputValue,
            ["OutputDetails"] = payload.TryGetValue("outputDetails", out var odet) ? odet : new Dictionary<string, object?> { ["included"] = true },
            ["StartDate"] = GetString(payload, "startDate"),
            ["StateMachineArn"] = GetString(payload, "stateMachineArn"),
            ["Status"] = status,
            ["StopDate"] = GetString(payload, "stopDate"),
        };
    }

    private static (string Error, string Cause) NestedExecutionFailure(Dictionary<string, object?> payload)
    {
        var output = GetString(payload, "output");
        if (output is not null)
        {
            try
            {
                var decoded = ParseJsonToDict(Encoding.UTF8.GetBytes(output));
                if (decoded.TryGetValue("Error", out var err) && err is not null)
                {
                    return (err.ToString()!, GetString(decoded, "Cause") ?? "");
                }
            }
            catch (JsonException)
            {
                // Ignore
            }
        }

        var execArn = GetString(payload, "executionArn") ?? "";
        var status = GetString(payload, "status") ?? "FAILED";
        return ("States.TaskFailed", $"Nested execution {execArn} ended with status {status}");
    }
    // -- aws-sdk dispatch -------------------------------------------------------

    private static readonly Dictionary<string, (string TargetPrefix, string Protocol, string? ServiceKey)> AwsSdkServiceMap = new(StringComparer.OrdinalIgnoreCase)
    {
        ["dynamodb"] = ("DynamoDB_20120810", "json", null),
        ["secretsmanager"] = ("secretsmanager", "json", null),
        ["sfn"] = ("AWSStepFunctions", "json", "states"),
        ["logs"] = ("Logs_20140328", "json", null),
        ["ssm"] = ("AmazonSSM", "json", null),
        ["eventbridge"] = ("AWSEvents", "json", "events"),
        ["kinesis"] = ("Kinesis_20131202", "json", null),
        ["glue"] = ("AWSGlue", "json", null),
        ["athena"] = ("AmazonAthena", "json", null),
        ["ecs"] = ("AmazonEC2ContainerServiceV20141113", "json", null),
        ["ecr"] = ("AmazonEC2ContainerRegistry_V20150921", "json", null),
        ["kms"] = ("TrentService", "json", null),
        ["sqs"] = ("", "query", null),
        ["sns"] = ("", "query", null),
        ["rds"] = ("", "query", null),
        ["elasticache"] = ("", "query", null),
        ["ec2"] = ("", "query", null),
        ["iam"] = ("", "query", null),
        ["sts"] = ("", "query", null),
        ["cloudwatch"] = ("", "query", "monitoring"),
        ["s3"] = ("", "rest", null),
        ["lambda"] = ("", "rest", null),
    };

    private Dictionary<string, object?> InvokeAwsSdkIntegration(string resource, Dictionary<string, object?> inputData)
    {
        var parts = resource.Replace(".sync", "").Replace(".waitForTaskToken", "").Split(':');
        if (parts.Length < 8 || parts[5] != "aws-sdk")
        {
            throw new ExecutionError("States.Runtime", $"Invalid aws-sdk resource ARN: {resource}");
        }

        var serviceName = parts[6].ToLowerInvariant();
        var action = parts[7];

        if (!AwsSdkServiceMap.TryGetValue(serviceName, out var serviceInfo))
        {
            throw new ExecutionError("States.Runtime", $"Service '{serviceName}' is not supported in MiniStack aws-sdk integrations");
        }

        if (serviceInfo.Protocol == "json")
        {
            return DispatchAwsSdkJson(serviceInfo, serviceName, action, inputData);
        }

        if (serviceInfo.Protocol == "query")
        {
            throw new ExecutionError("States.Runtime",
                $"aws-sdk integration for query-protocol service '{serviceName}' is not yet implemented; use native service integrations instead");
        }

        throw new ExecutionError("States.Runtime",
            $"aws-sdk integration for {serviceInfo.Protocol}-protocol service '{serviceName}' is not yet implemented; use native service integrations instead");
    }

    private Dictionary<string, object?> DispatchAwsSdkJson(
        (string TargetPrefix, string Protocol, string? ServiceKey) serviceInfo,
        string serviceName, string action, Dictionary<string, object?> inputData)
    {
        var pascalAction = string.IsNullOrEmpty(action) ? action : char.ToUpperInvariant(action[0]) + action[1..];
        var target = $"{serviceInfo.TargetPrefix}.{pascalAction}";
        var serviceKey = serviceInfo.ServiceKey ?? serviceName;

        var handler = _registry.Resolve(serviceKey);
        if (handler is null)
        {
            throw new ExecutionError("States.Runtime", $"Service '{serviceKey}' is not available in MiniStack");
        }

        var headers = new Dictionary<string, string>
        {
            ["x-amz-target"] = target,
            ["content-type"] = "application/x-amz-json-1.0",
            ["host"] = $"{serviceKey}.{Region}.amazonaws.com",
            ["authorization"] = $"AWS4-HMAC-SHA256 Credential=test/20260101/{Region}/{serviceKey}/aws4_request",
        };

        var bodyBytes = JsonSerializer.SerializeToUtf8Bytes(inputData);
        var request = new ServiceRequest("POST", "/", headers, bodyBytes, new Dictionary<string, string[]>());
        var response = handler.HandleAsync(request).GetAwaiter().GetResult();
        var decoded = ParseJsonToDict(response.Body);

        if (response.StatusCode >= 400)
        {
            var errorType = GetString(decoded, "__type") ?? $"{serviceName}.ServiceException";
            var errorMsg = GetString(decoded, "message") ?? JsonSerializer.Serialize(decoded);
            throw new ExecutionError(errorType, errorMsg);
        }

        // Convert top-level keys to SFN convention
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var kv in decoded)
        {
            result[ApiNameToSfnKey(kv.Key)] = kv.Value;
        }

        return result;
    }

    private static string ApiNameToSfnKey(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return name;
        }

        var sb = new StringBuilder(name.Length);
        var i = 0;
        while (i < name.Length)
        {
            if (i == 0)
            {
                sb.Append(char.ToUpperInvariant(name[i]));
                i++;
                continue;
            }

            if (char.IsUpper(name[i]))
            {
                var j = i;
                while (j < name.Length && char.IsUpper(name[j]))
                {
                    j++;
                }

                var runLen = j - i;
                if (runLen == 1)
                {
                    sb.Append(name[i]);
                    i++;
                }
                else
                {
                    if (j < name.Length && char.IsLower(name[j]))
                    {
                        sb.Append(name[i..(j - 1)].ToLowerInvariant());
                        sb.Append(name[j - 1]);
                    }
                    else
                    {
                        sb.Append(name[i..j].ToLowerInvariant());
                    }

                    i = j;
                }
            }
            else
            {
                sb.Append(name[i]);
                i++;
            }
        }

        return sb.ToString();
    }
    // -- TestState API ----------------------------------------------------------

    private ServiceResponse ActTestState(Dictionary<string, object?> data)
    {
        var definitionStr = GetString(data, "definition");
        if (string.IsNullOrEmpty(definitionStr))
        {
            return ErrorResponse("InvalidDefinition", "definition is required");
        }

        Dictionary<string, object?> definition;
        try
        {
            definition = ParseJsonToDict(Encoding.UTF8.GetBytes(definitionStr));
        }
        catch (JsonException)
        {
            return ErrorResponse("InvalidDefinition", "Invalid JSON in definition");
        }

        var inputStr = GetString(data, "input") ?? "{}";
        Dictionary<string, object?> inputData;
        try
        {
            inputData = ParseJsonToDict(Encoding.UTF8.GetBytes(inputStr));
        }
        catch (JsonException)
        {
            return ErrorResponse("InvalidExecutionInput", "Invalid JSON in input");
        }

        var inspectionLevel = GetString(data, "inspectionLevel") ?? "INFO";
        var stateName = GetString(data, "stateName");

        Dictionary<string, object?> stateDef;
        if (definition.ContainsKey("States"))
        {
            if (string.IsNullOrEmpty(stateName))
            {
                stateName = GetString(definition, "StartAt");
            }

            var states = GetDict(definition, "States");
            if (stateName is null || !states.ContainsKey(stateName))
            {
                return ErrorResponse("InvalidDefinition", $"State '{stateName}' not found in definition");
            }

            stateDef = GetDict(states, stateName);
        }
        else
        {
            stateDef = definition;
            stateName ??= "TestState";
        }

        var stateType = GetString(stateDef, "Type");
        if (string.IsNullOrEmpty(stateType))
        {
            return ErrorResponse("InvalidDefinition", "State must have a Type");
        }

        // Build context
        Dictionary<string, object?> ctx;
        var userCtxStr = GetString(data, "context");
        if (!string.IsNullOrEmpty(userCtxStr))
        {
            try
            {
                ctx = ParseJsonToDict(Encoding.UTF8.GetBytes(userCtxStr));
            }
            catch (JsonException)
            {
                ctx = new Dictionary<string, object?>();
            }
        }
        else
        {
            ctx = new Dictionary<string, object?>();
        }

        if (!ctx.ContainsKey("Execution"))
        {
            ctx["Execution"] = new Dictionary<string, object?>
            {
                ["Id"] = $"arn:aws:states:{Region}:{AccountContext.GetAccountId()}:execution:test:{HashHelpers.NewUuid()}",
                ["Name"] = "test",
                ["StartTime"] = NowIso(),
            };
        }

        if (!ctx.ContainsKey("StateMachine"))
        {
            ctx["StateMachine"] = new Dictionary<string, object?> { ["Id"] = "test", ["Name"] = "test" };
        }

        ctx["State"] = new Dictionary<string, object?> { ["Name"] = stateName, ["EnteredTime"] = NowIso() };

        var inspectionData = new Dictionary<string, object?>();
        if (inspectionLevel is "DEBUG" or "TRACE")
        {
            inspectionData["input"] = JsonSerializer.Serialize(inputData);
        }

        var result = new Dictionary<string, object?>();
        try
        {
            switch (stateType)
            {
                case "Pass":
                {
                    var (output, next) = ExecutePass(stateDef, inputData);
                    result["status"] = "SUCCEEDED";
                    result["output"] = SerializeOutput(output);
                    if (next is not null)
                    {
                        result["nextState"] = next;
                    }

                    break;
                }

                case "Choice":
                {
                    var (output, next) = ExecuteChoice(stateDef, inputData);
                    result["status"] = "SUCCEEDED";
                    result["output"] = SerializeOutput(output);
                    if (next is not null)
                    {
                        result["nextState"] = next;
                    }

                    break;
                }

                case "Wait":
                {
                    var dummyExec = new Dictionary<string, object?> { ["status"] = "RUNNING" };
                    var (output, next) = ExecuteWait(stateDef, inputData, dummyExec);
                    result["status"] = "SUCCEEDED";
                    result["output"] = SerializeOutput(output);
                    if (next is not null)
                    {
                        result["nextState"] = next;
                    }

                    break;
                }

                case "Succeed":
                {
                    var output = ApplyOutputPath(stateDef, ApplyInputPath(stateDef, inputData));
                    result["status"] = "SUCCEEDED";
                    result["output"] = SerializeOutput(output);
                    break;
                }

                case "Fail":
                {
                    result["status"] = "FAILED";
                    result["error"] = GetString(stateDef, "Error") ?? "States.Fail";
                    result["cause"] = GetString(stateDef, "Cause") ?? "";
                    break;
                }

                case "Task":
                {
                    var effective = ApplyInputPath(stateDef, inputData);
                    effective = ApplyParameters(stateDef, effective, ctx);

                    if (inspectionLevel is "DEBUG" or "TRACE")
                    {
                        inspectionData["afterInputPath"] = JsonSerializer.Serialize(ApplyInputPath(stateDef, inputData));
                        inspectionData["afterParameters"] = JsonSerializer.Serialize(effective);
                    }

                    // Check mock
                    var mockRaw = data.TryGetValue("mock", out var m) ? m : null;
                    Dictionary<string, object?>? mock = null;
                    if (mockRaw is Dictionary<string, object?> md)
                    {
                        mock = md;
                    }
                    else if (mockRaw is string mockStr)
                    {
                        try
                        {
                            mock = ParseJsonToDict(Encoding.UTF8.GetBytes(mockStr));
                        }
                        catch (JsonException)
                        {
                            // Ignore
                        }
                    }

                    if (mock is not null)
                    {
                        if (mock.ContainsKey("errorOutput"))
                        {
                            var errOut = GetDict(mock, "errorOutput");
                            var errorCode = GetString(errOut, "error") ?? "MockError";
                            var cause = GetString(errOut, "cause") ?? "Mocked failure";
                            var catchers = GetList(stateDef, "Catch");
                            var catcher = FindMatchingCatcher(catchers, errorCode);
                            if (catcher is not null)
                            {
                                var errorOutput = new Dictionary<string, object?> { ["Error"] = errorCode, ["Cause"] = cause };
                                var catchResultPath = GetString(catcher, "ResultPath") ?? "$";
                                var output = ApplyResultPathRaw(catchResultPath, inputData, errorOutput);
                                result["status"] = "CAUGHT_ERROR";
                                result["output"] = SerializeOutput(output);
                                result["error"] = errorCode;
                                result["cause"] = cause;
                                result["nextState"] = GetString(catcher, "Next");
                            }
                            else
                            {
                                result["status"] = "FAILED";
                                result["error"] = errorCode;
                                result["cause"] = cause;
                            }
                        }
                        else if (mock.ContainsKey("result"))
                        {
                            var mockResultRaw = mock["result"];
                            Dictionary<string, object?> mockResult2;
                            if (mockResultRaw is Dictionary<string, object?> mrd)
                            {
                                mockResult2 = mrd;
                            }
                            else if (mockResultRaw is string mrs)
                            {
                                try
                                {
                                    mockResult2 = ParseJsonToDict(Encoding.UTF8.GetBytes(mrs));
                                }
                                catch (JsonException)
                                {
                                    mockResult2 = new Dictionary<string, object?> { ["value"] = mrs };
                                }
                            }
                            else
                            {
                                mockResult2 = new Dictionary<string, object?>();
                            }

                            var taskResult = ApplyResultSelector(stateDef, mockResult2);
                            var output = ApplyResultPath(stateDef, inputData, taskResult);
                            output = ApplyOutputPath(stateDef, output);
                            result["status"] = "SUCCEEDED";
                            result["output"] = SerializeOutput(output);
                            var next = NextOrEnd(stateDef);
                            if (next is not null)
                            {
                                result["nextState"] = next;
                            }
                        }
                    }
                    else
                    {
                        // Real execution
                        var resource = GetString(stateDef, "Resource") ?? "";
                        try
                        {
                            var taskResult2 = InvokeResource(resource, effective);
                            taskResult2 = ApplyResultSelector(stateDef, taskResult2);
                            var output = ApplyResultPath(stateDef, inputData, taskResult2);
                            output = ApplyOutputPath(stateDef, output);
                            result["status"] = "SUCCEEDED";
                            result["output"] = SerializeOutput(output);
                            var next = NextOrEnd(stateDef);
                            if (next is not null)
                            {
                                result["nextState"] = next;
                            }
                        }
                        catch (ExecutionError err)
                        {
                            var catchers = GetList(stateDef, "Catch");
                            var catcher = FindMatchingCatcher(catchers, err.ErrorCode);
                            if (catcher is not null)
                            {
                                var errorOutput = new Dictionary<string, object?> { ["Error"] = err.ErrorCode, ["Cause"] = err.Cause };
                                var catchResultPath = GetString(catcher, "ResultPath") ?? "$";
                                var output = ApplyResultPathRaw(catchResultPath, inputData, errorOutput);
                                result["status"] = "CAUGHT_ERROR";
                                result["output"] = SerializeOutput(output);
                                result["error"] = err.ErrorCode;
                                result["cause"] = err.Cause;
                                result["nextState"] = GetString(catcher, "Next");
                            }
                            else
                            {
                                result["status"] = "FAILED";
                                result["error"] = err.ErrorCode;
                                result["cause"] = err.Cause;
                            }
                        }
                    }

                    break;
                }

                default:
                    return ErrorResponse("InvalidDefinition", $"Unsupported state type: {stateType}");
            }
        }
        catch (ExecutionError err2)
        {
            result["status"] = "FAILED";
            result["error"] = err2.ErrorCode;
            result["cause"] = err2.Cause;
        }

        if (inspectionLevel is "DEBUG" or "TRACE" && inspectionData.Count > 0)
        {
            result["inspectionData"] = inspectionData;
        }

        return JsonResp(result);
    }
    // -- Retry / Catch helpers --------------------------------------------------

    private static (Dictionary<string, object?>? Retrier, int Index) FindMatchingRetrier(
        List<object?> retriers, string error, Dictionary<int, int> retryCounts)
    {
        for (var idx = 0; idx < retriers.Count; idx++)
        {
            if (retriers[idx] is not Dictionary<string, object?> retrier)
            {
                continue;
            }

            var equals = GetList(retrier, "ErrorEquals");
            var maxAttempts = GetInt(retrier, "MaxAttempts", 3);
            if (retryCounts.GetValueOrDefault(idx, 0) >= maxAttempts)
            {
                continue;
            }

            if (equals.Any(e => e is string s && (s == "States.ALL" || s == "States.TaskFailed" || s == error)))
            {
                return (retrier, idx);
            }
        }

        return (null, -1);
    }

    private static Dictionary<string, object?>? FindMatchingCatcher(List<object?> catchers, string error)
    {
        foreach (var catcher in catchers)
        {
            if (catcher is not Dictionary<string, object?> cd)
            {
                continue;
            }

            var equals = GetList(cd, "ErrorEquals");
            if (equals.Any(e => e is string s && (s == "States.ALL" || s == "States.TaskFailed" || s == error)))
            {
                return cd;
            }
        }

        return null;
    }

    // -- Misc helpers -----------------------------------------------------------

    private static bool IsNum(object? v)
    {
        return v is int or long or float or double or decimal && v is not bool;
    }

    private static double ToDouble(object? v)
    {
        if (v is IConvertible c)
        {
            return Convert.ToDouble(c);
        }

        return 0;
    }

    private static bool IsTimestamp(object? v)
    {
        if (v is not string s)
        {
            return false;
        }

        return DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out _);
    }

    private static DateTimeOffset? ParseTs(object? v)
    {
        if (v is string s && DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dto))
        {
            return dto;
        }

        return null;
    }

    private static Dictionary<string, object?> DeepCopyDict(Dictionary<string, object?> source)
    {
        var copy = new Dictionary<string, object?>(source.Count, StringComparer.Ordinal);
        foreach (var kv in source)
        {
            copy[kv.Key] = DeepCopyValue(kv.Value);
        }

        return copy;
    }

    private static object? DeepCopyValue(object? value)
    {
        return value switch
        {
            Dictionary<string, object?> d => DeepCopyDict(d),
            List<object?> l => new List<object?>(l.Select(DeepCopyValue)),
            _ => value,
        };
    }
}

// -- Data models ---------------------------------------------------------------

internal sealed class TaskTokenInfo
{
    internal ManualResetEventSlim Event { get; set; } = new(false);
    internal string? Result { get; set; }
    internal Dictionary<string, object?>? Error { get; set; }
    internal string? Heartbeat { get; set; }
}

internal sealed class ExecutionError : Exception
{
    internal string ErrorCode { get; }
    internal string Cause { get; }

    internal ExecutionError(string errorCode, string cause)
        : base($"{errorCode}: {cause}")
    {
        ErrorCode = errorCode;
        Cause = cause;
    }
}