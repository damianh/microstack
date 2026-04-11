using System.Text;
using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.CloudWatchLogs;

/// <summary>
/// CloudWatch Logs service handler -- supports JSON protocol via X-Amz-Target (Logs_20140328).
///
/// Port of ministack/services/cloudwatch_logs.py.
///
/// Supports: CreateLogGroup, DeleteLogGroup, DescribeLogGroups,
///           CreateLogStream, DeleteLogStream, DescribeLogStreams,
///           PutLogEvents, GetLogEvents, FilterLogEvents,
///           PutRetentionPolicy, DeleteRetentionPolicy,
///           TagResource, UntagResource, ListTagsForResource,
///           TagLogGroup, UntagLogGroup, ListTagsLogGroup,
///           PutSubscriptionFilter, DescribeSubscriptionFilters, DeleteSubscriptionFilter,
///           PutMetricFilter, DescribeMetricFilters, DeleteMetricFilter,
///           PutDestination, DescribeDestinations, PutDestinationPolicy, DeleteDestination,
///           StartQuery, GetQueryResults, StopQuery.
/// </summary>
internal sealed class CloudWatchLogsServiceHandler : IServiceHandler
{
    private readonly AccountScopedDictionary<string, LogGroup> _logGroups = new(); // keyed by group name
    private readonly AccountScopedDictionary<string, Destination> _destinations = new(); // keyed by dest name
    private readonly AccountScopedDictionary<string, MetricFilter> _metricFilters = new(); // keyed by "groupName\0filterName"
    private readonly AccountScopedDictionary<string, InsightsQuery> _queries = new(); // keyed by query ID
    private readonly Lock _lock = new();

    private static readonly string Region =
        Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1";

    private static readonly HashSet<int> ValidRetentionDays =
    [
        1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180,
        365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653,
    ];

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "logs";

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
            "CreateLogGroup" => ActCreateLogGroup(data),
            "DeleteLogGroup" => ActDeleteLogGroup(data),
            "DescribeLogGroups" => ActDescribeLogGroups(data),
            "CreateLogStream" => ActCreateLogStream(data),
            "DeleteLogStream" => ActDeleteLogStream(data),
            "DescribeLogStreams" => ActDescribeLogStreams(data),
            "PutLogEvents" => ActPutLogEvents(data),
            "GetLogEvents" => ActGetLogEvents(data),
            "FilterLogEvents" => ActFilterLogEvents(data),
            "PutRetentionPolicy" => ActPutRetentionPolicy(data),
            "DeleteRetentionPolicy" => ActDeleteRetentionPolicy(data),
            "PutSubscriptionFilter" => ActPutSubscriptionFilter(data),
            "DeleteSubscriptionFilter" => ActDeleteSubscriptionFilter(data),
            "DescribeSubscriptionFilters" => ActDescribeSubscriptionFilters(data),
            "TagLogGroup" => ActTagLogGroup(data),
            "UntagLogGroup" => ActUntagLogGroup(data),
            "ListTagsLogGroup" => ActListTagsLogGroup(data),
            "TagResource" => ActTagResource(data),
            "UntagResource" => ActUntagResource(data),
            "ListTagsForResource" => ActListTagsForResource(data),
            "PutDestination" => ActPutDestination(data),
            "DeleteDestination" => ActDeleteDestination(data),
            "DescribeDestinations" => ActDescribeDestinations(data),
            "PutDestinationPolicy" => ActPutDestinationPolicy(data),
            "PutMetricFilter" => ActPutMetricFilter(data),
            "DeleteMetricFilter" => ActDeleteMetricFilter(data),
            "DescribeMetricFilters" => ActDescribeMetricFilters(data),
            "StartQuery" => ActStartQuery(data),
            "GetQueryResults" => ActGetQueryResults(data),
            "StopQuery" => ActStopQuery(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidOperationException", $"Unknown action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _logGroups.Clear();
            _destinations.Clear();
            _metricFilters.Clear();
            _queries.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // -- Helpers ---------------------------------------------------------------

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

    private static long GetLong(JsonElement el, string propertyName, long defaultValue)
    {
        if (!el.TryGetProperty(propertyName, out var prop))
        {
            return defaultValue;
        }

        return prop.TryGetInt64(out var val) ? val : defaultValue;
    }

    private static long? GetNullableLong(JsonElement el, string propertyName)
    {
        if (!el.TryGetProperty(propertyName, out var prop))
        {
            return null;
        }

        return prop.TryGetInt64(out var val) ? val : null;
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

    private static string MakeGroupArn(string name)
    {
        return $"arn:aws:logs:{Region}:{AccountContext.GetAccountId()}:log-group:{name}:*";
    }

    private string? ResolveGroupByArn(string arn)
    {
        var arnNormalized = arn.TrimEnd('*').TrimEnd(':');
        foreach (var (name, g) in _logGroups.Items)
        {
            var storedNormalized = g.Arn.TrimEnd('*').TrimEnd(':');
            if (string.Equals(storedNormalized, arnNormalized, StringComparison.Ordinal))
            {
                return name;
            }
        }

        return null;
    }

    private static int DecodeToken(string? token)
    {
        if (string.IsNullOrEmpty(token))
        {
            return 0;
        }

        try
        {
            return int.Parse(Encoding.UTF8.GetString(Convert.FromBase64String(token)));
        }
        catch
        {
            return 0;
        }
    }

    private static string EncodeToken(int offset)
    {
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(offset.ToString()));
    }

    private static string MetricFilterKey(string groupName, string filterName)
    {
        return $"{groupName}\0{filterName}";
    }

    private static Dictionary<string, string> ParseTagsDict(JsonElement data)
    {
        var tags = new Dictionary<string, string>(StringComparer.Ordinal);
        if (data.TryGetProperty("tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in tagsEl.EnumerateObject())
            {
                tags[prop.Name] = prop.Value.GetString() ?? "";
            }
        }

        return tags;
    }

    // -- Filter pattern compilation -------------------------------------------

    private static Func<string, bool> CompileFilterPattern(string? raw)
    {
        if (string.IsNullOrEmpty(raw))
        {
            return static _ => true;
        }

        raw = raw.Trim();

        // JSON-style patterns (starts with {) — treat as match-all for emulation
        if (raw.StartsWith('{'))
        {
            return static _ => true;
        }

        var terms = raw.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var include = new List<string>();
        var exclude = new List<string>();

        foreach (var t in terms)
        {
            if (t.StartsWith('-'))
            {
                exclude.Add(t[1..].Trim('"').ToLowerInvariant());
            }
            else
            {
                include.Add(t.TrimStart('+').Trim('"').ToLowerInvariant());
            }
        }

        return msg =>
        {
            var m = msg.ToLowerInvariant();
            foreach (var p in include)
            {
                if (!m.Contains(p, StringComparison.Ordinal))
                {
                    return false;
                }
            }

            foreach (var p in exclude)
            {
                if (m.Contains(p, StringComparison.Ordinal))
                {
                    return false;
                }
            }

            return true;
        };
    }

    // -- Log Groups ------------------------------------------------------------

    private ServiceResponse ActCreateLogGroup(JsonElement data)
    {
        var name = GetString(data, "logGroupName");
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException", "logGroupName is required.", 400);
        }

        lock (_lock)
        {
            if (_logGroups.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceAlreadyExistsException",
                    $"The specified log group already exists: {name}", 400);
            }

            _logGroups[name] = new LogGroup
            {
                Arn = MakeGroupArn(name),
                CreationTime = TimeHelpers.NowEpochMs(),
                Tags = ParseTagsDict(data),
            };
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDeleteLogGroup(JsonElement data)
    {
        var name = GetString(data, "logGroupName");

        lock (_lock)
        {
            if (name is null || !_logGroups.TryRemove(name, out _))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {name}", 400);
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDescribeLogGroups(JsonElement data)
    {
        var prefix = GetString(data, "logGroupNamePrefix");
        var pattern = GetString(data, "logGroupNamePattern");
        var limit = Math.Min(GetInt(data, "limit", 50), 50);
        var token = GetString(data, "nextToken");

        if (prefix is not null && pattern is not null)
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException",
                "logGroupNamePrefix and logGroupNamePattern are mutually exclusive.", 400);
        }

        lock (_lock)
        {
            var names = _logGroups.Keys.OrderBy(n => n, StringComparer.Ordinal).ToList();

            if (prefix is not null)
            {
                names = names.FindAll(n => n.StartsWith(prefix, StringComparison.Ordinal));
            }
            else if (pattern is not null)
            {
                var pat = pattern.ToLowerInvariant();
                names = names.FindAll(n => n.ToLowerInvariant().Contains(pat, StringComparison.Ordinal));
            }

            var start = DecodeToken(token);
            var page = names.GetRange(start, Math.Min(limit, names.Count - start));

            var groups = new List<Dictionary<string, object?>>();
            foreach (var n in page)
            {
                if (!_logGroups.TryGetValue(n, out var g))
                {
                    continue;
                }

                var storedBytes = 0L;
                foreach (var s in g.Streams.Values)
                {
                    foreach (var e in s.Events)
                    {
                        storedBytes += e.Message.Length;
                    }
                }

                var metricFilterCount = _metricFilters.Items
                    .Count(mf => string.Equals(mf.Value.LogGroupName, n, StringComparison.Ordinal));

                var entry = new Dictionary<string, object?>
                {
                    ["logGroupName"] = n,
                    ["arn"] = g.Arn,
                    ["creationTime"] = g.CreationTime,
                    ["storedBytes"] = storedBytes,
                    ["metricFilterCount"] = metricFilterCount,
                };

                if (g.RetentionInDays.HasValue)
                {
                    entry["retentionInDays"] = g.RetentionInDays.Value;
                }

                groups.Add(entry);
            }

            var resp = new Dictionary<string, object?>
            {
                ["logGroups"] = groups,
            };

            var end = start + limit;
            if (end < names.Count)
            {
                resp["nextToken"] = EncodeToken(end);
            }

            return AwsResponseHelpers.JsonResponse(resp);
        }
    }

    // -- Log Streams -----------------------------------------------------------

    private ServiceResponse ActCreateLogStream(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var stream = GetString(data, "logStreamName");

        if (string.IsNullOrEmpty(group) || string.IsNullOrEmpty(stream))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException", "logGroupName and logStreamName are required.", 400);
        }

        lock (_lock)
        {
            if (!_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            if (g.Streams.ContainsKey(stream))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceAlreadyExistsException",
                    $"The specified log stream already exists: {stream}", 400);
            }

            g.Streams[stream] = new LogStream
            {
                CreationTime = TimeHelpers.NowEpochMs(),
            };
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDeleteLogStream(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var stream = GetString(data, "logStreamName");

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            if (stream is null || !g.Streams.Remove(stream))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log stream does not exist: {stream}", 400);
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDescribeLogStreams(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var prefix = GetString(data, "logStreamNamePrefix") ?? "";
        var descending = GetBool(data, "descending", false);
        var limit = Math.Min(GetInt(data, "limit", 50), 50);
        var token = GetString(data, "nextToken");
        var order = GetString(data, "orderBy") ?? "LogStreamName";

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            var names = g.Streams.Keys.OrderBy(n => n, StringComparer.Ordinal).ToList();

            if (!string.IsNullOrEmpty(prefix))
            {
                names = names.FindAll(n => n.StartsWith(prefix, StringComparison.Ordinal));
            }

            if (string.Equals(order, "LastEventTime", StringComparison.Ordinal))
            {
                names.Sort((a, b) =>
                {
                    var tsA = g.Streams[a].LastEventTimestamp ?? 0;
                    var tsB = g.Streams[b].LastEventTimestamp ?? 0;
                    return descending ? tsB.CompareTo(tsA) : tsA.CompareTo(tsB);
                });
            }
            else if (descending)
            {
                names.Reverse();
            }

            var start = DecodeToken(token);
            var page = names.GetRange(start, Math.Min(limit, names.Count - start));

            var streams = new List<Dictionary<string, object?>>();
            foreach (var n in page)
            {
                var s = g.Streams[n];
                var entry = new Dictionary<string, object?>
                {
                    ["logStreamName"] = n,
                    ["creationTime"] = s.CreationTime,
                    ["storedBytes"] = s.Events.Sum(e => (long)e.Message.Length),
                    ["uploadSequenceToken"] = s.UploadSequenceToken,
                    ["arn"] = $"arn:aws:logs:{Region}:{AccountContext.GetAccountId()}:log-group:{group}:log-stream:{n}",
                };

                if (s.FirstEventTimestamp.HasValue)
                {
                    entry["firstEventTimestamp"] = s.FirstEventTimestamp.Value;
                }

                if (s.LastEventTimestamp.HasValue)
                {
                    entry["lastEventTimestamp"] = s.LastEventTimestamp.Value;
                }

                if (s.LastIngestionTime.HasValue)
                {
                    entry["lastIngestionTime"] = s.LastIngestionTime.Value;
                }

                streams.Add(entry);
            }

            var resp = new Dictionary<string, object?>
            {
                ["logStreams"] = streams,
            };

            var end = start + limit;
            if (end < names.Count)
            {
                resp["nextToken"] = EncodeToken(end);
            }

            return AwsResponseHelpers.JsonResponse(resp);
        }
    }

    // -- Log Events ------------------------------------------------------------

    private ServiceResponse ActPutLogEvents(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var stream = GetString(data, "logStreamName");

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            if (stream is null || !g.Streams.TryGetValue(stream, out var s))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log stream does not exist: {stream}", 400);
            }

            var nowMs = TimeHelpers.NowEpochMs();

            if (data.TryGetProperty("logEvents", out var eventsEl) && eventsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var eEl in eventsEl.EnumerateArray())
                {
                    var ts = GetLong(eEl, "timestamp", nowMs);
                    var msg = GetString(eEl, "message") ?? "";

                    s.Events.Add(new LogEvent { Timestamp = ts, Message = msg, IngestionTime = nowMs });

                    if (!s.FirstEventTimestamp.HasValue || ts < s.FirstEventTimestamp.Value)
                    {
                        s.FirstEventTimestamp = ts;
                    }

                    if (!s.LastEventTimestamp.HasValue || ts > s.LastEventTimestamp.Value)
                    {
                        s.LastEventTimestamp = ts;
                    }

                    s.LastIngestionTime = nowMs;
                }
            }

            var tokenVal = int.Parse(s.UploadSequenceToken) + 1;
            s.UploadSequenceToken = tokenVal.ToString();

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["nextSequenceToken"] = s.UploadSequenceToken,
            });
        }
    }

    private ServiceResponse ActGetLogEvents(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var stream = GetString(data, "logStreamName");
        var limit = Math.Min(GetInt(data, "limit", 10000), 10000);
        var startFromHead = GetBool(data, "startFromHead", false);
        var startTime = GetNullableLong(data, "startTime");
        var endTime = GetNullableLong(data, "endTime");
        var nextToken = GetString(data, "nextToken");

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            if (stream is null || !g.Streams.TryGetValue(stream, out var s))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log stream does not exist: {stream}", 400);
            }

            var allEvents = s.Events;
            var filtered = new List<LogEvent>(allEvents);

            if (startTime.HasValue)
            {
                filtered = filtered.FindAll(e => e.Timestamp >= startTime.Value);
            }

            if (endTime.HasValue)
            {
                filtered = filtered.FindAll(e => e.Timestamp <= endTime.Value);
            }

            // Parse offset from token: f/<offset> for forward, b/<offset> for backward
            var offset = 0;
            if (nextToken is not null)
            {
                var parts = nextToken.Split('/', 2);
                if (parts.Length == 2 && int.TryParse(parts[1], out var parsedOffset))
                {
                    offset = parsedOffset;
                }
            }

            List<LogEvent> page;
            string newForward;
            string newBackward;

            if (startFromHead || (nextToken is not null && nextToken.StartsWith("f/", StringComparison.Ordinal)))
            {
                page = filtered.GetRange(offset, Math.Min(limit, filtered.Count - offset));
                newForward = $"f/{offset + page.Count}";
                newBackward = $"b/{offset}";
            }
            else
            {
                var end = (nextToken is not null && nextToken.StartsWith("b/", StringComparison.Ordinal))
                    ? filtered.Count - offset
                    : filtered.Count;
                var start = Math.Max(0, end - limit);
                page = filtered.GetRange(start, end - start);
                newForward = $"f/{end}";
                newBackward = $"b/{filtered.Count - start}";
            }

            // AWS behaviour: when at end of stream, return the caller's token
            // so SDK clients stop paginating
            var forwardToken = (nextToken is not null && page.Count < limit)
                ? nextToken
                : newForward;
            var backwardToken = (nextToken is not null && offset == 0 && nextToken.StartsWith("b/", StringComparison.Ordinal))
                ? nextToken
                : newBackward;

            var eventDicts = page.ConvertAll(e => new Dictionary<string, object?>
            {
                ["timestamp"] = e.Timestamp,
                ["message"] = e.Message,
                ["ingestionTime"] = e.IngestionTime,
            });

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["events"] = eventDicts,
                ["nextForwardToken"] = forwardToken,
                ["nextBackwardToken"] = backwardToken,
            });
        }
    }

    private ServiceResponse ActFilterLogEvents(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var rawPattern = GetString(data, "filterPattern") ?? "";
        var patternFn = CompileFilterPattern(rawPattern);
        var limit = Math.Min(GetInt(data, "limit", 10000), 10000);
        var startTime = GetNullableLong(data, "startTime");
        var endTime = GetNullableLong(data, "endTime");

        var streamNames = new List<string>();
        if (data.TryGetProperty("logStreamNames", out var streamsEl) && streamsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in streamsEl.EnumerateArray())
            {
                var s = item.GetString();
                if (s is not null)
                {
                    streamNames.Add(s);
                }
            }
        }

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            var events = new List<Dictionary<string, object?>>();
            var searched = new List<Dictionary<string, object?>>();
            var targetStreams = streamNames.Count > 0 ? streamNames : g.Streams.Keys.ToList();

            foreach (var sn in targetStreams)
            {
                if (!g.Streams.TryGetValue(sn, out var s))
                {
                    continue;
                }

                searched.Add(new Dictionary<string, object?>
                {
                    ["logStreamName"] = sn,
                    ["searchedCompletely"] = true,
                });

                foreach (var e in s.Events)
                {
                    if (startTime.HasValue && e.Timestamp < startTime.Value)
                    {
                        continue;
                    }

                    if (endTime.HasValue && e.Timestamp > endTime.Value)
                    {
                        continue;
                    }

                    if (!patternFn(e.Message))
                    {
                        continue;
                    }

                    events.Add(new Dictionary<string, object?>
                    {
                        ["timestamp"] = e.Timestamp,
                        ["message"] = e.Message,
                        ["ingestionTime"] = e.IngestionTime,
                        ["logStreamName"] = sn,
                    });

                    if (events.Count >= limit)
                    {
                        break;
                    }
                }
            }

            events.Sort((a, b) => ((long)a["timestamp"]!).CompareTo((long)b["timestamp"]!));

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["events"] = events.Count > limit ? events.GetRange(0, limit) : events,
                ["searchedLogStreams"] = searched,
            });
        }
    }

    // -- Retention Policy ------------------------------------------------------

    private ServiceResponse ActPutRetentionPolicy(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var days = GetInt(data, "retentionInDays", -1);

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            if (!ValidRetentionDays.Contains(days))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "InvalidParameterException",
                    $"Invalid retentionInDays value: {days}.", 400);
            }

            g.RetentionInDays = days;
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDeleteRetentionPolicy(JsonElement data)
    {
        var group = GetString(data, "logGroupName");

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            g.RetentionInDays = null;
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    // -- Subscription Filters --------------------------------------------------

    private ServiceResponse ActPutSubscriptionFilter(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var filterName = GetString(data, "filterName");

        if (string.IsNullOrEmpty(group) || string.IsNullOrEmpty(filterName))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException", "logGroupName and filterName are required.", 400);
        }

        lock (_lock)
        {
            if (!_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            g.SubscriptionFilters[filterName] = new SubscriptionFilter
            {
                FilterName = filterName,
                LogGroupName = group,
                FilterPattern = GetString(data, "filterPattern") ?? "",
                DestinationArn = GetString(data, "destinationArn") ?? "",
                RoleArn = GetString(data, "roleArn") ?? "",
                Distribution = GetString(data, "distribution") ?? "ByLogStream",
                CreationTime = TimeHelpers.NowEpochMs(),
            };
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDeleteSubscriptionFilter(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var filterName = GetString(data, "filterName");

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            if (filterName is null || !g.SubscriptionFilters.Remove(filterName))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified subscription filter does not exist: {filterName}", 400);
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDescribeSubscriptionFilters(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var prefix = GetString(data, "filterNamePrefix") ?? "";
        var limit = Math.Min(GetInt(data, "limit", 50), 50);
        var token = GetString(data, "nextToken");

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            var allFilters = g.SubscriptionFilters.Values
                .OrderBy(f => f.FilterName, StringComparer.Ordinal)
                .ToList();

            if (!string.IsNullOrEmpty(prefix))
            {
                allFilters = allFilters.FindAll(f =>
                    f.FilterName.StartsWith(prefix, StringComparison.Ordinal));
            }

            var start = DecodeToken(token);
            var page = allFilters.GetRange(start, Math.Min(limit, allFilters.Count - start));

            var filterDicts = page.ConvertAll(f => new Dictionary<string, object?>
            {
                ["filterName"] = f.FilterName,
                ["logGroupName"] = f.LogGroupName,
                ["filterPattern"] = f.FilterPattern,
                ["destinationArn"] = f.DestinationArn,
                ["roleArn"] = f.RoleArn,
                ["distribution"] = f.Distribution,
                ["creationTime"] = f.CreationTime,
            });

            var resp = new Dictionary<string, object?>
            {
                ["subscriptionFilters"] = filterDicts,
            };

            var end = start + limit;
            if (end < allFilters.Count)
            {
                resp["nextToken"] = EncodeToken(end);
            }

            return AwsResponseHelpers.JsonResponse(resp);
        }
    }

    // -- Tags (Legacy log-group-name APIs) -------------------------------------

    private ServiceResponse ActTagLogGroup(JsonElement data)
    {
        var group = GetString(data, "logGroupName");

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            if (data.TryGetProperty("tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in tagsEl.EnumerateObject())
                {
                    g.Tags[prop.Name] = prop.Value.GetString() ?? "";
                }
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActUntagLogGroup(JsonElement data)
    {
        var group = GetString(data, "logGroupName");

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            if (data.TryGetProperty("tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in tagsEl.EnumerateArray())
                {
                    var key = item.GetString();
                    if (key is not null)
                    {
                        g.Tags.Remove(key);
                    }
                }
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActListTagsLogGroup(JsonElement data)
    {
        var group = GetString(data, "logGroupName");

        lock (_lock)
        {
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["tags"] = new Dictionary<string, string>(g.Tags),
            });
        }
    }

    // -- Tags (Modern ARN-based APIs) -----------------------------------------

    private ServiceResponse ActTagResource(JsonElement data)
    {
        var arn = GetString(data, "resourceArn") ?? "";

        lock (_lock)
        {
            var group = ResolveGroupByArn(arn);
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified resource does not exist: {arn}", 400);
            }

            if (data.TryGetProperty("tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in tagsEl.EnumerateObject())
                {
                    g.Tags[prop.Name] = prop.Value.GetString() ?? "";
                }
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActUntagResource(JsonElement data)
    {
        var arn = GetString(data, "resourceArn") ?? "";

        lock (_lock)
        {
            var group = ResolveGroupByArn(arn);
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified resource does not exist: {arn}", 400);
            }

            if (data.TryGetProperty("tagKeys", out var keysEl) && keysEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in keysEl.EnumerateArray())
                {
                    var key = item.GetString();
                    if (key is not null)
                    {
                        g.Tags.Remove(key);
                    }
                }
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActListTagsForResource(JsonElement data)
    {
        var arn = GetString(data, "resourceArn") ?? "";

        lock (_lock)
        {
            var group = ResolveGroupByArn(arn);
            if (group is null || !_logGroups.TryGetValue(group, out var g))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified resource does not exist: {arn}", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["tags"] = new Dictionary<string, string>(g.Tags),
            });
        }
    }

    // -- Destinations ----------------------------------------------------------

    private ServiceResponse ActPutDestination(JsonElement data)
    {
        var name = GetString(data, "destinationName");
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException", "destinationName is required.", 400);
        }

        var destArn = $"arn:aws:logs:{Region}:{AccountContext.GetAccountId()}:destination:{name}";

        lock (_lock)
        {
            _destinations[name] = new Destination
            {
                DestinationName = name,
                TargetArn = GetString(data, "targetArn") ?? "",
                RoleArn = GetString(data, "roleArn") ?? "",
                AccessPolicy = GetString(data, "accessPolicy") ?? "",
                Arn = destArn,
                CreationTime = TimeHelpers.NowEpochMs(),
            };

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["destination"] = DestinationToDict(_destinations[name]),
            });
        }
    }

    private ServiceResponse ActDeleteDestination(JsonElement data)
    {
        var name = GetString(data, "destinationName");

        lock (_lock)
        {
            if (name is null || !_destinations.TryRemove(name, out _))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified destination does not exist: {name}", 400);
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDescribeDestinations(JsonElement data)
    {
        var prefix = GetString(data, "DestinationNamePrefix") ?? "";
        var limit = Math.Min(GetInt(data, "limit", 50), 50);
        var token = GetString(data, "nextToken");

        lock (_lock)
        {
            var allDests = _destinations.Keys.OrderBy(n => n, StringComparer.Ordinal).ToList();

            if (!string.IsNullOrEmpty(prefix))
            {
                allDests = allDests.FindAll(n => n.StartsWith(prefix, StringComparison.Ordinal));
            }

            var start = DecodeToken(token);
            var page = allDests.GetRange(start, Math.Min(limit, allDests.Count - start));

            var dests = new List<Dictionary<string, object?>>();
            foreach (var n in page)
            {
                if (_destinations.TryGetValue(n, out var d))
                {
                    dests.Add(DestinationToDict(d));
                }
            }

            var resp = new Dictionary<string, object?>
            {
                ["destinations"] = dests,
            };

            var end = start + limit;
            if (end < allDests.Count)
            {
                resp["nextToken"] = EncodeToken(end);
            }

            return AwsResponseHelpers.JsonResponse(resp);
        }
    }

    private ServiceResponse ActPutDestinationPolicy(JsonElement data)
    {
        var name = GetString(data, "destinationName") ?? GetString(data, "DestinationName");
        var policy = GetString(data, "accessPolicy") ?? GetString(data, "AccessPolicy") ?? "";

        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException", "destinationName is required.", 400);
        }

        lock (_lock)
        {
            if (!_destinations.TryGetValue(name, out var d))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified destination does not exist: {name}", 400);
            }

            d.AccessPolicy = policy;
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private static Dictionary<string, object?> DestinationToDict(Destination d)
    {
        return new Dictionary<string, object?>
        {
            ["destinationName"] = d.DestinationName,
            ["targetArn"] = d.TargetArn,
            ["roleArn"] = d.RoleArn,
            ["accessPolicy"] = d.AccessPolicy,
            ["arn"] = d.Arn,
            ["creationTime"] = d.CreationTime,
        };
    }

    // -- Metric Filters --------------------------------------------------------

    private ServiceResponse ActPutMetricFilter(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var filterName = GetString(data, "filterName");

        if (string.IsNullOrEmpty(group) || string.IsNullOrEmpty(filterName))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException", "logGroupName and filterName are required.", 400);
        }

        lock (_lock)
        {
            if (!_logGroups.ContainsKey(group))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            var transformations = new List<Dictionary<string, object?>>();
            if (data.TryGetProperty("metricTransformations", out var mtEl) && mtEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in mtEl.EnumerateArray())
                {
                    var t = new Dictionary<string, object?>();
                    foreach (var prop in item.EnumerateObject())
                    {
                        t[prop.Name] = prop.Value.ValueKind == JsonValueKind.String
                            ? prop.Value.GetString()
                            : (object?)prop.Value.ToString();
                    }

                    transformations.Add(t);
                }
            }

            var key = MetricFilterKey(group, filterName);
            _metricFilters[key] = new MetricFilter
            {
                FilterName = filterName,
                LogGroupName = group,
                FilterPattern = GetString(data, "filterPattern") ?? "",
                MetricTransformations = transformations,
                CreationTime = TimeHelpers.NowEpochMs(),
            };
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDeleteMetricFilter(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var filterName = GetString(data, "filterName");

        if (group is null || filterName is null)
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "ResourceNotFoundException",
                $"The specified metric filter does not exist: {filterName}", 400);
        }

        var key = MetricFilterKey(group, filterName);

        lock (_lock)
        {
            if (!_metricFilters.TryRemove(key, out _))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified metric filter does not exist: {filterName}", 400);
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
    }

    private ServiceResponse ActDescribeMetricFilters(JsonElement data)
    {
        var group = GetString(data, "logGroupName");
        var prefix = GetString(data, "filterNamePrefix") ?? "";
        var limit = Math.Min(GetInt(data, "limit", 50), 50);
        var token = GetString(data, "nextToken");

        lock (_lock)
        {
            if (group is not null && !_logGroups.ContainsKey(group))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified log group does not exist: {group}", 400);
            }

            var filters = _metricFilters.Values
                .Where(mf => group is null || string.Equals(mf.LogGroupName, group, StringComparison.Ordinal))
                .Where(mf => string.IsNullOrEmpty(prefix) || mf.FilterName.StartsWith(prefix, StringComparison.Ordinal))
                .OrderBy(mf => mf.FilterName, StringComparer.Ordinal)
                .ToList();

            var start = DecodeToken(token);
            var page = filters.GetRange(start, Math.Min(limit, filters.Count - start));

            var filterDicts = page.ConvertAll(f => new Dictionary<string, object?>
            {
                ["filterName"] = f.FilterName,
                ["logGroupName"] = f.LogGroupName,
                ["filterPattern"] = f.FilterPattern,
                ["metricTransformations"] = f.MetricTransformations,
                ["creationTime"] = f.CreationTime,
            });

            var resp = new Dictionary<string, object?>
            {
                ["metricFilters"] = filterDicts,
            };

            var end = start + limit;
            if (end < filters.Count)
            {
                resp["nextToken"] = EncodeToken(end);
            }

            return AwsResponseHelpers.JsonResponse(resp);
        }
    }

    // -- CloudWatch Logs Insights (stubs) --------------------------------------

    private ServiceResponse ActStartQuery(JsonElement data)
    {
        var queryId = HashHelpers.NewUuid();

        lock (_lock)
        {
            _queries[queryId] = new InsightsQuery
            {
                QueryId = queryId,
                Status = "Complete",
            };
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["queryId"] = queryId,
        });
    }

    private ServiceResponse ActGetQueryResults(JsonElement data)
    {
        var queryId = GetString(data, "queryId");

        lock (_lock)
        {
            if (queryId is null || !_queries.TryGetValue(queryId, out var query))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException",
                    $"The specified query does not exist: {queryId}", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["status"] = query.Status,
                ["results"] = Array.Empty<object>(),
                ["statistics"] = new Dictionary<string, object?>
                {
                    ["recordsMatched"] = 0.0,
                    ["recordsScanned"] = 0.0,
                    ["bytesScanned"] = 0.0,
                },
            });
        }
    }

    private ServiceResponse ActStopQuery(JsonElement data)
    {
        var queryId = GetString(data, "queryId");

        lock (_lock)
        {
            if (queryId is not null && _queries.TryGetValue(queryId, out var query))
            {
                query.Status = "Cancelled";
            }
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["success"] = true,
        });
    }

    // -- Internal models -------------------------------------------------------

    private sealed class LogGroup
    {
        internal string Arn { get; init; } = "";
        internal long CreationTime { get; init; }
        internal int? RetentionInDays { get; set; }
        internal Dictionary<string, string> Tags { get; init; } = new(StringComparer.Ordinal);
        internal Dictionary<string, SubscriptionFilter> SubscriptionFilters { get; } = new(StringComparer.Ordinal);
        internal Dictionary<string, LogStream> Streams { get; } = new(StringComparer.Ordinal);
    }

    private sealed class LogStream
    {
        internal List<LogEvent> Events { get; } = [];
        internal string UploadSequenceToken { get; set; } = "1";
        internal long CreationTime { get; init; }
        internal long? FirstEventTimestamp { get; set; }
        internal long? LastEventTimestamp { get; set; }
        internal long? LastIngestionTime { get; set; }
    }

    private sealed class LogEvent
    {
        internal long Timestamp { get; init; }
        internal string Message { get; init; } = "";
        internal long IngestionTime { get; init; }
    }

    private sealed class SubscriptionFilter
    {
        internal string FilterName { get; init; } = "";
        internal string LogGroupName { get; init; } = "";
        internal string FilterPattern { get; init; } = "";
        internal string DestinationArn { get; init; } = "";
        internal string RoleArn { get; init; } = "";
        internal string Distribution { get; init; } = "ByLogStream";
        internal long CreationTime { get; init; }
    }

    private sealed class Destination
    {
        internal string DestinationName { get; init; } = "";
        internal string TargetArn { get; init; } = "";
        internal string RoleArn { get; init; } = "";
        internal string AccessPolicy { get; set; } = "";
        internal string Arn { get; init; } = "";
        internal long CreationTime { get; init; }
    }

    private sealed class MetricFilter
    {
        internal string FilterName { get; init; } = "";
        internal string LogGroupName { get; init; } = "";
        internal string FilterPattern { get; init; } = "";
        internal List<Dictionary<string, object?>> MetricTransformations { get; init; } = [];
        internal long CreationTime { get; init; }
    }

    private sealed class InsightsQuery
    {
        internal string QueryId { get; init; } = "";
        internal string Status { get; set; } = "Complete";
    }
}
