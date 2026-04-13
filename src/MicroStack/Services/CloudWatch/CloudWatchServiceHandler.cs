using System.Formats.Cbor;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Web;
using MicroStack.Internal;

namespace MicroStack.Services.CloudWatch;

/// <summary>
/// CloudWatch Metrics service handler — supports Query/XML protocol (form-encoded body with Action=...),
/// smithy-rpc-v2-cbor protocol (application/x-amz-cbor-1.1), and JSON/X-Amz-Target protocol.
///
/// Port of ministack/services/cloudwatch.py.
///
/// Supports: PutMetricData, GetMetricStatistics, GetMetricData, ListMetrics,
///           PutMetricAlarm, PutCompositeAlarm, DescribeAlarms, DescribeAlarmsForMetric,
///           DescribeAlarmHistory, DeleteAlarms,
///           EnableAlarmActions, DisableAlarmActions, SetAlarmState,
///           TagResource, UntagResource, ListTagsForResource,
///           PutDashboard, GetDashboard, DeleteDashboards, ListDashboards.
/// </summary>
internal sealed partial class CloudWatchServiceHandler : IServiceHandler
{
    public string ServiceName => "monitoring";

    private static string Region =>
        MicroStackOptions.Instance.Region;

    private const string CwNs = "http://monitoring.amazonaws.com/doc/2010-08-01/";
    private const long TwoWeeksSeconds = 14 * 24 * 3600;

    // Metric data: key = (Namespace, MetricName, DimsKey) → list of data points
    private readonly AccountScopedDictionary<string, List<MetricDataPoint>> _metrics = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _alarms = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _compositeAlarms = new();
    private readonly List<Dictionary<string, string>> _alarmHistory = [];
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _resourceTags = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _dashboards = new();

    private readonly Lock _lock = new();

    // -- IServiceHandler -------------------------------------------------------

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var contentType = request.GetHeader("content-type") ?? "";
        var target = request.GetHeader("x-amz-target") ?? "";
        var smithyProtocol = request.GetHeader("smithy-protocol") ?? "";
        var isCbor = contentType.Contains("cbor", StringComparison.OrdinalIgnoreCase)
                     || smithyProtocol.Contains("cbor", StringComparison.OrdinalIgnoreCase);
        var isJson = !isCbor && (contentType.Contains("json", StringComparison.OrdinalIgnoreCase) || !string.IsNullOrEmpty(target));

        var formParams = new Dictionary<string, string[]>(StringComparer.Ordinal);
        JsonElement jsonData = default;
        var hasJsonData = false;

        if (request.Body.Length > 0)
        {
            if (isCbor || isJson)
            {
                try
                {
                    // For CBOR, the AWS SDK v4 sends content-type application/x-amz-cbor-1.1,
                    // but the request is actually CBOR-encoded. We decode it as JSON since System.Text.Json
                    // can't read CBOR natively. We'll try JSON first (many CBOR-tagged requests
                    // are actually JSON-compatible), then try CBOR decoding.
                    using var doc = JsonDocument.Parse(request.Body);
                    jsonData = doc.RootElement.Clone();
                    hasJsonData = true;
                }
                catch (JsonException)
                {
                    // If the body is truly CBOR (binary), try to decode it
                    jsonData = TryCborDecode(request.Body);
                    hasJsonData = jsonData.ValueKind != JsonValueKind.Undefined;
                }
            }
            else
            {
                formParams = ParseFormBody(request.Body);
            }
        }

        if (!hasJsonData && (isCbor || isJson))
        {
            using var doc = JsonDocument.Parse("{}");
            jsonData = doc.RootElement.Clone();
        }

        // Determine action
        var action = "";
        if (!string.IsNullOrEmpty(target) && target.Contains('.', StringComparison.Ordinal))
        {
            action = target[(target.LastIndexOf('.') + 1)..];
        }

        if (string.IsNullOrEmpty(action))
        {
            var pathMatch = OperationPathRegex().Match(request.Path);
            if (pathMatch.Success)
            {
                action = pathMatch.Groups[1].Value;
            }
        }

        if (string.IsNullOrEmpty(action))
        {
            action = FormP(formParams, "Action");
        }

        ServiceResponse response;
        lock (_lock)
        {
            EvictOldMetrics();

            response = action switch
            {
                "PutMetricData" => PutMetricData(formParams, jsonData, isCbor, isJson),
                "GetMetricStatistics" => GetMetricStatistics(formParams, jsonData, isCbor, isJson),
                "GetMetricData" => GetMetricData(formParams, jsonData, isCbor, isJson),
                "ListMetrics" => ListMetrics(formParams, jsonData, isCbor, isJson),
                "PutMetricAlarm" => PutMetricAlarm(formParams, jsonData, isCbor, isJson),
                "PutCompositeAlarm" => PutCompositeAlarm(formParams, jsonData, isCbor, isJson),
                "DescribeAlarms" => DescribeAlarms(formParams, jsonData, isCbor, isJson),
                "DescribeAlarmsForMetric" => DescribeAlarmsForMetric(formParams, jsonData, isCbor, isJson),
                "DescribeAlarmHistory" => DescribeAlarmHistory(formParams, jsonData, isCbor, isJson),
                "DeleteAlarms" => DeleteAlarms(formParams, jsonData, isCbor, isJson),
                "EnableAlarmActions" => EnableAlarmActions(formParams, jsonData, isCbor, isJson),
                "DisableAlarmActions" => DisableAlarmActions(formParams, jsonData, isCbor, isJson),
                "SetAlarmState" => SetAlarmState(formParams, jsonData, isCbor, isJson),
                "TagResource" => TagResource(formParams, jsonData, isCbor, isJson),
                "UntagResource" => UntagResource(formParams, jsonData, isCbor, isJson),
                "ListTagsForResource" => ListTagsForResource(formParams, jsonData, isCbor, isJson),
                "PutDashboard" => PutDashboard(formParams, jsonData, isCbor, isJson),
                "GetDashboard" => GetDashboard(formParams, jsonData, isCbor, isJson),
                "DeleteDashboards" => DeleteDashboards(formParams, jsonData, isCbor, isJson),
                "ListDashboards" => ListDashboards(formParams, jsonData, isCbor, isJson),
                _ => ErrorResponse("InvalidAction", $"Unknown action: {action}", 400, isCbor, isJson),
            };
        }

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _metrics.Clear();
            _alarms.Clear();
            _compositeAlarms.Clear();
            _alarmHistory.Clear();
            _resourceTags.Clear();
            _dashboards.Clear();
        }
    }

    public JsonElement? GetState() => null;
    public void RestoreState(JsonElement state) { }

    // ── PutMetricData ─────────────────────────────────────────────────────────

    private ServiceResponse PutMetricData(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        if (isCbor || isJson)
        {
            var ns = JsonStr(json, "Namespace");
            if (json.TryGetProperty("MetricData", out var metricDataArr) &&
                metricDataArr.ValueKind == JsonValueKind.Array)
            {
                foreach (var md in metricDataArr.EnumerateArray())
                {
                    var mn = JsonStr(md, "MetricName");
                    var dims = ParseJsonDimensions(md);
                    var dimsKey = DimsKey(dims);
                    var key = MetricKey(ns, mn, dimsKey);
                    var unit = JsonStr(md, "Unit", "None");
                    var ts = ParseTimestamp(md, "Timestamp") ?? NowEpoch();

                    if (md.TryGetProperty("Values", out var valuesEl) && valuesEl.ValueKind == JsonValueKind.Array)
                    {
                        var values = new List<double>();
                        foreach (var v in valuesEl.EnumerateArray())
                            values.Add(v.GetDouble());

                        var counts = new List<double>();
                        if (md.TryGetProperty("Counts", out var countsEl) && countsEl.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var c in countsEl.EnumerateArray())
                                counts.Add(c.GetDouble());
                        }
                        else
                        {
                            counts.AddRange(Enumerable.Repeat(1.0, values.Count));
                        }

                        for (var i = 0; i < values.Count; i++)
                        {
                            var count = i < counts.Count ? (int)counts[i] : 1;
                            for (var j = 0; j < count; j++)
                            {
                                EnsureMetricList(key).Add(new MetricDataPoint
                                {
                                    Timestamp = ts,
                                    Value = values[i],
                                    Unit = unit,
                                    Dimensions = dims,
                                });
                            }
                        }
                    }
                    else if (md.TryGetProperty("StatisticValues", out var svEl) &&
                             svEl.ValueKind == JsonValueKind.Object)
                    {
                        var svSum = JsonDouble(svEl, "Sum");
                        var svCount = JsonDouble(svEl, "SampleCount", 1);
                        EnsureMetricList(key).Add(new MetricDataPoint
                        {
                            Timestamp = ts,
                            Value = svCount > 0 ? svSum / svCount : 0,
                            Unit = unit,
                            Dimensions = dims,
                        });
                    }
                    else
                    {
                        EnsureMetricList(key).Add(new MetricDataPoint
                        {
                            Timestamp = ts,
                            Value = JsonDouble(md, "Value"),
                            Unit = unit,
                            Dimensions = dims,
                        });
                    }
                }
            }
        }
        else
        {
            var ns = FormP(formParams, "Namespace");
            for (var i = 1; ; i++)
            {
                var mn = FormP(formParams, $"MetricData.member.{i}.MetricName");
                if (string.IsNullOrEmpty(mn)) break;

                var value = ParseDoubleOrDefault(FormP(formParams, $"MetricData.member.{i}.Value"), 0);
                var unit = FormP(formParams, $"MetricData.member.{i}.Unit", "None");
                var tsStr = FormP(formParams, $"MetricData.member.{i}.Timestamp");
                var ts = !string.IsNullOrEmpty(tsStr) ? ParseTimestampString(tsStr) ?? NowEpoch() : NowEpoch();
                var dims = ParseFormDimensions(formParams, $"MetricData.member.{i}");
                var dimsKey = DimsKey(dims);
                var key = MetricKey(ns, mn, dimsKey);

                EnsureMetricList(key).Add(new MetricDataPoint
                {
                    Timestamp = ts,
                    Value = value,
                    Unit = unit,
                    Dimensions = dims,
                });
            }
        }

        EvaluateAllAlarms();

        if (isCbor) return CborOk(new Dictionary<string, object?>());
        if (isJson) return JsonOk(new Dictionary<string, object?>());
        return XmlResponse(200, "PutMetricDataResponse", "");
    }

    // ── ListMetrics ───────────────────────────────────────────────────────────

    private ServiceResponse ListMetrics(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string? ns;
        string? metricName;

        if (isCbor || isJson)
        {
            ns = JsonStrOrNull(json, "Namespace");
            metricName = JsonStrOrNull(json, "MetricName");
        }
        else
        {
            ns = FormPOrNull(formParams, "Namespace");
            metricName = FormPOrNull(formParams, "MetricName");
        }

        var seen = new HashSet<string>();
        var result = new List<Dictionary<string, object>>();

        foreach (var (keyStr, points) in _metrics.ToArray())
        {
            ParseMetricKey(keyStr, out var kNs, out var kMn, out _);
            if (ns is not null && kNs != ns) continue;
            if (metricName is not null && kMn != metricName) continue;
            if (!seen.Add(keyStr)) continue;

            var dims = points.Count > 0 ? points[0].Dimensions : new Dictionary<string, string>();
            var dimsList = dims.Select(d => new Dictionary<string, string>
            {
                ["Name"] = d.Key,
                ["Value"] = d.Value,
            }).ToList();

            result.Add(new Dictionary<string, object>
            {
                ["Namespace"] = kNs,
                ["MetricName"] = kMn,
                ["Dimensions"] = dimsList,
            });
        }

        if (isCbor) return CborOk(new Dictionary<string, object?> { ["Metrics"] = result });
        if (isJson) return JsonOk(new Dictionary<string, object?> { ["Metrics"] = result });

        var members = new StringBuilder();
        foreach (var item in result)
        {
            var dimsXml = new StringBuilder();
            foreach (var d in (List<Dictionary<string, string>>)item["Dimensions"])
            {
                dimsXml.Append($"<member><Name>{Esc(d["Name"])}</Name><Value>{Esc(d["Value"])}</Value></member>");
            }

            members.Append($"<member><Namespace>{Esc((string)item["Namespace"])}</Namespace>"
                           + $"<MetricName>{Esc((string)item["MetricName"])}</MetricName>"
                           + $"<Dimensions>{dimsXml}</Dimensions></member>");
        }

        return XmlResponse(200, "ListMetricsResponse",
            $"<ListMetricsResult><Metrics>{members}</Metrics></ListMetricsResult>");
    }

    // ── GetMetricStatistics ───────────────────────────────────────────────────

    private ServiceResponse GetMetricStatistics(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string? ns;
        string? metricName;
        int period;
        double? startTime;
        double? endTime;
        List<string> reqStats;

        if (isCbor || isJson)
        {
            ns = JsonStrOrNull(json, "Namespace");
            metricName = JsonStrOrNull(json, "MetricName");
            period = JsonInt(json, "Period", 60);
            startTime = ParseTimestamp(json, "StartTime");
            endTime = ParseTimestamp(json, "EndTime");
            reqStats = JsonStringArray(json, "Statistics");
        }
        else
        {
            ns = FormPOrNull(formParams, "Namespace");
            metricName = FormPOrNull(formParams, "MetricName");
            period = ParseIntOrDefault(FormP(formParams, "Period"), 60);
            startTime = ParseTimestampString(FormP(formParams, "StartTime"));
            endTime = ParseTimestampString(FormP(formParams, "EndTime"));
            reqStats = [];
            for (var i = 1; ; i++)
            {
                var s = FormP(formParams, $"Statistics.member.{i}");
                if (string.IsNullOrEmpty(s)) break;
                reqStats.Add(s);
            }
        }

        if (reqStats.Count == 0)
            reqStats = ["SampleCount", "Sum", "Average", "Minimum", "Maximum"];

        var allPoints = CollectPoints(ns, metricName);

        if (startTime.HasValue)
            allPoints = allPoints.Where(p => p.Timestamp >= startTime.Value).ToList();
        if (endTime.HasValue)
            allPoints = allPoints.Where(p => p.Timestamp < endTime.Value).ToList();

        var buckets = new SortedDictionary<long, List<double>>();
        foreach (var pt in allPoints)
        {
            var bucketTs = (long)(pt.Timestamp / period) * period;
            if (!buckets.TryGetValue(bucketTs, out var list))
            {
                list = [];
                buckets[bucketTs] = list;
            }

            list.Add(pt.Value);
        }

        var datapoints = new List<Dictionary<string, object>>();
        foreach (var (ts, vals) in buckets)
        {
            var stats = CalcStats(vals);
            var dp = new Dictionary<string, object>
            {
                ["Timestamp"] = EpochToIso(ts),
                ["Unit"] = allPoints.Count > 0 ? allPoints[0].Unit : "None",
            };
            foreach (var s in reqStats)
            {
                if (stats.TryGetValue(s, out var v))
                    dp[s] = v;
            }

            datapoints.Add(dp);
        }

        if (isCbor) return CborOk(new Dictionary<string, object?> { ["Datapoints"] = datapoints.ConvertAll(CborTagTimestampsObj), ["Label"] = metricName ?? "" });
        if (isJson) return JsonOk(new Dictionary<string, object?> { ["Datapoints"] = datapoints, ["Label"] = metricName ?? "" });

        if (datapoints.Count == 0)
        {
            return XmlResponse(200, "GetMetricStatisticsResponse",
                $"<GetMetricStatisticsResult><Datapoints/><Label>{Esc(metricName ?? "")}</Label></GetMetricStatisticsResult>");
        }

        var dps = new StringBuilder();
        foreach (var dp in datapoints)
        {
            var inner = new StringBuilder();
            inner.Append($"<Timestamp>{dp["Timestamp"]}</Timestamp>");
            foreach (var (k, v) in dp)
            {
                if (k != "Timestamp")
                    inner.Append($"<{k}>{v}</{k}>");
            }

            dps.Append($"<member>{inner}</member>");
        }

        return XmlResponse(200, "GetMetricStatisticsResponse",
            $"<GetMetricStatisticsResult><Datapoints>{dps}</Datapoints>"
            + $"<Label>{Esc(metricName ?? "")}</Label></GetMetricStatisticsResult>");
    }

    // ── GetMetricData ─────────────────────────────────────────────────────────

    private ServiceResponse GetMetricData(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        List<MetricDataQuery> queries;
        double? startTime;
        double? endTime;

        if (isCbor || isJson)
        {
            queries = ParseJsonMetricDataQueries(json);
            startTime = ParseTimestamp(json, "StartTime");
            endTime = ParseTimestamp(json, "EndTime");
        }
        else
        {
            queries = ParseFormMetricDataQueries(formParams);
            startTime = ParseTimestampString(FormP(formParams, "StartTime"));
            endTime = ParseTimestampString(FormP(formParams, "EndTime"));
        }

        var results = new List<object>();
        var cborResults = new List<object>();
        foreach (var q in queries)
        {
            if (!string.IsNullOrEmpty(q.Expression))
            {
                var expResult = new Dictionary<string, object>
                {
                    ["Id"] = q.Id,
                    ["Label"] = q.Label,
                    ["StatusCode"] = "InternalError",
                    ["Messages"] = new List<object>
                    {
                        new Dictionary<string, string>
                        {
                            ["Code"] = "Unsupported",
                            ["Value"] = "Expressions not implemented",
                        },
                    },
                    ["Timestamps"] = new List<string>(),
                    ["Values"] = new List<double>(),
                };
                results.Add(expResult);
                cborResults.Add(expResult);
                continue;
            }

            var allPts = CollectPoints(q.MetricNamespace, q.MetricName);

            if (startTime.HasValue)
                allPts = allPts.Where(p => p.Timestamp >= startTime.Value).ToList();
            if (endTime.HasValue)
                allPts = allPts.Where(p => p.Timestamp < endTime.Value).ToList();

            var buckets = new SortedDictionary<long, List<double>>();
            foreach (var pt in allPts)
            {
                var bucketTs = (long)(pt.Timestamp / q.Period) * q.Period;
                if (!buckets.TryGetValue(bucketTs, out var list))
                {
                    list = [];
                    buckets[bucketTs] = list;
                }

                list.Add(pt.Value);
            }

            var timestamps = new List<string>();
            var cborTimestamps = new List<CborEpochTimestamp>();
            var values = new List<double>();
            foreach (var (ts, vals) in buckets)
            {
                var stats = CalcStats(vals);
                timestamps.Add(EpochToIso(ts));
                cborTimestamps.Add(new CborEpochTimestamp(ts));
                values.Add(StatValue(stats, q.Stat));
            }

            if (q.ReturnData)
            {
                results.Add(new Dictionary<string, object>
                {
                    ["Id"] = q.Id,
                    ["Label"] = q.Label,
                    ["Timestamps"] = timestamps,
                    ["Values"] = values,
                    ["StatusCode"] = "Complete",
                });
                cborResults.Add(new Dictionary<string, object>
                {
                    ["Id"] = q.Id,
                    ["Label"] = q.Label,
                    ["Timestamps"] = cborTimestamps,
                    ["Values"] = values,
                    ["StatusCode"] = "Complete",
                });
            }
        }

        if (isCbor) return CborOk(new Dictionary<string, object?> { ["MetricDataResults"] = cborResults });
        if (isJson) return JsonOk(new Dictionary<string, object?> { ["MetricDataResults"] = results });

        var members = new StringBuilder();
        foreach (var r in results)
        {
            var rd = (Dictionary<string, object>)r;
            var tsList = (List<string>)rd["Timestamps"];
            var valList = (List<double>)rd["Values"];
            var tsMembers = string.Join("", tsList.Select(t => $"<member>{t}</member>"));
            var valMembers = string.Join("", valList.Select(v => $"<member>{v}</member>"));
            members.Append("<member>"
                           + $"<Id>{Esc((string)rd["Id"])}</Id>"
                           + $"<Label>{Esc((string)rd["Label"])}</Label>"
                           + $"<StatusCode>{rd["StatusCode"]}</StatusCode>"
                           + $"<Timestamps>{tsMembers}</Timestamps>"
                           + $"<Values>{valMembers}</Values>"
                           + "</member>");
        }

        return XmlResponse(200, "GetMetricDataResponse",
            $"<GetMetricDataResult><MetricDataResults>{members}</MetricDataResults></GetMetricDataResult>");
    }

    // ── PutMetricAlarm ────────────────────────────────────────────────────────

    private ServiceResponse PutMetricAlarm(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string name;
        Dictionary<string, object?> alarm;

        if (isCbor || isJson)
        {
            name = JsonStr(json, "AlarmName");
            alarm = new Dictionary<string, object?>
            {
                ["AlarmName"] = name,
                ["AlarmArn"] = $"arn:aws:cloudwatch:{Region}:{AccountContext.GetAccountId()}:alarm:{name}",
                ["AlarmDescription"] = JsonStrOrNull(json, "AlarmDescription") ?? "",
                ["MetricName"] = JsonStrOrNull(json, "MetricName"),
                ["Namespace"] = JsonStrOrNull(json, "Namespace"),
                ["Statistic"] = JsonStr(json, "Statistic", "Average"),
                ["ExtendedStatistic"] = JsonStrOrNull(json, "ExtendedStatistic"),
                ["Period"] = JsonInt(json, "Period", 60),
                ["EvaluationPeriods"] = JsonInt(json, "EvaluationPeriods", 1),
                ["DatapointsToAlarm"] = JsonIntOrNull(json, "DatapointsToAlarm") ?? JsonInt(json, "EvaluationPeriods", 1),
                ["Threshold"] = JsonDouble(json, "Threshold"),
                ["ComparisonOperator"] = JsonStrOrNull(json, "ComparisonOperator"),
                ["TreatMissingData"] = JsonStr(json, "TreatMissingData", "missing"),
                ["StateValue"] = _alarms.TryGetValue(name, out var existing)
                    ? (string)(existing["StateValue"] ?? "INSUFFICIENT_DATA")
                    : "INSUFFICIENT_DATA",
                ["StateReason"] = _alarms.TryGetValue(name, out var existing2)
                    ? (string)(existing2["StateReason"] ?? "Unchecked: Initial alarm creation")
                    : "Unchecked: Initial alarm creation",
                ["StateUpdatedTimestamp"] = NowEpoch(),
                ["ActionsEnabled"] = JsonBool(json, "ActionsEnabled", true),
                ["AlarmActions"] = JsonStringArray(json, "AlarmActions"),
                ["OKActions"] = JsonStringArray(json, "OKActions"),
                ["InsufficientDataActions"] = JsonStringArray(json, "InsufficientDataActions"),
                ["Dimensions"] = JsonArrayOfObjects(json, "Dimensions"),
                ["Unit"] = JsonStrOrNull(json, "Unit"),
                ["AlarmConfigurationUpdatedTimestamp"] = NowEpoch(),
            };
        }
        else
        {
            name = FormP(formParams, "AlarmName");
            var dims = new List<Dictionary<string, string>>();
            for (var i = 1; ; i++)
            {
                var dn = FormP(formParams, $"Dimensions.member.{i}.Name");
                if (string.IsNullOrEmpty(dn)) break;
                dims.Add(new Dictionary<string, string>
                {
                    ["Name"] = dn,
                    ["Value"] = FormP(formParams, $"Dimensions.member.{i}.Value"),
                });
            }

            var alarmActions = new List<string>();
            for (var i = 1; ; i++)
            {
                var aa = FormP(formParams, $"AlarmActions.member.{i}");
                if (string.IsNullOrEmpty(aa)) break;
                alarmActions.Add(aa);
            }

            var okActions = new List<string>();
            for (var i = 1; ; i++)
            {
                var oa = FormP(formParams, $"OKActions.member.{i}");
                if (string.IsNullOrEmpty(oa)) break;
                okActions.Add(oa);
            }

            alarm = new Dictionary<string, object?>
            {
                ["AlarmName"] = name,
                ["AlarmArn"] = $"arn:aws:cloudwatch:{Region}:{AccountContext.GetAccountId()}:alarm:{name}",
                ["AlarmDescription"] = FormPOrNull(formParams, "AlarmDescription"),
                ["MetricName"] = FormPOrNull(formParams, "MetricName"),
                ["Namespace"] = FormPOrNull(formParams, "Namespace"),
                ["Statistic"] = FormP(formParams, "Statistic", "Average"),
                ["ExtendedStatistic"] = FormPOrNull(formParams, "ExtendedStatistic"),
                ["Period"] = ParseIntOrDefault(FormP(formParams, "Period"), 60),
                ["EvaluationPeriods"] = ParseIntOrDefault(FormP(formParams, "EvaluationPeriods"), 1),
                ["DatapointsToAlarm"] = ParseIntOrDefault(
                    FormP(formParams, "DatapointsToAlarm"),
                    ParseIntOrDefault(FormP(formParams, "EvaluationPeriods"), 1)),
                ["Threshold"] = ParseDoubleOrDefault(FormP(formParams, "Threshold"), 0),
                ["ComparisonOperator"] = FormPOrNull(formParams, "ComparisonOperator"),
                ["TreatMissingData"] = FormP(formParams, "TreatMissingData", "missing"),
                ["StateValue"] = _alarms.TryGetValue(name, out var existingForm)
                    ? (string)(existingForm["StateValue"] ?? "INSUFFICIENT_DATA")
                    : "INSUFFICIENT_DATA",
                ["StateReason"] = _alarms.TryGetValue(name, out var existingForm2)
                    ? (string)(existingForm2["StateReason"] ?? "Unchecked: Initial alarm creation")
                    : "Unchecked: Initial alarm creation",
                ["StateUpdatedTimestamp"] = NowEpoch(),
                ["ActionsEnabled"] = FormP(formParams, "ActionsEnabled") != "false",
                ["AlarmActions"] = alarmActions,
                ["OKActions"] = okActions,
                ["InsufficientDataActions"] = new List<string>(),
                ["Dimensions"] = dims,
                ["Unit"] = FormPOrNull(formParams, "Unit"),
                ["AlarmConfigurationUpdatedTimestamp"] = NowEpoch(),
            };
        }

        var isNew = !_alarms.ContainsKey(name);
        _alarms[name] = alarm;

        if (isNew)
        {
            RecordHistory(name, "INSUFFICIENT_DATA", "INSUFFICIENT_DATA", "Unchecked: Initial alarm creation");
        }

        EvaluateAlarm(alarm);

        if (isCbor) return CborOk(new Dictionary<string, object?>());
        if (isJson) return JsonOk(new Dictionary<string, object?>());
        return XmlResponse(200, "PutMetricAlarmResponse", "");
    }

    // ── PutCompositeAlarm ─────────────────────────────────────────────────────

    private ServiceResponse PutCompositeAlarm(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string name;
        string alarmRule;
        string desc;
        bool actionsEnabled;
        List<string> alarmActions;
        List<string> okActions;
        List<string> insuffActions;

        if (isCbor || isJson)
        {
            name = JsonStr(json, "AlarmName");
            alarmRule = JsonStr(json, "AlarmRule");
            desc = JsonStr(json, "AlarmDescription");
            actionsEnabled = JsonBool(json, "ActionsEnabled", true);
            alarmActions = JsonStringArray(json, "AlarmActions");
            okActions = JsonStringArray(json, "OKActions");
            insuffActions = JsonStringArray(json, "InsufficientDataActions");
        }
        else
        {
            name = FormP(formParams, "AlarmName");
            alarmRule = FormP(formParams, "AlarmRule");
            desc = FormP(formParams, "AlarmDescription");
            actionsEnabled = FormP(formParams, "ActionsEnabled") != "false";
            alarmActions = FormMemberList(formParams, "AlarmActions");
            okActions = FormMemberList(formParams, "OKActions");
            insuffActions = FormMemberList(formParams, "InsufficientDataActions");
        }

        _compositeAlarms[name] = new Dictionary<string, object?>
        {
            ["AlarmName"] = name,
            ["AlarmArn"] = $"arn:aws:cloudwatch:{Region}:{AccountContext.GetAccountId()}:alarm:{name}",
            ["AlarmDescription"] = desc,
            ["AlarmRule"] = alarmRule,
            ["StateValue"] = "INSUFFICIENT_DATA",
            ["StateReason"] = "Unchecked: Initial alarm creation",
            ["StateUpdatedTimestamp"] = NowEpoch(),
            ["ActionsEnabled"] = actionsEnabled,
            ["AlarmActions"] = alarmActions,
            ["OKActions"] = okActions,
            ["InsufficientDataActions"] = insuffActions,
            ["AlarmConfigurationUpdatedTimestamp"] = NowEpoch(),
        };

        if (isCbor) return CborOk(new Dictionary<string, object?>());
        if (isJson) return JsonOk(new Dictionary<string, object?>());
        return XmlResponse(200, "PutCompositeAlarmResponse", "<PutCompositeAlarmResult/>");
    }

    // ── DescribeAlarms ────────────────────────────────────────────────────────

    private ServiceResponse DescribeAlarms(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        List<string> names;
        string? prefix;
        string? state;
        List<string> alarmTypes;
        int maxRecords;

        if (isCbor || isJson)
        {
            names = JsonStringArray(json, "AlarmNames");
            prefix = JsonStrOrNull(json, "AlarmNamePrefix");
            state = JsonStrOrNull(json, "StateValue");
            alarmTypes = JsonStringArray(json, "AlarmTypes");
            if (alarmTypes.Count == 0)
                alarmTypes = ["MetricAlarm", "CompositeAlarm"];
            maxRecords = JsonInt(json, "MaxRecords", 100);
        }
        else
        {
            names = [];
            for (var i = 1; i <= 100; i++)
            {
                var n = FormP(formParams, $"AlarmNames.member.{i}");
                if (!string.IsNullOrEmpty(n)) names.Add(n);
            }

            prefix = FormPOrNull(formParams, "AlarmNamePrefix");
            state = FormPOrNull(formParams, "StateValue");
            alarmTypes = [];
            for (var i = 1; i <= 10; i++)
            {
                var at = FormP(formParams, $"AlarmTypes.member.{i}");
                if (!string.IsNullOrEmpty(at)) alarmTypes.Add(at);
            }

            if (alarmTypes.Count == 0)
                alarmTypes = ["MetricAlarm", "CompositeAlarm"];
            maxRecords = ParseIntOrDefault(FormP(formParams, "MaxRecords"), 100);
        }

        var metricAlarms = new List<Dictionary<string, object?>>();
        var compositeResults = new List<Dictionary<string, object?>>();

        if (alarmTypes.Contains("MetricAlarm"))
        {
            foreach (var (aname, alarm) in _alarms.ToArray())
            {
                if (names.Count > 0 && !names.Contains(aname)) continue;
                if (prefix is not null && !aname.StartsWith(prefix, StringComparison.Ordinal)) continue;
                if (state is not null && (string)(alarm["StateValue"] ?? "") != state) continue;
                metricAlarms.Add(alarm);
            }
        }

        if (alarmTypes.Contains("CompositeAlarm"))
        {
            foreach (var (aname, alarm) in _compositeAlarms.ToArray())
            {
                if (names.Count > 0 && !names.Contains(aname)) continue;
                if (prefix is not null && !aname.StartsWith(prefix, StringComparison.Ordinal)) continue;
                if (state is not null && (string)(alarm["StateValue"] ?? "") != state) continue;
                compositeResults.Add(alarm);
            }
        }

        if (metricAlarms.Count > maxRecords)
            metricAlarms = metricAlarms.Take(maxRecords).ToList();

        if (isCbor) return CborOk(new Dictionary<string, object?> { ["MetricAlarms"] = metricAlarms.ConvertAll(CborTagTimestamps), ["CompositeAlarms"] = compositeResults.ConvertAll(CborTagTimestamps) });
        if (isJson) return JsonOk(new Dictionary<string, object?> { ["MetricAlarms"] = metricAlarms, ["CompositeAlarms"] = compositeResults });

        var metricMembers = new StringBuilder();
        foreach (var a in metricAlarms)
        {
            metricMembers.Append(
                $"<member><AlarmName>{Esc(Str(a, "AlarmName"))}</AlarmName>"
                + $"<AlarmArn>{Esc(Str(a, "AlarmArn"))}</AlarmArn>"
                + $"<StateValue>{Esc(Str(a, "StateValue"))}</StateValue>"
                + $"<MetricName>{Esc(Str(a, "MetricName"))}</MetricName>"
                + $"<Namespace>{Esc(Str(a, "Namespace"))}</Namespace>"
                + $"<Threshold>{a.GetValueOrDefault("Threshold", "")}</Threshold>"
                + $"<ComparisonOperator>{Esc(Str(a, "ComparisonOperator"))}</ComparisonOperator>"
                + $"<EvaluationPeriods>{a.GetValueOrDefault("EvaluationPeriods", "")}</EvaluationPeriods>"
                + $"<StateReason>{Esc(Str(a, "StateReason"))}</StateReason>"
                + "</member>");
        }

        var compMembers = new StringBuilder();
        foreach (var a in compositeResults)
        {
            compMembers.Append(
                $"<member><AlarmName>{Esc(Str(a, "AlarmName"))}</AlarmName>"
                + $"<AlarmArn>{Esc(Str(a, "AlarmArn"))}</AlarmArn>"
                + $"<AlarmRule>{Esc(Str(a, "AlarmRule"))}</AlarmRule>"
                + $"<StateValue>{Esc(Str(a, "StateValue"))}</StateValue>"
                + $"<StateReason>{Esc(Str(a, "StateReason"))}</StateReason>"
                + "</member>");
        }

        return XmlResponse(200, "DescribeAlarmsResponse",
            $"<DescribeAlarmsResult>"
            + $"<MetricAlarms>{metricMembers}</MetricAlarms>"
            + $"<CompositeAlarms>{compMembers}</CompositeAlarms>"
            + "</DescribeAlarmsResult>");
    }

    // ── DescribeAlarmsForMetric ───────────────────────────────────────────────

    private ServiceResponse DescribeAlarmsForMetric(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string ns;
        string metricName;

        if (isCbor || isJson)
        {
            ns = JsonStr(json, "Namespace");
            metricName = JsonStr(json, "MetricName");
        }
        else
        {
            ns = FormP(formParams, "Namespace");
            metricName = FormP(formParams, "MetricName");
        }

        var result = _alarms.Values
            .Where(a => Str(a, "Namespace") == ns && Str(a, "MetricName") == metricName)
            .ToList();

        if (isCbor) return CborOk(new Dictionary<string, object?> { ["MetricAlarms"] = result.ConvertAll(CborTagTimestamps) });
        if (isJson) return JsonOk(new Dictionary<string, object?> { ["MetricAlarms"] = result });

        var members = new StringBuilder();
        foreach (var a in result)
        {
            members.Append(
                $"<member><AlarmName>{Esc(Str(a, "AlarmName"))}</AlarmName>"
                + $"<AlarmArn>{Esc(Str(a, "AlarmArn"))}</AlarmArn>"
                + $"<StateValue>{Esc(Str(a, "StateValue"))}</StateValue></member>");
        }

        return XmlResponse(200, "DescribeAlarmsForMetricResponse",
            $"<DescribeAlarmsForMetricResult><MetricAlarms>{members}</MetricAlarms></DescribeAlarmsForMetricResult>");
    }

    // ── DescribeAlarmHistory ──────────────────────────────────────────────────

    private ServiceResponse DescribeAlarmHistory(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string? alarmName;
        string? historyType;
        int maxRecords;

        if (isCbor || isJson)
        {
            alarmName = JsonStrOrNull(json, "AlarmName");
            historyType = JsonStrOrNull(json, "HistoryItemType");
            maxRecords = JsonInt(json, "MaxRecords", 100);
        }
        else
        {
            alarmName = FormPOrNull(formParams, "AlarmName");
            historyType = FormPOrNull(formParams, "HistoryItemType");
            maxRecords = ParseIntOrDefault(FormP(formParams, "MaxRecords"), 100);
        }

        var items = _alarmHistory.ToList();
        if (alarmName is not null)
            items = items.Where(h => h["AlarmName"] == alarmName).ToList();
        if (historyType is not null)
            items = items.Where(h => h["HistoryItemType"] == historyType).ToList();
        if (items.Count > maxRecords)
            items = items.Take(maxRecords).ToList();

        if (isCbor) return CborOk(new Dictionary<string, object?> { ["AlarmHistoryItems"] = items.ConvertAll(CborTagTimestamps) });
        if (isJson) return JsonOk(new Dictionary<string, object?> { ["AlarmHistoryItems"] = items });

        var members = new StringBuilder();
        foreach (var h in items)
        {
            members.Append(
                $"<member><AlarmName>{Esc(h["AlarmName"])}</AlarmName>"
                + $"<Timestamp>{h["Timestamp"]}</Timestamp>"
                + $"<HistoryItemType>{h["HistoryItemType"]}</HistoryItemType>"
                + $"<HistorySummary>{Esc(h["HistorySummary"])}</HistorySummary></member>");
        }

        return XmlResponse(200, "DescribeAlarmHistoryResponse",
            $"<DescribeAlarmHistoryResult><AlarmHistoryItems>{members}</AlarmHistoryItems></DescribeAlarmHistoryResult>");
    }

    // ── DeleteAlarms ──────────────────────────────────────────────────────────

    private ServiceResponse DeleteAlarms(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        List<string> names;
        if (isCbor || isJson)
        {
            names = JsonStringArray(json, "AlarmNames");
        }
        else
        {
            names = [];
            for (var i = 1; ; i++)
            {
                var n = FormP(formParams, $"AlarmNames.member.{i}");
                if (string.IsNullOrEmpty(n)) break;
                names.Add(n);
            }
        }

        foreach (var n in names)
        {
            _alarms.TryRemove(n, out _);
            _compositeAlarms.TryRemove(n, out _);
            _resourceTags.TryRemove(
                $"arn:aws:cloudwatch:{Region}:{AccountContext.GetAccountId()}:alarm:{n}", out _);
        }

        if (isCbor) return CborOk(new Dictionary<string, object?>());
        if (isJson) return JsonOk(new Dictionary<string, object?>());
        return XmlResponse(200, "DeleteAlarmsResponse", "");
    }

    // ── EnableAlarmActions ────────────────────────────────────────────────────

    private ServiceResponse EnableAlarmActions(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        var names = GetAlarmNames(formParams, json, isCbor || isJson);
        foreach (var n in names)
        {
            if (_alarms.TryGetValue(n, out var a)) a["ActionsEnabled"] = true;
            if (_compositeAlarms.TryGetValue(n, out var ca)) ca["ActionsEnabled"] = true;
        }

        if (isCbor) return CborOk(new Dictionary<string, object?>());
        if (isJson) return JsonOk(new Dictionary<string, object?>());
        return XmlResponse(200, "EnableAlarmActionsResponse", "");
    }

    // ── DisableAlarmActions ───────────────────────────────────────────────────

    private ServiceResponse DisableAlarmActions(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        var names = GetAlarmNames(formParams, json, isCbor || isJson);
        foreach (var n in names)
        {
            if (_alarms.TryGetValue(n, out var a)) a["ActionsEnabled"] = false;
            if (_compositeAlarms.TryGetValue(n, out var ca)) ca["ActionsEnabled"] = false;
        }

        if (isCbor) return CborOk(new Dictionary<string, object?>());
        if (isJson) return JsonOk(new Dictionary<string, object?>());
        return XmlResponse(200, "DisableAlarmActionsResponse", "");
    }

    // ── SetAlarmState ─────────────────────────────────────────────────────────

    private ServiceResponse SetAlarmState(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string name;
        string newState;
        string reason;

        if (isCbor || isJson)
        {
            name = JsonStr(json, "AlarmName");
            newState = JsonStr(json, "StateValue");
            reason = JsonStr(json, "StateReason");
        }
        else
        {
            name = FormP(formParams, "AlarmName");
            newState = FormP(formParams, "StateValue");
            reason = FormP(formParams, "StateReason");
        }

        Dictionary<string, object?>? alarm = null;
        if (_alarms.TryGetValue(name, out var a)) alarm = a;
        else if (_compositeAlarms.TryGetValue(name, out var ca)) alarm = ca;

        if (alarm is null)
            return ErrorResponse("ResourceNotFound", $"Alarm {name} not found", 404, isCbor, isJson);

        var oldState = (string)(alarm["StateValue"] ?? "INSUFFICIENT_DATA");
        alarm["StateValue"] = newState;
        alarm["StateReason"] = reason;
        alarm["StateUpdatedTimestamp"] = NowEpoch();

        if (oldState != newState)
            RecordHistory(name, oldState, newState, reason);

        if (isCbor) return CborOk(new Dictionary<string, object?>());
        if (isJson) return JsonOk(new Dictionary<string, object?>());
        return XmlResponse(200, "SetAlarmStateResponse", "");
    }

    // ── TagResource ───────────────────────────────────────────────────────────

    private ServiceResponse TagResource(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string arn;
        List<KeyValuePair<string, string>> tags;

        if (isCbor || isJson)
        {
            arn = JsonStr(json, "ResourceARN");
            tags = ParseJsonTags(json);
        }
        else
        {
            arn = FormP(formParams, "ResourceARN");
            tags = [];
            for (var i = 1; ; i++)
            {
                var k = FormP(formParams, $"Tags.member.{i}.Key");
                if (string.IsNullOrEmpty(k)) break;
                var v = FormP(formParams, $"Tags.member.{i}.Value");
                tags.Add(new KeyValuePair<string, string>(k, v));
            }
        }

        if (!_resourceTags.TryGetValue(arn, out var tagMap))
        {
            tagMap = new Dictionary<string, string>();
            _resourceTags[arn] = tagMap;
        }

        foreach (var (k, v) in tags)
        {
            tagMap[k] = v;
        }

        if (isCbor) return CborOk(new Dictionary<string, object?>());
        if (isJson) return JsonOk(new Dictionary<string, object?>());
        return XmlResponse(200, "TagResourceResponse", "<TagResourceResult/>");
    }

    // ── UntagResource ─────────────────────────────────────────────────────────

    private ServiceResponse UntagResource(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string arn;
        List<string> keys;

        if (isCbor || isJson)
        {
            arn = JsonStr(json, "ResourceARN");
            keys = JsonStringArray(json, "TagKeys");
        }
        else
        {
            arn = FormP(formParams, "ResourceARN");
            keys = [];
            for (var i = 1; ; i++)
            {
                var k = FormP(formParams, $"TagKeys.member.{i}");
                if (string.IsNullOrEmpty(k)) break;
                keys.Add(k);
            }
        }

        if (_resourceTags.TryGetValue(arn, out var tagMap))
        {
            foreach (var k in keys)
                tagMap.Remove(k);
        }

        if (isCbor) return CborOk(new Dictionary<string, object?>());
        if (isJson) return JsonOk(new Dictionary<string, object?>());
        return XmlResponse(200, "UntagResourceResponse", "<UntagResourceResult/>");
    }

    // ── ListTagsForResource ───────────────────────────────────────────────────

    private ServiceResponse ListTagsForResource(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string arn;

        if (isCbor || isJson)
            arn = JsonStr(json, "ResourceARN");
        else
            arn = FormP(formParams, "ResourceARN");

        var tags = new List<Dictionary<string, string>>();
        if (_resourceTags.TryGetValue(arn, out var tagMap))
        {
            tags.AddRange(tagMap.Select(kv => new Dictionary<string, string>
            {
                ["Key"] = kv.Key,
                ["Value"] = kv.Value,
            }));
        }

        if (isCbor) return CborOk(new Dictionary<string, object?> { ["Tags"] = tags });
        if (isJson) return JsonOk(new Dictionary<string, object?> { ["Tags"] = tags });

        var members = new StringBuilder();
        foreach (var t in tags)
        {
            members.Append($"<member><Key>{Esc(t["Key"])}</Key><Value>{Esc(t["Value"])}</Value></member>");
        }

        return XmlResponse(200, "ListTagsForResourceResponse",
            $"<ListTagsForResourceResult><Tags>{members}</Tags></ListTagsForResourceResult>");
    }

    // ── PutDashboard ──────────────────────────────────────────────────────────

    private ServiceResponse PutDashboard(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string name;
        string body;

        if (isCbor || isJson)
        {
            name = JsonStr(json, "DashboardName");
            body = JsonStr(json, "DashboardBody");
        }
        else
        {
            name = FormP(formParams, "DashboardName");
            body = FormP(formParams, "DashboardBody");
        }

        if (string.IsNullOrEmpty(name))
            return ErrorResponse("InvalidParameterValue", "DashboardName is required", 400, isCbor, isJson);

        _dashboards[name] = new Dictionary<string, object?>
        {
            ["DashboardName"] = name,
            ["DashboardBody"] = body,
            ["DashboardArn"] = $"arn:aws:cloudwatch::{AccountContext.GetAccountId()}:dashboard/{name}",
            ["LastModified"] = NowEpoch(),
            ["Size"] = body.Length,
        };

        if (isCbor) return CborOk(new Dictionary<string, object?> { ["DashboardValidationMessages"] = Array.Empty<object>() });
        if (isJson) return JsonOk(new Dictionary<string, object?> { ["DashboardValidationMessages"] = Array.Empty<object>() });
        return XmlResponse(200, "PutDashboardResponse",
            "<PutDashboardResult><DashboardValidationMessages/></PutDashboardResult>");
    }

    // ── GetDashboard ──────────────────────────────────────────────────────────

    private ServiceResponse GetDashboard(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string name;

        if (isCbor || isJson)
            name = JsonStr(json, "DashboardName");
        else
            name = FormP(formParams, "DashboardName");

        if (!_dashboards.TryGetValue(name, out var dash))
            return ErrorResponse("DashboardNotFoundError", $"Dashboard {name} does not exist", 404, isCbor, isJson);

        var dashArn = (string)(dash["DashboardArn"] ?? "");
        var dashBody = (string)(dash["DashboardBody"] ?? "");
        var dashName = (string)(dash["DashboardName"] ?? "");
        var result = new Dictionary<string, object?>
        {
            ["DashboardArn"] = dashArn,
            ["DashboardBody"] = dashBody,
            ["DashboardName"] = dashName,
        };

        if (isCbor) return CborOk(result);
        if (isJson) return JsonOk(result);

        return XmlResponse(200, "GetDashboardResponse",
            $"<GetDashboardResult>"
            + $"<DashboardArn>{Esc(dashArn)}</DashboardArn>"
            + $"<DashboardBody>{Esc(dashBody)}</DashboardBody>"
            + $"<DashboardName>{Esc(dashName)}</DashboardName>"
            + "</GetDashboardResult>");
    }

    // ── DeleteDashboards ──────────────────────────────────────────────────────

    private ServiceResponse DeleteDashboards(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        List<string> names;
        if (isCbor || isJson)
        {
            names = JsonStringArray(json, "DashboardNames");
        }
        else
        {
            names = [];
            for (var i = 1; ; i++)
            {
                var n = FormP(formParams, $"DashboardNames.member.{i}");
                if (string.IsNullOrEmpty(n)) break;
                names.Add(n);
            }
        }

        foreach (var n in names)
            _dashboards.TryRemove(n, out _);

        if (isCbor) return CborOk(new Dictionary<string, object?>());
        if (isJson) return JsonOk(new Dictionary<string, object?>());
        return XmlResponse(200, "DeleteDashboardsResponse", "<DeleteDashboardsResult/>");
    }

    // ── ListDashboards ────────────────────────────────────────────────────────

    private ServiceResponse ListDashboards(
        Dictionary<string, string[]> formParams, JsonElement json, bool isCbor, bool isJson)
    {
        string prefix;

        if (isCbor || isJson)
            prefix = JsonStr(json, "DashboardNamePrefix");
        else
            prefix = FormP(formParams, "DashboardNamePrefix");

        var entries = new List<Dictionary<string, object>>();
        foreach (var name in _dashboards.Keys.Order())
        {
            if (!string.IsNullOrEmpty(prefix) && !name.StartsWith(prefix, StringComparison.Ordinal))
                continue;
            var dash = _dashboards[name];
            entries.Add(new Dictionary<string, object>
            {
                ["DashboardName"] = (string)(dash["DashboardName"] ?? ""),
                ["DashboardArn"] = (string)(dash["DashboardArn"] ?? ""),
                ["Size"] = dash["Size"] ?? 0,
                ["LastModified"] = EpochToIso(Convert.ToDouble(dash["LastModified"])),
            });
        }

        if (isCbor) return CborOk(new Dictionary<string, object?> { ["DashboardEntries"] = entries.ConvertAll(CborTagTimestampsObj) });
        if (isJson) return JsonOk(new Dictionary<string, object?> { ["DashboardEntries"] = entries });

        var members = new StringBuilder();
        foreach (var e in entries)
        {
            members.Append(
                "<member>"
                + $"<DashboardName>{Esc((string)e["DashboardName"])}</DashboardName>"
                + $"<DashboardArn>{Esc((string)e["DashboardArn"])}</DashboardArn>"
                + $"<Size>{e["Size"]}</Size>"
                + $"<LastModified>{e["LastModified"]}</LastModified>"
                + "</member>");
        }

        return XmlResponse(200, "ListDashboardsResponse",
            $"<ListDashboardsResult><DashboardEntries>{members}</DashboardEntries></ListDashboardsResult>");
    }

    // ── Alarm evaluation ──────────────────────────────────────────────────────

    private void EvaluateAlarm(Dictionary<string, object?> alarm)
    {
        var ns = Str(alarm, "Namespace");
        var mn = Str(alarm, "MetricName");
        if (string.IsNullOrEmpty(ns) || string.IsNullOrEmpty(mn)) return;

        var allPts = CollectPoints(ns, mn);
        if (allPts.Count == 0) return;

        var period = Convert.ToInt32(alarm.GetValueOrDefault("Period", 60) ?? 60);
        var evalPeriods = Convert.ToInt32(alarm.GetValueOrDefault("EvaluationPeriods", 1) ?? 1);
        var cutoff = NowEpoch() - period * evalPeriods;
        var recent = allPts.Where(p => p.Timestamp >= cutoff).ToList();
        if (recent.Count == 0) return;

        var stats = CalcStats(recent.Select(p => p.Value).ToList());
        var statName = Str(alarm, "Statistic", "Average");
        var val = StatValue(stats, statName);
        var threshold = Convert.ToDouble(alarm.GetValueOrDefault("Threshold", 0.0) ?? 0.0);
        var op = Str(alarm, "ComparisonOperator");

        var breaching = op switch
        {
            "GreaterThanOrEqualToThreshold" => val >= threshold,
            "GreaterThanThreshold" => val > threshold,
            "LessThanThreshold" => val < threshold,
            "LessThanOrEqualToThreshold" => val <= threshold,
            "GreaterThanUpperThreshold" => val > threshold,
            "LessThanLowerThreshold" => val < threshold,
            "LessThanLowerOrGreaterThanUpperThreshold" => val < threshold,
            _ => false,
        };

        var oldState = (string)(alarm["StateValue"] ?? "INSUFFICIENT_DATA");
        var newState = breaching ? "ALARM" : "OK";
        if (oldState != newState)
        {
            var reason = $"Threshold Crossed: {statName} {val} {op} {threshold}";
            alarm["StateValue"] = newState;
            alarm["StateReason"] = reason;
            alarm["StateUpdatedTimestamp"] = NowEpoch();
            RecordHistory((string)(alarm["AlarmName"] ?? ""), oldState, newState, reason);
        }
    }

    private void EvaluateAllAlarms()
    {
        foreach (var alarm in _alarms.Values.ToArray())
        {
            try { EvaluateAlarm(alarm); }
            catch { /* swallow evaluation errors */ }
        }
    }

    private void RecordHistory(string alarmName, string oldState, string newState, string reason)
    {
        _alarmHistory.Add(new Dictionary<string, string>
        {
            ["AlarmName"] = alarmName,
            ["AlarmType"] = "MetricAlarm",
            ["Timestamp"] = EpochToIso(NowEpoch()),
            ["HistoryItemType"] = "StateUpdate",
            ["HistorySummary"] = $"Alarm updated from {oldState} to {newState}",
            ["HistoryData"] = DictionaryObjectJsonConverter.SerializeValue(new Dictionary<string, object?>
            {
                ["version"] = "1.0",
                ["oldState"] = new Dictionary<string, object?> { ["stateValue"] = oldState },
                ["newState"] = new Dictionary<string, object?> { ["stateValue"] = newState, ["stateReason"] = reason },
            }),
        });
    }

    // ── Metric eviction ───────────────────────────────────────────────────────

    private void EvictOldMetrics()
    {
        var cutoff = NowEpoch() - TwoWeeksSeconds;
        var emptyKeys = new List<string>();
        foreach (var (key, pts) in _metrics.ToArray())
        {
            pts.RemoveAll(p => p.Timestamp < cutoff);
            if (pts.Count == 0) emptyKeys.Add(key);
        }

        foreach (var key in emptyKeys)
            _metrics.TryRemove(key, out _);
    }

    // ── Metric collection helpers ─────────────────────────────────────────────

    private List<MetricDataPoint> CollectPoints(string? ns, string? mn)
    {
        var allPts = new List<MetricDataPoint>();
        foreach (var (keyStr, pts) in _metrics.ToArray())
        {
            ParseMetricKey(keyStr, out var kNs, out var kMn, out _);
            if (ns is not null && kNs != ns) continue;
            if (mn is not null && kMn != mn) continue;
            allPts.AddRange(pts);
        }

        return allPts;
    }

    private List<MetricDataPoint> EnsureMetricList(string key)
    {
        if (!_metrics.TryGetValue(key, out var list))
        {
            list = [];
            _metrics[key] = list;
        }

        return list;
    }

    // ── Statistics helpers ─────────────────────────────────────────────────────

    private static Dictionary<string, double> CalcStats(IList<double> values)
    {
        if (values.Count == 0) return new Dictionary<string, double>();
        var sum = values.Sum();
        return new Dictionary<string, double>
        {
            ["SampleCount"] = values.Count,
            ["Sum"] = sum,
            ["Average"] = sum / values.Count,
            ["Minimum"] = values.Min(),
            ["Maximum"] = values.Max(),
        };
    }

    private static double StatValue(Dictionary<string, double> stats, string statName)
    {
        if (stats.TryGetValue(statName, out var v)) return v;
        // Percentile approximation: use Average
        if (statName.StartsWith('p') && statName.Length > 1 &&
            double.TryParse(statName[1..], NumberStyles.Float, CultureInfo.InvariantCulture, out _))
            return stats.GetValueOrDefault("Average", 0);
        return stats.GetValueOrDefault("Average", 0);
    }

    // ── Metric key helpers ────────────────────────────────────────────────────

    private static string MetricKey(string ns, string mn, string dimsKey) =>
        $"{ns}\0{mn}\0{dimsKey}";

    private static void ParseMetricKey(string key, out string ns, out string mn, out string dimsKey)
    {
        var parts = key.Split('\0');
        ns = parts.Length > 0 ? parts[0] : "";
        mn = parts.Length > 1 ? parts[1] : "";
        dimsKey = parts.Length > 2 ? parts[2] : "";
    }

    private static string DimsKey(Dictionary<string, string> dims) =>
        string.Join("|", dims.OrderBy(d => d.Key).Select(d => $"{d.Key}={d.Value}"));

    // ── Alarm name list helper ────────────────────────────────────────────────

    private static List<string> GetAlarmNames(
        Dictionary<string, string[]> formParams, JsonElement json, bool isJsonOrCbor)
    {
        if (isJsonOrCbor)
            return JsonStringArray(json, "AlarmNames");

        var names = new List<string>();
        for (var i = 1; i <= 100; i++)
        {
            var n = FormP(formParams, $"AlarmNames.member.{i}");
            if (!string.IsNullOrEmpty(n)) names.Add(n);
        }

        return names;
    }

    // ── Metric Data Query parsing ─────────────────────────────────────────────

    private static List<MetricDataQuery> ParseJsonMetricDataQueries(JsonElement json)
    {
        var queries = new List<MetricDataQuery>();
        if (!json.TryGetProperty("MetricDataQueries", out var arr) || arr.ValueKind != JsonValueKind.Array)
            return queries;

        foreach (var q in arr.EnumerateArray())
        {
            var id = JsonStr(q, "Id");
            var label = JsonStr(q, "Label", id);
            var returnData = JsonBool(q, "ReturnData", true);
            var expression = JsonStrOrNull(q, "Expression");

            string? msNs = null;
            string? msMn = null;
            var msPeriod = 60;
            var msStat = "Average";

            if (q.TryGetProperty("MetricStat", out var msEl))
            {
                msPeriod = JsonInt(msEl, "Period", 60);
                msStat = JsonStr(msEl, "Stat", "Average");
                if (msEl.TryGetProperty("Metric", out var metricEl))
                {
                    msNs = JsonStrOrNull(metricEl, "Namespace");
                    msMn = JsonStrOrNull(metricEl, "MetricName");
                }
            }

            queries.Add(new MetricDataQuery
            {
                Id = id,
                Label = label,
                ReturnData = returnData,
                Expression = expression,
                MetricNamespace = msNs,
                MetricName = msMn,
                Period = msPeriod,
                Stat = msStat,
            });
        }

        return queries;
    }

    private static List<MetricDataQuery> ParseFormMetricDataQueries(Dictionary<string, string[]> formParams)
    {
        var queries = new List<MetricDataQuery>();
        for (var i = 1; ; i++)
        {
            var qid = FormP(formParams, $"MetricDataQueries.member.{i}.Id");
            if (string.IsNullOrEmpty(qid)) break;

            var label = FormP(formParams, $"MetricDataQueries.member.{i}.Label", qid);
            var returnDataStr = FormP(formParams, $"MetricDataQueries.member.{i}.ReturnData");
            var returnData = returnDataStr != "false";
            var ns = FormP(formParams, $"MetricDataQueries.member.{i}.MetricStat.Metric.Namespace");
            var mn = FormP(formParams, $"MetricDataQueries.member.{i}.MetricStat.Metric.MetricName");
            var period = ParseIntOrDefault(
                FormP(formParams, $"MetricDataQueries.member.{i}.MetricStat.Period"), 60);
            var stat = FormP(formParams, $"MetricDataQueries.member.{i}.MetricStat.Stat", "Average");

            queries.Add(new MetricDataQuery
            {
                Id = qid,
                Label = label,
                ReturnData = returnData,
                MetricNamespace = string.IsNullOrEmpty(ns) ? null : ns,
                MetricName = string.IsNullOrEmpty(mn) ? null : mn,
                Period = period,
                Stat = stat,
            });
        }

        return queries;
    }

    // ── Form member list helper ───────────────────────────────────────────────

    private static List<string> FormMemberList(Dictionary<string, string[]> formParams, string prefix)
    {
        var items = new List<string>();
        for (var i = 1; ; i++)
        {
            var val = FormP(formParams, $"{prefix}.member.{i}");
            if (string.IsNullOrEmpty(val)) break;
            items.Add(val);
        }

        return items;
    }

    // ── JSON helpers ──────────────────────────────────────────────────────────

    private static string JsonStr(JsonElement el, string prop)
    {
        if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(prop, out var v))
        {
            if (v.ValueKind == JsonValueKind.String) return v.GetString() ?? "";
            if (v.ValueKind == JsonValueKind.Number) return v.ToString();
        }

        return "";
    }

    private static string JsonStr(JsonElement el, string prop, string defaultValue)
    {
        var val = JsonStr(el, prop);
        return string.IsNullOrEmpty(val) ? defaultValue : val;
    }

    private static string? JsonStrOrNull(JsonElement el, string prop)
    {
        if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(prop, out var v))
        {
            if (v.ValueKind == JsonValueKind.String) return v.GetString();
            if (v.ValueKind == JsonValueKind.Number) return v.ToString();
            if (v.ValueKind == JsonValueKind.Null) return null;
        }

        return null;
    }

    private static double JsonDouble(JsonElement el, string prop, double defaultValue)
    {
        if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(prop, out var v))
        {
            if (v.ValueKind == JsonValueKind.Number) return v.GetDouble();
            if (v.ValueKind == JsonValueKind.String && double.TryParse(v.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                return d;
        }

        return defaultValue;
    }

    private static double JsonDouble(JsonElement el, string prop) =>
        JsonDouble(el, prop, 0);

    private static int JsonInt(JsonElement el, string prop, int defaultValue)
    {
        if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(prop, out var v))
        {
            if (v.ValueKind == JsonValueKind.Number) return v.GetInt32();
            if (v.ValueKind == JsonValueKind.String && int.TryParse(v.GetString(), out var i))
                return i;
        }

        return defaultValue;
    }

    private static int? JsonIntOrNull(JsonElement el, string prop)
    {
        if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(prop, out var v))
        {
            if (v.ValueKind == JsonValueKind.Number) return v.GetInt32();
            if (v.ValueKind == JsonValueKind.String && int.TryParse(v.GetString(), out var i))
                return i;
        }

        return null;
    }

    private static bool JsonBool(JsonElement el, string prop, bool defaultValue)
    {
        if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(prop, out var v))
        {
            if (v.ValueKind is JsonValueKind.True or JsonValueKind.False) return v.GetBoolean();
            if (v.ValueKind == JsonValueKind.String) return v.GetString() != "false";
        }

        return defaultValue;
    }

    private static List<string> JsonStringArray(JsonElement el, string prop)
    {
        var result = new List<string>();
        if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(prop, out var arr) &&
            arr.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in arr.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.String)
                    result.Add(item.GetString() ?? "");
                else
                    result.Add(item.ToString());
            }
        }

        return result;
    }

    private static List<Dictionary<string, string>> JsonArrayOfObjects(JsonElement el, string prop)
    {
        var result = new List<Dictionary<string, string>>();
        if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(prop, out var arr) &&
            arr.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in arr.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object) continue;
                var dict = new Dictionary<string, string>();
                foreach (var p in item.EnumerateObject())
                {
                    dict[p.Name] = p.Value.ValueKind == JsonValueKind.String
                        ? p.Value.GetString() ?? ""
                        : p.Value.ToString();
                }

                result.Add(dict);
            }
        }

        return result;
    }

    private static Dictionary<string, string> ParseJsonDimensions(JsonElement md)
    {
        var dims = new Dictionary<string, string>();
        if (md.TryGetProperty("Dimensions", out var dimsArr) && dimsArr.ValueKind == JsonValueKind.Array)
        {
            foreach (var d in dimsArr.EnumerateArray())
            {
                var name = JsonStr(d, "Name");
                var value = JsonStr(d, "Value");
                if (!string.IsNullOrEmpty(name))
                    dims[name] = value;
            }
        }

        return dims;
    }

    private static List<KeyValuePair<string, string>> ParseJsonTags(JsonElement json)
    {
        var tags = new List<KeyValuePair<string, string>>();
        if (json.TryGetProperty("Tags", out var arr) && arr.ValueKind == JsonValueKind.Array)
        {
            foreach (var t in arr.EnumerateArray())
            {
                var k = JsonStr(t, "Key");
                var v = JsonStr(t, "Value");
                if (!string.IsNullOrEmpty(k))
                    tags.Add(new KeyValuePair<string, string>(k, v));
            }
        }

        return tags;
    }

    // ── Timestamp parsing ─────────────────────────────────────────────────────

    private static double? ParseTimestamp(JsonElement el, string prop)
    {
        if (el.ValueKind != JsonValueKind.Object || !el.TryGetProperty(prop, out var v))
            return null;

        if (v.ValueKind == JsonValueKind.Number)
            return v.GetDouble();

        if (v.ValueKind == JsonValueKind.String)
            return ParseTimestampString(v.GetString());

        return null;
    }

    private static double? ParseTimestampString(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;

        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var epoch))
            return epoch;

        if (DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dto))
            return dto.ToUnixTimeMilliseconds() / 1000.0;

        return null;
    }

    private static string EpochToIso(double epoch) =>
        DateTimeOffset.FromUnixTimeSeconds((long)epoch).UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ");

    private static double NowEpoch() =>
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;

    // ── Form-encoded parsing ──────────────────────────────────────────────────

    private static Dictionary<string, string[]> ParseFormBody(byte[] body)
    {
        var result = new Dictionary<string, string[]>(StringComparer.Ordinal);
        if (body.Length == 0) return result;
        var text = Encoding.UTF8.GetString(body);
        if (string.IsNullOrEmpty(text)) return result;

        foreach (var pair in text.Split('&'))
        {
            var eqIdx = pair.IndexOf('=');
            if (eqIdx < 0) continue;
            var key = HttpUtility.UrlDecode(pair[..eqIdx]);
            var val = HttpUtility.UrlDecode(pair[(eqIdx + 1)..]);
            if (result.TryGetValue(key, out var existing))
            {
                var newArr = new string[existing.Length + 1];
                existing.CopyTo(newArr, 0);
                newArr[existing.Length] = val;
                result[key] = newArr;
            }
            else
            {
                result[key] = [val];
            }
        }

        return result;
    }

    private static string FormP(Dictionary<string, string[]> p, string key)
    {
        return p.TryGetValue(key, out var vals) && vals.Length > 0 ? vals[0] : "";
    }

    private static string FormP(Dictionary<string, string[]> p, string key, string defaultValue)
    {
        var val = FormP(p, key);
        return string.IsNullOrEmpty(val) ? defaultValue : val;
    }

    private static string? FormPOrNull(Dictionary<string, string[]> p, string key)
    {
        var val = FormP(p, key);
        return string.IsNullOrEmpty(val) ? null : val;
    }

    private static Dictionary<string, string> ParseFormDimensions(Dictionary<string, string[]> formParams, string prefix)
    {
        var dims = new Dictionary<string, string>();
        for (var j = 1; ; j++)
        {
            var name = FormP(formParams, $"{prefix}.Dimensions.member.{j}.Name");
            if (string.IsNullOrEmpty(name)) break;
            dims[name] = FormP(formParams, $"{prefix}.Dimensions.member.{j}.Value");
        }

        return dims;
    }

    // ── Number parsing helpers ────────────────────────────────────────────────

    private static int ParseIntOrDefault(string value, int defaultValue)
    {
        if (string.IsNullOrEmpty(value)) return defaultValue;
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result)
            ? result
            : defaultValue;
    }

    private static double ParseDoubleOrDefault(string value, double defaultValue)
    {
        if (string.IsNullOrEmpty(value)) return defaultValue;
        return double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var result)
            ? result
            : defaultValue;
    }

    // ── XML / JSON / CBOR response builders ───────────────────────────────────

    private static readonly IReadOnlyDictionary<string, string> XmlHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "application/xml" };

    private static readonly IReadOnlyDictionary<string, string> JsonResponseHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "application/x-amz-json-1.0" };

    private static readonly IReadOnlyDictionary<string, string> CborResponseHeaders =
        new Dictionary<string, string>
        {
            ["Content-Type"] = "application/cbor",
            ["smithy-protocol"] = "rpc-v2-cbor",
        };

    private static ServiceResponse XmlResponse(int status, string rootTag, string inner)
    {
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                   + $"<{rootTag} xmlns=\"{CwNs}\">\n"
                   + $"    {inner}\n"
                   + $"    <ResponseMetadata><RequestId>{requestId}</RequestId></ResponseMetadata>\n"
                   + $"</{rootTag}>";
        return new ServiceResponse(status, XmlHeaders, Encoding.UTF8.GetBytes(body));
    }

    private static ServiceResponse JsonOk(Dictionary<string, object?> data)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(data, MicroStackJsonContext.Default.DictionaryStringObject);
        return new ServiceResponse(200, JsonResponseHeaders, json);
    }

    private static ServiceResponse CborOk(Dictionary<string, object?> data)
    {
        var cborBytes = EncodeToCbor(data);
        return new ServiceResponse(200, CborResponseHeaders, cborBytes);
    }

    private static ServiceResponse ErrorResponse(string code, string message, int status, bool isCbor, bool isJson)
    {
        if (isCbor)
        {
            var data = new Dictionary<string, object?> { ["__type"] = code, ["message"] = message };
            var cborBytes = EncodeToCbor(data);
            var headers = new Dictionary<string, string>
            {
                ["Content-Type"] = "application/cbor",
                ["smithy-protocol"] = "rpc-v2-cbor",
                ["x-amzn-errortype"] = code,
            };
            return new ServiceResponse(status, headers, cborBytes);
        }

        if (isJson)
        {
            var json = JsonSerializer.SerializeToUtf8Bytes(new AwsJsonError(code, message), MicroStackJsonContext.Default.AwsJsonError);
            return new ServiceResponse(status, JsonResponseHeaders, json);
        }

        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                   + $"<ErrorResponse xmlns=\"{CwNs}\">\n"
                   + $"    <Error><Code>{code}</Code><Message>{Esc(message)}</Message></Error>\n"
                   + $"    <RequestId>{requestId}</RequestId>\n"
                   + "</ErrorResponse>";
        return new ServiceResponse(status, XmlHeaders, Encoding.UTF8.GetBytes(body));
    }

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false,
    };

    // ── CBOR encoding ───────────────────────────────────────────────────────────

    private static byte[] EncodeToCbor(object? value)
    {
        var writer = new CborWriter(CborConformanceMode.Lax);
        WriteCborValue(writer, value);
        return writer.Encode();
    }

    private static void WriteCborValue(CborWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNull();
                break;
            case CborEpochTimestamp ts:
                writer.WriteTag(CborTag.UnixTimeSeconds);
                writer.WriteDouble(ts.Epoch);
                break;
            case bool b:
                writer.WriteBoolean(b);
                break;
            case int i:
                writer.WriteInt32(i);
                break;
            case long l:
                writer.WriteInt64(l);
                break;
            case ulong u:
                writer.WriteUInt64(u);
                break;
            case double d:
                writer.WriteDouble(d);
                break;
            case float f:
                writer.WriteDouble(f);
                break;
            case string s:
                writer.WriteTextString(s);
                break;
            case Dictionary<string, object?> dict:
                writer.WriteStartMap(dict.Count);
                foreach (var (k, v) in dict)
                {
                    writer.WriteTextString(k);
                    WriteCborValue(writer, v);
                }
                writer.WriteEndMap();
                break;
            case Dictionary<string, string> sdict:
                writer.WriteStartMap(sdict.Count);
                foreach (var (k, v) in sdict)
                {
                    writer.WriteTextString(k);
                    writer.WriteTextString(v);
                }
                writer.WriteEndMap();
                break;
            case Dictionary<string, double> ddict:
                writer.WriteStartMap(ddict.Count);
                foreach (var (k, v) in ddict)
                {
                    writer.WriteTextString(k);
                    writer.WriteDouble(v);
                }
                writer.WriteEndMap();
                break;
            case IList<object> objList:
                writer.WriteStartArray(objList.Count);
                foreach (var item in objList)
                    WriteCborValue(writer, item);
                writer.WriteEndArray();
                break;
            case IList<string> strList:
                writer.WriteStartArray(strList.Count);
                foreach (var item in strList)
                    writer.WriteTextString(item);
                writer.WriteEndArray();
                break;
            case IList<double> dblList:
                writer.WriteStartArray(dblList.Count);
                foreach (var item in dblList)
                    writer.WriteDouble(item);
                writer.WriteEndArray();
                break;
            case IList<CborEpochTimestamp> tsList:
                writer.WriteStartArray(tsList.Count);
                foreach (var item in tsList)
                {
                    writer.WriteTag(CborTag.UnixTimeSeconds);
                    writer.WriteDouble(item.Epoch);
                }
                writer.WriteEndArray();
                break;
            case IList<Dictionary<string, string>> listDictStr:
                writer.WriteStartArray(listDictStr.Count);
                foreach (var item in listDictStr)
                    WriteCborValue(writer, item);
                writer.WriteEndArray();
                break;
            case IList<Dictionary<string, object>> listDictObj:
                writer.WriteStartArray(listDictObj.Count);
                foreach (var item in listDictObj)
                    WriteCborValue(writer, item);
                writer.WriteEndArray();
                break;
            default:
                // For anonymous types and other objects, serialize to JSON first then re-encode
                var json = DictionaryObjectJsonConverter.SerializeValue(value);
                using (var doc = JsonDocument.Parse(json))
                {
                    WriteJsonElementAsCbor(writer, doc.RootElement);
                }
                break;
        }
    }

    private static void WriteJsonElementAsCbor(CborWriter writer, JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var props = element.EnumerateObject().ToList();
                writer.WriteStartMap(props.Count);
                foreach (var prop in props)
                {
                    writer.WriteTextString(prop.Name);
                    WriteJsonElementAsCbor(writer, prop.Value);
                }
                writer.WriteEndMap();
                break;
            case JsonValueKind.Array:
                var items = element.EnumerateArray().ToList();
                writer.WriteStartArray(items.Count);
                foreach (var item in items)
                    WriteJsonElementAsCbor(writer, item);
                writer.WriteEndArray();
                break;
            case JsonValueKind.String:
                writer.WriteTextString(element.GetString() ?? "");
                break;
            case JsonValueKind.Number:
                if (element.TryGetInt64(out var longVal))
                    writer.WriteInt64(longVal);
                else
                    writer.WriteDouble(element.GetDouble());
                break;
            case JsonValueKind.True:
                writer.WriteBoolean(true);
                break;
            case JsonValueKind.False:
                writer.WriteBoolean(false);
                break;
            case JsonValueKind.Null:
            default:
                writer.WriteNull();
                break;
        }
    }

    // ── CBOR decoding ─────────────────────────────────────────────────────────

    private static JsonElement TryCborDecode(byte[] data)
    {
        try
        {
            var reader = new CborReader(data, CborConformanceMode.Lax);
            var result = ReadCborAsObject(reader);
            if (result is not null)
            {
                var json = DictionaryObjectJsonConverter.SerializeValue(result);
                using var doc = JsonDocument.Parse(json);
                return doc.RootElement.Clone();
            }
        }
        catch
        {
            // Fall through on decode failure
        }

        return default;
    }

    private static object? ReadCborAsObject(CborReader reader)
    {
        var state = reader.PeekState();
        switch (state)
        {
            case CborReaderState.UnsignedInteger:
                return reader.ReadUInt64();
            case CborReaderState.NegativeInteger:
                return reader.ReadInt64();
            case CborReaderState.TextString:
                return reader.ReadTextString();
            case CborReaderState.ByteString:
                return Convert.ToBase64String(reader.ReadByteString());
            case CborReaderState.StartArray:
            {
                var count = reader.ReadStartArray();
                var list = new List<object?>();
                while (reader.PeekState() != CborReaderState.EndArray)
                    list.Add(ReadCborAsObject(reader));
                reader.ReadEndArray();
                return list;
            }
            case CborReaderState.StartMap:
            {
                var count = reader.ReadStartMap();
                var dict = new Dictionary<string, object?>();
                while (reader.PeekState() != CborReaderState.EndMap)
                {
                    var key = reader.ReadTextString();
                    var val = ReadCborAsObject(reader);
                    dict[key] = val;
                }
                reader.ReadEndMap();
                return dict;
            }
            case CborReaderState.Boolean:
                return reader.ReadBoolean();
            case CborReaderState.Null:
                reader.ReadNull();
                return null;
            case CborReaderState.HalfPrecisionFloat:
            case CborReaderState.SinglePrecisionFloat:
            case CborReaderState.DoublePrecisionFloat:
                return reader.ReadDouble();
            case CborReaderState.Tag:
                reader.ReadTag();
                return ReadCborAsObject(reader);
            default:
                reader.SkipValue();
                return null;
        }
    }

    // ── CBOR timestamp helpers ────────────────────────────────────────────────

    private static readonly HashSet<string> TimestampFields =
    [
        "StateUpdatedTimestamp",
        "AlarmConfigurationUpdatedTimestamp",
        "Timestamp",
        "LastModified",
    ];

    /// <summary>
    /// Creates a CBOR-safe copy of a dictionary, wrapping epoch-double timestamp
    /// fields in <see cref="CborEpochTimestamp"/> so the encoder emits tag(1, value).
    /// </summary>
    private static Dictionary<string, object?> CborTagTimestamps(Dictionary<string, object?> source)
    {
        var result = new Dictionary<string, object?>(source.Count);
        foreach (var (key, value) in source)
        {
            if (TimestampFields.Contains(key) && value is double epoch)
                result[key] = new CborEpochTimestamp(epoch);
            else
                result[key] = value;
        }

        return result;
    }

    /// <summary>
    /// Overload for string-valued dictionaries (e.g. alarm history items)
    /// where timestamps are stored as ISO strings that must be parsed back to epoch.
    /// </summary>
    private static Dictionary<string, object?> CborTagTimestamps(Dictionary<string, string> source)
    {
        var result = new Dictionary<string, object?>(source.Count);
        foreach (var (key, value) in source)
        {
            if (TimestampFields.Contains(key))
            {
                var epoch = ParseTimestampString(value);
                result[key] = epoch.HasValue ? new CborEpochTimestamp(epoch.Value) : value;
            }
            else
            {
                result[key] = value;
            }
        }

        return result;
    }

    /// <summary>
    /// Converts an ISO timestamp string in a datapoint dictionary to a CBOR epoch tag.
    /// </summary>
    private static Dictionary<string, object> CborTagTimestampsObj(Dictionary<string, object> source)
    {
        var result = new Dictionary<string, object>(source.Count);
        foreach (var (key, value) in source)
        {
            if (TimestampFields.Contains(key) && value is string isoStr)
            {
                var epoch = ParseTimestampString(isoStr);
                result[key] = epoch.HasValue ? new CborEpochTimestamp(epoch.Value) : value;
            }
            else if (TimestampFields.Contains(key) && value is double epoch2)
            {
                result[key] = new CborEpochTimestamp(epoch2);
            }
            else
            {
                result[key] = value;
            }
        }

        return result;
    }

    // ── String helpers ────────────────────────────────────────────────────────

    private static string Str(Dictionary<string, object?> dict, string key)
    {
        return dict.TryGetValue(key, out var v) ? v?.ToString() ?? "" : "";
    }

    private static string Str(Dictionary<string, object?> dict, string key, string defaultValue)
    {
        var val = Str(dict, key);
        return string.IsNullOrEmpty(val) ? defaultValue : val;
    }

    private static string Esc(string value) =>
        System.Security.SecurityElement.Escape(value) ?? value;

    [GeneratedRegex(@"/operation/([^/?]+)")]
    private static partial Regex OperationPathRegex();

    // ── Data types ────────────────────────────────────────────────────────────

    /// <summary>
    /// Wraps an epoch double so the CBOR encoder emits tag(1, value).
    /// The AWS SDK CBOR unmarshaller expects CBOR tag 1 for DateTime fields.
    /// </summary>
    private readonly record struct CborEpochTimestamp(double Epoch);

    private sealed class MetricDataPoint
    {
        internal double Timestamp { get; init; }
        internal double Value { get; init; }
        internal string Unit { get; init; } = "None";
        internal Dictionary<string, string> Dimensions { get; init; } = new();
    }

    private sealed class MetricDataQuery
    {
        internal string Id { get; init; } = "";
        internal string Label { get; init; } = "";
        internal bool ReturnData { get; init; } = true;
        internal string? Expression { get; init; }
        internal string? MetricNamespace { get; init; }
        internal string? MetricName { get; init; }
        internal int Period { get; init; } = 60;
        internal string Stat { get; init; } = "Average";
    }
}
