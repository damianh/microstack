using System.Globalization;
using System.Text;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.CloudWatch;

internal sealed partial class CloudWatchServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("metric", "Metrics"), new("alarm", "Alarms"), new("dashboard", "Dashboards"),
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == "cloudwatch" ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != "cloudwatch") return [];
        lock (_lock)
        {
            var nodes = new List<AdminNode>();
            nodes.AddRange(_metrics.Items.OrderBy(x => x.Key, StringComparer.Ordinal).Select(x =>
            {
                ParseMetricKey(x.Key, out var ns, out var name, out var dimensions);
                return MetricNode(x.Key, ns, name, dimensions);
            }));
            nodes.AddRange(_alarms.Items.OrderBy(x => x.Key, StringComparer.Ordinal).Select(x => AlarmNode(x.Key, false)));
            nodes.AddRange(_compositeAlarms.Items.OrderBy(x => x.Key, StringComparer.Ordinal).Select(x => AlarmNode(x.Key, true)));
            nodes.AddRange(_dashboards.Items.OrderBy(x => x.Key, StringComparer.Ordinal).Select(x => DashboardNode(x.Key)));
            return nodes;
        }
    }

    private static string AdminId(string value) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes(value)).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private AdminNode MetricNode(string key, string metricNamespace, string name, string dimensions) =>
        AdminData.Node("metric", AdminId(key), $"{metricNamespace} / {name}") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Namespace", metricNamespace),
                AdminData.Field("Metric name", name),
                AdminData.Field("Dimensions", dimensions),
            ],
            ReadChildren = () => ReadMetricPoints(key),
        };

    private IEnumerable<AdminNode> ReadMetricPoints(string key)
    {
        lock (_lock)
        {
            if (!_metrics.TryGetValue(key, out var points)) return [];
            return points.Select((point, index) =>
            {
                var timestamp = DateTimeOffset.FromUnixTimeMilliseconds((long)(point.Timestamp * 1000));
                var dimensions = point.Dimensions.ToDictionary(x => x.Key, x => (object?)x.Value);
                var fields = point.Dimensions.OrderBy(x => x.Key, StringComparer.Ordinal)
                    .Select(x => AdminData.Field(x.Key, x.Value)).Prepend(AdminData.Field("Unit", point.Unit)).ToArray();
                return AdminData.Node("datapoint", $"{index}:{point.Timestamp:R}", AdminData.IsoUtc(timestamp),
                    status: point.Value.ToString("R", CultureInfo.InvariantCulture)) with
                {
                    ReadFields = () => fields,
                    ReadContent = () => AdminData.Json(new Dictionary<string, object?>
                    {
                        ["timestamp"] = AdminData.IsoUtc(timestamp), ["value"] = point.Value,
                        ["unit"] = point.Unit, ["dimensions"] = dimensions,
                    }),
                };
            }).ToArray();
        }
    }

    private AdminNode AlarmNode(string name, bool composite)
    {
        string arn;
        string state;
        lock (_lock)
        {
            var alarm = composite ? _compositeAlarms[name] : _alarms[name];
            arn = Str(alarm, "AlarmArn");
            state = Str(alarm, "StateValue");
        }
        return AdminData.Node("alarm", (composite ? "composite:" : "metric:") + name, name,
            arn, state) with
        {
            ReadFields = () => ReadAlarmFields(name, composite),
            ReadConnections = () => ReadAlarmConnections(name, composite),
        };
    }

    private IReadOnlyList<AdminField> ReadAlarmFields(string name, bool composite)
    {
        lock (_lock)
        {
            if (!(composite ? _compositeAlarms : _alarms).TryGetValue(name, out var alarm)) return [];
            return alarm.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => AdminData.Field(x.Key, Scalar(x.Value))).ToArray();
        }
    }

    private IReadOnlyList<AdminConnection> ReadAlarmConnections(string name, bool composite)
    {
        lock (_lock)
        {
            if (composite || !_alarms.TryGetValue(name, out var alarm)) return [];
            var metricName = Str(alarm, "MetricName");
            var metricNamespace = Str(alarm, "Namespace");
            var match = _metrics.Keys.FirstOrDefault(key =>
            {
                ParseMetricKey(key, out var ns, out var mn, out _);
                return ns == metricNamespace && mn == metricName;
            });
            return match is null ? [] :
            [
                new("Metric", "configured", "cloudwatch", [new("metric", AdminId(match))]),
            ];
        }
    }

    private AdminNode DashboardNode(string name) =>
        AdminData.Node("dashboard", name, name,
            $"arn:aws:cloudwatch::{AccountContext.GetAccountId()}:dashboard/{name}") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_dashboards.TryGetValue(name, out var dashboard)) return [];
                    return
                    [
                        AdminData.Field("Last modified", Epoch(dashboard.GetValueOrDefault("LastModified")), format: "datetime"),
                        AdminData.Field("Size", Scalar(dashboard.GetValueOrDefault("Size")), format: "bytes"),
                    ];
                }
            },
            ReadContent = () =>
            {
                lock (_lock)
                    return _dashboards.TryGetValue(name, out var dashboard)
                        ? AdminData.JsonText(Str(dashboard, "DashboardBody"))
                        : AdminData.Unavailable("Dashboard no longer exists.", "application/json");
            },
        };

    private static string? Scalar(object? value) => value switch
    {
        null => null, string text => text, bool flag => flag ? "true" : "false",
        IFormattable valueWithFormat => valueWithFormat.ToString(null, CultureInfo.InvariantCulture),
        IEnumerable<string> strings => string.Join(", ", strings), _ => value.ToString(),
    };

    private static string? Epoch(object? value) =>
        value is double epoch ? AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds((long)(epoch * 1000))) : Scalar(value);
}
