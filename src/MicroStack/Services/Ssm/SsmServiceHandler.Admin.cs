using System.Globalization;
using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Ssm;

internal sealed partial class SsmServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds = [new("parameter", "Parameters")];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == "ssm" ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != "ssm") return [];
        lock (_lock)
        {
            return _parameters.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => ParameterNode(x.Key, x.Value.Arn, x.Value.Type, x.Value.Version)).ToArray();
        }
    }

    private AdminNode ParameterNode(string name, string arn, string type, int version)
    {
        var secure = type == "SecureString";
        return AdminData.Node("parameter", name, name, arn, $"{type} v{version}") with
        {
            ReadFields = () => ReadParameterFields(name),
            ReadChildren = () => ReadParameterChildren(name),
            ReadContent = () => ReadParameterContent(name),
            RevealableFields = secure ? ["Value"] : [],
            RevealField = secure ? field => RevealParameter(name, field) : null,
        };
    }

    private IReadOnlyList<AdminField> ReadParameterFields(string name)
    {
        lock (_lock)
        {
            if (!_parameters.TryGetValue(name, out var parameter)) return [];
            var secure = parameter.Type == "SecureString";
            return
            [
                AdminData.Field("Value", secure ? null : parameter.OriginalValue, secure, secure),
                AdminData.Field("Type", parameter.Type),
                AdminData.Field("Version", parameter.Version.ToString(CultureInfo.InvariantCulture)),
                AdminData.Field("Description", parameter.Description),
                AdminData.Field("Tier", parameter.Tier),
                AdminData.Field("Data type", parameter.DataType),
                AdminData.Field("Key ID", parameter.KeyId),
                AdminData.Field("Allowed pattern", parameter.AllowedPattern),
                AdminData.Field("Last modified", Seconds(parameter.LastModifiedDate), format: "datetime"),
            ];
        }
    }

    private AdminContent ReadParameterContent(string name)
    {
        lock (_lock)
        {
            if (!_parameters.TryGetValue(name, out var parameter))
                return AdminData.Unavailable("Parameter no longer exists.", "text/plain");
            return parameter.Type == "SecureString"
                ? AdminData.Unavailable("SecureString values require an explicit reveal.", "text/plain", sensitive: true)
                : AdminData.Text(parameter.OriginalValue);
        }
    }

    private AdminContent RevealParameter(string name, string field)
    {
        lock (_lock)
        {
            if (field != "Value" || !_parameters.TryGetValue(name, out var parameter) ||
                parameter.Type != "SecureString")
                return AdminData.Unavailable("The requested revealable field does not exist.", "text/plain", sensitive: true);
            return AdminData.Text(parameter.OriginalValue, sensitive: true);
        }
    }

    private IEnumerable<AdminNode> ReadParameterChildren(string name)
    {
        lock (_lock)
        {
            var nodes = new List<AdminNode>();
            if (_parameterHistory.TryGetValue(name, out var history))
            {
                nodes.AddRange(history.OrderByDescending(x => x.Version).Select(entry =>
                {
                    var secure = entry.Type == "SecureString";
                    var version = entry.Version;
                    var value = entry.OriginalValue;
                    var modified = Seconds(entry.LastModifiedDate);
                    var modifiedBy = entry.LastModifiedUser;
                    var description = entry.Description;
                    var labels = string.Join(", ", entry.Labels);
                    return AdminData.Node("parameter-version", version.ToString(CultureInfo.InvariantCulture),
                        $"Version {version}", status: entry.Type) with
                    {
                        ReadFields = () =>
                        [
                            AdminData.Field("Value", secure ? null : value, secure, secure),
                            AdminData.Field("Modified", modified, format: "datetime"),
                            AdminData.Field("Modified by", modifiedBy),
                            AdminData.Field("Description", description),
                            AdminData.Field("Labels", labels),
                        ],
                        ReadContent = () => secure
                            ? AdminData.Unavailable("SecureString values require an explicit reveal.", "text/plain", sensitive: true)
                            : AdminData.Text(value),
                        RevealableFields = secure ? ["Value"] : [],
                        RevealField = secure ? field => field == "Value"
                            ? AdminData.Text(value, sensitive: true)
                            : AdminData.Unavailable("The requested revealable field does not exist.", "text/plain", sensitive: true) : null,
                    };
                }));
            }

            if (_parameters.TryGetValue(name, out var parameter) && _tags.TryGetValue(parameter.Arn, out var tags))
                nodes.AddRange(tags.OrderBy(x => x.Key, StringComparer.Ordinal).Select(x =>
                    AdminData.Node("tag", x.Key, x.Key) with
                    {
                        ReadFields = () => [AdminData.Field("Value", x.Value)],
                    }));
            return nodes;
        }
    }

    private static string Seconds(double value) =>
        AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds((long)(value * 1000)));
}
