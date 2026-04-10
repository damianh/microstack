namespace MicroStack.Internal;

/// <summary>
/// Registry of all IServiceHandler instances, resolved by service name.
/// Services register themselves here at startup.
/// Handles the SERVICES env var filter and LOCALSTACK_PERSISTENCE compatibility.
/// Port of ministack/app.py SERVICE_NAME_ALIASES and SERVICES filtering logic.
/// </summary>
internal sealed class ServiceRegistry
{
    private readonly Dictionary<string, IServiceHandler> _handlers = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string>? _allowedServices;

    internal static readonly Dictionary<string, string> Aliases = new(StringComparer.OrdinalIgnoreCase)
    {
        ["cloudwatch-logs"]  = "logs",
        ["cloudwatch"]       = "monitoring",
        ["eventbridge"]      = "events",
        ["step-functions"]   = "states",
        ["stepfunctions"]    = "states",
        ["execute-api"]      = "apigateway",
        ["apigatewayv2"]     = "apigateway",
        ["kinesis-firehose"] = "firehose",
        ["elbv2"]            = "elasticloadbalancing",
        ["elb"]              = "elasticloadbalancing",
        ["route53"]          = "route53",
        ["cognito-idp"]      = "cognito-idp",
        ["cognito-identity"] = "cognito-identity",
        ["ecr"]              = "ecr",
        ["rds-data"]         = "rds-data",
    };

    internal ServiceRegistry()
    {
        // Handle LOCALSTACK_PERSISTENCE=1 -> S3_PERSIST=1 compatibility
        if (Environment.GetEnvironmentVariable("LOCALSTACK_PERSISTENCE") == "1"
            && Environment.GetEnvironmentVariable("S3_PERSIST") != "1")
        {
            Environment.SetEnvironmentVariable("S3_PERSIST", "1");
        }

        // Parse SERVICES env var filter
        var servicesEnv = Environment.GetEnvironmentVariable("SERVICES")?.Trim();
        if (!string.IsNullOrEmpty(servicesEnv))
        {
            _allowedServices = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var name in servicesEnv.Split(','))
            {
                var trimmed = name.Trim();
                if (string.IsNullOrEmpty(trimmed)) continue;
                var canonical = Aliases.TryGetValue(trimmed, out var alias) ? alias : trimmed;
                _allowedServices.Add(canonical);
            }
        }
    }

    internal void Register(IServiceHandler handler)
    {
        // Respect SERVICES filter — skip if not in allowed set
        if (_allowedServices is not null && !_allowedServices.Contains(handler.ServiceName))
            return;

        _handlers[handler.ServiceName] = handler;
    }

    internal IServiceHandler? Resolve(string serviceName)
    {
        if (Aliases.TryGetValue(serviceName, out var canonical))
            serviceName = canonical;

        return _handlers.GetValueOrDefault(serviceName);
    }

    internal IReadOnlyCollection<IServiceHandler> All => _handlers.Values;

    internal void ResetAll()
    {
        foreach (var handler in _handlers.Values)
            handler.Reset();
    }

    /// <summary>Returns service names and their availability status for the health endpoint.</summary>
    internal Dictionary<string, string> GetServiceStatus()
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, _) in _handlers)
            result[name] = "available";
        return result;
    }
}
