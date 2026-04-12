namespace MicroStack.Internal;

/// <summary>
/// Strongly-typed configuration options for MicroStack.
/// Consolidates all environment variable reads into a single location.
/// Set once at startup via <see cref="Instance"/> and consumed throughout the application.
/// </summary>
internal sealed class MicroStackOptions
{
    /// <summary>
    /// Singleton instance set once at startup in Program.cs.
    /// All service handlers and infrastructure read from this.
    /// </summary>
    internal static MicroStackOptions Instance { get; private set; } = new();

    internal int GatewayPort { get; set; } = 4566;

    internal string Host { get; set; } = "localhost";

    internal string Region { get; set; } = "us-east-1";

    internal string DefaultAccountId { get; set; } = "000000000000";

    internal bool PersistState { get; set; }

    internal string StateDir { get; set; } = Path.Combine(Path.GetTempPath(), "ministack-state");

    internal bool S3Persist { get; set; }

    internal string? Services { get; set; }

    /// <summary>
    /// Binds configuration from environment variables.
    /// Called once at startup. Sets <see cref="Instance"/> for global access.
    /// </summary>
    internal static MicroStackOptions BindFromEnvironment()
    {
        var options = new MicroStackOptions();

        var portStr = Environment.GetEnvironmentVariable("GATEWAY_PORT")
                      ?? Environment.GetEnvironmentVariable("EDGE_PORT");
        if (int.TryParse(portStr, out var port))
        {
            options.GatewayPort = port;
        }

        options.Host = Environment.GetEnvironmentVariable("MINISTACK_HOST") ?? options.Host;
        options.Region = Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? options.Region;
        options.DefaultAccountId = Environment.GetEnvironmentVariable("MINISTACK_ACCOUNT_ID") ?? options.DefaultAccountId;
        options.PersistState = Environment.GetEnvironmentVariable("PERSIST_STATE") == "1";
        options.StateDir = Environment.GetEnvironmentVariable("STATE_DIR") ?? options.StateDir;
        options.Services = Environment.GetEnvironmentVariable("SERVICES")?.Trim();

        // Handle LOCALSTACK_PERSISTENCE=1 → S3_PERSIST=true compatibility
        var s3Persist = Environment.GetEnvironmentVariable("S3_PERSIST") == "1";
        var localstackPersistence = Environment.GetEnvironmentVariable("LOCALSTACK_PERSISTENCE") == "1";
        options.S3Persist = s3Persist || localstackPersistence;

        Instance = options;
        return options;
    }
}
