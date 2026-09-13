using MicroStack.Internal;
using MicroStack.Services.DynamoDb;
using MicroStack.Services.Ec2;
using MicroStack.Services.Iam;
using MicroStack.Services.S3;
using MicroStack.Services.Sns;
using MicroStack.Services.Sqs;
using MicroStack.Services.SecretsManager;
using MicroStack.Services.Ssm;
using MicroStack.Services.Kms;
using MicroStack.Services.ApiGateway;
using MicroStack.Services.Lambda;
using MicroStack.Services.StepFunctions;
using MicroStack.Services.Alb;
using MicroStack.Services.Route53;
using MicroStack.Services.Acm;
using MicroStack.Services.CloudWatchLogs;
using MicroStack.Services.CloudWatch;
using MicroStack.Services.Sts;
using MicroStack.Services.Ecs;
using MicroStack.Services.Rds;
using MicroStack.Services.ElastiCache;
using MicroStack.Services.Ecr;
using MicroStack.Services.RdsData;
using MicroStack.Services.EventBridge;
using MicroStack.Services.Kinesis;
using MicroStack.Services.Firehose;
using MicroStack.Services.Glue;
using MicroStack.Services.Athena;
using MicroStack.Services.Ses;
using MicroStack.Services.Waf;
using MicroStack.Services.Efs;
using MicroStack.Services.Emr;
using MicroStack.Services.AppSync;
using MicroStack.Services.CloudFront;
using MicroStack.Services.ServiceDiscovery;
using MicroStack.Services.CloudFormation;
using MicroStack.Services.Cognito;
using MicroStack.Services.S3Files;
using MicroStack.Internal.Admin;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseStaticWebAssets();

// Configure JSON serialization for minimal API endpoints (required for native AOT)
builder.Services.ConfigureHttpJsonOptions(jsonOptions =>
{
    jsonOptions.SerializerOptions.TypeInfoResolverChain.Insert(0, MicroStackJsonContext.Default);
});

// Bind all environment variables into strongly-typed options (single source of truth)
var options = MicroStackOptions.BindFromEnvironment();
builder.Services.AddSingleton(options);

const string adminCorsPolicy = "MicroStackAdmin";
builder.Services.AddCors(cors => cors.AddPolicy(adminCorsPolicy, policy =>
    policy.SetIsOriginAllowed(_ => false).WithMethods("GET", "POST", "DELETE").AllowAnyHeader()));

if (!builder.Environment.IsEnvironment("Testing"))
    builder.WebHost.UseUrls($"http://0.0.0.0:{options.GatewayPort}");

// Configure static services that can't use constructor injection
AccountContext.Configure(options);

// Register core infrastructure
// Note: ServiceRegistry and StatePersistence have internal constructors (per coding standards),
// so we use factory lambdas instead of letting DI locate a public constructor.
builder.Services.AddSingleton<AwsServiceRouter>();
builder.Services.AddSingleton<ServiceRegistry>(_ => new ServiceRegistry(options));
builder.Services.AddSingleton<RequestLog>(_ => new RequestLog());
builder.Services.AddSingleton<StatePersistence>(sp => new StatePersistence(
    sp.GetRequiredService<ILogger<StatePersistence>>(),
    sp.GetRequiredService<ServiceRegistry>(),
    options));

var app = builder.Build();

var adminUiEnabled = AdminUiHosting.Map(app, options);
foreach (var obsoleteSetting in new[] { "MICROSTACK_UI_PORT", "MICROSTACK_API_URL" })
{
    if (Environment.GetEnvironmentVariable(obsoleteSetting) is not null)
        app.Logger.LogWarning("{Setting} is ignored because the UI is served from the gateway at /ui/.", obsoleteSetting);
}

// Restore persisted state on startup
var persistence = app.Services.GetRequiredService<StatePersistence>();
persistence.RestoreAll();

// Wire up admin endpoints before the main AWS middleware
var registry = app.Services.GetRequiredService<ServiceRegistry>();

// Register service handlers
var sqsHandler = new SqsServiceHandler();
registry.Register(sqsHandler);
var ddbHandler = new DynamoDbServiceHandler();
registry.Register(ddbHandler);
registry.Register(new S3ServiceHandler());
registry.Register(new SnsServiceHandler(sqsHandler));
var iamHandler = new IamServiceHandler();
registry.Register(iamHandler);
registry.Register(new StsServiceHandler(iamHandler));
registry.Register(new SecretsManagerServiceHandler());
registry.Register(new SsmServiceHandler());
registry.Register(new KmsServiceHandler());
var lambdaHandler = new LambdaServiceHandler(sqsHandler, ddbHandler);
registry.Register(lambdaHandler);
registry.Register(new ApiGatewayV2ServiceHandler(lambdaHandler));
var sfnHandler = new StepFunctionsServiceHandler(lambdaHandler, registry);
registry.Register(sfnHandler);
registry.Register(new Ec2ServiceHandler());
registry.Register(new AlbServiceHandler());
var route53Handler = new Route53ServiceHandler();
registry.Register(route53Handler);
registry.Register(new AcmServiceHandler());
registry.Register(new CloudWatchLogsServiceHandler());
registry.Register(new CloudWatchServiceHandler());
registry.Register(new EcsServiceHandler());
registry.Register(new RdsServiceHandler());
registry.Register(new ElastiCacheServiceHandler());
registry.Register(new EcrServiceHandler());
registry.Register(new RdsDataServiceHandler());
registry.Register(new EventBridgeServiceHandler());
registry.Register(new KinesisServiceHandler());
registry.Register(new FirehoseServiceHandler());
registry.Register(new GlueServiceHandler());
registry.Register(new AthenaServiceHandler());
registry.Register(new SesServiceHandler());
registry.Register(new WafServiceHandler());
registry.Register(new EfsServiceHandler());
registry.Register(new EmrServiceHandler());
registry.Register(new AppSyncServiceHandler());
registry.Register(new CloudFrontServiceHandler());
registry.Register(new ServiceDiscoveryServiceHandler(route53Handler));
var cognitoIdpHandler = new CognitoIdpServiceHandler();
registry.Register(cognitoIdpHandler);
registry.Register(new CognitoIdentityServiceHandler(cognitoIdpHandler));
registry.Register(new CloudFormationServiceHandler(registry));
registry.Register(new S3FilesServiceHandler());

app.MapAdminApi(registry, app.Services.GetRequiredService<RequestLog>(), options, adminCorsPolicy);

// Health endpoint (multiple aliases for LocalStack compatibility)
foreach (var healthPath in new[] { "/_microstack/health", "/health", "/_localstack/health" })
{
    app.MapGet(healthPath, () =>
    {
        var services = registry.GetServiceStatus();
        return Results.Ok(new HealthResponse(services, "light", "0.1.0"));
    }).RequireCors(adminCorsPolicy);
}

// Reset endpoint
app.MapPost("/_microstack/reset", () =>
{
    registry.ResetAll();
    persistence.DeleteAll();
    return Results.Ok(new ResetResponse("ok"));
}).RequireCors(adminCorsPolicy);

// Config endpoint (stub — populated when services implement it)
app.MapPost("/_microstack/config", async (HttpContext ctx) =>
{
    using var reader = new StreamReader(ctx.Request.Body);
    var bodyText = await reader.ReadToEndAsync();
    var applied = new Dictionary<string, string>();
    if (!string.IsNullOrEmpty(bodyText))
    {
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(bodyText);
            if (doc.RootElement.TryGetProperty("stepfunctions", out var sfnEl)
                && sfnEl.ValueKind == System.Text.Json.JsonValueKind.Object
                && sfnEl.TryGetProperty("_sfn_mock_config", out var mockEl))
            {
                var mockConfig = DictionaryObjectJsonConverter.DeserializeElementDeep(mockEl);
                sfnHandler.SetMockConfig(mockConfig);
                applied["stepfunctions._sfn_mock_config"] = "applied";
            }
        }
        catch (System.Text.Json.JsonException)
        {
            // Ignore invalid JSON
        }
    }

    return Results.Ok(new ConfigResponse(applied));
}).RequireCors(adminCorsPolicy);

// Request log endpoint
app.MapGet("/_microstack/requests", (HttpContext ctx) =>
{
    var limitText = ctx.Request.Query["limit"].ToString();
    var limit = int.TryParse(limitText, out var parsed) ? parsed : 1000;
    var requestLog = ctx.RequestServices.GetRequiredService<RequestLog>();
    return Results.Ok(requestLog.GetEntries(limit));
}).RequireCors(adminCorsPolicy);

// Request log clear endpoint
app.MapDelete("/_microstack/requests", (RequestLog requestLog) =>
{
    requestLog.Clear();
    return Results.Ok(new RequestLogClearResponse(true));
}).RequireCors(adminCorsPolicy);

// Resource explorer endpoint
app.MapGet("/_microstack/resources", (ServiceRegistry serviceRegistry) =>
{
    var resources = serviceRegistry.All
        .OfType<IResourceProvider>()
        .Select(provider => provider.GetResources())
        .OrderBy(summary => summary.Service, StringComparer.Ordinal)
        .ToList();
    return Results.Ok(resources);
}).RequireCors(adminCorsPolicy);

// Enable routing so endpoint matching runs before our AWS middleware.
// This ensures admin endpoints (health, reset, config) take priority.
app.UseRouting();
app.UseCors();

// Keep failures from the inspection API machine-readable without hiding provider faults.
app.Use(async (ctx, next) =>
{
    try
    {
        await next(ctx);
    }
    catch (OperationCanceledException) when (ctx.RequestAborted.IsCancellationRequested)
    {
        throw;
    }
    catch (Exception exception) when (ctx.Request.Path.StartsWithSegments("/_microstack/admin/v1"))
    {
        var logger = ctx.RequestServices.GetRequiredService<ILogger<Program>>();
        logger.LogError(exception, "Admin API request failed for {Path}", ctx.Request.Path);
        if (ctx.Response.HasStarted)
            throw;
        ctx.Response.Clear();
        ctx.Response.StatusCode = StatusCodes.Status500InternalServerError;
        await ctx.Response.WriteAsJsonAsync(
            new MicroStack.Admin.Contracts.AdminError(
                "internal_error", "The admin request failed. Refresh and try again."),
            MicroStack.Admin.Contracts.AdminJsonContext.Default.AdminError,
            cancellationToken: ctx.RequestAborted);
    }
});

// Handle OPTIONS (CORS pre-flight) before the routing layer can emit a 405.
// AWS SDKs send OPTIONS pre-flight; we must return CORS headers for all paths.
app.Use(async (ctx, next) =>
{
    if (ctx.Request.Method.Equals("OPTIONS", StringComparison.OrdinalIgnoreCase))
    {
        ctx.Response.Headers["Access-Control-Allow-Origin"]  = "*";
        ctx.Response.Headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH";
        ctx.Response.Headers["Access-Control-Allow-Headers"] = "*";
        ctx.Response.Headers["Access-Control-Expose-Headers"] = "*";
        ctx.Response.StatusCode = 204;
        return;
    }
    await next(ctx);
});

// Main AWS request middleware
app.UseMiddleware<RequestLogMiddleware>();
app.UseMiddleware<AwsRequestMiddleware>();

// Activate the mapped endpoints (health, reset, config) registered above.
app.UseEndpoints(_ => { });

// Save state on shutdown
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
lifetime.ApplicationStopping.Register(() => persistence.SaveAll());

// Print ASCII art banner on startup
lifetime.ApplicationStarted.Register(() =>
{
    var logger = app.Services.GetRequiredService<ILogger<Program>>();
    var enabledServices = registry.GetServiceStatus();
    var serviceCount = enabledServices.Count;

    const string banner =
        "        _                __ _             _    \n" +
        "  /\\/\\ (_) ___ _ __ ___ / _\\ |_ __ _  ___| | __\n" +
        " /    \\| |/ __| '__/ _ \\\\ \\| __/ _` |/ __| |/ /\n" +
        "/ /\\/\\ \\ | (__| | | (_) |\\ \\ || (_| | (__|   < \n" +
        "\\/    \\/_|\\___|_|  \\___/\\__/\\__\\__,_|\\___|_|\\_\\";

    logger.LogInformation(banner);
    logger.LogInformation("Services enabled: {ServiceCount}", serviceCount);
    if (adminUiEnabled)
        logger.LogInformation("UI available at: http://{Host}:{GatewayPort}/ui/", options.Host, options.GatewayPort);
});

app.Run();

// Expose Program for WebApplicationFactory in tests
public partial class Program { }

// Named record types replacing anonymous types (AOT-safe)
internal sealed record HealthResponse(Dictionary<string, string> Services, string Edition, string Version);
internal sealed record ResetResponse(string Reset);
internal sealed record ConfigResponse(Dictionary<string, string> Applied);
internal sealed record RequestLogClearResponse(bool Cleared);
