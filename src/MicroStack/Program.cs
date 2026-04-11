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
using MicroStack.Services.Sts;

var builder = WebApplication.CreateBuilder(args);

var port = int.Parse(
    Environment.GetEnvironmentVariable("GATEWAY_PORT")
    ?? Environment.GetEnvironmentVariable("EDGE_PORT")
    ?? "4566");

builder.WebHost.UseUrls($"http://0.0.0.0:{port}");

// Register core infrastructure
// Note: ServiceRegistry and StatePersistence have internal constructors (per coding standards),
// so we use factory lambdas instead of letting DI locate a public constructor.
builder.Services.AddSingleton<AwsServiceRouter>();
builder.Services.AddSingleton<ServiceRegistry>(_ => new ServiceRegistry());
builder.Services.AddSingleton<StatePersistence>(sp => new StatePersistence(
    sp.GetRequiredService<ILogger<StatePersistence>>(),
    sp.GetRequiredService<ServiceRegistry>()));

var app = builder.Build();

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

// Health endpoint (multiple aliases for LocalStack compatibility)
foreach (var healthPath in new[] { "/_ministack/health", "/health", "/_localstack/health" })
{
    app.MapGet(healthPath, () =>
    {
        var services = registry.GetServiceStatus();
        return Results.Ok(new
        {
            services,
            edition = "light",
            version = "0.1.0"
        });
    });
}

// Reset endpoint
app.MapPost("/_ministack/reset", () =>
{
    registry.ResetAll();
    persistence.DeleteAll();
    return Results.Ok(new { reset = "ok" });
});

// Config endpoint (stub — populated when services implement it)
app.MapPost("/_ministack/config", async (HttpContext ctx) =>
{
    using var reader = new StreamReader(ctx.Request.Body);
    var bodyText = await reader.ReadToEndAsync();
    var applied = new Dictionary<string, object>();
    if (!string.IsNullOrEmpty(bodyText))
    {
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(bodyText);
            if (doc.RootElement.TryGetProperty("stepfunctions", out var sfnEl)
                && sfnEl.ValueKind == System.Text.Json.JsonValueKind.Object
                && sfnEl.TryGetProperty("_sfn_mock_config", out var mockEl))
            {
                var mockConfig = JsonElementToDict(mockEl);
                sfnHandler.SetMockConfig(mockConfig);
                applied["stepfunctions._sfn_mock_config"] = "applied";
            }
        }
        catch (System.Text.Json.JsonException)
        {
            // Ignore invalid JSON
        }
    }

    return Results.Ok(new { applied });
});

static Dictionary<string, object?> JsonElementToDict(System.Text.Json.JsonElement element)
{
    var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
    if (element.ValueKind != System.Text.Json.JsonValueKind.Object)
    {
        return dict;
    }

    foreach (var prop in element.EnumerateObject())
    {
        dict[prop.Name] = JsonElementToObject(prop.Value);
    }

    return dict;
}

static object? JsonElementToObject(System.Text.Json.JsonElement element)
{
    return element.ValueKind switch
    {
        System.Text.Json.JsonValueKind.Object => JsonElementToDict(element),
        System.Text.Json.JsonValueKind.Array => element.EnumerateArray().Select(JsonElementToObject).ToList(),
        System.Text.Json.JsonValueKind.String => element.GetString(),
        System.Text.Json.JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
        System.Text.Json.JsonValueKind.True => true,
        System.Text.Json.JsonValueKind.False => false,
        _ => null,
    };
}

// Enable routing so endpoint matching runs before our AWS middleware.
// This ensures admin endpoints (health, reset, config) take priority.
app.UseRouting();

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
app.UseMiddleware<AwsRequestMiddleware>();

// Activate the mapped endpoints (health, reset, config) registered above.
app.UseEndpoints(_ => { });

// Save state on shutdown
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
lifetime.ApplicationStopping.Register(() => persistence.SaveAll());

app.Run();

// Expose Program for WebApplicationFactory in tests
public partial class Program { }
