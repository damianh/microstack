using System.Text.Json;
using Amazon.Lambda.Core;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace SimpleHandler;

/// <summary>
/// Test Lambda handler for MicroStack .NET Lambda integration tests.
/// Provides several handler methods covering different use cases.
/// </summary>
public sealed class Function
{
    /// <summary>
    /// Returns the input event back to the caller unchanged.
    /// Handler: SimpleHandler::SimpleHandler.Function::EchoHandler
    /// </summary>
    public JsonElement EchoHandler(JsonElement input, ILambdaContext context)
    {
        return input;
    }

    /// <summary>
    /// Returns a greeting using the "name" field from the input event.
    /// Handler: SimpleHandler::SimpleHandler.Function::GreetHandler
    /// </summary>
    public JsonElement GreetHandler(JsonElement input, ILambdaContext context)
    {
        var name = "world";
        if (input.ValueKind == JsonValueKind.Object && input.TryGetProperty("name", out var nameProp))
        {
            name = nameProp.GetString() ?? "world";
        }

        return JsonDocument.Parse($$$"""{"greeting":"hello {{{name}}}"}""").RootElement.Clone();
    }

    /// <summary>
    /// Throws an exception — tests error handling.
    /// Handler: SimpleHandler::SimpleHandler.Function::ErrorHandler
    /// </summary>
    public JsonElement ErrorHandler(JsonElement input, ILambdaContext context)
    {
        throw new InvalidOperationException("something went wrong");
    }

    /// <summary>
    /// Returns Lambda context metadata.
    /// Handler: SimpleHandler::SimpleHandler.Function::ContextHandler
    /// </summary>
    public JsonElement ContextHandler(JsonElement input, ILambdaContext context)
    {
        var obj = new
        {
            function_name = context.FunctionName,
            memory_limit = context.MemoryLimitInMB,
            has_request_id = context.AwsRequestId.Length > 0,
        };
        var json = System.Text.Json.JsonSerializer.Serialize(obj);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    /// <summary>
    /// Returns environment variable values.
    /// Handler: SimpleHandler::SimpleHandler.Function::EnvHandler
    /// </summary>
    public JsonElement EnvHandler(JsonElement input, ILambdaContext context)
    {
        var myVar = System.Environment.GetEnvironmentVariable("MY_VAR") ?? "not-set";
        var myRegion = System.Environment.GetEnvironmentVariable("MY_REGION") ?? "not-set";
        return JsonDocument.Parse($$$"""{"myvar":"{{{myVar}}}","region":"{{{myRegion}}}"}""").RootElement.Clone();
    }

    /// <summary>
    /// Sums the items array from the input event.
    /// Handler: SimpleHandler::SimpleHandler.Function::SumHandler
    /// </summary>
    public JsonElement SumHandler(JsonElement input, ILambdaContext context)
    {
        var count = 0;
        var sum = 0;
        if (input.ValueKind == JsonValueKind.Object && input.TryGetProperty("items", out var items) && items.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in items.EnumerateArray())
            {
                count++;
                sum += item.GetInt32();
            }
        }

        return JsonDocument.Parse($"{{\"count\":{count},\"sum\":{sum}}}").RootElement.Clone();
    }
}
