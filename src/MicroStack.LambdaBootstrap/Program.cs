using System.Reflection;
using System.Text;
using System.Text.Json;

namespace MicroStack.LambdaBootstrap;

/// <summary>
/// Bootstrap entry point for .NET Lambda workers.
/// Communicates with MicroStack via the same stdin/stdout JSON-line protocol used by Python and Node.js workers.
///
/// Protocol:
///   Init:  stdin  → {"code_dir":"...","handler":"Assembly::Namespace.Class::Method","env":{...},"function_name":"...","memory":128,"arn":"..."}
///          stdout ← {"status":"ready","cold":true}
///   Invoke: stdin → {"_request_id":"...",&lt;event fields&gt;}
///           stdout ← {"status":"ok","result":&lt;json&gt;} | {"status":"error","error":"...","trace":"..."}
///   Stderr is used for function log output.
/// </summary>
internal static class Program
{
    internal static async Task<int> Main()
    {
        // Redirect Console.Out to stderr so handler Console.WriteLine calls appear as logs.
        // Keep a reference to real stdout for the JSON protocol.
        var realStdout = new StreamWriter(Console.OpenStandardOutput(), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false)) { AutoFlush = true };
        Console.SetOut(Console.Error);

        try
        {
            // --- Init phase ---
            var initLine = Console.In.ReadLine();
            if (initLine is null)
            {
                return 1;
            }

            JsonElement initDoc;
            try
            {
                using var doc = JsonDocument.Parse(initLine);
                initDoc = doc.RootElement.Clone();
            }
            catch (JsonException ex)
            {
                WriteError(realStdout, $"Invalid init JSON: {ex.Message}", "");
                return 1;
            }

            var codeDir = GetString(initDoc, "code_dir") ?? ".";
            var handler = GetString(initDoc, "handler") ?? "";
            var functionName = GetString(initDoc, "function_name") ?? "";
            var memory = initDoc.TryGetProperty("memory", out var memEl) && memEl.TryGetInt32(out var memInt) ? memInt : 128;
            var arn = GetString(initDoc, "arn") ?? "";

            // Set environment variables from init payload
            if (initDoc.TryGetProperty("env", out var envEl) && envEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in envEl.EnumerateObject())
                {
                    System.Environment.SetEnvironmentVariable(prop.Name, prop.Value.GetString() ?? "");
                }
            }

            // Parse handler: "AssemblyName::Namespace.Class::Method"
            var parts = handler.Split("::");
            if (parts.Length != 3)
            {
                WriteError(realStdout, $"Invalid .NET handler format '{handler}'. Expected 'AssemblyName::Namespace.Class::Method'.", "");
                return 1;
            }

            var assemblyName = parts[0].Trim();
            var typeName = parts[1].Trim();
            var methodName = parts[2].Trim();

            // Load the handler assembly
            var loadContext = new LambdaAssemblyLoadContext(codeDir);
            Assembly handlerAssembly;
            try
            {
                var dllPath = Path.Combine(codeDir, assemblyName + ".dll");
                if (!File.Exists(dllPath))
                {
                    WriteError(realStdout, $"Assembly '{assemblyName}.dll' not found in code directory '{codeDir}'.", "");
                    return 1;
                }

                handlerAssembly = loadContext.LoadFromAssemblyPath(dllPath);
            }
            catch (Exception ex)
            {
                WriteError(realStdout, $"Failed to load assembly '{assemblyName}': {ex.Message}", ex.StackTrace ?? "");
                return 1;
            }

            // Find the handler type
            var handlerType = handlerAssembly.GetType(typeName, throwOnError: false);
            if (handlerType is null)
            {
                WriteError(realStdout, $"Type '{typeName}' not found in assembly '{assemblyName}'.", "");
                return 1;
            }

            // Find the handler method
            var method = handlerType.GetMethod(methodName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static);
            if (method is null)
            {
                WriteError(realStdout, $"Method '{methodName}' not found on type '{typeName}'.", "");
                return 1;
            }

            // Determine the method's invocation strategy
            var strategy = ResolveStrategy(method);
            if (strategy is null)
            {
                WriteError(realStdout, $"Method '{methodName}' has an unsupported signature. Supported: (TInput, ILambdaContext), (Stream, ILambdaContext), (TInput), (Stream).", "");
                return 1;
            }

            // Create handler instance (null for static methods)
            object? handlerInstance = null;
            if (!method.IsStatic)
            {
                try
                {
                    handlerInstance = Activator.CreateInstance(handlerType);
                }
                catch (Exception ex)
                {
                    WriteError(realStdout, $"Failed to create instance of '{typeName}': {ex.Message}", ex.StackTrace ?? "");
                    return 1;
                }
            }

            // Signal ready
            realStdout.WriteLine("""{"status":"ready","cold":true}""");

            // --- Event loop ---
            string? line;
            while ((line = Console.In.ReadLine()) is not null)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                string requestId;
                string eventJson;
                try
                {
                    (requestId, eventJson) = ExtractRequestId(line);
                }
                catch (JsonException ex)
                {
                    WriteError(realStdout, $"Invalid event JSON: {ex.Message}", "");
                    continue;
                }

                var context = new LambdaContext(functionName, memory, arn, requestId);

                try
                {
                    var resultJson = await strategy(handlerInstance, eventJson, context);
                    realStdout.WriteLine($$$"""{"status":"ok","result":{{{resultJson}}}}""");
                }
                catch (TargetInvocationException ex) when (ex.InnerException is not null)
                {
                    var inner = ex.InnerException;
                    WriteError(realStdout, inner.Message, inner.StackTrace ?? "");
                }
                catch (Exception ex)
                {
                    WriteError(realStdout, ex.Message, ex.StackTrace ?? "");
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Bootstrap fatal error: {ex}");
            return 1;
        }
    }

    /// <summary>
    /// Resolves the invocation strategy for a handler method.
    /// Returns a delegate that invokes the method and returns the result as a JSON string.
    /// Returns null if the signature is not supported.
    /// </summary>
    private static Func<object?, string, LambdaContext, Task<string>>? ResolveStrategy(MethodInfo method)
    {
        var parameters = method.GetParameters();
        var returnType = method.ReturnType;

        // Determine input type
        Type? inputType = parameters.Length >= 1 ? parameters[0].ParameterType : null;
        var hasContext = parameters.Length >= 2 && typeof(Amazon.Lambda.Core.ILambdaContext).IsAssignableFrom(parameters[1].ParameterType);

        if (inputType is null)
        {
            // No parameters
            return null;
        }

        var isStreamInput = typeof(Stream).IsAssignableFrom(inputType);

        // Determine output handling
        var isTask = returnType == typeof(Task);
        var isVoid = returnType == typeof(void);
        var isTaskOfT = returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(Task<>);
        Type? outputType = isTaskOfT ? returnType.GenericTypeArguments[0] : (!isTask && !isVoid ? returnType : null);
        var isStreamOutput = outputType is not null && typeof(Stream).IsAssignableFrom(outputType);

        return async (instance, eventJson, context) =>
        {
            // Build input argument
            object? inputArg;
            if (isStreamInput)
            {
                inputArg = new MemoryStream(Encoding.UTF8.GetBytes(eventJson));
            }
            else
            {
                // Deserialize JSON into the input parameter type
                try
                {
                    inputArg = JsonSerializer.Deserialize(eventJson, inputType);
                }
                catch (JsonException ex)
                {
                    throw new InvalidOperationException($"Failed to deserialize event JSON to {inputType.Name}: {ex.Message}", ex);
                }
            }

            // Build args array
            object?[] args = hasContext ? [inputArg, context] : [inputArg];

            // Invoke the method
            object? rawResult;
            try
            {
                rawResult = method.Invoke(instance, args);
            }
            catch (TargetInvocationException)
            {
                throw;
            }

            // Await if needed
            if (rawResult is Task task)
            {
                await task.ConfigureAwait(false);

                if (isTaskOfT)
                {
                    // Get the Task<T>.Result property
                    var resultProp = task.GetType().GetProperty("Result");
                    rawResult = resultProp?.GetValue(task);
                }
                else
                {
                    // Task (void return)
                    return "null";
                }
            }

            if (rawResult is null || isVoid || isTask)
            {
                return "null";
            }

            // Serialize the result to JSON
            if (isStreamOutput && rawResult is Stream outputStream)
            {
                using var ms = new MemoryStream();
                await outputStream.CopyToAsync(ms).ConfigureAwait(false);
                var bytes = ms.ToArray();
                // Try to parse as JSON; if it is, return it raw; otherwise return as JSON string
                var str = Encoding.UTF8.GetString(bytes);
                return TryParseAsJson(str) ? str : JsonSerializer.Serialize(str);
            }

            // For string output: if it's valid JSON, return raw; otherwise as JSON string
            if (rawResult is string strResult)
            {
                return TryParseAsJson(strResult) ? strResult : JsonSerializer.Serialize(strResult);
            }

            return JsonSerializer.Serialize(rawResult, rawResult.GetType());
        };
    }

    private static bool TryParseAsJson(string s)
    {
        try
        {
            using var _ = JsonDocument.Parse(s);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static (string requestId, string eventJson) ExtractRequestId(string line)
    {
        using var doc = JsonDocument.Parse(line);
        var root = doc.RootElement;

        var requestId = root.TryGetProperty("_request_id", out var ridEl) ? ridEl.GetString() ?? "unknown" : "unknown";

        // Re-serialize without _request_id
        using var ms = new MemoryStream();
        using var writer = new Utf8JsonWriter(ms);
        writer.WriteStartObject();
        foreach (var prop in root.EnumerateObject())
        {
            if (prop.Name == "_request_id")
            {
                continue;
            }

            prop.WriteTo(writer);
        }

        writer.WriteEndObject();
        writer.Flush();
        return (requestId, Encoding.UTF8.GetString(ms.ToArray()));
    }

    private static string? GetString(JsonElement el, string name)
    {
        return el.TryGetProperty(name, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;
    }

    private static void WriteError(StreamWriter stdout, string error, string trace)
    {
        var errObj = new { status = "error", error, trace };
        stdout.WriteLine(JsonSerializer.Serialize(errObj));
    }
}
