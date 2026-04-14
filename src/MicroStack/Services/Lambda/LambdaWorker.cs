using System.Diagnostics;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Lambda;

/// <summary>
/// Manages a single Lambda worker subprocess (Python or Node.js).
/// Communicates with the worker via a JSON-line protocol over stdin/stdout.
/// Stderr is captured as function log output.
/// </summary>
internal sealed class LambdaWorker : IDisposable
{
    private Process? _process;
    private string? _tempDir;
    private readonly Lock _invokeLock = new();
    private bool _cold = true;
    private readonly string _functionName;
    private readonly Dictionary<string, object?> _config;
    private readonly byte[] _codeZip;
    private StringBuilder? _stderrBuffer;
    private Thread? _stderrThread;

    private const int InitTimeoutMs = 15_000;
    private const int InvokeTimeoutMs = 30_000;
    private const string BootstrapTargetFramework = "net10.0";

    internal LambdaWorker(string functionName, Dictionary<string, object?> config, byte[] codeZip)
    {
        _functionName = functionName;
        _config = config;
        _codeZip = codeZip;
    }

    internal bool IsAlive => _process is not null && !_process.HasExited;

    /// <summary>
    /// Invokes the Lambda function with the given event payload.
    /// Thread-safe: only one invocation at a time per worker (stdin/stdout are not reentrant).
    /// </summary>
    internal WorkerResult Invoke(byte[] eventPayload, string requestId)
    {
        lock (_invokeLock)
        {
            if (!IsAlive)
            {
                Spawn();
            }

            var coldStart = _cold;
            _cold = false;

            // Clear stderr buffer for this invocation
            _stderrBuffer?.Clear();

            // Build event JSON with _request_id injected
            var eventJson = InjectRequestId(eventPayload, requestId);

            try
            {
                // Write event + newline to stdin
                _process!.StandardInput.WriteLine(eventJson);
                _process.StandardInput.Flush();

                // Read response line from stdout with timeout
                var responseLine = ReadLineWithTimeout(_process.StandardOutput, InvokeTimeoutMs);
                if (responseLine is null)
                {
                    Kill();
                    return new WorkerResult(
                        false,
                        null,
                        "Lambda function timed out",
                        null,
                        coldStart,
                        DrainStderr());
                }

                // Parse JSON response, skipping non-JSON lines
                var parsed = TryParseJsonLine(responseLine);
                if (parsed is null)
                {
                    Kill();
                    return new WorkerResult(
                        false,
                        null,
                        $"Invalid response from worker: {responseLine}",
                        null,
                        coldStart,
                        DrainStderr());
                }

                var status = GetStringProperty(parsed.Value, "status");

                if (string.Equals(status, "ok", StringComparison.Ordinal))
                {
                    string? resultJson = null;
                    if (parsed.Value.TryGetProperty("result", out var resultEl))
                    {
                        resultJson = resultEl.ValueKind == JsonValueKind.Null
                            ? "null"
                            : resultEl.GetRawText();
                    }

                    return new WorkerResult(
                        true,
                        resultJson,
                        null,
                        null,
                        coldStart,
                        DrainStderr());
                }

                // Error case
                var error = GetStringProperty(parsed.Value, "error") ?? "Unknown error";
                var trace = GetStringProperty(parsed.Value, "trace");

                return new WorkerResult(
                    false,
                    null,
                    error,
                    trace,
                    coldStart,
                    DrainStderr());
            }
            catch (Exception ex) when (ex is IOException or InvalidOperationException or ObjectDisposedException)
            {
                Kill();
                return new WorkerResult(
                    false,
                    null,
                    $"Worker process error: {ex.Message}",
                    null,
                    coldStart,
                    DrainStderr());
            }
        }
    }

    private void Spawn()
    {
        // Clean up previous temp dir if any
        CleanupTempDir();

        // Create temp directory
        _tempDir = Path.Combine(Path.GetTempPath(), $"microstack-lambda-{_functionName}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);

        var codeDir = Path.Combine(_tempDir, "code");
        Directory.CreateDirectory(codeDir);

        // Extract code zip
        using (var zipStream = new MemoryStream(_codeZip))
        using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Read))
        {
            archive.ExtractToDirectory(codeDir);
        }

        // Determine runtime
        var runtime = _config.TryGetValue("Runtime", out var rtVal) ? rtVal?.ToString() ?? "" : "";
        var handler = _config.TryGetValue("Handler", out var hVal) ? hVal?.ToString() ?? "index.handler" : "index.handler";
        var memorySize = _config.TryGetValue("MemorySize", out var mVal) && mVal is int memInt ? memInt : 128;
        var functionArn = _config.TryGetValue("FunctionArn", out var arnVal) ? arnVal?.ToString() ?? "" : "";

        // Parse handler into module.function
        var handlerParts = handler.Split('.');
        var module = handlerParts.Length >= 2 ? handlerParts[0] : "index";
        var handlerFn = handlerParts.Length >= 2 ? handlerParts[1] : handler;

        // Build environment variables
        var envVars = new Dictionary<string, string>(StringComparer.Ordinal);
        if (_config.TryGetValue("Environment", out var envObj) && envObj is Dictionary<string, object?> envDict)
        {
            if (envDict.TryGetValue("Variables", out var varsObj) && varsObj is Dictionary<string, object?> vars)
            {
                foreach (var (key, value) in vars)
                {
                    envVars[key] = value?.ToString() ?? "";
                }
            }
        }

        string executableName;
        string arguments;

        if (runtime.StartsWith("python", StringComparison.OrdinalIgnoreCase))
        {
            var scriptPath = Path.Combine(_tempDir, "_worker.py");
            File.WriteAllText(scriptPath, PythonWorkerScript, Encoding.UTF8);
            executableName = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "python" : "python3";
            arguments = scriptPath;
        }
        else if (runtime.StartsWith("nodejs", StringComparison.OrdinalIgnoreCase))
        {
            var scriptPath = Path.Combine(_tempDir, "_worker.js");
            File.WriteAllText(scriptPath, NodeJsWorkerScript, Encoding.UTF8);
            executableName = "node";
            arguments = scriptPath;
        }
        else if (runtime.StartsWith("dotnet", StringComparison.OrdinalIgnoreCase))
        {
            var bootstrapPath = ResolveBootstrapPath();
            if (bootstrapPath is null)
            {
                throw new InvalidOperationException(
                    "MicroStack.LambdaBootstrap could not be found. " +
                    "Set the MICROSTACK_LAMBDA_BOOTSTRAP_PATH environment variable to the path of MicroStack.LambdaBootstrap.dll, " +
                    "or ensure the bootstrap project has been built.");
            }

            executableName = "dotnet";
            arguments = $"exec \"{bootstrapPath}\"";
        }
        else
        {
            // Unsupported runtime — don't spawn
            return;
        }

        var psi = new ProcessStartInfo
        {
            FileName = executableName,
            Arguments = arguments,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = codeDir,
        };

        _process = Process.Start(psi);
        if (_process is null)
        {
            throw new InvalidOperationException($"Failed to start {executableName} for Lambda worker.");
        }

        // Start stderr capture thread
        _stderrBuffer = new StringBuilder();
        _stderrThread = new Thread(CaptureStderr)
        {
            IsBackground = true,
            Name = $"lambda-stderr-{_functionName}",
        };
        _stderrThread.Start();

        // Send init payload
        // For dotnet runtimes, pass the full handler string (e.g. "Assembly::Namespace.Class::Method").
        // For python/node runtimes, pass the split module + handler function name.
        Dictionary<string, object?> initPayload;
        if (runtime.StartsWith("dotnet", StringComparison.OrdinalIgnoreCase))
        {
            initPayload = new Dictionary<string, object?>
            {
                ["code_dir"] = codeDir.Replace('\\', '/'),
                ["handler"] = handler,
                ["env"] = envVars,
                ["function_name"] = _functionName,
                ["memory"] = memorySize,
                ["arn"] = functionArn,
            };
        }
        else
        {
            initPayload = new Dictionary<string, object?>
            {
                ["code_dir"] = codeDir.Replace('\\', '/'),
                ["module"] = module,
                ["handler"] = handlerFn,
                ["env"] = envVars,
                ["function_name"] = _functionName,
                ["memory"] = memorySize,
                ["arn"] = functionArn,
            };
        }

        var initJson = DictionaryObjectJsonConverter.SerializeValue(initPayload);
        _process.StandardInput.WriteLine(initJson);
        _process.StandardInput.Flush();

        // Read init response with timeout
        var initResponse = ReadLineWithTimeout(_process.StandardOutput, InitTimeoutMs);
        if (initResponse is null)
        {
            Kill();
            throw new InvalidOperationException("Lambda worker did not respond to init within timeout.");
        }

        var initParsed = TryParseJsonLine(initResponse);
        if (initParsed is null)
        {
            Kill();
            throw new InvalidOperationException($"Lambda worker returned invalid init response: {initResponse}");
        }

        var initStatus = GetStringProperty(initParsed.Value, "status");
        if (!string.Equals(initStatus, "ready", StringComparison.Ordinal))
        {
            var initError = GetStringProperty(initParsed.Value, "error") ?? "unknown init error";
            Kill();
            throw new InvalidOperationException($"Lambda worker init failed: {initError}");
        }

        _cold = true;
    }

    private void CaptureStderr()
    {
        try
        {
            var process = _process;
            if (process is null)
            {
                return;
            }

            while (!process.HasExited)
            {
                var line = process.StandardError.ReadLine();
                if (line is null)
                {
                    break;
                }

                _stderrBuffer?.AppendLine(line);
            }
        }
        catch
        {
            // Ignore errors during stderr capture — process may have exited
        }
    }

    private string DrainStderr()
    {
        return _stderrBuffer?.ToString() ?? "";
    }

    internal void Kill()
    {
        var process = _process;
        _process = null;

        if (process is not null)
        {
            try
            {
                if (!process.HasExited)
                {
                    // Try graceful shutdown by closing stdin
                    try
                    {
                        process.StandardInput.Close();
                    }
                    catch
                    {
                        // Ignore
                    }

                    if (!process.WaitForExit(1000))
                    {
                        process.Kill(entireProcessTree: true);
                    }
                }
            }
            catch
            {
                // Best effort — process may already be gone
            }
            finally
            {
                process.Dispose();
            }
        }

        CleanupTempDir();
    }

    public void Dispose()
    {
        Kill();
    }

    private void CleanupTempDir()
    {
        var dir = _tempDir;
        _tempDir = null;

        if (dir is not null && Directory.Exists(dir))
        {
            try
            {
                Directory.Delete(dir, recursive: true);
            }
            catch
            {
                // Best effort cleanup
            }
        }
    }

    private static string? ReadLineWithTimeout(StreamReader reader, int timeoutMs)
    {
        string? result = null;
        var readTask = Task.Run(() => result = reader.ReadLine());

        if (readTask.Wait(timeoutMs))
        {
            return result;
        }

        return null;
    }

    private static JsonElement? TryParseJsonLine(string line)
    {
        var trimmed = line.Trim();
        if (trimmed.Length == 0 || trimmed[0] != '{')
        {
            return null;
        }

        try
        {
            using var doc = JsonDocument.Parse(trimmed);
            return doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string? GetStringProperty(JsonElement el, string propertyName)
    {
        return el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;
    }

    /// <summary>
    /// Injects a "_request_id" field into the event JSON payload.
    /// </summary>
    private static string InjectRequestId(byte[] eventPayload, string requestId)
    {
        if (eventPayload.Length == 0)
        {
            return DictionaryObjectJsonConverter.SerializeValue(new Dictionary<string, object?> { ["_request_id"] = requestId });
        }

        try
        {
            using var doc = JsonDocument.Parse(eventPayload);
            var root = doc.RootElement;

            if (root.ValueKind == JsonValueKind.Object)
            {
                var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var prop in root.EnumerateObject())
                {
                    dict[prop.Name] = DeserializeJsonElement(prop.Value);
                }

                dict["_request_id"] = requestId;
                return DictionaryObjectJsonConverter.SerializeValue(dict);
            }

            // Non-object payload — wrap it
            return DictionaryObjectJsonConverter.SerializeValue(new Dictionary<string, object?>
            {
                ["_payload"] = DeserializeJsonElement(root),
                ["_request_id"] = requestId,
            });
        }
        catch (JsonException)
        {
            // Not valid JSON — send as string
            var text = Encoding.UTF8.GetString(eventPayload);
            return DictionaryObjectJsonConverter.SerializeValue(new Dictionary<string, object?>
            {
                ["_payload"] = text,
                ["_request_id"] = requestId,
            });
        }
    }

    private static object? DeserializeJsonElement(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? (object)l : el.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Object => DeserializeJsonObject(el),
            JsonValueKind.Array => el.EnumerateArray().Select(DeserializeJsonElement).ToList(),
            _ => null,
        };
    }

    private static Dictionary<string, object?> DeserializeJsonObject(JsonElement el)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var prop in el.EnumerateObject())
        {
            dict[prop.Name] = DeserializeJsonElement(prop.Value);
        }

        return dict;
    }

    // -- Bootstrap path resolution ---------------------------------------------

    private static string? _cachedBootstrapPath;
    private static readonly Lock _bootstrapLock = new();

    /// <summary>
    /// Resolves the path to the MicroStack.LambdaBootstrap.dll.
    /// Resolution order:
    ///   1. Environment variable MICROSTACK_LAMBDA_BOOTSTRAP_PATH (explicit override)
    ///   2. Sibling directory: {MicroStack assembly dir}/lambda-bootstrap/MicroStack.LambdaBootstrap.dll
    ///   3. Development fallback: walk up from the MicroStack assembly looking for the src/ build output
    /// Returns null if the bootstrap cannot be found.
    /// </summary>
    private static string? ResolveBootstrapPath()
    {
        lock (_bootstrapLock)
        {
            if (_cachedBootstrapPath is not null)
            {
                return _cachedBootstrapPath;
            }

            // 1. Explicit environment variable override
            var envPath = System.Environment.GetEnvironmentVariable("MICROSTACK_LAMBDA_BOOTSTRAP_PATH");
            if (!string.IsNullOrEmpty(envPath) && File.Exists(envPath))
            {
                _cachedBootstrapPath = envPath;
                return envPath;
            }

            // 2. Published sibling: {assembly dir}/lambda-bootstrap/MicroStack.LambdaBootstrap.dll
            var assemblyDir = AppContext.BaseDirectory;
            var siblingPath = Path.Combine(assemblyDir, "lambda-bootstrap", "MicroStack.LambdaBootstrap.dll");
            if (File.Exists(siblingPath))
            {
                _cachedBootstrapPath = siblingPath;
                return siblingPath;
            }

            // 3. Development fallback: search up from the assembly directory for src/MicroStack.LambdaBootstrap/bin/
            var dir = assemblyDir;
            for (var i = 0; i < 10; i++)
            {
                if (dir is null)
                {
                    break;
                }

                // Look for src/MicroStack.LambdaBootstrap/bin/*/net10.0/MicroStack.LambdaBootstrap.dll
                var bootstrapBinDir = Path.Combine(dir, "src", "MicroStack.LambdaBootstrap", "bin");
                if (Directory.Exists(bootstrapBinDir))
                {
                    foreach (var configDir in Directory.GetDirectories(bootstrapBinDir))
                    {
                        var tfmDir = Path.Combine(configDir, BootstrapTargetFramework);
                        var candidate = Path.Combine(tfmDir, "MicroStack.LambdaBootstrap.dll");
                        if (File.Exists(candidate))
                        {
                            _cachedBootstrapPath = candidate;
                            return candidate;
                        }
                    }
                }

                dir = Path.GetDirectoryName(dir);
            }

            return null;
        }
    }

    // -- Embedded worker scripts ------------------------------------------------

    private const string PythonWorkerScript = """
        import sys, os, json, importlib, traceback

        # Redirect stdout to stderr (logs), keep real stdout for JSON protocol
        _real_stdout = sys.__stdout__
        sys.stdout = sys.stderr

        def _write(obj):
            _real_stdout.write(json.dumps(obj) + '\n')
            _real_stdout.flush()

        # Read init payload
        init = json.loads(input())
        env_vars = init.get('env', {})
        for k, v in env_vars.items():
            os.environ[k] = str(v)
        sys.path.insert(0, init['code_dir'])

        # Import handler
        mod = importlib.import_module(init['module'])
        handler_fn = getattr(mod, init['handler'])

        _write({"status": "ready", "cold": True})

        # Event loop
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                request_id = event.pop('_request_id', 'unknown')

                # Create context
                context = type('LambdaContext', (), {
                    'function_name': init.get('function_name', ''),
                    'memory_limit_in_mb': init.get('memory', 128),
                    'invoked_function_arn': init.get('arn', ''),
                    'aws_request_id': request_id,
                    'log_group_name': '/aws/lambda/' + init.get('function_name', ''),
                    'log_stream_name': 'test-stream',
                    'get_remaining_time_in_millis': lambda: 300000,
                })()

                result = handler_fn(event, context)
                _write({"status": "ok", "result": result})
            except Exception as e:
                _write({"status": "error", "error": str(e), "trace": traceback.format_exc()})
        """;

    private const string NodeJsWorkerScript = """
        const readline = require('readline');
        const path = require('path');

        // Redirect console to stderr
        console.log = (...args) => process.stderr.write(args.join(' ') + '\n');
        console.error = (...args) => process.stderr.write(args.join(' ') + '\n');
        console.warn = (...args) => process.stderr.write(args.join(' ') + '\n');

        function _write(obj) {
            process.stdout.write(JSON.stringify(obj) + '\n');
        }

        const rl = readline.createInterface({ input: process.stdin, terminal: false });
        const lines = rl[Symbol.asyncIterator]();

        (async () => {
            // Read init
            const initLine = (await lines.next()).value;
            const init = JSON.parse(initLine);

            Object.assign(process.env, init.env || {});

            // Load handler
            const modPath = path.join(init.code_dir, init.module);
            const mod = require(modPath);
            const handlerFn = mod[init.handler];

            if (typeof handlerFn !== 'function') {
                _write({status: 'error', error: init.handler + ' is not a function'});
                process.exit(1);
            }

            _write({status: 'ready', cold: true});

            // Event loop
            for await (const line of lines) {
                if (!line.trim()) continue;
                try {
                    const event = JSON.parse(line);
                    const requestId = event._request_id || 'unknown';
                    delete event._request_id;

                    const context = {
                        functionName: init.function_name || '',
                        memoryLimitInMB: init.memory || 128,
                        invokedFunctionArn: init.arn || '',
                        awsRequestId: requestId,
                        getRemainingTimeInMillis: () => 300000,
                        done: (err, res) => { if (err) throw err; return res; },
                        succeed: (res) => res,
                        fail: (err) => { throw err; },
                    };

                    let result;
                    if (handlerFn.length >= 3) {
                        // Callback style
                        result = await new Promise((resolve, reject) => {
                            handlerFn(event, context, (err, res) => {
                                if (err) reject(err); else resolve(res);
                            });
                        });
                    } else {
                        result = await Promise.resolve(handlerFn(event, context));
                    }

                    _write({status: 'ok', result: result});
                } catch (e) {
                    _write({status: 'error', error: e.message || String(e), trace: e.stack || ''});
                }
            }
        })();
        """;
}

/// <summary>
/// Result of invoking a Lambda worker.
/// </summary>
internal sealed record WorkerResult(
    bool Success,
    string? ResultJson,
    string? Error,
    string? Trace,
    bool ColdStart,
    string Log);
