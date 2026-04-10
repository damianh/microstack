using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MicroStack.Internal;
using MicroStack.Services.DynamoDb;
using MicroStack.Services.Sqs;

namespace MicroStack.Services.Lambda;

/// <summary>
/// Lambda service handler -- supports REST API via HTTP method + URL path routing.
///
/// Port of ministack/services/lambda_service.py (control plane only).
///
/// Supports: Function CRUD, Versioning, Aliases, Layers, Tags, Permissions,
///           Concurrency, Function URLs, Event Source Mappings CRUD, Invoke stub,
///           Event Invoke Config, Provisioned Concurrency, Code Signing Config stub.
/// </summary>
internal sealed class LambdaServiceHandler : IServiceHandler
{
    private readonly AccountScopedDictionary<string, FunctionRecord> _functions = new();
    private readonly AccountScopedDictionary<string, LayerRecord> _layers = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _esms = new();
    private readonly LambdaWorkerPool _workerPool = new();
    private readonly Lock _lock = new();

    private readonly SqsServiceHandler? _sqsHandler;
    private readonly DynamoDbServiceHandler? _ddbHandler;
    private EventSourceMappingPoller? _poller;

    private static readonly string Region =
        Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1";

    internal LambdaServiceHandler() { }

    internal LambdaServiceHandler(SqsServiceHandler sqsHandler, DynamoDbServiceHandler ddbHandler)
    {
        _sqsHandler = sqsHandler;
        _ddbHandler = ddbHandler;
    }

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "lambda";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var path = request.Path;
        var method = request.Method.ToUpperInvariant();

        // Lambda layer content download
        if (path.StartsWith("/_ministack/lambda-layers/", StringComparison.OrdinalIgnoreCase) && method == "GET")
        {
            return Task.FromResult(HandleLayerContentDownload(path));
        }

        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);

        // URL-decode each segment
        for (var i = 0; i < segments.Length; i++)
        {
            segments[i] = Uri.UnescapeDataString(segments[i]);
        }

        var response = Route(method, segments, request);
        return Task.FromResult(response);
    }

    public void Reset()
    {
        _poller?.Stop();
        _poller = null;
        _workerPool.Reset();
        lock (_lock)
        {
            _functions.Clear();
            _layers.Clear();
            _esms.Clear();
            _urlConfigs.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // -- Routing ---------------------------------------------------------------

    private ServiceResponse Route(string method, string[] segments, ServiceRequest request)
    {
        // segments[0] is the API version prefix like "2015-03-31" or "2019-09-25" etc.
        if (segments.Length < 2)
        {
            return ErrorResponse("InvalidRequestException", "Unknown Lambda path", 404);
        }

        var version = segments[0];

        // /2015-03-31/functions or /2017-10-31/functions
        if ((version == "2015-03-31" || version == "2017-10-31") && segments.Length >= 2 && segments[1] == "functions")
        {
            return RouteFunctions20150331(method, segments, request);
        }

        // /2015-03-31/layers or /2018-10-31/layers
        if ((version == "2015-03-31" || version == "2018-10-31") && segments.Length >= 2 && segments[1] == "layers")
        {
            return RouteLayers(method, segments, request);
        }

        // /2015-03-31/event-source-mappings
        if (version == "2015-03-31" && segments.Length >= 2 && segments[1] == "event-source-mappings")
        {
            return RouteEsm(method, segments, request);
        }

        // /2015-03-31/tags/{arn+} or /2017-03-31/tags/{arn+}
        if ((version == "2015-03-31" || version == "2017-03-31") && segments.Length >= 2 && segments[1] == "tags")
        {
            return RouteTags(method, segments, request);
        }

        // /2019-09-25/functions/{name}/event-invoke-config
        if (version == "2019-09-25" && segments.Length >= 4 && segments[1] == "functions" && segments[3] == "event-invoke-config")
        {
            var funcName = segments[2];
            return RouteEventInvokeConfig(method, funcName, request);
        }

        // /2019-09-30/functions/{name}/provisioned-concurrency
        if (version == "2019-09-30" && segments.Length >= 4 && segments[1] == "functions" && segments[3] == "provisioned-concurrency")
        {
            var funcName = segments[2];
            return RouteProvisionedConcurrency(method, funcName, request);
        }

        // /2019-09-30/functions/{name}/concurrency (GetFunctionConcurrency, DeleteFunctionConcurrency)
        if (version == "2019-09-30" && segments.Length >= 4 && segments[1] == "functions" && segments[3] == "concurrency")
        {
            var funcName = segments[2];
            if (method == "GET")
            {
                return HandleGetFunctionConcurrency(funcName);
            }

            if (method == "DELETE")
            {
                return HandleDeleteFunctionConcurrency(funcName);
            }

            if (method == "PUT")
            {
                return HandlePutFunctionConcurrency(funcName, request);
            }

            return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
        }

        // /2021-10-31/functions/{name}/url
        if (version == "2021-10-31" && segments.Length >= 4 && segments[1] == "functions" && segments[3] == "url")
        {
            var funcName = segments[2];
            return RouteFunctionUrl(method, funcName, request);
        }

        // /2020-04-22/code-signing-configs
        if (version == "2020-04-22" && segments.Length >= 2 && segments[1] == "code-signing-configs")
        {
            return HandleCodeSigningConfig(method);
        }

        return ErrorResponse("InvalidRequestException", "Unknown Lambda path", 404);
    }

    private ServiceResponse RouteFunctions20150331(string method, string[] segments, ServiceRequest request)
    {
        // /2015-03-31/functions -- list or create
        if (segments.Length == 2)
        {
            if (method == "GET")
            {
                return HandleListFunctions(request);
            }

            if (method == "POST")
            {
                return HandleCreateFunction(request);
            }

            return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
        }

        var funcName = segments[2];

        // /2015-03-31/functions/{name}
        if (segments.Length == 3)
        {
            if (method == "GET")
            {
                return HandleGetFunction(funcName, request);
            }

            if (method == "DELETE")
            {
                return HandleDeleteFunction(funcName, request);
            }

            return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
        }

        var subResource = segments[3];

        // /2015-03-31/functions/{name}/invocations
        if (subResource == "invocations" && segments.Length == 4)
        {
            return HandleInvoke(funcName, request);
        }

        // /2015-03-31/functions/{name}/configuration
        if (subResource == "configuration" && segments.Length == 4)
        {
            if (method == "GET")
            {
                return HandleGetFunctionConfiguration(funcName, request);
            }

            if (method == "PUT")
            {
                return HandleUpdateFunctionConfiguration(funcName, request);
            }

            return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
        }

        // /2015-03-31/functions/{name}/code
        if (subResource == "code" && segments.Length == 4)
        {
            if (method == "PUT")
            {
                return HandleUpdateFunctionCode(funcName, request);
            }

            return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
        }

        // /2015-03-31/functions/{name}/versions
        if (subResource == "versions" && segments.Length == 4)
        {
            if (method == "GET")
            {
                return HandleListVersionsByFunction(funcName, request);
            }

            if (method == "POST")
            {
                return HandlePublishVersion(funcName, request);
            }

            return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
        }

        // /2015-03-31/functions/{name}/aliases
        if (subResource == "aliases")
        {
            if (segments.Length == 4)
            {
                if (method == "GET")
                {
                    return HandleListAliases(funcName, request);
                }

                if (method == "POST")
                {
                    return HandleCreateAlias(funcName, request);
                }

                return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
            }

            // /2015-03-31/functions/{name}/aliases/{alias}
            if (segments.Length == 5)
            {
                var aliasName = segments[4];
                if (method == "GET")
                {
                    return HandleGetAlias(funcName, aliasName);
                }

                if (method == "PUT")
                {
                    return HandleUpdateAlias(funcName, aliasName, request);
                }

                if (method == "DELETE")
                {
                    return HandleDeleteAlias(funcName, aliasName);
                }

                return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
            }
        }

        // /2015-03-31/functions/{name}/concurrency
        if (subResource == "concurrency" && segments.Length == 4)
        {
            if (method == "PUT")
            {
                return HandlePutFunctionConcurrency(funcName, request);
            }

            if (method == "GET")
            {
                return HandleGetFunctionConcurrency(funcName);
            }

            if (method == "DELETE")
            {
                return HandleDeleteFunctionConcurrency(funcName);
            }

            return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
        }

        // /2015-03-31/functions/{name}/policy
        if (subResource == "policy")
        {
            if (segments.Length == 4)
            {
                if (method == "GET")
                {
                    return HandleGetPolicy(funcName);
                }

                if (method == "POST")
                {
                    return HandleAddPermission(funcName, request);
                }

                return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
            }

            // /2015-03-31/functions/{name}/policy/{sid}
            if (segments.Length == 5 && method == "DELETE")
            {
                var sid = segments[4];
                return HandleRemovePermission(funcName, sid);
            }
        }

        return ErrorResponse("InvalidRequestException", "Unknown Lambda path", 404);
    }

    private ServiceResponse RouteLayers(string method, string[] segments, ServiceRequest request)
    {
        // /2015-03-31/layers
        if (segments.Length == 2 && method == "GET")
        {
            return HandleListLayers();
        }

        if (segments.Length < 3)
        {
            return ErrorResponse("InvalidRequestException", "Unknown Lambda path", 404);
        }

        var layerName = segments[2];

        // /2015-03-31/layers/{name}/versions
        if (segments.Length == 4 && segments[3] == "versions")
        {
            if (method == "GET")
            {
                return HandleListLayerVersions(layerName);
            }

            if (method == "POST")
            {
                return HandlePublishLayerVersion(layerName, request);
            }

            return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
        }

        // /2015-03-31/layers/{name}/versions/{version}
        if (segments.Length == 5 && segments[3] == "versions")
        {
            if (!int.TryParse(segments[4], out var versionNum))
            {
                return ErrorResponse("InvalidParameterValueException", "Invalid layer version number.", 400);
            }

            if (method == "GET")
            {
                return HandleGetLayerVersion(layerName, versionNum);
            }

            if (method == "DELETE")
            {
                return HandleDeleteLayerVersion(layerName, versionNum);
            }

            return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
        }

        return ErrorResponse("InvalidRequestException", "Unknown Lambda path", 404);
    }

    private ServiceResponse RouteEsm(string method, string[] segments, ServiceRequest request)
    {
        // /2015-03-31/event-source-mappings
        if (segments.Length == 2)
        {
            if (method == "GET")
            {
                return HandleListEsm(request);
            }

            if (method == "POST")
            {
                return HandleCreateEsm(request);
            }

            return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
        }

        // /2015-03-31/event-source-mappings/{uuid}
        if (segments.Length == 3)
        {
            var uuid = segments[2];
            if (method == "GET")
            {
                return HandleGetEsm(uuid);
            }

            if (method == "PUT")
            {
                return HandleUpdateEsm(uuid, request);
            }

            if (method == "DELETE")
            {
                return HandleDeleteEsm(uuid);
            }

            return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
        }

        return ErrorResponse("InvalidRequestException", "Unknown Lambda path", 404);
    }

    private ServiceResponse RouteTags(string method, string[] segments, ServiceRequest request)
    {
        // /2015-03-31/tags/{arn...} - the ARN is the rest of the path after "tags/"
        if (segments.Length < 3)
        {
            return ErrorResponse("InvalidRequestException", "ARN is required for tag operations.", 400);
        }

        // Reconstruct the ARN from segments[2..] (it may contain slashes)
        var arn = string.Join("/", segments[2..]);

        if (method == "GET")
        {
            return HandleListTags(arn);
        }

        if (method == "POST")
        {
            return HandleTagResource(arn, request);
        }

        if (method == "DELETE")
        {
            return HandleUntagResource(arn, request);
        }

        return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
    }

    private ServiceResponse RouteEventInvokeConfig(string method, string funcName, ServiceRequest request)
    {
        if (method == "PUT")
        {
            return HandlePutEventInvokeConfig(funcName, request);
        }

        if (method == "GET")
        {
            return HandleGetEventInvokeConfig(funcName);
        }

        if (method == "DELETE")
        {
            return HandleDeleteEventInvokeConfig(funcName);
        }

        return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
    }

    private ServiceResponse RouteProvisionedConcurrency(string method, string funcName, ServiceRequest request)
    {
        if (method == "PUT")
        {
            return HandlePutProvisionedConcurrency(funcName, request);
        }

        if (method == "GET")
        {
            return HandleGetProvisionedConcurrency(funcName, request);
        }

        if (method == "DELETE")
        {
            return HandleDeleteProvisionedConcurrency(funcName, request);
        }

        return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
    }

    private ServiceResponse RouteFunctionUrl(string method, string funcName, ServiceRequest request)
    {
        if (method == "POST")
        {
            return HandleCreateFunctionUrlConfig(funcName, request);
        }

        if (method == "GET")
        {
            return HandleGetFunctionUrlConfig(funcName, request);
        }

        if (method == "PUT")
        {
            return HandleUpdateFunctionUrlConfig(funcName, request);
        }

        if (method == "DELETE")
        {
            return HandleDeleteFunctionUrlConfig(funcName, request);
        }

        return ErrorResponse("InvalidRequestException", "Unsupported method", 405);
    }

    // -- Function CRUD ---------------------------------------------------------

    private ServiceResponse HandleCreateFunction(ServiceRequest request)
    {
        var data = ParseBody(request);
        if (data.ValueKind == JsonValueKind.Undefined)
        {
            return ErrorResponse("InvalidParameterValueException", "Invalid JSON body.", 400);
        }

        var funcName = GetString(data, "FunctionName");
        if (string.IsNullOrEmpty(funcName))
        {
            return ErrorResponse("InvalidParameterValueException", "FunctionName is required.", 400);
        }

        lock (_lock)
        {
            if (_functions.ContainsKey(funcName))
            {
                return ErrorResponse("ResourceConflictException",
                    $"Function already exist: {funcName}", 409);
            }

            var accountId = AccountContext.GetAccountId();
            var functionArn = $"arn:aws:lambda:{Region}:{accountId}:function:{funcName}";

            byte[]? codeZip = null;
            if (data.TryGetProperty("Code", out var codeEl))
            {
                if (codeEl.TryGetProperty("ZipFile", out var zipProp))
                {
                    var zipBase64 = zipProp.GetString();
                    if (!string.IsNullOrEmpty(zipBase64))
                    {
                        codeZip = Convert.FromBase64String(zipBase64);
                    }
                }
            }

            var codeSize = codeZip?.Length ?? 0;
            var codeSha256 = codeZip is not null
                ? Convert.ToBase64String(SHA256.HashData(codeZip))
                : "";

            var now = FormatLastModified(DateTime.UtcNow);
            var revisionId = HashHelpers.NewUuid();

            var runtime = GetString(data, "Runtime") ?? "python3.9";
            var handler = GetString(data, "Handler") ?? "index.handler";
            var role = GetString(data, "Role") ?? $"arn:aws:iam::{accountId}:role/lambda-role";
            var description = GetString(data, "Description") ?? "";
            var timeout = GetInt(data, "Timeout", 3);
            var memorySize = GetInt(data, "MemorySize", 128);
            var packageType = GetString(data, "PackageType") ?? "Zip";

            var architectures = new List<object?> { "x86_64" };
            if (data.TryGetProperty("Architectures", out var archEl) && archEl.ValueKind == JsonValueKind.Array)
            {
                architectures = new List<object?>();
                foreach (var item in archEl.EnumerateArray())
                {
                    architectures.Add(item.GetString());
                }
            }

            var layers = new List<object?>();
            if (data.TryGetProperty("Layers", out var layersEl) && layersEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in layersEl.EnumerateArray())
                {
                    var layerArn = item.GetString() ?? "";
                    layers.Add(new Dictionary<string, object?>
                    {
                        ["Arn"] = layerArn,
                        ["CodeSize"] = 0,
                    });
                }
            }

            var environment = new Dictionary<string, object?>();
            if (data.TryGetProperty("Environment", out var envEl))
            {
                if (envEl.TryGetProperty("Variables", out var varsEl) && varsEl.ValueKind == JsonValueKind.Object)
                {
                    var vars = new Dictionary<string, object?>();
                    foreach (var prop in varsEl.EnumerateObject())
                    {
                        vars[prop.Name] = prop.Value.GetString();
                    }

                    environment["Variables"] = vars;
                }
            }

            var config = new Dictionary<string, object?>
            {
                ["FunctionName"] = funcName,
                ["FunctionArn"] = functionArn,
                ["Runtime"] = runtime,
                ["Role"] = role,
                ["Handler"] = handler,
                ["CodeSize"] = codeSize,
                ["CodeSha256"] = codeSha256,
                ["Description"] = description,
                ["Timeout"] = timeout,
                ["MemorySize"] = memorySize,
                ["LastModified"] = now,
                ["Version"] = "$LATEST",
                ["State"] = "Active",
                ["LastUpdateStatus"] = "Successful",
                ["PackageType"] = packageType,
                ["Architectures"] = architectures,
                ["EphemeralStorage"] = new Dictionary<string, object?> { ["Size"] = 512 },
                ["SnapStart"] = new Dictionary<string, object?>
                {
                    ["ApplyOn"] = "None",
                    ["OptimizationStatus"] = "Off",
                },
                ["LoggingConfig"] = new Dictionary<string, object?>
                {
                    ["LogFormat"] = "Text",
                    ["LogGroup"] = $"/aws/lambda/{funcName}",
                },
                ["RevisionId"] = revisionId,
            };

            if (layers.Count > 0)
            {
                config["Layers"] = layers;
            }

            if (environment.Count > 0)
            {
                config["Environment"] = environment;
            }

            var tags = new Dictionary<string, string>(StringComparer.Ordinal);
            if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in tagsEl.EnumerateObject())
                {
                    tags[prop.Name] = prop.Value.GetString() ?? "";
                }
            }

            var record = new FunctionRecord
            {
                Config = config,
                CodeZip = codeZip,
                Tags = tags,
            };

            // If Publish=true, publish version immediately
            var publish = GetBool(data, "Publish", false);
            if (publish)
            {
                var versionNum = record.NextVersion;
                record.NextVersion++;

                var versionConfig = CloneConfig(config);
                versionConfig["Version"] = versionNum.ToString();
                versionConfig["FunctionArn"] = $"{functionArn}:{versionNum}";

                record.Versions[versionNum.ToString()] = new VersionedSnapshot
                {
                    Config = versionConfig,
                    CodeZip = codeZip,
                };

                // Return published version config
                _functions[funcName] = record;
                return JsonResponse(versionConfig, 201);
            }

            _functions[funcName] = record;
            return JsonResponse(config, 201);
        }
    }

    private ServiceResponse HandleGetFunction(string funcName, ServiceRequest request)
    {
        lock (_lock)
        {
            var (name, qualifier) = ParseFunctionReference(funcName);
            if (qualifier is null)
            {
                qualifier = request.GetQueryParam("Qualifier");
            }

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            Dictionary<string, object?> config;
            if (qualifier is not null && qualifier != "$LATEST")
            {
                if (record.Aliases.TryGetValue(qualifier, out var alias))
                {
                    qualifier = alias.FunctionVersion;
                }

                if (record.Versions.TryGetValue(qualifier, out var snapshot))
                {
                    config = snapshot.Config;
                }
                else
                {
                    return ErrorResponse("ResourceNotFoundException",
                        $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}:{qualifier}", 404);
                }
            }
            else
            {
                config = record.Config;
            }

            var accountId = AccountContext.GetAccountId();
            var functionArn = $"arn:aws:lambda:{Region}:{accountId}:function:{name}";

            var codeLocation = new Dictionary<string, object?>
            {
                ["RepositoryType"] = "S3",
                ["Location"] = $"https://awslambda-{Region}-tasks.s3.{Region}.amazonaws.com/snapshots/{accountId}/{name}",
            };

            var result = new Dictionary<string, object?>
            {
                ["Configuration"] = config,
                ["Code"] = codeLocation,
                ["Tags"] = record.Tags,
            };

            return JsonResponse(result);
        }
    }

    private ServiceResponse HandleGetFunctionConfiguration(string funcName, ServiceRequest request)
    {
        lock (_lock)
        {
            var (name, qualifier) = ParseFunctionReference(funcName);
            if (qualifier is null)
            {
                qualifier = request.GetQueryParam("Qualifier");
            }

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            if (qualifier is not null && qualifier != "$LATEST")
            {
                if (record.Aliases.TryGetValue(qualifier, out var alias))
                {
                    qualifier = alias.FunctionVersion;
                }

                if (record.Versions.TryGetValue(qualifier, out var snapshot))
                {
                    return JsonResponse(snapshot.Config);
                }

                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}:{qualifier}", 404);
            }

            return JsonResponse(record.Config);
        }
    }

    private ServiceResponse HandleListFunctions(ServiceRequest request)
    {
        lock (_lock)
        {
            var marker = request.GetQueryParam("Marker");
            var maxItemsStr = request.GetQueryParam("MaxItems");
            var maxItems = 50;
            if (!string.IsNullOrEmpty(maxItemsStr) && int.TryParse(maxItemsStr, out var parsed))
            {
                maxItems = parsed;
            }

            var allNames = _functions.Keys.OrderBy(n => n, StringComparer.Ordinal).ToList();

            var start = 0;
            if (!string.IsNullOrEmpty(marker))
            {
                start = allNames.IndexOf(marker);
                if (start < 0)
                {
                    start = 0;
                }
                else
                {
                    start++; // Start after the marker
                }
            }

            var page = allNames.Skip(start).Take(maxItems).ToList();
            var functions = new List<Dictionary<string, object?>>();

            foreach (var name in page)
            {
                if (_functions.TryGetValue(name, out var record))
                {
                    functions.Add(record.Config);
                }
            }

            var result = new Dictionary<string, object?>
            {
                ["Functions"] = functions,
            };

            if (start + maxItems < allNames.Count)
            {
                result["NextMarker"] = page[^1];
            }

            return JsonResponse(result);
        }
    }

    private ServiceResponse HandleDeleteFunction(string funcName, ServiceRequest request)
    {
        lock (_lock)
        {
            var (name, qualifier) = ParseFunctionReference(funcName);
            if (qualifier is null)
            {
                qualifier = request.GetQueryParam("Qualifier");
            }

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            if (qualifier is not null && qualifier != "$LATEST")
            {
                // Delete specific version or alias
                if (record.Aliases.ContainsKey(qualifier))
                {
                    record.Aliases.Remove(qualifier);
                }
                else
                {
                    record.Versions.Remove(qualifier);
                }
            }
            else
            {
                _functions.TryRemove(name, out _);
                _workerPool.Invalidate(name);
            }

            return new ServiceResponse(204, EmptyHeaders, []);
        }
    }

    private ServiceResponse HandleUpdateFunctionCode(string funcName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            byte[]? codeZip = null;
            if (data.TryGetProperty("ZipFile", out var zipProp))
            {
                var zipBase64 = zipProp.GetString();
                if (!string.IsNullOrEmpty(zipBase64))
                {
                    codeZip = Convert.FromBase64String(zipBase64);
                }
            }

            if (codeZip is not null)
            {
                record.CodeZip = codeZip;
                record.Config["CodeSize"] = codeZip.Length;
                record.Config["CodeSha256"] = Convert.ToBase64String(SHA256.HashData(codeZip));
            }

            record.Config["LastModified"] = FormatLastModified(DateTime.UtcNow);
            record.Config["RevisionId"] = HashHelpers.NewUuid();

            // Invalidate any warm worker since code changed
            _workerPool.Invalidate(name);

            var publish = GetBool(data, "Publish", false);
            if (publish)
            {
                var versionNum = record.NextVersion;
                record.NextVersion++;

                var versionConfig = CloneConfig(record.Config);
                versionConfig["Version"] = versionNum.ToString();
                var baseFuncArn = $"arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}";
                versionConfig["FunctionArn"] = $"{baseFuncArn}:{versionNum}";

                record.Versions[versionNum.ToString()] = new VersionedSnapshot
                {
                    Config = versionConfig,
                    CodeZip = record.CodeZip,
                };

                return JsonResponse(versionConfig);
            }

            return JsonResponse(record.Config);
        }
    }

    private ServiceResponse HandleUpdateFunctionConfiguration(string funcName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            if (data.TryGetProperty("Handler", out var handlerProp))
            {
                record.Config["Handler"] = handlerProp.GetString();
            }

            if (data.TryGetProperty("Runtime", out var runtimeProp))
            {
                record.Config["Runtime"] = runtimeProp.GetString();
            }

            if (data.TryGetProperty("Description", out var descProp))
            {
                record.Config["Description"] = descProp.GetString();
            }

            if (data.TryGetProperty("Timeout", out var timeoutProp) && timeoutProp.TryGetInt32(out var timeoutVal))
            {
                record.Config["Timeout"] = timeoutVal;
            }

            if (data.TryGetProperty("MemorySize", out var memProp) && memProp.TryGetInt32(out var memVal))
            {
                record.Config["MemorySize"] = memVal;
            }

            if (data.TryGetProperty("Role", out var roleProp))
            {
                record.Config["Role"] = roleProp.GetString();
            }

            if (data.TryGetProperty("Environment", out var envProp))
            {
                var environment = new Dictionary<string, object?>();
                if (envProp.TryGetProperty("Variables", out var vars) && vars.ValueKind == JsonValueKind.Object)
                {
                    var variables = new Dictionary<string, object?>();
                    foreach (var prop in vars.EnumerateObject())
                    {
                        variables[prop.Name] = prop.Value.GetString();
                    }

                    environment["Variables"] = variables;
                }

                record.Config["Environment"] = environment;
            }

            if (data.TryGetProperty("Layers", out var layersProp) && layersProp.ValueKind == JsonValueKind.Array)
            {
                var layers = new List<object?>();
                foreach (var item in layersProp.EnumerateArray())
                {
                    var layerArn = item.GetString() ?? "";
                    layers.Add(new Dictionary<string, object?>
                    {
                        ["Arn"] = layerArn,
                        ["CodeSize"] = 0,
                    });
                }

                record.Config["Layers"] = layers;
            }

            record.Config["LastModified"] = FormatLastModified(DateTime.UtcNow);
            record.Config["RevisionId"] = HashHelpers.NewUuid();

            return JsonResponse(record.Config);
        }
    }

    // -- Versioning ------------------------------------------------------------

    private ServiceResponse HandlePublishVersion(string funcName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var versionNum = record.NextVersion;
            record.NextVersion++;

            var description = GetString(data, "Description");

            var versionConfig = CloneConfig(record.Config);
            versionConfig["Version"] = versionNum.ToString();
            var baseFuncArn = $"arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}";
            versionConfig["FunctionArn"] = $"{baseFuncArn}:{versionNum}";

            if (description is not null)
            {
                versionConfig["Description"] = description;
            }

            record.Versions[versionNum.ToString()] = new VersionedSnapshot
            {
                Config = versionConfig,
                CodeZip = record.CodeZip,
            };

            return JsonResponse(versionConfig, 201);
        }
    }

    private ServiceResponse HandleListVersionsByFunction(string funcName, ServiceRequest request)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var versions = new List<Dictionary<string, object?>>
            {
                record.Config, // $LATEST
            };

            foreach (var (_, snapshot) in record.Versions.OrderBy(kv => kv.Key, StringComparer.Ordinal))
            {
                versions.Add(snapshot.Config);
            }

            _ = request; // Marker/MaxItems not typically used for versions in tests

            return JsonResponse(new Dictionary<string, object?>
            {
                ["Versions"] = versions,
            });
        }
    }

    // -- Aliases ---------------------------------------------------------------

    private ServiceResponse HandleCreateAlias(string funcName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var aliasName = GetString(data, "Name");
            if (string.IsNullOrEmpty(aliasName))
            {
                return ErrorResponse("InvalidParameterValueException", "Alias name is required.", 400);
            }

            if (record.Aliases.ContainsKey(aliasName))
            {
                return ErrorResponse("ResourceConflictException",
                    $"Alias already exists: {aliasName}", 409);
            }

            var functionVersion = GetString(data, "FunctionVersion") ?? "$LATEST";
            var description = GetString(data, "Description") ?? "";
            var accountId = AccountContext.GetAccountId();
            var aliasArn = $"arn:aws:lambda:{Region}:{accountId}:function:{name}:{aliasName}";

            var aliasRecord = new AliasRecord
            {
                AliasArn = aliasArn,
                Name = aliasName,
                FunctionVersion = functionVersion,
                Description = description,
                RevisionId = HashHelpers.NewUuid(),
            };

            record.Aliases[aliasName] = aliasRecord;

            return JsonResponse(new Dictionary<string, object?>
            {
                ["AliasArn"] = aliasRecord.AliasArn,
                ["Name"] = aliasRecord.Name,
                ["FunctionVersion"] = aliasRecord.FunctionVersion,
                ["Description"] = aliasRecord.Description,
                ["RevisionId"] = aliasRecord.RevisionId,
            }, 201);
        }
    }

    private ServiceResponse HandleGetAlias(string funcName, string aliasName)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            if (!record.Aliases.TryGetValue(aliasName, out var alias))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Alias not found: {aliasName}", 404);
            }

            return JsonResponse(new Dictionary<string, object?>
            {
                ["AliasArn"] = alias.AliasArn,
                ["Name"] = alias.Name,
                ["FunctionVersion"] = alias.FunctionVersion,
                ["Description"] = alias.Description,
                ["RevisionId"] = alias.RevisionId,
            });
        }
    }

    private ServiceResponse HandleUpdateAlias(string funcName, string aliasName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            if (!record.Aliases.TryGetValue(aliasName, out var alias))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Alias not found: {aliasName}", 404);
            }

            if (data.TryGetProperty("FunctionVersion", out var fvProp))
            {
                alias.FunctionVersion = fvProp.GetString() ?? alias.FunctionVersion;
            }

            if (data.TryGetProperty("Description", out var descProp))
            {
                alias.Description = descProp.GetString() ?? alias.Description;
            }

            alias.RevisionId = HashHelpers.NewUuid();

            return JsonResponse(new Dictionary<string, object?>
            {
                ["AliasArn"] = alias.AliasArn,
                ["Name"] = alias.Name,
                ["FunctionVersion"] = alias.FunctionVersion,
                ["Description"] = alias.Description,
                ["RevisionId"] = alias.RevisionId,
            });
        }
    }

    private ServiceResponse HandleDeleteAlias(string funcName, string aliasName)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            record.Aliases.Remove(aliasName);

            return new ServiceResponse(204, EmptyHeaders, []);
        }
    }

    private ServiceResponse HandleListAliases(string funcName, ServiceRequest request)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);
            _ = request; // pagination not needed for now

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var aliases = new List<Dictionary<string, object?>>();
            foreach (var alias in record.Aliases.Values.OrderBy(a => a.Name, StringComparer.Ordinal))
            {
                aliases.Add(new Dictionary<string, object?>
                {
                    ["AliasArn"] = alias.AliasArn,
                    ["Name"] = alias.Name,
                    ["FunctionVersion"] = alias.FunctionVersion,
                    ["Description"] = alias.Description,
                    ["RevisionId"] = alias.RevisionId,
                });
            }

            return JsonResponse(new Dictionary<string, object?>
            {
                ["Aliases"] = aliases,
            });
        }
    }

    // -- Layers ----------------------------------------------------------------

    private ServiceResponse HandlePublishLayerVersion(string layerName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var accountId = AccountContext.GetAccountId();

            var record = _layers.GetOrAdd(layerName, _ => new LayerRecord
            {
                LayerName = layerName,
                LayerArn = $"arn:aws:lambda:{Region}:{accountId}:layer:{layerName}",
            });

            var versionNum = record.NextVersion;
            record.NextVersion++;

            byte[]? contentZip = null;
            if (data.TryGetProperty("Content", out var contentEl))
            {
                if (contentEl.TryGetProperty("ZipFile", out var zipProp))
                {
                    var zipBase64 = zipProp.GetString();
                    if (!string.IsNullOrEmpty(zipBase64))
                    {
                        contentZip = Convert.FromBase64String(zipBase64);
                    }
                }
            }

            var description = GetString(data, "Description") ?? "";

            var compatibleRuntimes = new List<object?>();
            if (data.TryGetProperty("CompatibleRuntimes", out var crEl) && crEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in crEl.EnumerateArray())
                {
                    compatibleRuntimes.Add(item.GetString());
                }
            }

            var layerVersionArn = $"{record.LayerArn}:{versionNum}";
            var codeSha256 = contentZip is not null
                ? Convert.ToBase64String(SHA256.HashData(contentZip))
                : "";
            var codeSize = contentZip?.Length ?? 0;

            var layerVersion = new LayerVersionRecord
            {
                VersionNumber = versionNum,
                Description = description,
                ContentZip = contentZip,
                CodeSha256 = codeSha256,
                CodeSize = codeSize,
                CompatibleRuntimes = compatibleRuntimes,
                LayerVersionArn = layerVersionArn,
                CreatedDate = FormatLastModified(DateTime.UtcNow),
            };

            record.Versions[versionNum] = layerVersion;

            return JsonResponse(new Dictionary<string, object?>
            {
                ["LayerArn"] = record.LayerArn,
                ["LayerVersionArn"] = layerVersionArn,
                ["Version"] = versionNum,
                ["Description"] = description,
                ["CompatibleRuntimes"] = compatibleRuntimes,
                ["CreatedDate"] = layerVersion.CreatedDate,
                ["Content"] = new Dictionary<string, object?>
                {
                    ["CodeSha256"] = codeSha256,
                    ["CodeSize"] = codeSize,
                    ["Location"] = $"/_ministack/lambda-layers/{accountId}/{layerName}/{versionNum}",
                },
            }, 201);
        }
    }

    private ServiceResponse HandleGetLayerVersion(string layerName, int versionNum)
    {
        lock (_lock)
        {
            if (!_layers.TryGetValue(layerName, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Layer not found: {layerName}", 404);
            }

            if (!record.Versions.TryGetValue(versionNum, out var version))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Layer version not found: {layerName}:{versionNum}", 404);
            }

            var accountId = AccountContext.GetAccountId();

            return JsonResponse(new Dictionary<string, object?>
            {
                ["LayerArn"] = record.LayerArn,
                ["LayerVersionArn"] = version.LayerVersionArn,
                ["Version"] = version.VersionNumber,
                ["Description"] = version.Description,
                ["CompatibleRuntimes"] = version.CompatibleRuntimes,
                ["CreatedDate"] = version.CreatedDate,
                ["Content"] = new Dictionary<string, object?>
                {
                    ["CodeSha256"] = version.CodeSha256,
                    ["CodeSize"] = version.CodeSize,
                    ["Location"] = $"/_ministack/lambda-layers/{accountId}/{layerName}/{versionNum}",
                },
            });
        }
    }

    private ServiceResponse HandleListLayerVersions(string layerName)
    {
        lock (_lock)
        {
            if (!_layers.TryGetValue(layerName, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Layer not found: {layerName}", 404);
            }

            var versions = new List<Dictionary<string, object?>>();
            foreach (var (_, version) in record.Versions.OrderBy(kv => kv.Key))
            {
                versions.Add(new Dictionary<string, object?>
                {
                    ["LayerVersionArn"] = version.LayerVersionArn,
                    ["Version"] = version.VersionNumber,
                    ["Description"] = version.Description,
                    ["CompatibleRuntimes"] = version.CompatibleRuntimes,
                    ["CreatedDate"] = version.CreatedDate,
                });
            }

            return JsonResponse(new Dictionary<string, object?>
            {
                ["LayerVersions"] = versions,
            });
        }
    }

    private ServiceResponse HandleListLayers()
    {
        lock (_lock)
        {
            var layers = new List<Dictionary<string, object?>>();
            foreach (var (_, record) in _layers.Items.OrderBy(kv => kv.Key, StringComparer.Ordinal))
            {
                var latestVersion = record.Versions.OrderByDescending(kv => kv.Key).FirstOrDefault();
                if (latestVersion.Value is not null)
                {
                    layers.Add(new Dictionary<string, object?>
                    {
                        ["LayerName"] = record.LayerName,
                        ["LayerArn"] = record.LayerArn,
                        ["LatestMatchingVersion"] = new Dictionary<string, object?>
                        {
                            ["LayerVersionArn"] = latestVersion.Value.LayerVersionArn,
                            ["Version"] = latestVersion.Value.VersionNumber,
                            ["Description"] = latestVersion.Value.Description,
                            ["CompatibleRuntimes"] = latestVersion.Value.CompatibleRuntimes,
                            ["CreatedDate"] = latestVersion.Value.CreatedDate,
                        },
                    });
                }
            }

            return JsonResponse(new Dictionary<string, object?>
            {
                ["Layers"] = layers,
            });
        }
    }

    private ServiceResponse HandleDeleteLayerVersion(string layerName, int versionNum)
    {
        lock (_lock)
        {
            if (_layers.TryGetValue(layerName, out var record))
            {
                record.Versions.Remove(versionNum);
            }

            return new ServiceResponse(204, EmptyHeaders, []);
        }
    }

    private ServiceResponse HandleLayerContentDownload(string path)
    {
        // /_ministack/lambda-layers/{accountId}/{layerName}/{version}
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 5)
        {
            return ErrorResponse("ResourceNotFoundException", "Layer content not found.", 404);
        }

        var layerName = segments[3];
        if (!int.TryParse(segments[4], out var versionNum))
        {
            return ErrorResponse("ResourceNotFoundException", "Invalid layer version.", 404);
        }

        lock (_lock)
        {
            if (!_layers.TryGetValue(layerName, out var record))
            {
                return ErrorResponse("ResourceNotFoundException", "Layer not found.", 404);
            }

            if (!record.Versions.TryGetValue(versionNum, out var version))
            {
                return ErrorResponse("ResourceNotFoundException", "Layer version not found.", 404);
            }

            if (version.ContentZip is null)
            {
                return ErrorResponse("ResourceNotFoundException", "No content for layer version.", 404);
            }

            var headers = new Dictionary<string, string>
            {
                ["Content-Type"] = "application/zip",
            };

            return new ServiceResponse(200, headers, version.ContentZip);
        }
    }

    // -- Tags ------------------------------------------------------------------

    private ServiceResponse HandleListTags(string arn)
    {
        lock (_lock)
        {
            var funcName = ExtractFunctionNameFromArn(arn);
            if (funcName is null || !_functions.TryGetValue(funcName, out var record))
            {
                return ErrorResponse("ResourceNotFoundException", $"Resource not found: {arn}", 404);
            }

            return JsonResponse(new Dictionary<string, object?>
            {
                ["Tags"] = record.Tags,
            });
        }
    }

    private ServiceResponse HandleTagResource(string arn, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var funcName = ExtractFunctionNameFromArn(arn);
            if (funcName is null || !_functions.TryGetValue(funcName, out var record))
            {
                return ErrorResponse("ResourceNotFoundException", $"Resource not found: {arn}", 404);
            }

            if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in tagsEl.EnumerateObject())
                {
                    record.Tags[prop.Name] = prop.Value.GetString() ?? "";
                }
            }

            return new ServiceResponse(204, EmptyHeaders, []);
        }
    }

    private ServiceResponse HandleUntagResource(string arn, ServiceRequest request)
    {
        lock (_lock)
        {
            var funcName = ExtractFunctionNameFromArn(arn);
            if (funcName is null || !_functions.TryGetValue(funcName, out var record))
            {
                return ErrorResponse("ResourceNotFoundException", $"Resource not found: {arn}", 404);
            }

            // TagKeys come as query params: ?tagKeys=key1&tagKeys=key2
            if (request.QueryParams.TryGetValue("tagKeys", out var keys))
            {
                foreach (var key in keys)
                {
                    record.Tags.Remove(key);
                }
            }

            return new ServiceResponse(204, EmptyHeaders, []);
        }
    }

    // -- Permissions -----------------------------------------------------------

    private ServiceResponse HandleAddPermission(string funcName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var sid = GetString(data, "StatementId") ?? HashHelpers.NewUuid();
            var action = GetString(data, "Action") ?? "lambda:InvokeFunction";
            var principal = GetString(data, "Principal") ?? "*";
            var sourceArn = GetString(data, "SourceArn");
            var sourceAccount = GetString(data, "SourceAccount");

            record.Policy ??= new PolicyDocument
            {
                Version = "2012-10-17",
            };

            var statement = new Dictionary<string, object?>
            {
                ["Sid"] = sid,
                ["Effect"] = "Allow",
                ["Principal"] = new Dictionary<string, object?> { ["Service"] = principal },
                ["Action"] = action,
                ["Resource"] = $"arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}",
            };

            if (sourceArn is not null)
            {
                statement["Condition"] = new Dictionary<string, object?>
                {
                    ["ArnLike"] = new Dictionary<string, object?> { ["AWS:SourceArn"] = sourceArn },
                };
            }

            if (sourceAccount is not null)
            {
                statement["Condition"] = new Dictionary<string, object?>
                {
                    ["StringEquals"] = new Dictionary<string, object?> { ["AWS:SourceAccount"] = sourceAccount },
                };
            }

            record.Policy.Statements.Add(statement);

            var policyJson = JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["Version"] = record.Policy.Version,
                ["Id"] = "default",
                ["Statement"] = record.Policy.Statements,
            });

            return JsonResponse(new Dictionary<string, object?>
            {
                ["Statement"] = policyJson,
            }, 201);
        }
    }

    private ServiceResponse HandleGetPolicy(string funcName)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            if (record.Policy is null || record.Policy.Statements.Count == 0)
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"No policy is associated with the given resource.", 404);
            }

            var policyJson = JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["Version"] = record.Policy.Version,
                ["Id"] = "default",
                ["Statement"] = record.Policy.Statements,
            });

            return JsonResponse(new Dictionary<string, object?>
            {
                ["Policy"] = policyJson,
                ["RevisionId"] = HashHelpers.NewUuid(),
            });
        }
    }

    private ServiceResponse HandleRemovePermission(string funcName, string statementId)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            if (record.Policy is not null)
            {
                record.Policy.Statements.RemoveAll(s =>
                {
                    if (s.TryGetValue("Sid", out var sid) && sid is string sidStr)
                    {
                        return string.Equals(sidStr, statementId, StringComparison.Ordinal);
                    }

                    return false;
                });
            }

            return new ServiceResponse(204, EmptyHeaders, []);
        }
    }

    // -- Concurrency -----------------------------------------------------------

    private ServiceResponse HandlePutFunctionConcurrency(string funcName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var concurrency = GetInt(data, "ReservedConcurrentExecutions", 0);
            record.Concurrency = concurrency;

            return JsonResponse(new Dictionary<string, object?>
            {
                ["ReservedConcurrentExecutions"] = concurrency,
            });
        }
    }

    private ServiceResponse HandleGetFunctionConcurrency(string funcName)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var result = new Dictionary<string, object?>
            {
                ["ReservedConcurrentExecutions"] = record.Concurrency ?? 0,
            };

            return JsonResponse(result);
        }
    }

    private ServiceResponse HandleDeleteFunctionConcurrency(string funcName)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            record.Concurrency = null;

            return new ServiceResponse(204, EmptyHeaders, []);
        }
    }

    // -- Provisioned Concurrency (stubs) ----------------------------------------

    private ServiceResponse HandlePutProvisionedConcurrency(string funcName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);
            var qualifier = request.GetQueryParam("Qualifier") ?? "$LATEST";

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var requested = GetInt(data, "ProvisionedConcurrentExecutions", 1);

            record.ProvisionedConcurrency[qualifier] = new ProvisionedConcurrencyRecord
            {
                RequestedProvisionedConcurrentExecutions = requested,
                AvailableProvisionedConcurrentExecutions = requested,
                AllocatedProvisionedConcurrentExecutions = requested,
                Status = "READY",
                LastModified = FormatLastModified(DateTime.UtcNow),
            };

            return JsonResponse(new Dictionary<string, object?>
            {
                ["RequestedProvisionedConcurrentExecutions"] = requested,
                ["AvailableProvisionedConcurrentExecutions"] = requested,
                ["AllocatedProvisionedConcurrentExecutions"] = requested,
                ["Status"] = "READY",
                ["LastModified"] = record.ProvisionedConcurrency[qualifier].LastModified,
            }, 202);
        }
    }

    private ServiceResponse HandleGetProvisionedConcurrency(string funcName, ServiceRequest request)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);
            var qualifier = request.GetQueryParam("Qualifier") ?? "$LATEST";

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            if (!record.ProvisionedConcurrency.TryGetValue(qualifier, out var pc))
            {
                return ErrorResponse("ProvisionedConcurrencyConfigNotFoundException",
                    $"No Provisioned Concurrency Config found for function: {name}", 404);
            }

            return JsonResponse(new Dictionary<string, object?>
            {
                ["RequestedProvisionedConcurrentExecutions"] = pc.RequestedProvisionedConcurrentExecutions,
                ["AvailableProvisionedConcurrentExecutions"] = pc.AvailableProvisionedConcurrentExecutions,
                ["AllocatedProvisionedConcurrentExecutions"] = pc.AllocatedProvisionedConcurrentExecutions,
                ["Status"] = pc.Status,
                ["LastModified"] = pc.LastModified,
            });
        }
    }

    private ServiceResponse HandleDeleteProvisionedConcurrency(string funcName, ServiceRequest request)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);
            var qualifier = request.GetQueryParam("Qualifier") ?? "$LATEST";

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            record.ProvisionedConcurrency.Remove(qualifier);

            return new ServiceResponse(204, EmptyHeaders, []);
        }
    }

    // -- Event Invoke Config ---------------------------------------------------

    private ServiceResponse HandlePutEventInvokeConfig(string funcName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var maxRetry = GetInt(data, "MaximumRetryAttempts", 2);
            var maxAge = GetInt(data, "MaximumEventAgeInSeconds", 21600);
            var functionArn = $"arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}";

            Dictionary<string, object?>? destinationConfig = null;
            if (data.TryGetProperty("DestinationConfig", out var destEl))
            {
                destinationConfig = JsonElementToDict(destEl);
            }

            record.EventInvokeConfig = new EventInvokeConfig
            {
                MaximumRetryAttempts = maxRetry,
                MaximumEventAgeInSeconds = maxAge,
                FunctionArn = functionArn,
                LastModified = TimeHelpers.NowEpoch(),
                DestinationConfig = destinationConfig,
            };

            var result = new Dictionary<string, object?>
            {
                ["FunctionArn"] = functionArn,
                ["MaximumRetryAttempts"] = maxRetry,
                ["MaximumEventAgeInSeconds"] = maxAge,
                ["LastModified"] = record.EventInvokeConfig.LastModified,
            };

            if (destinationConfig is not null)
            {
                result["DestinationConfig"] = destinationConfig;
            }

            return JsonResponse(result);
        }
    }

    private ServiceResponse HandleGetEventInvokeConfig(string funcName)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            if (record.EventInvokeConfig is null)
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"No EventInvokeConfig found for function: {name}", 404);
            }

            var result = new Dictionary<string, object?>
            {
                ["FunctionArn"] = record.EventInvokeConfig.FunctionArn,
                ["MaximumRetryAttempts"] = record.EventInvokeConfig.MaximumRetryAttempts,
                ["MaximumEventAgeInSeconds"] = record.EventInvokeConfig.MaximumEventAgeInSeconds,
                ["LastModified"] = record.EventInvokeConfig.LastModified,
            };

            if (record.EventInvokeConfig.DestinationConfig is not null)
            {
                result["DestinationConfig"] = record.EventInvokeConfig.DestinationConfig;
            }

            return JsonResponse(result);
        }
    }

    private ServiceResponse HandleDeleteEventInvokeConfig(string funcName)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            record.EventInvokeConfig = null;

            return new ServiceResponse(204, EmptyHeaders, []);
        }
    }

    // -- Invoke ----------------------------------------------------------------

    private ServiceResponse HandleInvoke(string funcName, ServiceRequest request)
    {
        // Resolve the function record and qualifier inside the lock,
        // but run the actual worker invocation outside it to avoid holding
        // the lock during a potentially long subprocess call.
        string name;
        string executedVersion;
        Dictionary<string, object?> config;
        byte[]? codeZip;
        string runtime;

        lock (_lock)
        {
            string? qualifier;
            (name, qualifier) = ParseFunctionReference(funcName);
            if (qualifier is null)
            {
                qualifier = request.GetQueryParam("Qualifier");
            }

            if (!_functions.TryGetValue(name, out var record))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            executedVersion = "$LATEST";
            config = record.Config;
            codeZip = record.CodeZip;

            if (qualifier is not null && qualifier != "$LATEST")
            {
                if (record.Aliases.TryGetValue(qualifier, out var alias))
                {
                    executedVersion = alias.FunctionVersion;
                }
                else if (record.Versions.ContainsKey(qualifier))
                {
                    executedVersion = qualifier;
                }
                else
                {
                    return ErrorResponse("ResourceNotFoundException",
                        $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}:{qualifier}", 404);
                }
            }

            // Resolve config + code for the specific version
            if (executedVersion != "$LATEST" && record.Versions.TryGetValue(executedVersion, out var snapshot))
            {
                config = snapshot.Config;
                codeZip = snapshot.CodeZip;
            }

            var invocationType = request.GetHeader("x-amz-invocation-type") ?? "RequestResponse";

            if (string.Equals(invocationType, "DryRun", StringComparison.OrdinalIgnoreCase))
            {
                var dryRunHeaders = new Dictionary<string, string>
                {
                    ["X-Amz-Executed-Version"] = executedVersion,
                    ["Content-Type"] = "application/json",
                };
                return new ServiceResponse(204, dryRunHeaders, []);
            }

            if (string.Equals(invocationType, "Event", StringComparison.OrdinalIgnoreCase))
            {
                var eventHeaders = new Dictionary<string, string>
                {
                    ["X-Amz-Executed-Version"] = executedVersion,
                    ["Content-Type"] = "application/json",
                };
                return new ServiceResponse(202, eventHeaders, []);
            }

            runtime = config.TryGetValue("Runtime", out var rtVal) ? rtVal?.ToString() ?? "" : "";
        }

        // RequestResponse — attempt actual execution for supported runtimes
        if (codeZip is not null && codeZip.Length > 0 && IsSupportedRuntime(runtime))
        {
            var requestId = HashHelpers.NewUuid();
            WorkerResult workerResult;

            try
            {
                var worker = _workerPool.GetOrCreate(name, config, codeZip);
                workerResult = worker.Invoke(request.Body, requestId);
            }
            catch (InvalidOperationException ex)
            {
                // Worker failed to start (e.g. python/node not available)
                var errorHeaders = new Dictionary<string, string>
                {
                    ["X-Amz-Executed-Version"] = executedVersion,
                    ["X-Amz-Function-Error"] = "Unhandled",
                    ["Content-Type"] = "application/json",
                    ["X-Amz-Log-Result"] = "",
                };
                var errorPayload = JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object?>
                {
                    ["errorMessage"] = ex.Message,
                    ["errorType"] = "Runtime.InitError",
                });
                return new ServiceResponse(200, errorHeaders, errorPayload);
            }

            var invokeHeaders = new Dictionary<string, string>
            {
                ["X-Amz-Executed-Version"] = executedVersion,
                ["Content-Type"] = "application/json",
            };

            if (!string.IsNullOrEmpty(workerResult.Log))
            {
                // Encode log as base64 for X-Amz-Log-Result header
                var logBytes = Encoding.UTF8.GetBytes(workerResult.Log);
                invokeHeaders["X-Amz-Log-Result"] = Convert.ToBase64String(logBytes);
            }

            if (workerResult.Success)
            {
                var resultBytes = workerResult.ResultJson is not null
                    ? Encoding.UTF8.GetBytes(workerResult.ResultJson)
                    : Encoding.UTF8.GetBytes("null");
                return new ServiceResponse(200, invokeHeaders, resultBytes);
            }

            // Error response
            invokeHeaders["X-Amz-Function-Error"] = "Unhandled";
            var errorBody = JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object?>
            {
                ["errorMessage"] = workerResult.Error,
                ["errorType"] = "Error",
                ["stackTrace"] = workerResult.Trace is not null
                    ? workerResult.Trace.Split('\n', StringSplitOptions.RemoveEmptyEntries).ToList()
                    : new List<string>(),
            });
            return new ServiceResponse(200, invokeHeaders, errorBody);
        }

        // Unsupported runtime or no code — stub returning empty JSON
        var stubHeaders = new Dictionary<string, string>
        {
            ["X-Amz-Executed-Version"] = executedVersion,
            ["Content-Type"] = "application/json",
        };
        return new ServiceResponse(200, stubHeaders, Encoding.UTF8.GetBytes("{}"));
    }

    private static bool IsSupportedRuntime(string runtime)
    {
        return runtime.StartsWith("python", StringComparison.OrdinalIgnoreCase)
            || runtime.StartsWith("nodejs", StringComparison.OrdinalIgnoreCase);
    }

    // -- Event Source Mappings -------------------------------------------------

    private ServiceResponse HandleCreateEsm(ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var uuid = HashHelpers.NewUuid();
            var funcName = GetString(data, "FunctionName") ?? "";
            var eventSourceArn = GetString(data, "EventSourceArn") ?? "";
            var batchSize = GetInt(data, "BatchSize", 10);
            var enabled = GetBool(data, "Enabled", true);

            var accountId = AccountContext.GetAccountId();
            var esmArn = $"arn:aws:lambda:{Region}:{accountId}:event-source-mapping:{uuid}";

            var esm = new Dictionary<string, object?>
            {
                ["UUID"] = uuid,
                ["FunctionArn"] = funcName.Contains(':')
                    ? funcName
                    : $"arn:aws:lambda:{Region}:{accountId}:function:{funcName}",
                ["EventSourceArn"] = eventSourceArn,
                ["BatchSize"] = batchSize,
                ["State"] = enabled ? "Enabled" : "Disabled",
                ["StateTransitionReason"] = "USER_INITIATED",
                ["LastModified"] = TimeHelpers.NowEpoch(),
                ["EventSourceMappingArn"] = esmArn,
            };

            // Copy over extra fields
            foreach (var propName in new[] { "StartingPosition", "MaximumBatchingWindowInSeconds", "ParallelizationFactor", "BisectBatchOnFunctionError" })
            {
                if (data.TryGetProperty(propName, out var propVal))
                {
                    esm[propName] = JsonElementToObject(propVal);
                }
            }

            _esms[uuid] = esm;

            // Start the ESM background poller if SQS and DynamoDB handlers are available
            if (_sqsHandler is not null && _ddbHandler is not null)
            {
                _poller ??= new EventSourceMappingPoller(this, _sqsHandler, _ddbHandler);
                _poller.EnsureStarted();
            }

            return JsonResponse(esm, 202);
        }
    }

    private ServiceResponse HandleGetEsm(string uuid)
    {
        lock (_lock)
        {
            if (!_esms.TryGetValue(uuid, out var esm))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Event source mapping not found: {uuid}", 404);
            }

            return JsonResponse(esm);
        }
    }

    private ServiceResponse HandleListEsm(ServiceRequest request)
    {
        lock (_lock)
        {
            var funcName = request.GetQueryParam("FunctionName");
            var eventSourceArn = request.GetQueryParam("EventSourceArn");

            var results = new List<Dictionary<string, object?>>();

            foreach (var esm in _esms.Values)
            {
                if (funcName is not null)
                {
                    var esmFunc = esm.TryGetValue("FunctionArn", out var fa) ? fa?.ToString() : null;
                    if (esmFunc is null || (!esmFunc.Contains(funcName, StringComparison.Ordinal)))
                    {
                        continue;
                    }
                }

                if (eventSourceArn is not null)
                {
                    var esmSource = esm.TryGetValue("EventSourceArn", out var sa) ? sa?.ToString() : null;
                    if (!string.Equals(esmSource, eventSourceArn, StringComparison.Ordinal))
                    {
                        continue;
                    }
                }

                results.Add(esm);
            }

            return JsonResponse(new Dictionary<string, object?>
            {
                ["EventSourceMappings"] = results,
            });
        }
    }

    private ServiceResponse HandleUpdateEsm(string uuid, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            if (!_esms.TryGetValue(uuid, out var esm))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Event source mapping not found: {uuid}", 404);
            }

            if (data.TryGetProperty("FunctionName", out var fnProp))
            {
                var fn = fnProp.GetString() ?? "";
                var accountId = AccountContext.GetAccountId();
                esm["FunctionArn"] = fn.Contains(':')
                    ? fn
                    : $"arn:aws:lambda:{Region}:{accountId}:function:{fn}";
            }

            if (data.TryGetProperty("BatchSize", out var bsProp) && bsProp.TryGetInt32(out var bs))
            {
                esm["BatchSize"] = bs;
            }

            if (data.TryGetProperty("Enabled", out var enProp))
            {
                var enabled = enProp.ValueKind == JsonValueKind.True;
                esm["State"] = enabled ? "Enabled" : "Disabled";
            }

            esm["LastModified"] = TimeHelpers.NowEpoch();

            return JsonResponse(esm);
        }
    }

    private ServiceResponse HandleDeleteEsm(string uuid)
    {
        lock (_lock)
        {
            if (!_esms.TryGetValue(uuid, out var esm))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Event source mapping not found: {uuid}", 404);
            }

            esm["State"] = "Deleting";
            _esms.TryRemove(uuid, out _);

            return JsonResponse(esm, 202);
        }
    }

    // -- ESM Poller Helpers (internal — used by EventSourceMappingPoller) ------

    /// <summary>
    /// Returns all enabled ESMs grouped by account ID.
    /// Uses <see cref="AccountScopedDictionary{TKey,TValue}.ToRaw"/> to iterate
    /// across all accounts without requiring an account context.
    /// </summary>
    internal Dictionary<string, List<Dictionary<string, object?>>> GetEnabledEsmsByAccount()
    {
        lock (_lock)
        {
            var result = new Dictionary<string, List<Dictionary<string, object?>>>(StringComparer.Ordinal);
            foreach (var kv in _esms.ToRaw())
            {
                var accountId = kv.Key.AccountId;
                var esm = kv.Value;
                if (!esm.TryGetValue("State", out var s)
                    || !string.Equals(s?.ToString(), "Enabled", StringComparison.Ordinal))
                {
                    continue;
                }

                if (!result.TryGetValue(accountId, out var list))
                {
                    list = [];
                    result[accountId] = list;
                }

                list.Add(esm);
            }

            return result;
        }
    }

    /// <summary>
    /// Invokes a Lambda function for an ESM event. Called by the background poller.
    /// Returns true if the invocation succeeded, false otherwise.
    /// </summary>
    internal bool InvokeForEsm(string functionArnOrName, Dictionary<string, object?> eventPayload)
    {
        // Resolve function name from ARN (e.g., "arn:aws:lambda:us-east-1:000000000000:function:my-func")
        var funcName = functionArnOrName;
        if (funcName.Contains(':'))
        {
            var parts = funcName.Split(':');
            funcName = parts[^1]; // last segment is function name
        }

        string name;
        Dictionary<string, object?> config;
        byte[]? codeZip;
        string runtime;

        lock (_lock)
        {
            (name, _) = ParseFunctionReference(funcName);
            if (!_functions.TryGetValue(name, out var record))
            {
                return false;
            }

            config = record.Config;
            codeZip = record.CodeZip;
            runtime = config.TryGetValue("Runtime", out var rtVal) ? rtVal?.ToString() ?? "" : "";
        }

        if (codeZip is null || codeZip.Length == 0 || !IsSupportedRuntime(runtime))
        {
            return false;
        }

        try
        {
            var requestId = HashHelpers.NewUuid();
            var eventJson = JsonSerializer.SerializeToUtf8Bytes(eventPayload);
            var worker = _workerPool.GetOrCreate(name, config, codeZip);
            var workerResult = worker.Invoke(eventJson, requestId);
            return workerResult.Success;
        }
        catch (Exception)
        {
            return false;
        }
    }

    // -- Function URL Config ---------------------------------------------------

    private ServiceResponse HandleCreateFunctionUrlConfig(string funcName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);
            var qualifier = request.GetQueryParam("Qualifier") ?? "$LATEST";

            if (!_functions.TryGetValue(name, out _))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var key = $"{name}:{qualifier}";
            var accountId = AccountContext.GetAccountId();
            var urlId = HashHelpers.NewUuidNoDashes()[..12];
            var functionUrl = $"https://{urlId}.lambda-url.{Region}.on.aws/";
            var authType = GetString(data, "AuthType") ?? "NONE";

            var urlConfig = new Dictionary<string, object?>
            {
                ["FunctionArn"] = $"arn:aws:lambda:{Region}:{accountId}:function:{name}",
                ["FunctionUrl"] = functionUrl,
                ["AuthType"] = authType,
                ["CreationTime"] = FormatLastModified(DateTime.UtcNow),
                ["LastModifiedTime"] = FormatLastModified(DateTime.UtcNow),
            };

            if (data.TryGetProperty("Cors", out var corsEl))
            {
                urlConfig["Cors"] = JsonElementToDict(corsEl);
            }

            if (data.TryGetProperty("InvokeMode", out var invokeModeEl))
            {
                urlConfig["InvokeMode"] = invokeModeEl.GetString();
            }

            _functions.TryGetValue(name, out _); // We already validated above
            // Store function URL config keyed by function:qualifier
            StoreUrlConfig(key, urlConfig);

            return JsonResponse(urlConfig, 201);
        }
    }

    private ServiceResponse HandleGetFunctionUrlConfig(string funcName, ServiceRequest request)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);
            var qualifier = request.GetQueryParam("Qualifier") ?? "$LATEST";

            if (!_functions.TryGetValue(name, out _))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var key = $"{name}:{qualifier}";
            var urlConfig = GetUrlConfig(key);
            if (urlConfig is null)
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"No function URL configuration found for function: {name}", 404);
            }

            return JsonResponse(urlConfig);
        }
    }

    private ServiceResponse HandleUpdateFunctionUrlConfig(string funcName, ServiceRequest request)
    {
        var data = ParseBody(request);
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);
            var qualifier = request.GetQueryParam("Qualifier") ?? "$LATEST";

            if (!_functions.TryGetValue(name, out _))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var key = $"{name}:{qualifier}";
            var urlConfig = GetUrlConfig(key);
            if (urlConfig is null)
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"No function URL configuration found for function: {name}", 404);
            }

            if (data.TryGetProperty("AuthType", out var authTypeProp))
            {
                urlConfig["AuthType"] = authTypeProp.GetString();
            }

            if (data.TryGetProperty("Cors", out var corsEl))
            {
                urlConfig["Cors"] = JsonElementToDict(corsEl);
            }

            if (data.TryGetProperty("InvokeMode", out var invokeModeEl))
            {
                urlConfig["InvokeMode"] = invokeModeEl.GetString();
            }

            urlConfig["LastModifiedTime"] = FormatLastModified(DateTime.UtcNow);

            return JsonResponse(urlConfig);
        }
    }

    private ServiceResponse HandleDeleteFunctionUrlConfig(string funcName, ServiceRequest request)
    {
        lock (_lock)
        {
            var (name, _) = ParseFunctionReference(funcName);
            var qualifier = request.GetQueryParam("Qualifier") ?? "$LATEST";

            if (!_functions.TryGetValue(name, out _))
            {
                return ErrorResponse("ResourceNotFoundException",
                    $"Function not found: arn:aws:lambda:{Region}:{AccountContext.GetAccountId()}:function:{name}", 404);
            }

            var key = $"{name}:{qualifier}";
            RemoveUrlConfig(key);

            return new ServiceResponse(204, EmptyHeaders, []);
        }
    }

    // -- Code Signing Config (stub) --------------------------------------------

    private static ServiceResponse HandleCodeSigningConfig(string method)
    {
        if (method == "GET")
        {
            return JsonResponse(new Dictionary<string, object?>
            {
                ["CodeSigningConfigs"] = Array.Empty<object>(),
            });
        }

        return ErrorResponse("InvalidRequestException", "Code signing config stub - not implemented.", 501);
    }

    // -- Function URL storage helpers ------------------------------------------
    // We use a ConcurrentDictionary-like pattern; since AccountScopedDictionary
    // doesn't support composite keys easily, we use a convention of key="{name}:{qualifier}"
    // and store as a regular dict inside the lock.

    private readonly Dictionary<string, Dictionary<string, object?>> _urlConfigs = new(StringComparer.Ordinal);

    private void StoreUrlConfig(string key, Dictionary<string, object?> config)
    {
        _urlConfigs[key] = config;
    }

    private Dictionary<string, object?>? GetUrlConfig(string key)
    {
        return _urlConfigs.TryGetValue(key, out var config) ? config : null;
    }

    private void RemoveUrlConfig(string key)
    {
        _urlConfigs.Remove(key);
    }

    // -- Helpers ---------------------------------------------------------------

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();

    private static readonly IReadOnlyDictionary<string, string> JsonContentTypeHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "application/json" };

    private static JsonElement ParseBody(ServiceRequest request)
    {
        if (request.Body.Length == 0)
        {
            using var emptyDoc = JsonDocument.Parse("{}");
            return emptyDoc.RootElement.Clone();
        }

        try
        {
            using var doc = JsonDocument.Parse(request.Body);
            return doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            return default;
        }
    }

    private static ServiceResponse JsonResponse(object data, int statusCode)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(data, JsonOpts);
        return new ServiceResponse(statusCode, JsonContentTypeHeaders, json);
    }

    private static ServiceResponse JsonResponse(object data)
    {
        return JsonResponse(data, 200);
    }

    private static ServiceResponse ErrorResponse(string code, string message, int statusCode)
    {
        var data = new Dictionary<string, object?>
        {
            ["Type"] = code,
            ["Message"] = message,
            ["__type"] = code,
            ["message"] = message,
        };
        return JsonResponse(data, statusCode);
    }

    private static string? GetString(JsonElement el, string propertyName)
    {
        return el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;
    }

    private static int GetInt(JsonElement el, string propertyName, int defaultValue)
    {
        if (!el.TryGetProperty(propertyName, out var prop))
        {
            return defaultValue;
        }

        return prop.TryGetInt32(out var val) ? val : defaultValue;
    }

    private static bool GetBool(JsonElement el, string propertyName, bool defaultValue)
    {
        if (!el.TryGetProperty(propertyName, out var prop))
        {
            return defaultValue;
        }

        return prop.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => defaultValue,
        };
    }

    /// <summary>
    /// Parse a function reference like "my-func", "my-func:prod", or an ARN.
    /// Returns (name, qualifier).
    /// </summary>
    private static (string Name, string? Qualifier) ParseFunctionReference(string reference)
    {
        if (reference.StartsWith("arn:", StringComparison.Ordinal))
        {
            // arn:aws:lambda:us-east-1:000000000000:function:my-func[:qualifier]
            var parts = reference.Split(':');
            if (parts.Length >= 7)
            {
                var name = parts[6];
                var qualifier = parts.Length >= 8 ? parts[7] : null;
                return (name, qualifier);
            }

            return (reference, null);
        }

        var colonIndex = reference.IndexOf(':');
        if (colonIndex >= 0)
        {
            return (reference[..colonIndex], reference[(colonIndex + 1)..]);
        }

        return (reference, null);
    }

    private static string? ExtractFunctionNameFromArn(string arn)
    {
        // arn:aws:lambda:us-east-1:000000000000:function:my-func
        if (!arn.StartsWith("arn:", StringComparison.Ordinal))
        {
            return arn; // might just be the name
        }

        var parts = arn.Split(':');
        if (parts.Length >= 7 && parts[5] == "function")
        {
            return parts[6];
        }

        return null;
    }

    private static string FormatLastModified(DateTime dt)
    {
        return dt.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "+0000";
    }

    private static Dictionary<string, object?> CloneConfig(Dictionary<string, object?> config)
    {
        // Deep clone by serializing and deserializing
        var json = JsonSerializer.SerializeToUtf8Bytes(config, JsonOpts);
        using var doc = JsonDocument.Parse(json);
        return JsonElementToDict(doc.RootElement);
    }

    private static Dictionary<string, object?> JsonElementToDict(JsonElement el)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (el.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in el.EnumerateObject())
            {
                dict[prop.Name] = JsonElementToObject(prop.Value);
            }
        }

        return dict;
    }

    private static object? JsonElementToObject(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? (object)l : el.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Object => JsonElementToDict(el),
            JsonValueKind.Array => el.EnumerateArray().Select(JsonElementToObject).ToList(),
            _ => null,
        };
    }

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false,
    };

    // -- Data Structures -------------------------------------------------------

    private sealed class FunctionRecord
    {
        internal Dictionary<string, object?> Config { get; set; } = new();
        internal byte[]? CodeZip { get; set; }
        internal Dictionary<string, VersionedSnapshot> Versions { get; set; } = new(StringComparer.Ordinal);
        internal int NextVersion { get; set; } = 1;
        internal Dictionary<string, string> Tags { get; set; } = new(StringComparer.Ordinal);
        internal PolicyDocument? Policy { get; set; }
        internal Dictionary<string, AliasRecord> Aliases { get; set; } = new(StringComparer.Ordinal);
        internal int? Concurrency { get; set; }
        internal Dictionary<string, ProvisionedConcurrencyRecord> ProvisionedConcurrency { get; set; } = new(StringComparer.Ordinal);
        internal EventInvokeConfig? EventInvokeConfig { get; set; }
    }

    private sealed class VersionedSnapshot
    {
        internal Dictionary<string, object?> Config { get; set; } = new();
        internal byte[]? CodeZip { get; set; }
    }

    private sealed class AliasRecord
    {
        internal string AliasArn { get; set; } = "";
        internal string Name { get; set; } = "";
        internal string FunctionVersion { get; set; } = "";
        internal string Description { get; set; } = "";
        internal string RevisionId { get; set; } = "";
    }

    private sealed class PolicyDocument
    {
        internal string Version { get; set; } = "2012-10-17";
        internal List<Dictionary<string, object?>> Statements { get; set; } = [];
    }

    private sealed class LayerRecord
    {
        internal string LayerName { get; set; } = "";
        internal string LayerArn { get; set; } = "";
        internal int NextVersion { get; set; } = 1;
        internal Dictionary<int, LayerVersionRecord> Versions { get; set; } = [];
    }

    private sealed class LayerVersionRecord
    {
        internal int VersionNumber { get; set; }
        internal string Description { get; set; } = "";
        internal byte[]? ContentZip { get; set; }
        internal string CodeSha256 { get; set; } = "";
        internal int CodeSize { get; set; }
        internal List<object?> CompatibleRuntimes { get; set; } = [];
        internal string LayerVersionArn { get; set; } = "";
        internal string CreatedDate { get; set; } = "";
    }

    private sealed class ProvisionedConcurrencyRecord
    {
        internal int RequestedProvisionedConcurrentExecutions { get; set; }
        internal int AvailableProvisionedConcurrentExecutions { get; set; }
        internal int AllocatedProvisionedConcurrentExecutions { get; set; }
        internal string Status { get; set; } = "";
        internal string LastModified { get; set; } = "";
    }

    private sealed class EventInvokeConfig
    {
        internal int MaximumRetryAttempts { get; set; }
        internal int MaximumEventAgeInSeconds { get; set; }
        internal string FunctionArn { get; set; } = "";
        internal double LastModified { get; set; }
        internal Dictionary<string, object?>? DestinationConfig { get; set; }
    }
}
