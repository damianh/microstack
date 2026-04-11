using System.Text;
using System.Text.Json;
using MicroStack.Internal;
using MicroStack.Services.Lambda;

namespace MicroStack.Services.ApiGateway;

/// <summary>
/// API Gateway REST API v1 emulator.
///
/// Control plane: REST API at /restapis/... for managing REST APIs, resources,
/// methods, integrations, deployments, stages, authorizers, models, API keys,
/// usage plans, domain names, base path mappings, and tags.
///
/// Data plane: Requests to {apiId}.execute-api.localhost are forwarded
/// to Lambda (AWS_PROXY) or return MOCK responses.
///
/// Port of ministack/services/apigateway_v1.py.
/// </summary>
internal sealed class ApiGatewayV1ServiceHandler
{
    private readonly LambdaServiceHandler _lambdaHandler;

    // -- State ------------------------------------------------------------------

    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _restApis = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _resources = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _stages = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _deployments = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _authorizers = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _models = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _apiKeys = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _usagePlans = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _usagePlanKeys = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _domainNames = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _basePathMappings = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _v1Tags = new();

    private static readonly string Region =
        Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1";

    private static readonly JsonSerializerOptions s_jsonOpts = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false,
    };

    internal ApiGatewayV1ServiceHandler(LambdaServiceHandler lambdaHandler)
    {
        _lambdaHandler = lambdaHandler;
    }

    // -- Public API used by the composite handler ------------------------------

    /// <summary>Returns true if this handler owns the given API ID (for execute-api dispatch).</summary>
    internal bool OwnsApiId(string apiId) => _restApis.ContainsKey(apiId);

    internal void Reset()
    {
        _restApis.Clear();
        _resources.Clear();
        _stages.Clear();
        _deployments.Clear();
        _authorizers.Clear();
        _models.Clear();
        _apiKeys.Clear();
        _usagePlans.Clear();
        _usagePlanKeys.Clear();
        _domainNames.Clear();
        _basePathMappings.Clear();
        _v1Tags.Clear();
    }

    // -- Response helpers -------------------------------------------------------

    private static ServiceResponse V1Response(Dictionary<string, object?> data, int statusCode)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(data, s_jsonOpts);
        return new ServiceResponse(statusCode,
            new Dictionary<string, string> { ["Content-Type"] = "application/json" }, json);
    }

    private static ServiceResponse V1Response(Dictionary<string, object?> data)
    {
        return V1Response(data, 200);
    }

    private static ServiceResponse V1Error(string code, string message, int statusCode)
    {
        var data = new Dictionary<string, object?> { ["message"] = message, ["__type"] = code };
        return V1Response(data, statusCode);
    }

    private static ServiceResponse EmptyResponse(int statusCode)
    {
        return new ServiceResponse(statusCode, new Dictionary<string, string>(), []);
    }

    private static string RestApiArn(string apiId) =>
        $"arn:aws:apigateway:{Region}::/restapis/{apiId}";

    private static string NewId() => HashHelpers.NewUuidNoDashes()[..10];

    private static long NowUnix() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    // -- Control plane router ---------------------------------------------------

    internal ServiceResponse HandleControlPlane(ServiceRequest request)
    {
        var method = request.Method.ToUpperInvariant();
        var path = request.Path;

        Dictionary<string, object?> data;
        try
        {
            data = request.Body.Length > 0
                ? JsonSerializer.Deserialize<Dictionary<string, object?>>(request.Body, s_jsonOpts)
                  ?? new Dictionary<string, object?>()
                : new Dictionary<string, object?>();
        }
        catch (JsonException)
        {
            data = new Dictionary<string, object?>();
        }

        var parts = path.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length == 0)
        {
            return V1Error("NotFoundException", $"Unknown path: {path}", 404);
        }

        var top = parts[0];

        // /tags/{resourceArn} — ARN may contain slashes
        if (top == "tags")
        {
            var resourceArn = parts.Length > 1
                ? Uri.UnescapeDataString(string.Join("/", parts[1..]))
                : "";
            if (method == "GET") return GetV1Tags(resourceArn);
            if (method is "PUT" or "POST") return TagV1Resource(resourceArn, data);
            if (method == "DELETE")
            {
                var tagKeys = request.QueryParams.TryGetValue("tagKeys", out var keys) ? keys : [];
                return UntagV1Resource(resourceArn, tagKeys);
            }
        }

        // /apikeys[/{keyId}]
        if (top == "apikeys")
        {
            var keyId = parts.Length > 1 ? parts[1] : null;
            if (keyId is null)
            {
                if (method == "GET") return GetApiKeys();
                if (method == "POST") return CreateApiKey(data);
            }
            else
            {
                if (method == "GET") return GetApiKey(keyId);
                if (method == "DELETE") return DeleteApiKey(keyId);
                if (method == "PATCH") return UpdateApiKey(keyId, data);
            }
        }

        // /usageplans[/{planId}[/keys[/{keyId}]]]
        if (top == "usageplans")
        {
            var planId = parts.Length > 1 ? parts[1] : null;
            var sub = parts.Length > 2 ? parts[2] : null;
            var subId = parts.Length > 3 ? parts[3] : null;
            if (planId is null)
            {
                if (method == "GET") return GetUsagePlans();
                if (method == "POST") return CreateUsagePlan(data);
            }
            else if (sub == "keys")
            {
                if (subId is null)
                {
                    if (method == "GET") return GetUsagePlanKeys(planId);
                    if (method == "POST") return CreateUsagePlanKey(planId, data);
                }
                else
                {
                    if (method == "DELETE") return DeleteUsagePlanKey(planId, subId);
                }
            }
            else
            {
                if (method == "GET") return GetUsagePlan(planId);
                if (method == "DELETE") return DeleteUsagePlan(planId);
                if (method == "PATCH") return UpdateUsagePlan(planId, data);
            }
        }

        // /domainnames[/{domainName}[/basepathmappings[/{basePath}]]]
        if (top == "domainnames")
        {
            var domainName = parts.Length > 1 ? parts[1] : null;
            var sub = parts.Length > 2 ? parts[2] : null;
            var subId = parts.Length > 3 ? parts[3] : null;
            if (domainName is null)
            {
                if (method == "GET") return GetDomainNames();
                if (method == "POST") return CreateDomainName(data);
            }
            else if (sub == "basepathmappings")
            {
                if (subId is null)
                {
                    if (method == "GET") return GetBasePathMappings(domainName);
                    if (method == "POST") return CreateBasePathMapping(domainName, data);
                }
                else
                {
                    if (method == "GET") return GetBasePathMapping(domainName, subId);
                    if (method == "DELETE") return DeleteBasePathMapping(domainName, subId);
                }
            }
            else
            {
                if (method == "GET") return GetDomainName(domainName);
                if (method == "DELETE") return DeleteDomainName(domainName);
            }
        }

        // /restapis[/{apiId}[/...]]
        if (top == "restapis")
        {
            if (parts.Length == 1)
            {
                if (method == "POST") return CreateRestApi(data);
                if (method == "GET") return GetRestApis();
            }

            var apiId = parts[1];

            if (parts.Length == 2)
            {
                if (method == "GET") return GetRestApi(apiId);
                if (method == "DELETE") return DeleteRestApi(apiId);
                if (method == "PATCH") return UpdateRestApi(apiId, data);
            }

            var sub = parts.Length > 2 ? parts[2] : null;

            // /restapis/{id}/resources[/{resourceId}[/methods/{httpMethod}[/...]]]
            if (sub == "resources")
            {
                var resourceId = parts.Length > 3 ? parts[3] : null;
                var methodPart = parts.Length > 4 ? parts[4] : null;
                var httpMethod = parts.Length > 5 ? parts[5] : null;
                var afterMethod = parts.Length > 6 ? parts[6] : null;
                var afterMethodId = parts.Length > 7 ? parts[7] : null;

                if (resourceId is null)
                {
                    if (method == "GET") return GetResources(apiId);
                }
                else if (methodPart is null)
                {
                    if (method == "GET") return GetResource(apiId, resourceId);
                    if (method == "POST") return CreateResource(apiId, resourceId, data);
                    if (method == "PATCH") return UpdateResource(apiId, resourceId, data);
                    if (method == "DELETE") return DeleteResource(apiId, resourceId);
                }
                else if (methodPart == "methods")
                {
                    if (httpMethod is null)
                    {
                        return V1Error("NotFoundException", "Method not specified", 404);
                    }

                    if (afterMethod is null)
                    {
                        if (method == "PUT") return PutMethod(apiId, resourceId, httpMethod, data);
                        if (method == "GET") return GetMethod(apiId, resourceId, httpMethod);
                        if (method == "DELETE") return DeleteMethod(apiId, resourceId, httpMethod);
                        if (method == "PATCH") return UpdateMethod(apiId, resourceId, httpMethod, data);
                    }
                    else if (afterMethod == "responses")
                    {
                        var statusCode = afterMethodId;
                        if (statusCode is null)
                        {
                            return V1Error("NotFoundException", "Status code not specified", 404);
                        }
                        if (method == "PUT") return PutMethodResponse(apiId, resourceId, httpMethod, statusCode, data);
                        if (method == "GET") return GetMethodResponse(apiId, resourceId, httpMethod, statusCode);
                        if (method == "DELETE") return DeleteMethodResponse(apiId, resourceId, httpMethod, statusCode);
                    }
                    else if (afterMethod == "integration")
                    {
                        var intSub = parts.Length > 7 ? parts[7] : null;
                        var intSubId = parts.Length > 8 ? parts[8] : null;

                        if (afterMethodId is null && intSub is null)
                        {
                            if (method == "PUT") return PutIntegration(apiId, resourceId, httpMethod, data);
                            if (method == "GET") return GetIntegration(apiId, resourceId, httpMethod);
                            if (method == "DELETE") return DeleteIntegration(apiId, resourceId, httpMethod);
                            if (method == "PATCH") return UpdateIntegration(apiId, resourceId, httpMethod, data);
                        }
                        else if (afterMethodId == "responses")
                        {
                            if (intSubId is null)
                            {
                                return V1Error("NotFoundException", "Status code not specified", 404);
                            }
                            if (method == "PUT") return PutIntegrationResponse(apiId, resourceId, httpMethod, intSubId, data);
                            if (method == "GET") return GetIntegrationResponse(apiId, resourceId, httpMethod, intSubId);
                            if (method == "DELETE") return DeleteIntegrationResponse(apiId, resourceId, httpMethod, intSubId);
                        }
                    }
                }
            }
            // /restapis/{id}/deployments[/{deploymentId}]
            else if (sub == "deployments")
            {
                var deploymentId = parts.Length > 3 ? parts[3] : null;
                if (deploymentId is null)
                {
                    if (method == "POST") return CreateDeployment(apiId, data);
                    if (method == "GET") return GetDeployments(apiId);
                }
                else
                {
                    if (method == "GET") return GetDeployment(apiId, deploymentId);
                    if (method == "PATCH") return UpdateDeployment(apiId, deploymentId, data);
                    if (method == "DELETE") return DeleteDeployment(apiId, deploymentId);
                }
            }
            // /restapis/{id}/stages[/{stageName}]
            else if (sub == "stages")
            {
                var stageName = parts.Length > 3 ? parts[3] : null;
                if (stageName is null)
                {
                    if (method == "POST") return CreateStage(apiId, data);
                    if (method == "GET") return GetStages(apiId);
                }
                else
                {
                    if (method == "GET") return GetStage(apiId, stageName);
                    if (method == "PATCH") return UpdateStage(apiId, stageName, data);
                    if (method == "DELETE") return DeleteStage(apiId, stageName);
                }
            }
            // /restapis/{id}/authorizers[/{authorizerId}]
            else if (sub == "authorizers")
            {
                var authId = parts.Length > 3 ? parts[3] : null;
                if (authId is null)
                {
                    if (method == "POST") return CreateAuthorizer(apiId, data);
                    if (method == "GET") return GetAuthorizers(apiId);
                }
                else
                {
                    if (method == "GET") return GetAuthorizer(apiId, authId);
                    if (method == "PATCH") return UpdateAuthorizer(apiId, authId, data);
                    if (method == "DELETE") return DeleteAuthorizer(apiId, authId);
                }
            }
            // /restapis/{id}/models[/{modelName}]
            else if (sub == "models")
            {
                var modelName = parts.Length > 3 ? parts[3] : null;
                if (modelName is null)
                {
                    if (method == "POST") return CreateModel(apiId, data);
                    if (method == "GET") return GetModels(apiId);
                }
                else
                {
                    if (method == "GET") return GetModel(apiId, modelName);
                    if (method == "DELETE") return DeleteModel(apiId, modelName);
                }
            }
        }

        return V1Error("NotFoundException", $"Unknown API Gateway v1 path: {path}", 404);
    }

    // -- Data plane: Execute API ------------------------------------------------

    internal ServiceResponse HandleExecute(string apiId, ServiceRequest request)
    {
        if (!_restApis.TryGetValue(apiId, out _))
        {
            return JsonResponse(new Dictionary<string, object?> { ["message"] = "Not Found" }, 404);
        }

        var method = request.Method.ToUpperInvariant();
        var path = request.Path;

        // Parse /{stage}/{remaining-path}
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length == 0)
        {
            return JsonResponse(new Dictionary<string, object?> { ["message"] = "Not Found" }, 404);
        }

        var stageName = segments[0];
        var requestPath = segments.Length > 1
            ? "/" + string.Join("/", segments[1..])
            : "/";

        if (!_stages.TryGetValue(apiId, out var apiStages) || !apiStages.ContainsKey(stageName))
        {
            return JsonResponse(new Dictionary<string, object?> { ["message"] = $"Stage '{stageName}' not found" }, 404);
        }

        var stage = apiStages[stageName];

        // Match path against resource tree
        var pathSegments = requestPath.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);
        var (resource, pathParams) = MatchResourceTree(apiId, pathSegments);

        if (resource is null)
        {
            return JsonResponse(new Dictionary<string, object?> { ["message"] = "Missing Authentication Token" }, 404);
        }

        // Look up method
        var resourceMethods = resource.TryGetValue("resourceMethods", out var rmObj) && rmObj is Dictionary<string, object?> rm
            ? rm
            : new Dictionary<string, object?>();

        object? methodObj = null;
        if (!resourceMethods.TryGetValue(method, out methodObj))
        {
            resourceMethods.TryGetValue("ANY", out methodObj);
        }

        if (methodObj is not Dictionary<string, object?> methodDict)
        {
            return JsonResponse(new Dictionary<string, object?> { ["message"] = "Method Not Allowed" }, 405);
        }

        var integration = methodDict.TryGetValue("methodIntegration", out var intObj) && intObj is Dictionary<string, object?> intDict
            ? intDict
            : null;

        if (integration is null)
        {
            return JsonResponse(new Dictionary<string, object?> { ["message"] = "No integration configured" }, 500);
        }

        var intType = GetStringFromDict(integration, "type") ?? "";

        if (intType is "AWS_PROXY" or "AWS")
        {
            return InvokeLambdaProxyV1(integration, apiId, stageName, stage, resource, requestPath, method,
                request.Headers, request.Body, request.QueryParams, pathParams);
        }
        if (intType == "MOCK")
        {
            return InvokeMockV1(integration);
        }

        return JsonResponse(new Dictionary<string, object?> { ["message"] = $"Unsupported integration type: {intType}" }, 500);
    }

    // -- Resource tree matching -------------------------------------------------

    private (Dictionary<string, object?>? Resource, Dictionary<string, string> PathParams) MatchResourceTree(
        string apiId, string[] segments)
    {
        if (!_resources.TryGetValue(apiId, out var resources))
        {
            return (null, new Dictionary<string, string>());
        }

        Dictionary<string, object?>? root = null;
        foreach (var r in resources.Values)
        {
            if (GetStringFromDict(r, "path") == "/")
            {
                root = r;
                break;
            }
        }

        if (root is null)
        {
            return (null, new Dictionary<string, string>());
        }

        if (segments.Length == 0)
        {
            return (root, new Dictionary<string, string>());
        }

        var rootId = GetStringFromDict(root, "id") ?? "";
        return MatchRecursive(resources, rootId, segments, new Dictionary<string, string>());
    }

    private static (Dictionary<string, object?>? Resource, Dictionary<string, string> Params) MatchRecursive(
        Dictionary<string, Dictionary<string, object?>> resources,
        string parentId,
        ReadOnlySpan<string> segments,
        Dictionary<string, string> parms)
    {
        if (segments.Length == 0)
        {
            return (null, parms);
        }

        var segment = segments[0];
        var remaining = segments[1..];

        foreach (var child in resources.Values)
        {
            if (GetStringFromDict(child, "parentId") != parentId) continue;

            var pp = GetStringFromDict(child, "pathPart") ?? "";

            // Greedy {proxy+}
            if (pp.StartsWith('{') && pp.EndsWith("+}"))
            {
                var paramName = pp[1..^2];
                var newParams = new Dictionary<string, string>(parms)
                {
                    [paramName] = string.Join("/", CombineSegments(segment, remaining)),
                };
                return (child, newParams);
            }

            // Single path param {param}
            if (pp.StartsWith('{') && pp.EndsWith('}'))
            {
                var paramName = pp[1..^1];
                var newParams = new Dictionary<string, string>(parms) { [paramName] = segment };
                if (remaining.Length == 0)
                {
                    return (child, newParams);
                }
                var childId = GetStringFromDict(child, "id") ?? "";
                var (result, rp) = MatchRecursive(resources, childId, remaining, newParams);
                if (result is not null)
                {
                    return (result, rp);
                }
            }
            // Exact match
            else if (pp == segment)
            {
                if (remaining.Length == 0)
                {
                    return (child, parms);
                }
                var childId = GetStringFromDict(child, "id") ?? "";
                var (result, rp) = MatchRecursive(resources, childId, remaining, new Dictionary<string, string>(parms));
                if (result is not null)
                {
                    return (result, rp);
                }
            }
        }

        return (null, parms);
    }

    private static string[] CombineSegments(string first, ReadOnlySpan<string> rest)
    {
        var result = new string[1 + rest.Length];
        result[0] = first;
        for (var i = 0; i < rest.Length; i++)
        {
            result[i + 1] = rest[i];
        }
        return result;
    }

    // -- Data plane: Lambda proxy v1 -------------------------------------------

    private ServiceResponse InvokeLambdaProxyV1(
        Dictionary<string, object?> integration,
        string apiId, string stageName,
        Dictionary<string, object?> stage,
        Dictionary<string, object?> resource,
        string requestPath, string method,
        IReadOnlyDictionary<string, string> headers,
        byte[] body,
        IReadOnlyDictionary<string, string[]> queryParams,
        Dictionary<string, string> pathParams)
    {
        var uri = GetStringFromDict(integration, "uri") ?? "";
        string funcName;
        if (uri.Contains("function:"))
        {
            funcName = uri.Split("function:")[^1].Split('/')[0].Split(':')[0];
        }
        else
        {
            funcName = uri;
        }

        // Build query string parameters (single value — first element)
        Dictionary<string, object?>? qsParams = null;
        Dictionary<string, object?>? mvQsParams = null;
        if (queryParams.Count > 0)
        {
            qsParams = new Dictionary<string, object?>();
            mvQsParams = new Dictionary<string, object?>();
            foreach (var (key, values) in queryParams)
            {
                qsParams[key] = values[0];
                mvQsParams[key] = values.ToList();
            }
        }

        // Build single and multi-value header dicts
        var singleHeaders = new Dictionary<string, object?>();
        var multiHeaders = new Dictionary<string, object?>();
        foreach (var (key, value) in headers)
        {
            singleHeaders[key] = value;
            multiHeaders[key] = new List<string> { value };
        }

        var now = DateTimeOffset.UtcNow;
        var nowEpochMs = now.ToUnixTimeMilliseconds();
        var requestTime = now.UtcDateTime.ToString("dd/MMM/yyyy:HH:mm:ss +0000");
        var requestId = HashHelpers.NewUuid();
        var resourcePath = GetStringFromDict(resource, "path") ?? "/";
        var resourceId = GetStringFromDict(resource, "id") ?? "";

        var sourceIp = "127.0.0.1";
        if (headers.TryGetValue("x-forwarded-for", out var xff) && xff.Contains(','))
        {
            sourceIp = xff.Split(',')[0].Trim();
        }
        else if (!string.IsNullOrEmpty(xff))
        {
            sourceIp = xff;
        }

        var stageVariables = stage.TryGetValue("variables", out var svObj) && svObj is Dictionary<string, object?> sv && sv.Count > 0
            ? sv
            : null;

        var lambdaEvent = new Dictionary<string, object?>
        {
            ["version"] = "1.0",
            ["resource"] = resourcePath,
            ["path"] = requestPath,
            ["httpMethod"] = method,
            ["headers"] = singleHeaders,
            ["multiValueHeaders"] = multiHeaders,
            ["queryStringParameters"] = qsParams,
            ["multiValueQueryStringParameters"] = mvQsParams,
            ["pathParameters"] = pathParams.Count > 0 ? pathParams : null,
            ["stageVariables"] = stageVariables,
            ["requestContext"] = new Dictionary<string, object?>
            {
                ["accountId"] = AccountContext.GetAccountId(),
                ["resourceId"] = resourceId,
                ["stage"] = stageName,
                ["requestId"] = requestId,
                ["extendedRequestId"] = requestId,
                ["requestTime"] = requestTime,
                ["requestTimeEpoch"] = nowEpochMs,
                ["path"] = $"/{stageName}{requestPath}",
                ["protocol"] = "HTTP/1.1",
                ["identity"] = new Dictionary<string, object?>
                {
                    ["sourceIp"] = sourceIp,
                    ["userAgent"] = headers.TryGetValue("user-agent", out var ua) ? ua
                        : (headers.TryGetValue("User-Agent", out var ua2) ? ua2 : ""),
                },
                ["resourcePath"] = resourcePath,
                ["httpMethod"] = method,
                ["apiId"] = apiId,
            },
            ["body"] = body.Length > 0 ? Encoding.UTF8.GetString(body) : null,
            ["isBase64Encoded"] = false,
        };

        var eventPayload = JsonSerializer.SerializeToUtf8Bytes(lambdaEvent, s_jsonOpts);

        // Use the full URI to resolve via LambdaServiceHandler
        var (success, responsePayload, error) = _lambdaHandler.InvokeForApiGateway(uri, eventPayload);

        if (!success)
        {
            return JsonResponse(new Dictionary<string, object?> { ["message"] = error ?? "Lambda invocation error" }, 502);
        }

        // Parse the Lambda response
        Dictionary<string, object?>? lambdaResponse;
        try
        {
            lambdaResponse = responsePayload is not null
                ? JsonSerializer.Deserialize<Dictionary<string, object?>>(responsePayload, s_jsonOpts)
                : null;
        }
        catch (JsonException)
        {
            lambdaResponse = null;
        }

        if (lambdaResponse is null)
        {
            return JsonResponse(new Dictionary<string, object?> { ["message"] = "Invalid Lambda response" }, 502);
        }

        var statusCode = 200;
        if (lambdaResponse.TryGetValue("statusCode", out var scObj) && scObj is JsonElement scEl)
        {
            statusCode = scEl.GetInt32();
        }

        var respHeaders = new Dictionary<string, string> { ["Content-Type"] = "application/json" };
        if (lambdaResponse.TryGetValue("headers", out var hObj) && hObj is JsonElement hEl
            && hEl.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in hEl.EnumerateObject())
            {
                respHeaders[prop.Name] = prop.Value.GetString() ?? "";
            }
        }

        byte[] respBody = [];
        if (lambdaResponse.TryGetValue("body", out var bObj))
        {
            if (bObj is JsonElement bEl)
            {
                respBody = bEl.ValueKind switch
                {
                    JsonValueKind.String => Encoding.UTF8.GetBytes(bEl.GetString() ?? ""),
                    JsonValueKind.Object or JsonValueKind.Array =>
                        Encoding.UTF8.GetBytes(bEl.GetRawText()),
                    _ => Encoding.UTF8.GetBytes(bEl.GetRawText()),
                };
            }
        }

        return new ServiceResponse(statusCode, respHeaders, respBody);
    }

    // -- Data plane: MOCK integration ------------------------------------------

    private static ServiceResponse InvokeMockV1(Dictionary<string, object?> integration)
    {
        var intResponses = integration.TryGetValue("integrationResponses", out var irObj) && irObj is Dictionary<string, object?> ir
            ? ir
            : new Dictionary<string, object?>();

        if (intResponses.Count == 0)
        {
            return JsonResponse(new Dictionary<string, object?>(), 200);
        }

        // Prefer an explicit "200" entry
        Dictionary<string, object?>? selected = null;
        if (intResponses.TryGetValue("200", out var r200) && r200 is Dictionary<string, object?> r200Dict)
        {
            selected = r200Dict;
        }
        else
        {
            // Fall back to entry with empty selectionPattern
            foreach (var resp in intResponses.Values)
            {
                if (resp is Dictionary<string, object?> respDict)
                {
                    var pattern = GetStringFromDict(respDict, "selectionPattern") ?? "";
                    if (string.IsNullOrEmpty(pattern))
                    {
                        selected = respDict;
                        break;
                    }
                }
            }
            // Last resort: first entry
            selected ??= intResponses.Values.OfType<Dictionary<string, object?>>().FirstOrDefault();
        }

        if (selected is null)
        {
            return JsonResponse(new Dictionary<string, object?>(), 200);
        }

        var statusCodeStr = GetStringFromDict(selected, "statusCode") ?? "200";
        _ = int.TryParse(statusCodeStr, out var statusCode);
        if (statusCode == 0) statusCode = 200;

        var respHeaders = new Dictionary<string, string> { ["Content-Type"] = "application/json" };

        // Apply responseParameters
        if (selected.TryGetValue("responseParameters", out var rpObj) && rpObj is Dictionary<string, object?> rp)
        {
            foreach (var (dest, src) in rp)
            {
                if (dest.StartsWith("method.response.header.", StringComparison.Ordinal))
                {
                    var headerName = dest["method.response.header.".Length..];
                    var srcStr = src?.ToString() ?? "";
                    var value = srcStr.StartsWith('\'') ? srcStr.Trim('\'') : srcStr;
                    respHeaders[headerName] = value;
                }
            }
        }

        // Apply response template
        var bodyTemplate = "";
        if (selected.TryGetValue("responseTemplates", out var rtObj) && rtObj is Dictionary<string, object?> rt)
        {
            if (rt.TryGetValue("application/json", out var tplObj))
            {
                bodyTemplate = tplObj?.ToString() ?? "";
            }
        }

        if (!string.IsNullOrEmpty(bodyTemplate))
        {
            return new ServiceResponse(statusCode, respHeaders, Encoding.UTF8.GetBytes(bodyTemplate));
        }

        return new ServiceResponse(statusCode, respHeaders, Encoding.UTF8.GetBytes("{}"));
    }

    private static ServiceResponse JsonResponse(Dictionary<string, object?> data, int statusCode)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(data, s_jsonOpts);
        return new ServiceResponse(statusCode,
            new Dictionary<string, string> { ["Content-Type"] = "application/json" }, json);
    }

    // -- Control plane: REST APIs -----------------------------------------------

    private ServiceResponse CreateRestApi(Dictionary<string, object?> data)
    {
        var apiId = NewId()[..8];
        var api = new Dictionary<string, object?>
        {
            ["id"] = apiId,
            ["name"] = GetString(data, "name") ?? "unnamed",
            ["description"] = GetString(data, "description") ?? "",
            ["createdDate"] = NowUnix(),
            ["version"] = GetString(data, "version") ?? "",
            ["binaryMediaTypes"] = GetObject(data, "binaryMediaTypes") ?? new List<object?>(),
            ["minimumCompressionSize"] = GetObject(data, "minimumCompressionSize"),
            ["apiKeySource"] = GetString(data, "apiKeySource") ?? "HEADER",
            ["endpointConfiguration"] = GetObject(data, "endpointConfiguration")
                ?? new Dictionary<string, object?> { ["types"] = new List<object?> { "REGIONAL" } },
            ["policy"] = GetObject(data, "policy"),
            ["tags"] = GetObject(data, "tags") ?? new Dictionary<string, object?>(),
            ["disableExecuteApiEndpoint"] = GetBool(data, "disableExecuteApiEndpoint"),
        };

        _restApis[apiId] = api;
        _resources[apiId] = new Dictionary<string, Dictionary<string, object?>>();
        _stages[apiId] = new Dictionary<string, Dictionary<string, object?>>();
        _deployments[apiId] = new Dictionary<string, Dictionary<string, object?>>();
        _authorizers[apiId] = new Dictionary<string, Dictionary<string, object?>>();
        _models[apiId] = new Dictionary<string, Dictionary<string, object?>>();

        // Create root resource "/"
        var rootId = NewId()[..8];
        var rootResource = new Dictionary<string, object?>
        {
            ["id"] = rootId,
            ["parentId"] = (object?)null,
            ["pathPart"] = "",
            ["path"] = "/",
            ["resourceMethods"] = new Dictionary<string, object?>(),
        };
        _resources[apiId][rootId] = rootResource;

        // Store initial tags
        _v1Tags[RestApiArn(apiId)] = GetStringDict(data, "tags");

        return V1Response(api, 201);
    }

    private ServiceResponse GetRestApi(string apiId)
    {
        if (!_restApis.TryGetValue(apiId, out var api))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        return V1Response(api);
    }

    private ServiceResponse GetRestApis()
    {
        return V1Response(new Dictionary<string, object?>
        {
            ["item"] = _restApis.Values.ToList(),
            ["nextToken"] = null,
        });
    }

    private ServiceResponse UpdateRestApi(string apiId, Dictionary<string, object?> data)
    {
        if (!_restApis.TryGetValue(apiId, out var api))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        var patchOps = GetPatchOperations(data);
        ApplyPatch(api, patchOps);
        return V1Response(api);
    }

    private ServiceResponse DeleteRestApi(string apiId)
    {
        if (!_restApis.ContainsKey(apiId))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        _restApis.TryRemove(apiId, out _);
        _resources.TryRemove(apiId, out _);
        _stages.TryRemove(apiId, out _);
        _deployments.TryRemove(apiId, out _);
        _authorizers.TryRemove(apiId, out _);
        _models.TryRemove(apiId, out _);
        _v1Tags.TryRemove(RestApiArn(apiId), out _);
        return EmptyResponse(202);
    }

    // -- Control plane: Resources -----------------------------------------------

    private ServiceResponse GetResources(string apiId)
    {
        if (!_restApis.ContainsKey(apiId))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        var resources = _resources.TryGetValue(apiId, out var r)
            ? r.Values.Cast<object?>().ToList()
            : new List<object?>();
        return V1Response(new Dictionary<string, object?> { ["item"] = resources });
    }

    private ServiceResponse GetResource(string apiId, string resourceId)
    {
        if (!_resources.TryGetValue(apiId, out var resources) || !resources.TryGetValue(resourceId, out var resource))
        {
            return V1Error("NotFoundException", "Invalid Resource identifier specified", 404);
        }
        return V1Response(resource);
    }

    private ServiceResponse CreateResource(string apiId, string parentId, Dictionary<string, object?> data)
    {
        if (!_restApis.ContainsKey(apiId))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        if (!_resources.TryGetValue(apiId, out var resources) || !resources.ContainsKey(parentId))
        {
            return V1Error("NotFoundException", "Invalid Resource identifier specified", 404);
        }
        var pathPart = GetString(data, "pathPart") ?? "";
        var resourceId = NewId()[..8];
        var resource = new Dictionary<string, object?>
        {
            ["id"] = resourceId,
            ["parentId"] = parentId,
            ["pathPart"] = pathPart,
            ["path"] = "",
            ["resourceMethods"] = new Dictionary<string, object?>(),
        };
        resources[resourceId] = resource;
        resource["path"] = ComputePath(apiId, resourceId);
        return V1Response(resource, 201);
    }

    private ServiceResponse UpdateResource(string apiId, string resourceId, Dictionary<string, object?> data)
    {
        if (!_resources.TryGetValue(apiId, out var resources) || !resources.TryGetValue(resourceId, out var resource))
        {
            return V1Error("NotFoundException", "Invalid Resource identifier specified", 404);
        }
        var patchOps = GetPatchOperations(data);
        ApplyPatch(resource, patchOps);
        resource["path"] = ComputePath(apiId, resourceId);
        return V1Response(resource);
    }

    private ServiceResponse DeleteResource(string apiId, string resourceId)
    {
        if (!_resources.TryGetValue(apiId, out var resources) || !resources.ContainsKey(resourceId))
        {
            return V1Error("NotFoundException", "Invalid Resource identifier specified", 404);
        }
        resources.Remove(resourceId);
        return EmptyResponse(202);
    }

    // -- Control plane: Methods -------------------------------------------------

    private ServiceResponse PutMethod(string apiId, string resourceId, string httpMethod, Dictionary<string, object?> data)
    {
        var resource = GetResourceDict(apiId, resourceId);
        if (resource is null)
        {
            return V1Error("NotFoundException", "Invalid Resource identifier specified", 404);
        }
        var methodObj = new Dictionary<string, object?>
        {
            ["httpMethod"] = httpMethod,
            ["authorizationType"] = GetString(data, "authorizationType") ?? "NONE",
            ["authorizerId"] = GetString(data, "authorizerId"),
            ["apiKeyRequired"] = GetBool(data, "apiKeyRequired"),
            ["operationName"] = GetString(data, "operationName") ?? "",
            ["requestParameters"] = GetObject(data, "requestParameters") ?? new Dictionary<string, object?>(),
            ["requestModels"] = GetObject(data, "requestModels") ?? new Dictionary<string, object?>(),
            ["methodResponses"] = new Dictionary<string, object?>(),
            ["methodIntegration"] = (object?)null,
        };
        var resourceMethods = GetOrCreateSubDict(resource, "resourceMethods");
        resourceMethods[httpMethod] = methodObj;
        return V1Response(methodObj, 201);
    }

    private ServiceResponse GetMethod(string apiId, string resourceId, string httpMethod)
    {
        var resource = GetResourceDict(apiId, resourceId);
        if (resource is null)
        {
            return V1Error("NotFoundException", "Invalid Resource identifier specified", 404);
        }
        var methods = GetSubDict(resource, "resourceMethods");
        if (methods is null || !methods.TryGetValue(httpMethod, out var methodObj) || methodObj is not Dictionary<string, object?> method)
        {
            return V1Error("NotFoundException", "Invalid Method identifier specified", 404);
        }
        return V1Response(method);
    }

    private ServiceResponse DeleteMethod(string apiId, string resourceId, string httpMethod)
    {
        var resource = GetResourceDict(apiId, resourceId);
        if (resource is null)
        {
            return V1Error("NotFoundException", "Invalid Resource identifier specified", 404);
        }
        var methods = GetSubDict(resource, "resourceMethods");
        methods?.Remove(httpMethod);
        return EmptyResponse(204);
    }

    private ServiceResponse UpdateMethod(string apiId, string resourceId, string httpMethod, Dictionary<string, object?> data)
    {
        var resource = GetResourceDict(apiId, resourceId);
        if (resource is null)
        {
            return V1Error("NotFoundException", "Invalid Resource identifier specified", 404);
        }
        var methods = GetSubDict(resource, "resourceMethods");
        if (methods is null || !methods.TryGetValue(httpMethod, out var methodObj) || methodObj is not Dictionary<string, object?> method)
        {
            return V1Error("NotFoundException", "Invalid Method identifier specified", 404);
        }
        var patchOps = GetPatchOperations(data);
        ApplyPatch(method, patchOps);
        return V1Response(method);
    }

    // -- Control plane: Method Responses ----------------------------------------

    private ServiceResponse PutMethodResponse(string apiId, string resourceId, string httpMethod, string statusCode, Dictionary<string, object?> data)
    {
        var method = GetMethodDict(apiId, resourceId, httpMethod);
        if (method is null)
        {
            return V1Error("NotFoundException", "Invalid Method identifier specified", 404);
        }
        var methodResponse = new Dictionary<string, object?>
        {
            ["statusCode"] = statusCode,
            ["responseParameters"] = GetObject(data, "responseParameters") ?? new Dictionary<string, object?>(),
            ["responseModels"] = GetObject(data, "responseModels") ?? new Dictionary<string, object?>(),
        };
        var methodResponses = GetOrCreateSubDict(method, "methodResponses");
        methodResponses[statusCode] = methodResponse;
        return V1Response(methodResponse);
    }

    private ServiceResponse GetMethodResponse(string apiId, string resourceId, string httpMethod, string statusCode)
    {
        var method = GetMethodDict(apiId, resourceId, httpMethod);
        if (method is null)
        {
            return V1Error("NotFoundException", "Invalid Method identifier specified", 404);
        }
        var responses = GetSubDict(method, "methodResponses");
        if (responses is null || !responses.TryGetValue(statusCode, out var respObj) || respObj is not Dictionary<string, object?> resp)
        {
            return V1Error("NotFoundException", "Invalid Response status code specified", 404);
        }
        return V1Response(resp);
    }

    private ServiceResponse DeleteMethodResponse(string apiId, string resourceId, string httpMethod, string statusCode)
    {
        var method = GetMethodDict(apiId, resourceId, httpMethod);
        if (method is not null)
        {
            var responses = GetSubDict(method, "methodResponses");
            responses?.Remove(statusCode);
        }
        return EmptyResponse(204);
    }

    // -- Control plane: Integration ---------------------------------------------

    private ServiceResponse PutIntegration(string apiId, string resourceId, string httpMethod, Dictionary<string, object?> data)
    {
        var method = GetMethodDict(apiId, resourceId, httpMethod);
        if (method is null)
        {
            return V1Error("NotFoundException", "Invalid Method identifier specified", 404);
        }
        var integration = new Dictionary<string, object?>
        {
            ["type"] = GetString(data, "type") ?? "AWS_PROXY",
            ["httpMethod"] = GetString(data, "httpMethod") ?? "POST",
            ["uri"] = GetString(data, "uri") ?? "",
            ["connectionType"] = GetString(data, "connectionType") ?? "INTERNET",
            ["credentials"] = GetString(data, "credentials"),
            ["requestParameters"] = GetObject(data, "requestParameters") ?? new Dictionary<string, object?>(),
            ["requestTemplates"] = GetObject(data, "requestTemplates") ?? new Dictionary<string, object?>(),
            ["passthroughBehavior"] = GetString(data, "passthroughBehavior") ?? "WHEN_NO_MATCH",
            ["timeoutInMillis"] = GetInt(data, "timeoutInMillis", 29000),
            ["cacheNamespace"] = resourceId,
            ["cacheKeyParameters"] = GetObject(data, "cacheKeyParameters") ?? new List<object?>(),
            ["integrationResponses"] = new Dictionary<string, object?>(),
        };
        method["methodIntegration"] = integration;
        return V1Response(integration);
    }

    private ServiceResponse GetIntegration(string apiId, string resourceId, string httpMethod)
    {
        var method = GetMethodDict(apiId, resourceId, httpMethod);
        if (method is null)
        {
            return V1Error("NotFoundException", "Invalid Method identifier specified", 404);
        }
        if (method.TryGetValue("methodIntegration", out var intObj) && intObj is Dictionary<string, object?> integration)
        {
            return V1Response(integration);
        }
        return V1Error("NotFoundException", "Invalid Integration identifier specified", 404);
    }

    private ServiceResponse DeleteIntegration(string apiId, string resourceId, string httpMethod)
    {
        var method = GetMethodDict(apiId, resourceId, httpMethod);
        if (method is not null)
        {
            method["methodIntegration"] = null;
        }
        return EmptyResponse(204);
    }

    private ServiceResponse UpdateIntegration(string apiId, string resourceId, string httpMethod, Dictionary<string, object?> data)
    {
        var method = GetMethodDict(apiId, resourceId, httpMethod);
        if (method is null)
        {
            return V1Error("NotFoundException", "Invalid Method identifier specified", 404);
        }
        if (method.TryGetValue("methodIntegration", out var intObj) && intObj is Dictionary<string, object?> integration)
        {
            var patchOps = GetPatchOperations(data);
            ApplyPatch(integration, patchOps);
            return V1Response(integration);
        }
        return V1Error("NotFoundException", "Invalid Integration identifier specified", 404);
    }

    // -- Control plane: Integration Responses -----------------------------------

    private ServiceResponse PutIntegrationResponse(string apiId, string resourceId, string httpMethod, string statusCode, Dictionary<string, object?> data)
    {
        var integration = GetIntegrationDict(apiId, resourceId, httpMethod);
        if (integration is null)
        {
            return V1Error("NotFoundException", "Invalid Integration identifier specified", 404);
        }
        var intResponse = new Dictionary<string, object?>
        {
            ["statusCode"] = statusCode,
            ["selectionPattern"] = GetString(data, "selectionPattern") ?? "",
            ["responseParameters"] = GetObject(data, "responseParameters") ?? new Dictionary<string, object?>(),
            ["responseTemplates"] = GetObject(data, "responseTemplates") ?? new Dictionary<string, object?>(),
            ["contentHandling"] = GetString(data, "contentHandling"),
        };
        var intResponses = GetOrCreateSubDict(integration, "integrationResponses");
        intResponses[statusCode] = intResponse;
        return V1Response(intResponse);
    }

    private ServiceResponse GetIntegrationResponse(string apiId, string resourceId, string httpMethod, string statusCode)
    {
        var integration = GetIntegrationDict(apiId, resourceId, httpMethod);
        if (integration is null)
        {
            return V1Error("NotFoundException", "Invalid Integration identifier specified", 404);
        }
        var responses = GetSubDict(integration, "integrationResponses");
        if (responses is null || !responses.TryGetValue(statusCode, out var respObj) || respObj is not Dictionary<string, object?> resp)
        {
            return V1Error("NotFoundException", "Invalid Response status code specified", 404);
        }
        return V1Response(resp);
    }

    private ServiceResponse DeleteIntegrationResponse(string apiId, string resourceId, string httpMethod, string statusCode)
    {
        var integration = GetIntegrationDict(apiId, resourceId, httpMethod);
        if (integration is not null)
        {
            var responses = GetSubDict(integration, "integrationResponses");
            responses?.Remove(statusCode);
        }
        return EmptyResponse(204);
    }

    // -- Control plane: Deployments ---------------------------------------------

    private ServiceResponse CreateDeployment(string apiId, Dictionary<string, object?> data)
    {
        if (!_restApis.ContainsKey(apiId))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        var deploymentId = NewId()[..8];
        var deployment = new Dictionary<string, object?>
        {
            ["id"] = deploymentId,
            ["description"] = GetString(data, "description") ?? "",
            ["createdDate"] = NowUnix(),
            ["apiSummary"] = BuildApiSummary(apiId),
        };

        var apiDeployments = _deployments.GetOrAdd(apiId,
            _ => new Dictionary<string, Dictionary<string, object?>>());
        apiDeployments[deploymentId] = deployment;

        // If stageName is provided, create/update the stage automatically
        var stageName = GetString(data, "stageName");
        if (stageName is not null)
        {
            var apiStages = _stages.GetOrAdd(apiId,
                _ => new Dictionary<string, Dictionary<string, object?>>());
            if (apiStages.TryGetValue(stageName, out var existingStage))
            {
                existingStage["deploymentId"] = deploymentId;
                existingStage["lastUpdatedDate"] = NowUnix();
            }
            else
            {
                var stage = new Dictionary<string, object?>
                {
                    ["stageName"] = stageName,
                    ["deploymentId"] = deploymentId,
                    ["description"] = GetString(data, "stageDescription") ?? "",
                    ["createdDate"] = NowUnix(),
                    ["lastUpdatedDate"] = NowUnix(),
                    ["variables"] = new Dictionary<string, object?>(),
                    ["methodSettings"] = new Dictionary<string, object?>(),
                    ["accessLogSettings"] = new Dictionary<string, object?>(),
                    ["cacheClusterEnabled"] = false,
                    ["cacheClusterSize"] = (object?)null,
                    ["tracingEnabled"] = false,
                    ["tags"] = new Dictionary<string, object?>(),
                    ["documentationVersion"] = (object?)null,
                };
                apiStages[stageName] = stage;
            }
        }

        return V1Response(deployment, 201);
    }

    private ServiceResponse GetDeployments(string apiId)
    {
        if (!_restApis.ContainsKey(apiId))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        var deployments = _deployments.TryGetValue(apiId, out var d)
            ? d.Values.Cast<object?>().ToList()
            : new List<object?>();
        return V1Response(new Dictionary<string, object?> { ["item"] = deployments });
    }

    private ServiceResponse GetDeployment(string apiId, string deploymentId)
    {
        if (!_deployments.TryGetValue(apiId, out var deployments) || !deployments.TryGetValue(deploymentId, out var deployment))
        {
            return V1Error("NotFoundException", "Invalid Deployment identifier specified", 404);
        }
        return V1Response(deployment);
    }

    private ServiceResponse UpdateDeployment(string apiId, string deploymentId, Dictionary<string, object?> data)
    {
        if (!_deployments.TryGetValue(apiId, out var deployments) || !deployments.TryGetValue(deploymentId, out var deployment))
        {
            return V1Error("NotFoundException", "Invalid Deployment identifier specified", 404);
        }
        var patchOps = GetPatchOperations(data);
        ApplyPatch(deployment, patchOps);
        return V1Response(deployment);
    }

    private ServiceResponse DeleteDeployment(string apiId, string deploymentId)
    {
        if (!_deployments.TryGetValue(apiId, out var deployments) || !deployments.ContainsKey(deploymentId))
        {
            return V1Error("NotFoundException", "Invalid Deployment identifier specified", 404);
        }
        deployments.Remove(deploymentId);
        return EmptyResponse(202);
    }

    // -- Control plane: Stages --------------------------------------------------

    private ServiceResponse CreateStage(string apiId, Dictionary<string, object?> data)
    {
        if (!_restApis.ContainsKey(apiId))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        var stageName = GetString(data, "stageName") ?? "";
        if (string.IsNullOrEmpty(stageName))
        {
            return V1Error("BadRequestException", "Stage name is required", 400);
        }
        var stage = new Dictionary<string, object?>
        {
            ["stageName"] = stageName,
            ["deploymentId"] = GetString(data, "deploymentId") ?? "",
            ["description"] = GetString(data, "description") ?? "",
            ["createdDate"] = NowUnix(),
            ["lastUpdatedDate"] = NowUnix(),
            ["variables"] = GetObject(data, "variables") ?? new Dictionary<string, object?>(),
            ["methodSettings"] = GetObject(data, "methodSettings") ?? new Dictionary<string, object?>(),
            ["accessLogSettings"] = GetObject(data, "accessLogSettings") ?? new Dictionary<string, object?>(),
            ["cacheClusterEnabled"] = GetBool(data, "cacheClusterEnabled"),
            ["cacheClusterSize"] = GetObject(data, "cacheClusterSize"),
            ["tracingEnabled"] = GetBool(data, "tracingEnabled"),
            ["tags"] = GetObject(data, "tags") ?? new Dictionary<string, object?>(),
            ["documentationVersion"] = GetString(data, "documentationVersion"),
        };
        var apiStages = _stages.GetOrAdd(apiId,
            _ => new Dictionary<string, Dictionary<string, object?>>());
        apiStages[stageName] = stage;
        return V1Response(stage, 201);
    }

    private ServiceResponse GetStages(string apiId)
    {
        if (!_restApis.ContainsKey(apiId))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        var stages = _stages.TryGetValue(apiId, out var s)
            ? s.Values.Cast<object?>().ToList()
            : new List<object?>();
        return V1Response(new Dictionary<string, object?> { ["item"] = stages });
    }

    private ServiceResponse GetStage(string apiId, string stageName)
    {
        if (!_stages.TryGetValue(apiId, out var stages) || !stages.TryGetValue(stageName, out var stage))
        {
            return V1Error("NotFoundException", "Invalid Stage identifier specified", 404);
        }
        return V1Response(stage);
    }

    private ServiceResponse UpdateStage(string apiId, string stageName, Dictionary<string, object?> data)
    {
        if (!_stages.TryGetValue(apiId, out var stages) || !stages.TryGetValue(stageName, out var stage))
        {
            return V1Error("NotFoundException", "Invalid Stage identifier specified", 404);
        }
        var patchOps = GetPatchOperations(data);
        ApplyPatch(stage, patchOps);
        stage["lastUpdatedDate"] = NowUnix();
        return V1Response(stage);
    }

    private ServiceResponse DeleteStage(string apiId, string stageName)
    {
        if (!_stages.TryGetValue(apiId, out var stages) || !stages.ContainsKey(stageName))
        {
            return V1Error("NotFoundException", "Invalid Stage identifier specified", 404);
        }
        stages.Remove(stageName);
        return EmptyResponse(202);
    }

    // -- Control plane: Authorizers ---------------------------------------------

    private ServiceResponse CreateAuthorizer(string apiId, Dictionary<string, object?> data)
    {
        if (!_restApis.ContainsKey(apiId))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        var authId = NewId()[..8];
        var authorizer = new Dictionary<string, object?>
        {
            ["id"] = authId,
            ["name"] = GetString(data, "name") ?? "",
            ["type"] = GetString(data, "type") ?? "TOKEN",
            ["authorizerUri"] = GetString(data, "authorizerUri") ?? "",
            ["authorizerCredentials"] = GetString(data, "authorizerCredentials"),
            ["identitySource"] = GetString(data, "identitySource") ?? "method.request.header.Authorization",
            ["identityValidationExpression"] = GetString(data, "identityValidationExpression") ?? "",
            ["authorizerResultTtlInSeconds"] = GetInt(data, "authorizerResultTtlInSeconds", 300),
            ["providerARNs"] = GetObject(data, "providerARNs") ?? new List<object?>(),
        };
        var apiAuthorizers = _authorizers.GetOrAdd(apiId,
            _ => new Dictionary<string, Dictionary<string, object?>>());
        apiAuthorizers[authId] = authorizer;
        return V1Response(authorizer, 201);
    }

    private ServiceResponse GetAuthorizers(string apiId)
    {
        if (!_restApis.ContainsKey(apiId))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        var authorizers = _authorizers.TryGetValue(apiId, out var a)
            ? a.Values.Cast<object?>().ToList()
            : new List<object?>();
        return V1Response(new Dictionary<string, object?> { ["item"] = authorizers });
    }

    private ServiceResponse GetAuthorizer(string apiId, string authId)
    {
        if (!_authorizers.TryGetValue(apiId, out var authorizers) || !authorizers.TryGetValue(authId, out var authorizer))
        {
            return V1Error("NotFoundException", "Invalid Authorizer identifier specified", 404);
        }
        return V1Response(authorizer);
    }

    private ServiceResponse UpdateAuthorizer(string apiId, string authId, Dictionary<string, object?> data)
    {
        if (!_authorizers.TryGetValue(apiId, out var authorizers) || !authorizers.TryGetValue(authId, out var authorizer))
        {
            return V1Error("NotFoundException", "Invalid Authorizer identifier specified", 404);
        }
        var patchOps = GetPatchOperations(data);
        ApplyPatch(authorizer, patchOps);
        return V1Response(authorizer);
    }

    private ServiceResponse DeleteAuthorizer(string apiId, string authId)
    {
        if (!_authorizers.TryGetValue(apiId, out var authorizers) || !authorizers.ContainsKey(authId))
        {
            return V1Error("NotFoundException", "Invalid Authorizer identifier specified", 404);
        }
        authorizers.Remove(authId);
        return EmptyResponse(202);
    }

    // -- Control plane: Models --------------------------------------------------

    private ServiceResponse CreateModel(string apiId, Dictionary<string, object?> data)
    {
        if (!_restApis.ContainsKey(apiId))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        var modelName = GetString(data, "name") ?? "";
        if (string.IsNullOrEmpty(modelName))
        {
            return V1Error("BadRequestException", "Model name is required", 400);
        }
        var model = new Dictionary<string, object?>
        {
            ["id"] = NewId()[..8],
            ["name"] = modelName,
            ["description"] = GetString(data, "description") ?? "",
            ["schema"] = GetString(data, "schema") ?? "",
            ["contentType"] = GetString(data, "contentType") ?? "application/json",
        };
        var apiModels = _models.GetOrAdd(apiId,
            _ => new Dictionary<string, Dictionary<string, object?>>());
        apiModels[modelName] = model;
        return V1Response(model, 201);
    }

    private ServiceResponse GetModels(string apiId)
    {
        if (!_restApis.ContainsKey(apiId))
        {
            return V1Error("NotFoundException", "Invalid API identifier specified", 404);
        }
        var models = _models.TryGetValue(apiId, out var m)
            ? m.Values.Cast<object?>().ToList()
            : new List<object?>();
        return V1Response(new Dictionary<string, object?> { ["item"] = models });
    }

    private ServiceResponse GetModel(string apiId, string modelName)
    {
        if (!_models.TryGetValue(apiId, out var models) || !models.TryGetValue(modelName, out var model))
        {
            return V1Error("NotFoundException", "Invalid Model identifier specified", 404);
        }
        return V1Response(model);
    }

    private ServiceResponse DeleteModel(string apiId, string modelName)
    {
        if (!_models.TryGetValue(apiId, out var models) || !models.ContainsKey(modelName))
        {
            return V1Error("NotFoundException", "Invalid Model identifier specified", 404);
        }
        models.Remove(modelName);
        return EmptyResponse(202);
    }

    // -- Control plane: API Keys ------------------------------------------------

    private ServiceResponse CreateApiKey(Dictionary<string, object?> data)
    {
        var keyId = NewId()[..8];
        var keyValue = HashHelpers.NewUuidNoDashes();
        var apiKey = new Dictionary<string, object?>
        {
            ["id"] = keyId,
            ["name"] = GetString(data, "name") ?? "",
            ["description"] = GetString(data, "description") ?? "",
            ["enabled"] = GetBool(data, "enabled"),
            ["createdDate"] = NowUnix(),
            ["lastUpdatedDate"] = NowUnix(),
            ["value"] = keyValue,
            ["stageKeys"] = GetObject(data, "stageKeys") ?? new List<object?>(),
            ["tags"] = GetObject(data, "tags") ?? new Dictionary<string, object?>(),
        };
        _apiKeys[keyId] = apiKey;
        return V1Response(apiKey, 201);
    }

    private ServiceResponse GetApiKeys()
    {
        return V1Response(new Dictionary<string, object?>
        {
            ["item"] = _apiKeys.Values.ToList(),
        });
    }

    private ServiceResponse GetApiKey(string keyId)
    {
        if (!_apiKeys.TryGetValue(keyId, out var apiKey))
        {
            return V1Error("NotFoundException", "Invalid API Key identifier specified", 404);
        }
        return V1Response(apiKey);
    }

    private ServiceResponse UpdateApiKey(string keyId, Dictionary<string, object?> data)
    {
        if (!_apiKeys.TryGetValue(keyId, out var apiKey))
        {
            return V1Error("NotFoundException", "Invalid API Key identifier specified", 404);
        }
        var patchOps = GetPatchOperations(data);
        ApplyPatch(apiKey, patchOps);
        apiKey["lastUpdatedDate"] = NowUnix();
        return V1Response(apiKey);
    }

    private ServiceResponse DeleteApiKey(string keyId)
    {
        if (!_apiKeys.ContainsKey(keyId))
        {
            return V1Error("NotFoundException", "Invalid API Key identifier specified", 404);
        }
        _apiKeys.TryRemove(keyId, out _);
        return EmptyResponse(202);
    }

    // -- Control plane: Usage Plans ---------------------------------------------

    private ServiceResponse CreateUsagePlan(Dictionary<string, object?> data)
    {
        var planId = NewId()[..8];
        var plan = new Dictionary<string, object?>
        {
            ["id"] = planId,
            ["name"] = GetString(data, "name") ?? "",
            ["description"] = GetString(data, "description") ?? "",
            ["apiStages"] = GetObject(data, "apiStages") ?? new List<object?>(),
            ["throttle"] = GetObject(data, "throttle") ?? new Dictionary<string, object?>(),
            ["quota"] = GetObject(data, "quota") ?? new Dictionary<string, object?>(),
            ["tags"] = GetObject(data, "tags") ?? new Dictionary<string, object?>(),
        };
        _usagePlans[planId] = plan;
        _usagePlanKeys[planId] = new Dictionary<string, Dictionary<string, object?>>();
        return V1Response(plan, 201);
    }

    private ServiceResponse GetUsagePlans()
    {
        return V1Response(new Dictionary<string, object?>
        {
            ["item"] = _usagePlans.Values.ToList(),
        });
    }

    private ServiceResponse GetUsagePlan(string planId)
    {
        if (!_usagePlans.TryGetValue(planId, out var plan))
        {
            return V1Error("NotFoundException", "Invalid Usage Plan identifier specified", 404);
        }
        return V1Response(plan);
    }

    private ServiceResponse UpdateUsagePlan(string planId, Dictionary<string, object?> data)
    {
        if (!_usagePlans.TryGetValue(planId, out var plan))
        {
            return V1Error("NotFoundException", "Invalid Usage Plan identifier specified", 404);
        }
        var patchOps = GetPatchOperations(data);
        ApplyPatch(plan, patchOps);
        return V1Response(plan);
    }

    private ServiceResponse DeleteUsagePlan(string planId)
    {
        if (!_usagePlans.ContainsKey(planId))
        {
            return V1Error("NotFoundException", "Invalid Usage Plan identifier specified", 404);
        }
        _usagePlans.TryRemove(planId, out _);
        _usagePlanKeys.TryRemove(planId, out _);
        return EmptyResponse(202);
    }

    private ServiceResponse CreateUsagePlanKey(string planId, Dictionary<string, object?> data)
    {
        if (!_usagePlans.ContainsKey(planId))
        {
            return V1Error("NotFoundException", "Invalid Usage Plan identifier specified", 404);
        }
        var keyId = GetString(data, "keyId") ?? "";
        var keyType = GetString(data, "keyType") ?? "API_KEY";
        var planKey = new Dictionary<string, object?>
        {
            ["id"] = keyId,
            ["type"] = keyType,
            ["name"] = _apiKeys.TryGetValue(keyId, out var ak)
                ? GetStringFromDict(ak, "name") ?? ""
                : "",
            ["value"] = _apiKeys.TryGetValue(keyId, out var ak2)
                ? GetStringFromDict(ak2, "value") ?? ""
                : "",
        };
        var keys = _usagePlanKeys.GetOrAdd(planId,
            _ => new Dictionary<string, Dictionary<string, object?>>());
        keys[keyId] = planKey;
        return V1Response(planKey, 201);
    }

    private ServiceResponse GetUsagePlanKeys(string planId)
    {
        if (!_usagePlans.ContainsKey(planId))
        {
            return V1Error("NotFoundException", "Invalid Usage Plan identifier specified", 404);
        }
        var keys = _usagePlanKeys.TryGetValue(planId, out var k)
            ? k.Values.Cast<object?>().ToList()
            : new List<object?>();
        return V1Response(new Dictionary<string, object?> { ["item"] = keys });
    }

    private ServiceResponse DeleteUsagePlanKey(string planId, string keyId)
    {
        if (!_usagePlans.ContainsKey(planId))
        {
            return V1Error("NotFoundException", "Invalid Usage Plan identifier specified", 404);
        }
        if (_usagePlanKeys.TryGetValue(planId, out var keys))
        {
            keys.Remove(keyId);
        }
        return EmptyResponse(202);
    }

    // -- Control plane: Domain Names --------------------------------------------

    private ServiceResponse CreateDomainName(Dictionary<string, object?> data)
    {
        var domainName = GetString(data, "domainName") ?? "";
        if (string.IsNullOrEmpty(domainName))
        {
            return V1Error("BadRequestException", "Domain name is required", 400);
        }
        var dn = new Dictionary<string, object?>
        {
            ["domainName"] = domainName,
            ["certificateName"] = GetString(data, "certificateName") ?? "",
            ["certificateArn"] = GetString(data, "certificateArn") ?? "",
            ["distributionDomainName"] = $"{domainName}.cloudfront.net",
            ["regionalDomainName"] = $"{domainName}.execute-api.{Region}.amazonaws.com",
            ["regionalHostedZoneId"] = "Z1UJRXOUMOOFQ8",
            ["endpointConfiguration"] = GetObject(data, "endpointConfiguration")
                ?? new Dictionary<string, object?> { ["types"] = new List<object?> { "REGIONAL" } },
            ["tags"] = GetObject(data, "tags") ?? new Dictionary<string, object?>(),
        };
        _domainNames[domainName] = dn;
        _basePathMappings[domainName] = new Dictionary<string, Dictionary<string, object?>>();
        return V1Response(dn, 201);
    }

    private ServiceResponse GetDomainNames()
    {
        return V1Response(new Dictionary<string, object?>
        {
            ["item"] = _domainNames.Values.ToList(),
        });
    }

    private ServiceResponse GetDomainName(string domainName)
    {
        if (!_domainNames.TryGetValue(domainName, out var dn))
        {
            return V1Error("NotFoundException", "Invalid domain name identifier specified", 404);
        }
        return V1Response(dn);
    }

    private ServiceResponse DeleteDomainName(string domainName)
    {
        if (!_domainNames.ContainsKey(domainName))
        {
            return V1Error("NotFoundException", "Invalid domain name identifier specified", 404);
        }
        _domainNames.TryRemove(domainName, out _);
        _basePathMappings.TryRemove(domainName, out _);
        return EmptyResponse(202);
    }

    // -- Control plane: Base Path Mappings --------------------------------------

    private ServiceResponse CreateBasePathMapping(string domainName, Dictionary<string, object?> data)
    {
        if (!_domainNames.ContainsKey(domainName))
        {
            return V1Error("NotFoundException", "Invalid domain name identifier specified", 404);
        }
        var basePath = GetString(data, "basePath") ?? "(none)";
        var mapping = new Dictionary<string, object?>
        {
            ["basePath"] = basePath,
            ["restApiId"] = GetString(data, "restApiId") ?? "",
            ["stage"] = GetString(data, "stage") ?? "",
        };
        var mappings = _basePathMappings.GetOrAdd(domainName,
            _ => new Dictionary<string, Dictionary<string, object?>>());
        mappings[basePath] = mapping;
        return V1Response(mapping, 201);
    }

    private ServiceResponse GetBasePathMappings(string domainName)
    {
        if (!_domainNames.ContainsKey(domainName))
        {
            return V1Error("NotFoundException", "Invalid domain name identifier specified", 404);
        }
        var mappings = _basePathMappings.TryGetValue(domainName, out var m)
            ? m.Values.Cast<object?>().ToList()
            : new List<object?>();
        return V1Response(new Dictionary<string, object?> { ["item"] = mappings });
    }

    private ServiceResponse GetBasePathMapping(string domainName, string basePath)
    {
        if (!_basePathMappings.TryGetValue(domainName, out var mappings)
            || !mappings.TryGetValue(basePath, out var mapping))
        {
            return V1Error("NotFoundException", "Invalid base path mapping identifier specified", 404);
        }
        return V1Response(mapping);
    }

    private ServiceResponse DeleteBasePathMapping(string domainName, string basePath)
    {
        if (_basePathMappings.TryGetValue(domainName, out var mappings))
        {
            mappings.Remove(basePath);
        }
        return EmptyResponse(202);
    }

    // -- Control plane: Tags ----------------------------------------------------

    private ServiceResponse GetV1Tags(string resourceArn)
    {
        var tags = _v1Tags.TryGetValue(resourceArn, out var t) ? t : new Dictionary<string, string>();
        return V1Response(new Dictionary<string, object?> { ["tags"] = tags });
    }

    private ServiceResponse TagV1Resource(string resourceArn, Dictionary<string, object?> data)
    {
        var tags = GetStringDict(data, "tags");
        if (!_v1Tags.TryGetValue(resourceArn, out var existing))
        {
            existing = new Dictionary<string, string>();
            _v1Tags[resourceArn] = existing;
        }
        foreach (var (key, value) in tags)
        {
            existing[key] = value;
        }
        return EmptyResponse(204);
    }

    private ServiceResponse UntagV1Resource(string resourceArn, string[] tagKeys)
    {
        if (_v1Tags.TryGetValue(resourceArn, out var existing))
        {
            foreach (var key in tagKeys)
            {
                existing.Remove(key);
            }
        }
        return EmptyResponse(204);
    }

    // -- Helpers ----------------------------------------------------------------

    private string ComputePath(string apiId, string resourceId)
    {
        if (!_resources.TryGetValue(apiId, out var resources))
        {
            return "/";
        }
        var parts = new List<string>();
        var rid = resourceId;
        while (!string.IsNullOrEmpty(rid))
        {
            if (!resources.TryGetValue(rid, out var r)) break;
            var pp = GetStringFromDict(r, "pathPart") ?? "";
            if (!string.IsNullOrEmpty(pp))
            {
                parts.Add(pp);
            }
            rid = GetStringFromDict(r, "parentId") ?? "";
        }
        if (parts.Count == 0) return "/";
        parts.Reverse();
        return "/" + string.Join("/", parts);
    }

    private Dictionary<string, object?> BuildApiSummary(string apiId)
    {
        var summary = new Dictionary<string, object?>();
        if (!_resources.TryGetValue(apiId, out var resources)) return summary;

        foreach (var resource in resources.Values)
        {
            var path = GetStringFromDict(resource, "path") ?? "/";
            var methods = GetSubDict(resource, "resourceMethods");
            if (methods is null) continue;

            foreach (var (httpMethod, methodVal) in methods)
            {
                if (methodVal is not Dictionary<string, object?> methodDict) continue;
                if (!summary.ContainsKey(path))
                {
                    summary[path] = new Dictionary<string, object?>();
                }
                if (summary[path] is Dictionary<string, object?> pathSummary)
                {
                    pathSummary[httpMethod] = new Dictionary<string, object?>
                    {
                        ["authorizationScopes"] = new List<object?>(),
                        ["apiKeyRequired"] = methodDict.TryGetValue("apiKeyRequired", out var akr) && akr is true,
                    };
                }
            }
        }
        return summary;
    }

    private Dictionary<string, object?>? GetResourceDict(string apiId, string resourceId)
    {
        if (!_resources.TryGetValue(apiId, out var resources) || !resources.TryGetValue(resourceId, out var resource))
        {
            return null;
        }
        return resource;
    }

    private Dictionary<string, object?>? GetMethodDict(string apiId, string resourceId, string httpMethod)
    {
        var resource = GetResourceDict(apiId, resourceId);
        if (resource is null) return null;
        var methods = GetSubDict(resource, "resourceMethods");
        if (methods is null) return null;
        if (!methods.TryGetValue(httpMethod, out var methodObj) || methodObj is not Dictionary<string, object?> method)
        {
            return null;
        }
        return method;
    }

    private Dictionary<string, object?>? GetIntegrationDict(string apiId, string resourceId, string httpMethod)
    {
        var method = GetMethodDict(apiId, resourceId, httpMethod);
        if (method is null) return null;
        if (method.TryGetValue("methodIntegration", out var intObj) && intObj is Dictionary<string, object?> integration)
        {
            return integration;
        }
        return null;
    }

    private static Dictionary<string, object?> GetOrCreateSubDict(Dictionary<string, object?> parent, string key)
    {
        if (parent.TryGetValue(key, out var val) && val is Dictionary<string, object?> existing)
        {
            return existing;
        }
        var d = new Dictionary<string, object?>();
        parent[key] = d;
        return d;
    }

    private static Dictionary<string, object?>? GetSubDict(Dictionary<string, object?> parent, string key)
    {
        if (parent.TryGetValue(key, out var val) && val is Dictionary<string, object?> d)
        {
            return d;
        }
        return null;
    }

    // -- JSON Patch operations --------------------------------------------------

    private static List<PatchOp> GetPatchOperations(Dictionary<string, object?> data)
    {
        var result = new List<PatchOp>();
        if (!data.TryGetValue("patchOperations", out var val) || val is null) return result;

        if (val is JsonElement je && je.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in je.EnumerateArray())
            {
                var op = item.TryGetProperty("op", out var opProp) ? opProp.GetString() ?? "replace" : "replace";
                var path = item.TryGetProperty("path", out var pathProp) ? pathProp.GetString() ?? "" : "";
                string? value = null;
                if (item.TryGetProperty("value", out var valueProp))
                {
                    value = valueProp.ValueKind == JsonValueKind.String
                        ? valueProp.GetString()
                        : valueProp.GetRawText();
                }
                result.Add(new PatchOp(op, path, value));
            }
        }

        return result;
    }

    private static void ApplyPatch(Dictionary<string, object?> obj, List<PatchOp> patchOps)
    {
        foreach (var op in patchOps)
        {
            var keys = op.Path.TrimStart('/').Split('/');
            if (keys.Length == 0 || (keys.Length == 1 && keys[0] == ""))
            {
                continue;
            }

            if (op.Operation is "replace" or "add")
            {
                if (keys.Length == 1)
                {
                    obj[keys[0]] = op.Value;
                }
                else
                {
                    var target = obj;
                    for (var i = 0; i < keys.Length - 1; i++)
                    {
                        if (!target.TryGetValue(keys[i], out var sub) || sub is not Dictionary<string, object?> subDict)
                        {
                            subDict = new Dictionary<string, object?>();
                            target[keys[i]] = subDict;
                        }
                        target = subDict;
                    }
                    target[keys[^1]] = op.Value;
                }
            }
            else if (op.Operation == "remove")
            {
                if (keys.Length == 1)
                {
                    obj.Remove(keys[0]);
                }
                else
                {
                    var target = obj;
                    for (var i = 0; i < keys.Length - 1; i++)
                    {
                        if (!target.TryGetValue(keys[i], out var sub) || sub is not Dictionary<string, object?> subDict)
                        {
                            target = null!;
                            break;
                        }
                        target = subDict;
                    }
                    target?.Remove(keys[^1]);
                }
            }
        }
    }

    // -- JSON helpers -----------------------------------------------------------

    private static string? GetString(Dictionary<string, object?> data, string key)
    {
        if (!data.TryGetValue(key, out var value) || value is null) return null;
        if (value is JsonElement je)
        {
            return je.ValueKind == JsonValueKind.String ? je.GetString() : je.GetRawText();
        }
        return value.ToString();
    }

    private static string? GetStringFromDict(Dictionary<string, object?> data, string key)
    {
        if (!data.TryGetValue(key, out var value) || value is null) return null;
        return value.ToString();
    }

    private static bool GetBool(Dictionary<string, object?> data, string key)
    {
        if (!data.TryGetValue(key, out var value) || value is null) return false;
        if (value is JsonElement je) return je.ValueKind == JsonValueKind.True;
        return value is true;
    }

    private static int GetInt(Dictionary<string, object?> data, string key, int defaultValue)
    {
        if (!data.TryGetValue(key, out var value) || value is null) return defaultValue;
        if (value is JsonElement je && je.TryGetInt32(out var i)) return i;
        if (value is int iv) return iv;
        return defaultValue;
    }

    private static object? GetObject(Dictionary<string, object?> data, string key)
    {
        if (!data.TryGetValue(key, out var value) || value is null) return null;
        if (value is JsonElement je)
        {
            return ConvertJsonElement(je);
        }
        return value;
    }

    private static object? ConvertJsonElement(JsonElement je)
    {
        return je.ValueKind switch
        {
            JsonValueKind.Object => ConvertJsonObject(je),
            JsonValueKind.Array => je.EnumerateArray().Select(ConvertJsonElement).ToList(),
            JsonValueKind.String => je.GetString(),
            JsonValueKind.Number => je.TryGetInt64(out var l) ? l : je.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    private static Dictionary<string, object?> ConvertJsonObject(JsonElement je)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var prop in je.EnumerateObject())
        {
            dict[prop.Name] = ConvertJsonElement(prop.Value);
        }
        return dict;
    }

    private static Dictionary<string, string> GetStringDict(Dictionary<string, object?> data, string key)
    {
        var result = new Dictionary<string, string>();
        if (!data.TryGetValue(key, out var value) || value is null) return result;
        if (value is JsonElement je && je.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in je.EnumerateObject())
            {
                result[prop.Name] = prop.Value.GetString() ?? "";
            }
        }
        return result;
    }

    private sealed record PatchOp(string Operation, string Path, string? Value);
}
