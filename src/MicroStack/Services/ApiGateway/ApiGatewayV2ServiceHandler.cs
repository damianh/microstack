using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using MicroStack.Internal;
using MicroStack.Services.Lambda;

namespace MicroStack.Services.ApiGateway;

/// <summary>
/// API Gateway HTTP API v2 emulator.
///
/// Control plane: REST API at /v2/apis/... for managing APIs, routes,
/// integrations, stages, deployments, authorizers, and tags.
///
/// Data plane: Requests to {apiId}.execute-api.localhost are forwarded
/// to Lambda (AWS_PROXY) via the LambdaServiceHandler.
///
/// Port of ministack/services/apigateway.py.
/// </summary>
internal sealed partial class ApiGatewayV2ServiceHandler : IServiceHandler
{
    private readonly LambdaServiceHandler _lambdaHandler;
    private readonly ApiGatewayV1ServiceHandler _v1Handler;

    // -- State ------------------------------------------------------------------

    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _apis = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _routes = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _integrations = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _stages = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _deployments = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _authorizers = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _apiTags = new();

    private static string Region => MicroStackOptions.Instance.Region;

    private static string Host => MicroStackOptions.Instance.Host;

    private static string Port => MicroStackOptions.Instance.GatewayPort.ToString();

    private static readonly JsonSerializerOptions s_jsonOpts = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false,
    };

    [GeneratedRegex(@"^([a-f0-9]{8})\.execute-api\.", RegexOptions.IgnoreCase)]
    private static partial Regex ExecuteApiRegex();

    internal ApiGatewayV2ServiceHandler(LambdaServiceHandler lambdaHandler)
    {
        _lambdaHandler = lambdaHandler;
        _v1Handler = new ApiGatewayV1ServiceHandler(lambdaHandler);
    }

    // -- IServiceHandler --------------------------------------------------------

    public string ServiceName => "apigateway";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var host = request.GetHeader("host") ?? "";
        var executeMatch = ExecuteApiRegex().Match(host);
        if (executeMatch.Success)
        {
            var apiId = executeMatch.Groups[1].Value;
            // Route to v1 handler if the API ID belongs to a REST API
            if (_v1Handler.OwnsApiId(apiId))
            {
                return Task.FromResult(_v1Handler.HandleExecute(apiId, request));
            }
            return Task.FromResult(HandleExecute(apiId, request));
        }

        // Route v1 control plane paths to the v1 handler
        var pathLower = request.Path.TrimStart('/').ToLowerInvariant();
        if (pathLower.StartsWith("restapis", StringComparison.Ordinal)
            || pathLower.StartsWith("apikeys", StringComparison.Ordinal)
            || pathLower.StartsWith("usageplans", StringComparison.Ordinal)
            || pathLower.StartsWith("domainnames", StringComparison.Ordinal)
            || (pathLower.StartsWith("tags/", StringComparison.Ordinal)
                && pathLower.Contains("restapis", StringComparison.Ordinal)))
        {
            return Task.FromResult(_v1Handler.HandleControlPlane(request));
        }

        return Task.FromResult(HandleControlPlane(request));
    }

    public void Reset()
    {
        _apis.Clear();
        _routes.Clear();
        _integrations.Clear();
        _stages.Clear();
        _deployments.Clear();
        _authorizers.Clear();
        _apiTags.Clear();
        _v1Handler.Reset();
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // -- Response helpers -------------------------------------------------------

    private static ServiceResponse ApigwResponse(Dictionary<string, object?> data, int statusCode)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(data, s_jsonOpts);
        return new ServiceResponse(statusCode,
            new Dictionary<string, string> { ["Content-Type"] = "application/json" }, json);
    }

    private static ServiceResponse ApigwResponse(Dictionary<string, object?> data)
    {
        return ApigwResponse(data, 200);
    }

    private static ServiceResponse ApigwError(string code, string message, int statusCode)
    {
        var data = new Dictionary<string, object?> { ["message"] = message, ["__type"] = code };
        return ApigwResponse(data, statusCode);
    }

    private static ServiceResponse EmptyResponse(int statusCode)
    {
        return new ServiceResponse(statusCode, new Dictionary<string, string>(), []);
    }

    private static string ApiArn(string apiId) =>
        $"arn:aws:apigateway:{Region}::/apis/{apiId}";

    // -- Control plane router ---------------------------------------------------

    private ServiceResponse HandleControlPlane(ServiceRequest request)
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

        if (parts.Length == 0 || parts[0] != "v2")
        {
            return ApigwError("NotFoundException", $"Unknown path: {path}", 404);
        }

        var resource = parts.Length > 1 ? parts[1] : "";

        // /v2/tags/{resourceArn}
        if (resource == "tags")
        {
            var resourceArn = parts.Length > 2
                ? string.Join("/", parts[2..])
                : "";
            // URL-decode the ARN (it may be URL-encoded by the SDK)
            resourceArn = Uri.UnescapeDataString(resourceArn);

            if (method == "GET")
            {
                return GetTags(resourceArn);
            }
            if (method == "POST")
            {
                return TagResource(resourceArn, data);
            }
            if (method == "DELETE")
            {
                var tagKeys = request.QueryParams.TryGetValue("tagKeys", out var keys) ? keys : [];
                return UntagResource(resourceArn, tagKeys);
            }
        }

        if (resource == "apis")
        {
            var apiId = parts.Length > 2 ? parts[2] : null;
            var sub = parts.Length > 3 ? parts[3] : null;
            var subId = parts.Length > 4 ? parts[4] : null;

            // /v2/apis
            if (apiId is null)
            {
                if (method == "POST") return CreateApi(data);
                if (method == "GET") return GetApis();
            }

            // /v2/apis/{apiId}
            if (apiId is not null && sub is null)
            {
                if (method == "GET") return GetApi(apiId);
                if (method == "DELETE") return DeleteApi(apiId);
                if (method == "PATCH") return UpdateApi(apiId, data);
            }

            // /v2/apis/{apiId}/routes[/{routeId}]
            if (apiId is not null && sub == "routes")
            {
                if (subId is null)
                {
                    if (method == "POST") return CreateRoute(apiId, data);
                    if (method == "GET") return GetRoutes(apiId);
                }
                else
                {
                    if (method == "GET") return GetRoute(apiId, subId);
                    if (method == "PATCH") return UpdateRoute(apiId, subId, data);
                    if (method == "DELETE") return DeleteRoute(apiId, subId);
                }
            }

            // /v2/apis/{apiId}/integrations[/{integrationId}]
            if (apiId is not null && sub == "integrations")
            {
                if (subId is null)
                {
                    if (method == "POST") return CreateIntegration(apiId, data);
                    if (method == "GET") return GetIntegrations(apiId);
                }
                else
                {
                    if (method == "GET") return GetIntegration(apiId, subId);
                    if (method == "PATCH") return UpdateIntegration(apiId, subId, data);
                    if (method == "DELETE") return DeleteIntegration(apiId, subId);
                }
            }

            // /v2/apis/{apiId}/stages[/{stageName}]
            if (apiId is not null && sub == "stages")
            {
                if (subId is null)
                {
                    if (method == "POST") return CreateStage(apiId, data);
                    if (method == "GET") return GetStages(apiId);
                }
                else
                {
                    if (method == "GET") return GetStage(apiId, subId);
                    if (method == "PATCH") return UpdateStage(apiId, subId, data);
                    if (method == "DELETE") return DeleteStage(apiId, subId);
                }
            }

            // /v2/apis/{apiId}/deployments[/{deploymentId}]
            if (apiId is not null && sub == "deployments")
            {
                if (subId is null)
                {
                    if (method == "POST") return CreateDeployment(apiId, data);
                    if (method == "GET") return GetDeployments(apiId);
                }
                else
                {
                    if (method == "GET") return GetDeployment(apiId, subId);
                    if (method == "DELETE") return DeleteDeployment(apiId, subId);
                }
            }

            // /v2/apis/{apiId}/authorizers[/{authorizerId}]
            if (apiId is not null && sub == "authorizers")
            {
                if (subId is null)
                {
                    if (method == "POST") return CreateAuthorizer(apiId, data);
                    if (method == "GET") return GetAuthorizers(apiId);
                }
                else
                {
                    if (method == "GET") return GetAuthorizer(apiId, subId);
                    if (method == "PATCH") return UpdateAuthorizer(apiId, subId, data);
                    if (method == "DELETE") return DeleteAuthorizer(apiId, subId);
                }
            }
        }

        return ApigwError("NotFoundException", $"Unknown API Gateway path: {path}", 404);
    }

    // -- Control plane: APIs ----------------------------------------------------

    private ServiceResponse CreateApi(Dictionary<string, object?> data)
    {
        var apiId = HashHelpers.NewUuid()[..8];
        var now = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
        var api = new Dictionary<string, object?>
        {
            ["apiId"] = apiId,
            ["name"] = GetString(data, "name") ?? "unnamed",
            ["protocolType"] = GetString(data, "protocolType") ?? "HTTP",
            ["apiEndpoint"] = $"http://{apiId}.execute-api.{Host}:{Port}",
            ["createdDate"] = now,
            ["routeSelectionExpression"] = GetString(data, "routeSelectionExpression")
                ?? "$request.method $request.path",
            ["tags"] = GetObject(data, "tags") ?? new Dictionary<string, object?>(),
            ["corsConfiguration"] = GetObject(data, "corsConfiguration") ?? new Dictionary<string, object?>(),
            ["disableSchemaValidation"] = GetBool(data, "disableSchemaValidation"),
            ["disableExecuteApiEndpoint"] = GetBool(data, "disableExecuteApiEndpoint"),
            ["version"] = GetString(data, "version") ?? "",
        };

        _apis[apiId] = api;
        _routes[apiId] = new Dictionary<string, Dictionary<string, object?>>();
        _integrations[apiId] = new Dictionary<string, Dictionary<string, object?>>();
        _stages[apiId] = new Dictionary<string, Dictionary<string, object?>>();
        _deployments[apiId] = new Dictionary<string, Dictionary<string, object?>>();

        // Store initial tags
        var tags = GetStringDict(data, "tags");
        _apiTags[ApiArn(apiId)] = tags;

        return ApigwResponse(api, 201);
    }

    private ServiceResponse GetApi(string apiId)
    {
        if (!_apis.TryGetValue(apiId, out var api))
        {
            return ApigwError("NotFoundException", $"API {apiId} not found", 404);
        }
        return ApigwResponse(api);
    }

    private ServiceResponse GetApis()
    {
        var items = _apis.Values.ToList();
        var result = new Dictionary<string, object?>
        {
            ["items"] = items,
            ["nextToken"] = null,
        };
        return ApigwResponse(result);
    }

    private ServiceResponse DeleteApi(string apiId)
    {
        _apis.TryRemove(apiId, out _);
        _routes.TryRemove(apiId, out _);
        _integrations.TryRemove(apiId, out _);
        _stages.TryRemove(apiId, out _);
        _deployments.TryRemove(apiId, out _);
        _apiTags.TryRemove(ApiArn(apiId), out _);
        return EmptyResponse(204);
    }

    private ServiceResponse UpdateApi(string apiId, Dictionary<string, object?> data)
    {
        if (!_apis.TryGetValue(apiId, out var api))
        {
            return ApigwError("NotFoundException", $"API {apiId} not found", 404);
        }

        foreach (var key in new[] { "name", "corsConfiguration", "routeSelectionExpression",
                                    "disableSchemaValidation", "disableExecuteApiEndpoint", "version" })
        {
            if (data.ContainsKey(key))
            {
                api[key] = data[key];
            }
        }

        return ApigwResponse(api);
    }

    // -- Control plane: Routes --------------------------------------------------

    private ServiceResponse CreateRoute(string apiId, Dictionary<string, object?> data)
    {
        if (!_apis.ContainsKey(apiId))
        {
            return ApigwError("NotFoundException", $"API {apiId} not found", 404);
        }

        var routeId = HashHelpers.NewUuid()[..8];
        var route = new Dictionary<string, object?>
        {
            ["routeId"] = routeId,
            ["routeKey"] = GetString(data, "routeKey") ?? "$default",
            ["target"] = GetString(data, "target") ?? "",
            ["authorizationType"] = GetString(data, "authorizationType") ?? "NONE",
            ["apiKeyRequired"] = GetBool(data, "apiKeyRequired"),
            ["operationName"] = GetString(data, "operationName") ?? "",
            ["requestModels"] = GetObject(data, "requestModels") ?? new Dictionary<string, object?>(),
            ["requestParameters"] = GetObject(data, "requestParameters") ?? new Dictionary<string, object?>(),
        };

        var apiRoutes = _routes.GetOrAdd(apiId, _ => new Dictionary<string, Dictionary<string, object?>>());
        apiRoutes[routeId] = route;
        return ApigwResponse(route, 201);
    }

    private ServiceResponse GetRoutes(string apiId)
    {
        var routes = _routes.TryGetValue(apiId, out var r)
            ? r.Values.ToList()
            : new List<Dictionary<string, object?>>();
        return ApigwResponse(new Dictionary<string, object?>
        {
            ["items"] = routes,
            ["nextToken"] = null,
        });
    }

    private ServiceResponse GetRoute(string apiId, string routeId)
    {
        if (!_routes.TryGetValue(apiId, out var routes) || !routes.TryGetValue(routeId, out var route))
        {
            return ApigwError("NotFoundException", $"Route {routeId} not found", 404);
        }
        return ApigwResponse(route);
    }

    private ServiceResponse UpdateRoute(string apiId, string routeId, Dictionary<string, object?> data)
    {
        if (!_routes.TryGetValue(apiId, out var routes) || !routes.TryGetValue(routeId, out var route))
        {
            return ApigwError("NotFoundException", $"Route {routeId} not found", 404);
        }

        foreach (var key in new[] { "routeKey", "target", "authorizationType", "apiKeyRequired", "operationName" })
        {
            if (data.ContainsKey(key))
            {
                route[key] = data[key];
            }
        }

        return ApigwResponse(route);
    }

    private ServiceResponse DeleteRoute(string apiId, string routeId)
    {
        if (_routes.TryGetValue(apiId, out var routes))
        {
            routes.Remove(routeId);
        }
        return EmptyResponse(204);
    }

    // -- Control plane: Integrations --------------------------------------------

    private ServiceResponse CreateIntegration(string apiId, Dictionary<string, object?> data)
    {
        if (!_apis.ContainsKey(apiId))
        {
            return ApigwError("NotFoundException", $"API {apiId} not found", 404);
        }

        var intId = HashHelpers.NewUuid()[..8];
        var integration = new Dictionary<string, object?>
        {
            ["integrationId"] = intId,
            ["integrationType"] = GetString(data, "integrationType") ?? "AWS_PROXY",
            ["integrationUri"] = GetString(data, "integrationUri") ?? "",
            ["integrationMethod"] = GetString(data, "integrationMethod") ?? "POST",
            ["payloadFormatVersion"] = GetString(data, "payloadFormatVersion") ?? "2.0",
            ["timeoutInMillis"] = GetInt(data, "timeoutInMillis", 30000),
            ["connectionType"] = GetString(data, "connectionType") ?? "INTERNET",
            ["description"] = GetString(data, "description") ?? "",
            ["requestParameters"] = GetObject(data, "requestParameters") ?? new Dictionary<string, object?>(),
            ["requestTemplates"] = GetObject(data, "requestTemplates") ?? new Dictionary<string, object?>(),
            ["responseParameters"] = GetObject(data, "responseParameters") ?? new Dictionary<string, object?>(),
        };

        var apiIntegrations = _integrations.GetOrAdd(apiId,
            _ => new Dictionary<string, Dictionary<string, object?>>());
        apiIntegrations[intId] = integration;
        return ApigwResponse(integration, 201);
    }

    private ServiceResponse GetIntegrations(string apiId)
    {
        var integrations = _integrations.TryGetValue(apiId, out var i)
            ? i.Values.ToList()
            : new List<Dictionary<string, object?>>();
        return ApigwResponse(new Dictionary<string, object?>
        {
            ["items"] = integrations,
            ["nextToken"] = null,
        });
    }

    private ServiceResponse GetIntegration(string apiId, string intId)
    {
        if (!_integrations.TryGetValue(apiId, out var integrations)
            || !integrations.TryGetValue(intId, out var integration))
        {
            return ApigwError("NotFoundException", $"Integration {intId} not found", 404);
        }
        return ApigwResponse(integration);
    }

    private ServiceResponse UpdateIntegration(string apiId, string intId, Dictionary<string, object?> data)
    {
        if (!_integrations.TryGetValue(apiId, out var integrations)
            || !integrations.TryGetValue(intId, out var integration))
        {
            return ApigwError("NotFoundException", $"Integration {intId} not found", 404);
        }

        foreach (var key in new[] { "integrationType", "integrationUri", "integrationMethod",
                                    "payloadFormatVersion", "timeoutInMillis", "connectionType",
                                    "description", "requestParameters", "requestTemplates", "responseParameters" })
        {
            if (data.ContainsKey(key))
            {
                integration[key] = data[key];
            }
        }

        return ApigwResponse(integration);
    }

    private ServiceResponse DeleteIntegration(string apiId, string intId)
    {
        if (_integrations.TryGetValue(apiId, out var integrations))
        {
            integrations.Remove(intId);
        }
        return EmptyResponse(204);
    }

    // -- Control plane: Stages --------------------------------------------------

    private ServiceResponse CreateStage(string apiId, Dictionary<string, object?> data)
    {
        if (!_apis.ContainsKey(apiId))
        {
            return ApigwError("NotFoundException", $"API {apiId} not found", 404);
        }

        var stageName = GetString(data, "stageName") ?? "$default";
        var now = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
        var stage = new Dictionary<string, object?>
        {
            ["stageName"] = stageName,
            ["autoDeploy"] = GetBool(data, "autoDeploy"),
            ["createdDate"] = now,
            ["lastUpdatedDate"] = now,
            ["stageVariables"] = GetObject(data, "stageVariables") ?? new Dictionary<string, object?>(),
            ["description"] = GetString(data, "description") ?? "",
            ["defaultRouteSettings"] = GetObject(data, "defaultRouteSettings") ?? new Dictionary<string, object?>(),
            ["routeSettings"] = GetObject(data, "routeSettings") ?? new Dictionary<string, object?>(),
            ["tags"] = GetObject(data, "tags") ?? new Dictionary<string, object?>(),
        };

        var apiStages = _stages.GetOrAdd(apiId,
            _ => new Dictionary<string, Dictionary<string, object?>>());
        apiStages[stageName] = stage;
        return ApigwResponse(stage, 201);
    }

    private ServiceResponse GetStages(string apiId)
    {
        var stages = _stages.TryGetValue(apiId, out var s)
            ? s.Values.ToList()
            : new List<Dictionary<string, object?>>();
        return ApigwResponse(new Dictionary<string, object?>
        {
            ["items"] = stages,
            ["nextToken"] = null,
        });
    }

    private ServiceResponse GetStage(string apiId, string stageName)
    {
        if (!_stages.TryGetValue(apiId, out var stages) || !stages.TryGetValue(stageName, out var stage))
        {
            return ApigwError("NotFoundException", $"Stage '{stageName}' not found", 404);
        }
        return ApigwResponse(stage);
    }

    private ServiceResponse UpdateStage(string apiId, string stageName, Dictionary<string, object?> data)
    {
        if (!_stages.TryGetValue(apiId, out var stages) || !stages.TryGetValue(stageName, out var stage))
        {
            return ApigwError("NotFoundException", $"Stage '{stageName}' not found", 404);
        }

        foreach (var key in new[] { "autoDeploy", "stageVariables", "description",
                                    "defaultRouteSettings", "routeSettings" })
        {
            if (data.ContainsKey(key))
            {
                stage[key] = data[key];
            }
        }

        stage["lastUpdatedDate"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
        return ApigwResponse(stage);
    }

    private ServiceResponse DeleteStage(string apiId, string stageName)
    {
        if (_stages.TryGetValue(apiId, out var stages))
        {
            stages.Remove(stageName);
        }
        return EmptyResponse(204);
    }

    // -- Control plane: Deployments ---------------------------------------------

    private ServiceResponse CreateDeployment(string apiId, Dictionary<string, object?> data)
    {
        if (!_apis.ContainsKey(apiId))
        {
            return ApigwError("NotFoundException", $"API {apiId} not found", 404);
        }

        var deploymentId = HashHelpers.NewUuid()[..8];
        var now = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
        var deployment = new Dictionary<string, object?>
        {
            ["deploymentId"] = deploymentId,
            ["deploymentStatus"] = "DEPLOYED",
            ["createdDate"] = now,
            ["description"] = GetString(data, "description") ?? "",
        };

        var apiDeployments = _deployments.GetOrAdd(apiId,
            _ => new Dictionary<string, Dictionary<string, object?>>());
        apiDeployments[deploymentId] = deployment;
        return ApigwResponse(deployment, 201);
    }

    private ServiceResponse GetDeployments(string apiId)
    {
        var deployments = _deployments.TryGetValue(apiId, out var d)
            ? d.Values.ToList()
            : new List<Dictionary<string, object?>>();
        return ApigwResponse(new Dictionary<string, object?>
        {
            ["items"] = deployments,
            ["nextToken"] = null,
        });
    }

    private ServiceResponse GetDeployment(string apiId, string deploymentId)
    {
        if (!_deployments.TryGetValue(apiId, out var deployments)
            || !deployments.TryGetValue(deploymentId, out var deployment))
        {
            return ApigwError("NotFoundException", $"Deployment {deploymentId} not found", 404);
        }
        return ApigwResponse(deployment);
    }

    private ServiceResponse DeleteDeployment(string apiId, string deploymentId)
    {
        if (_deployments.TryGetValue(apiId, out var deployments))
        {
            deployments.Remove(deploymentId);
        }
        return EmptyResponse(204);
    }

    // -- Control plane: Tags ----------------------------------------------------

    private ServiceResponse GetTags(string resourceArn)
    {
        var tags = _apiTags.TryGetValue(resourceArn, out var t) ? t : new Dictionary<string, string>();
        return ApigwResponse(new Dictionary<string, object?> { ["tags"] = tags });
    }

    private ServiceResponse TagResource(string resourceArn, Dictionary<string, object?> data)
    {
        var tags = GetStringDict(data, "tags");
        if (!_apiTags.TryGetValue(resourceArn, out var existing))
        {
            existing = new Dictionary<string, string>();
            _apiTags[resourceArn] = existing;
        }
        foreach (var (key, value) in tags)
        {
            existing[key] = value;
        }
        return EmptyResponse(201);
    }

    private ServiceResponse UntagResource(string resourceArn, string[] tagKeys)
    {
        if (_apiTags.TryGetValue(resourceArn, out var existing))
        {
            foreach (var key in tagKeys)
            {
                existing.Remove(key);
            }
        }
        return EmptyResponse(204);
    }

    // -- Control plane: Authorizers ---------------------------------------------

    private ServiceResponse CreateAuthorizer(string apiId, Dictionary<string, object?> data)
    {
        if (!_apis.ContainsKey(apiId))
        {
            return ApigwError("NotFoundException", $"API {apiId} not found", 404);
        }

        var authId = HashHelpers.NewUuid()[..8];
        var authorizer = new Dictionary<string, object?>
        {
            ["authorizerId"] = authId,
            ["authorizerType"] = GetString(data, "authorizerType") ?? "JWT",
            ["name"] = GetString(data, "name") ?? "",
            ["identitySource"] = GetObject(data, "identitySource"),
            ["jwtConfiguration"] = GetObject(data, "jwtConfiguration") ?? new Dictionary<string, object?>(),
            ["authorizerUri"] = GetString(data, "authorizerUri") ?? "",
            ["authorizerPayloadFormatVersion"] = GetString(data, "authorizerPayloadFormatVersion") ?? "2.0",
            ["authorizerResultTtlInSeconds"] = GetInt(data, "authorizerResultTtlInSeconds", 300),
            ["enableSimpleResponses"] = GetBool(data, "enableSimpleResponses"),
            ["authorizerCredentialsArn"] = GetString(data, "authorizerCredentialsArn") ?? "",
        };

        var apiAuthorizers = _authorizers.GetOrAdd(apiId,
            _ => new Dictionary<string, Dictionary<string, object?>>());
        apiAuthorizers[authId] = authorizer;
        return ApigwResponse(authorizer, 201);
    }

    private ServiceResponse GetAuthorizers(string apiId)
    {
        var authorizers = _authorizers.TryGetValue(apiId, out var a)
            ? a.Values.ToList()
            : new List<Dictionary<string, object?>>();
        return ApigwResponse(new Dictionary<string, object?>
        {
            ["items"] = authorizers,
            ["nextToken"] = null,
        });
    }

    private ServiceResponse GetAuthorizer(string apiId, string authId)
    {
        if (!_authorizers.TryGetValue(apiId, out var authorizers)
            || !authorizers.TryGetValue(authId, out var authorizer))
        {
            return ApigwError("NotFoundException", $"Authorizer {authId} not found", 404);
        }
        return ApigwResponse(authorizer);
    }

    private ServiceResponse UpdateAuthorizer(string apiId, string authId, Dictionary<string, object?> data)
    {
        if (!_authorizers.TryGetValue(apiId, out var authorizers)
            || !authorizers.TryGetValue(authId, out var authorizer))
        {
            return ApigwError("NotFoundException", $"Authorizer {authId} not found", 404);
        }

        foreach (var key in new[] { "name", "identitySource", "jwtConfiguration", "authorizerUri",
                                    "authorizerPayloadFormatVersion", "authorizerResultTtlInSeconds",
                                    "enableSimpleResponses", "authorizerCredentialsArn" })
        {
            if (data.ContainsKey(key))
            {
                authorizer[key] = data[key];
            }
        }

        return ApigwResponse(authorizer);
    }

    private ServiceResponse DeleteAuthorizer(string apiId, string authId)
    {
        if (_authorizers.TryGetValue(apiId, out var authorizers))
        {
            authorizers.Remove(authId);
        }
        return EmptyResponse(204);
    }

    // -- Data plane: Execute API ------------------------------------------------

    private ServiceResponse HandleExecute(string apiId, ServiceRequest request)
    {
        if (!_apis.TryGetValue(apiId, out _))
        {
            return JsonNotFound("Not Found");
        }

        var method = request.Method.ToUpperInvariant();
        var path = request.Path;

        // Parse /{stage}/{remaining-path}
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        string stage;
        string requestPath;

        if (segments.Length == 0)
        {
            stage = "$default";
            requestPath = "/";
        }
        else
        {
            stage = Uri.UnescapeDataString(segments[0]);
            requestPath = segments.Length > 1
                ? "/" + string.Join("/", segments[1..].Select(Uri.UnescapeDataString))
                : "/";
        }

        // Check stage exists (allow $default implicitly)
        if (_stages.TryGetValue(apiId, out var apiStages))
        {
            if (!apiStages.ContainsKey(stage) && stage != "$default")
            {
                return JsonNotFound($"Stage '{stage}' not found");
            }
        }
        else if (stage != "$default")
        {
            return JsonNotFound($"Stage '{stage}' not found");
        }

        // Match route
        var route = MatchRoute(apiId, method, requestPath);
        if (route is null)
        {
            return JsonNotFound("No route found");
        }

        var integrationTarget = route.TryGetValue("target", out var targetObj)
            ? targetObj?.ToString() ?? ""
            : "";
        var integrationId = integrationTarget.Replace("integrations/", "");

        Dictionary<string, object?>? integration = null;
        if (_integrations.TryGetValue(apiId, out var apiIntegrations))
        {
            apiIntegrations.TryGetValue(integrationId, out integration);
        }

        if (integration is null)
        {
            return JsonError("No integration configured", 500);
        }

        var integrationType = integration.TryGetValue("integrationType", out var itObj)
            ? itObj?.ToString() ?? ""
            : "";

        if (integrationType == "AWS_PROXY")
        {
            var routeKey = route.TryGetValue("routeKey", out var rkObj)
                ? rkObj?.ToString() ?? "$default"
                : "$default";

            Dictionary<string, string>? pathParams = null;
            var rkParts = routeKey.Split(' ', 2);
            if (rkParts.Length == 2)
            {
                pathParams = ExtractPathParams(rkParts[1], requestPath);
                if (pathParams is not null && pathParams.Count == 0)
                {
                    pathParams = null;
                }
            }

            return InvokeLambdaProxy(integration, apiId, stage, requestPath, method,
                request.Headers, request.Body, request.QueryParams, routeKey, pathParams);
        }

        // HTTP_PROXY and others: stub
        return JsonError($"Unsupported integration type: {integrationType}", 500);
    }

    private Dictionary<string, object?>? MatchRoute(string apiId, string method, string path)
    {
        if (!_routes.TryGetValue(apiId, out var routes))
        {
            return null;
        }

        // First pass: specific method+path match (skip $default)
        foreach (var route in routes.Values)
        {
            var key = route.TryGetValue("routeKey", out var rkObj)
                ? rkObj?.ToString() ?? ""
                : "";
            if (key == "$default")
            {
                continue;
            }
            var parts = key.Split(' ', 2);
            if (parts.Length == 2)
            {
                var rMethod = parts[0];
                var rPath = parts[1];
                if ((rMethod == "ANY" || rMethod == method) && PathMatches(rPath, path))
                {
                    return route;
                }
            }
        }

        // Second pass: $default catch-all
        foreach (var route in routes.Values)
        {
            var key = route.TryGetValue("routeKey", out var rkObj)
                ? rkObj?.ToString() ?? ""
                : "";
            if (key == "$default")
            {
                return route;
            }
        }

        return null;
    }

    private static bool PathMatches(string routePath, string requestPath)
    {
        return ExtractPathParams(routePath, requestPath) is not null;
    }

    private static Dictionary<string, string>? ExtractPathParams(string routePath, string requestPath)
    {
        var parts = Regex.Split(routePath, @"(\{[^}]+\})");
        var patternParts = new List<string>();
        var paramNames = new List<string>();

        foreach (var part in parts)
        {
            if (part.StartsWith('{') && part.EndsWith('}'))
            {
                var inner = part[1..^1];
                if (inner.EndsWith('+'))
                {
                    paramNames.Add(inner[..^1]);
                    patternParts.Add("(.+)");
                }
                else
                {
                    paramNames.Add(inner);
                    patternParts.Add("([^/]+)");
                }
            }
            else
            {
                patternParts.Add(Regex.Escape(part));
            }
        }

        var pattern = string.Concat(patternParts);
        var match = Regex.Match(requestPath, $"^{pattern}$");
        if (!match.Success)
        {
            return null;
        }

        if (paramNames.Count == 0)
        {
            return new Dictionary<string, string>();
        }

        var result = new Dictionary<string, string>();
        for (var i = 0; i < paramNames.Count; i++)
        {
            result[paramNames[i]] = match.Groups[i + 1].Value;
        }
        return result;
    }

    private ServiceResponse InvokeLambdaProxy(
        Dictionary<string, object?> integration,
        string apiId,
        string stage,
        string path,
        string method,
        IReadOnlyDictionary<string, string> headers,
        byte[] body,
        IReadOnlyDictionary<string, string[]> queryParams,
        string routeKey,
        Dictionary<string, string>? pathParams)
    {
        var uri = integration.TryGetValue("integrationUri", out var uriObj)
            ? uriObj?.ToString() ?? ""
            : "";

        // Build query string parameters (multi-value joined with commas per AWS spec)
        Dictionary<string, object?>? qs = null;
        if (queryParams.Count > 0)
        {
            qs = new Dictionary<string, object?>();
            foreach (var (key, values) in queryParams)
            {
                qs[key] = string.Join(",", values);
            }
        }

        // Build raw query string
        var rawQsParts = new List<string>();
        foreach (var (key, values) in queryParams)
        {
            foreach (var val in values)
            {
                rawQsParts.Add($"{key}={val}");
            }
        }
        var rawQs = string.Join("&", rawQsParts);

        // Build headers dict
        var headerDict = new Dictionary<string, object?>();
        foreach (var (key, value) in headers)
        {
            headerDict[key.ToLowerInvariant()] = value;
        }

        var now = DateTimeOffset.UtcNow;
        var lambdaEvent = new Dictionary<string, object?>
        {
            ["version"] = "2.0",
            ["routeKey"] = routeKey,
            ["rawPath"] = path,
            ["rawQueryString"] = rawQs,
            ["headers"] = headerDict,
            ["queryStringParameters"] = qs,
            ["requestContext"] = new Dictionary<string, object?>
            {
                ["accountId"] = AccountContext.GetAccountId(),
                ["apiId"] = apiId,
                ["domainName"] = $"{apiId}.execute-api.{Host}",
                ["http"] = new Dictionary<string, object?>
                {
                    ["method"] = method,
                    ["path"] = path,
                    ["protocol"] = "HTTP/1.1",
                    ["sourceIp"] = "127.0.0.1",
                    ["userAgent"] = headers.TryGetValue("user-agent", out var ua)
                        ? ua
                        : (headers.TryGetValue("User-Agent", out var ua2) ? ua2 : ""),
                },
                ["requestId"] = HashHelpers.NewUuid(),
                ["routeKey"] = routeKey,
                ["stage"] = stage,
                ["time"] = now.ToString("dd/MMM/yyyy:HH:mm:ss +0000"),
                ["timeEpoch"] = now.ToUnixTimeMilliseconds(),
            },
            ["pathParameters"] = pathParams,
            ["body"] = body.Length > 0 ? Encoding.UTF8.GetString(body) : null,
            ["isBase64Encoded"] = false,
        };

        var eventPayload = JsonSerializer.SerializeToUtf8Bytes(lambdaEvent, s_jsonOpts);
        var (success, responsePayload, error) = _lambdaHandler.InvokeForApiGateway(uri, eventPayload);

        if (!success)
        {
            return JsonError(error ?? "Lambda invocation failed", 502);
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
            return JsonError("Invalid Lambda response", 502);
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

    private static ServiceResponse JsonNotFound(string message)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object?>
        {
            ["message"] = message,
        }, s_jsonOpts);
        return new ServiceResponse(404,
            new Dictionary<string, string> { ["Content-Type"] = "application/json" }, body);
    }

    private static ServiceResponse JsonError(string message, int statusCode)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, object?>
        {
            ["message"] = message,
        }, s_jsonOpts);
        return new ServiceResponse(statusCode,
            new Dictionary<string, string> { ["Content-Type"] = "application/json" }, body);
    }

    // -- JSON helpers -----------------------------------------------------------

    private static string? GetString(Dictionary<string, object?> data, string key)
    {
        if (!data.TryGetValue(key, out var value) || value is null)
        {
            return null;
        }
        if (value is JsonElement je)
        {
            return je.ValueKind == JsonValueKind.String ? je.GetString() : je.GetRawText();
        }
        return value.ToString();
    }

    private static bool GetBool(Dictionary<string, object?> data, string key)
    {
        if (!data.TryGetValue(key, out var value) || value is null)
        {
            return false;
        }
        if (value is JsonElement je)
        {
            return je.ValueKind == JsonValueKind.True;
        }
        return value is true;
    }

    private static int GetInt(Dictionary<string, object?> data, string key, int defaultValue)
    {
        if (!data.TryGetValue(key, out var value) || value is null)
        {
            return defaultValue;
        }
        if (value is JsonElement je && je.TryGetInt32(out var i))
        {
            return i;
        }
        if (value is int iv)
        {
            return iv;
        }
        return defaultValue;
    }

    private static object? GetObject(Dictionary<string, object?> data, string key)
    {
        if (!data.TryGetValue(key, out var value) || value is null)
        {
            return null;
        }
        return value;
    }

    private static Dictionary<string, string> GetStringDict(Dictionary<string, object?> data, string key)
    {
        var result = new Dictionary<string, string>();
        if (!data.TryGetValue(key, out var value) || value is null)
        {
            return result;
        }

        if (value is JsonElement je && je.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in je.EnumerateObject())
            {
                result[prop.Name] = prop.Value.GetString() ?? "";
            }
        }

        return result;
    }
}
