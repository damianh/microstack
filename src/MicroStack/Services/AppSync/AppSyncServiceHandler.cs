using System.Text.Json;
using System.Text.RegularExpressions;
using MicroStack.Internal;

namespace MicroStack.Services.AppSync;

/// <summary>
/// AppSync service handler -- REST/JSON protocol with path-based routing under /v1/apis.
///
/// Port of ministack/services/appsync.py (management plane only, no GraphQL data plane).
///
/// Supports:
///   GraphQL APIs:  CreateGraphQLApi, GetGraphQLApi, ListGraphQLApis,
///                  UpdateGraphQLApi, DeleteGraphQLApi
///   API Keys:      CreateApiKey, ListApiKeys, DeleteApiKey
///   Data Sources:  CreateDataSource, GetDataSource, ListDataSources, DeleteDataSource
///   Resolvers:     CreateResolver, GetResolver, ListResolvers, DeleteResolver
///   Types:         CreateType, ListTypes, GetType
///   Tags:          TagResource, UntagResource, ListTagsForResource
/// </summary>
internal sealed partial class AppSyncServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // In-memory state
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _apis = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _apiKeys = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _dataSources = new();
    // resolvers: apiId -> typeName -> fieldName -> resolver record
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, Dictionary<string, object?>>>> _resolvers = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> _types = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _tags = new();

    // ── Path regex ────────────────────────────────────────────────────────────
    [GeneratedRegex(@"^/v1/apis(?:/([^/]+))?(?:/([^/]+))?(?:/([^/]+))?(?:/([^/]+))?(?:/([^/]+))?")]
    private static partial Regex PathRegex();

    // ── IServiceHandler ──────────────────────────────────────────────────────

    public string ServiceName => "appsync";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var response = HandleRequest(request);
        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _apis.Clear();
            _apiKeys.Clear();
            _dataSources.Clear();
            _resolvers.Clear();
            _types.Clear();
            _tags.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static long NowEpoch() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    private static string ApiArn(string apiId) =>
        $"arn:aws:appsync:{Region}:{AccountContext.GetAccountId()}:apis/{apiId}";

    private static Dictionary<string, object?> ParseBody(byte[] body)
    {
        if (body.Length == 0)
        {
            return new Dictionary<string, object?>(StringComparer.Ordinal);
        }

        try
        {
            using var doc = JsonDocument.Parse(body);
            return JsonElementToDict(doc.RootElement);
        }
        catch
        {
            return new Dictionary<string, object?>(StringComparer.Ordinal);
        }
    }

    private static Dictionary<string, object?> JsonElementToDict(JsonElement el)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (el.ValueKind != JsonValueKind.Object)
        {
            return dict;
        }

        foreach (var prop in el.EnumerateObject())
        {
            dict[prop.Name] = JsonElementToObject(prop.Value);
        }

        return dict;
    }

    private static object? JsonElementToObject(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.Object => JsonElementToDict(el),
            JsonValueKind.Array => el.EnumerateArray().Select(JsonElementToObject).ToList(),
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? l : el.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    private static string GetStringOrDefault(Dictionary<string, object?> dict, string key, string defaultValue = "")
    {
        return dict.TryGetValue(key, out var val) && val is string s ? s : defaultValue;
    }

    // ── Request router ──────────────────────────────────────────────────────

    private ServiceResponse HandleRequest(ServiceRequest request)
    {
        var path = request.Path;
        var method = request.Method;

        // Tags endpoint: /v1/tags/{resourceArn}
        if (path.StartsWith("/v1/tags/", StringComparison.Ordinal))
        {
            var arn = Uri.UnescapeDataString(path["/v1/tags/".Length..]);
            if (method == "POST")
            {
                var data = ParseBody(request.Body);
                return TagResource(arn, data);
            }

            if (method == "DELETE")
            {
                return UntagResource(arn, request.QueryParams);
            }

            // GET
            return ListTagsForResource(arn);
        }

        var m = PathRegex().Match(path);
        if (!m.Success)
        {
            return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Unknown path: {path}", 404);
        }

        var apiId = m.Groups[1].Value;
        var sub1 = m.Groups[2].Value;
        var sub2 = m.Groups[3].Value;
        var sub3 = m.Groups[4].Value;
        var sub4 = m.Groups[5].Value;

        // Use empty string check instead of null
        if (string.IsNullOrEmpty(apiId) && string.IsNullOrEmpty(sub1))
        {
            apiId = "";
            sub1 = "";
        }

        var body = ParseBody(request.Body);

        // POST /v1/apis — CreateGraphQLApi, GET /v1/apis — ListGraphQLApis
        if (string.IsNullOrEmpty(apiId))
        {
            if (method == "POST")
            {
                return CreateGraphqlApi(body);
            }

            if (method == "GET")
            {
                return ListGraphqlApis();
            }
        }

        // /v1/apis/{apiId}
        if (!string.IsNullOrEmpty(apiId) && string.IsNullOrEmpty(sub1))
        {
            if (method == "GET")
            {
                return GetGraphqlApi(apiId);
            }

            if (method == "POST")
            {
                return UpdateGraphqlApi(apiId, body);
            }

            if (method == "DELETE")
            {
                return DeleteGraphqlApi(apiId);
            }
        }

        // /v1/apis/{apiId}/apikeys
        if (sub1 == "apikeys")
        {
            if (string.IsNullOrEmpty(sub2))
            {
                if (method == "POST")
                {
                    return CreateApiKey(apiId, body);
                }

                if (method == "GET")
                {
                    return ListApiKeys(apiId);
                }
            }
            else
            {
                if (method == "DELETE")
                {
                    return DeleteApiKey(apiId, sub2);
                }
            }
        }

        // /v1/apis/{apiId}/datasources
        if (sub1 == "datasources")
        {
            if (string.IsNullOrEmpty(sub2))
            {
                if (method == "POST")
                {
                    return CreateDataSource(apiId, body);
                }

                if (method == "GET")
                {
                    return ListDataSources(apiId);
                }
            }
            else
            {
                if (method == "GET")
                {
                    return GetDataSource(apiId, sub2);
                }

                if (method == "DELETE")
                {
                    return DeleteDataSource(apiId, sub2);
                }
            }
        }

        // /v1/apis/{apiId}/types
        if (sub1 == "types")
        {
            if (string.IsNullOrEmpty(sub2))
            {
                if (method == "POST")
                {
                    return CreateType(apiId, body);
                }

                if (method == "GET")
                {
                    return ListTypes(apiId);
                }
            }
            else if (sub3 == "resolvers")
            {
                var typeName = sub2;
                if (string.IsNullOrEmpty(sub4))
                {
                    if (method == "POST")
                    {
                        return CreateResolver(apiId, typeName, body);
                    }

                    if (method == "GET")
                    {
                        return ListResolvers(apiId, typeName);
                    }
                }
                else
                {
                    if (method == "GET")
                    {
                        return GetResolver(apiId, typeName, sub4);
                    }

                    if (method == "DELETE")
                    {
                        return DeleteResolver(apiId, typeName, sub4);
                    }
                }
            }
            else
            {
                // /v1/apis/{apiId}/types/{typeName} — GetType
                if (string.IsNullOrEmpty(sub3) && method == "GET")
                {
                    return GetType(apiId, sub2);
                }
            }
        }

        return AwsResponseHelpers.ErrorResponseJson("BadRequestException", $"Unsupported route: {method} {path}", 400);
    }

    // ── GraphQL APIs ─────────────────────────────────────────────────────────

    private ServiceResponse CreateGraphqlApi(Dictionary<string, object?> body)
    {
        lock (_lock)
        {
            var apiId = HashHelpers.NewUuid()[..8];
            var name = GetStringOrDefault(body, "name");
            var authType = GetStringOrDefault(body, "authenticationType", "API_KEY");
            var xray = body.TryGetValue("xrayEnabled", out var xVal) && xVal is true;
            var tags = body.TryGetValue("tags", out var tagsObj) && tagsObj is Dictionary<string, object?> td
                ? td.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? "", StringComparer.Ordinal)
                : new Dictionary<string, string>(StringComparer.Ordinal);

            var arn = ApiArn(apiId);
            var now = NowEpoch();

            var record = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["apiId"] = apiId,
                ["name"] = name,
                ["authenticationType"] = authType,
                ["arn"] = arn,
                ["uris"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["GRAPHQL"] = $"https://{apiId}.appsync-api.{Region}.amazonaws.com/graphql",
                    ["REALTIME"] = $"wss://{apiId}.appsync-realtime-api.{Region}.amazonaws.com/graphql",
                },
                ["additionalAuthenticationProviders"] = body.TryGetValue("additionalAuthenticationProviders", out var aap) ? aap : new List<object?>(),
                ["xrayEnabled"] = xray,
                ["wafWebAclArn"] = body.TryGetValue("wafWebAclArn", out var waf) ? waf : null,
                ["createdAt"] = now,
                ["lastUpdatedAt"] = now,
            };

            if (body.TryGetValue("logConfig", out var lc))
            {
                record["logConfig"] = lc;
            }

            if (body.TryGetValue("userPoolConfig", out var upc))
            {
                record["userPoolConfig"] = upc;
            }

            if (body.TryGetValue("openIDConnectConfig", out var oidc))
            {
                record["openIDConnectConfig"] = oidc;
            }

            if (body.TryGetValue("lambdaAuthorizerConfig", out var lac))
            {
                record["lambdaAuthorizerConfig"] = lac;
            }

            _apis[apiId] = record;
            _apiKeys[apiId] = new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal);
            _dataSources[apiId] = new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal);
            _resolvers[apiId] = new Dictionary<string, Dictionary<string, Dictionary<string, object?>>>(StringComparer.Ordinal);
            _types[apiId] = new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal);

            if (tags.Count > 0)
            {
                _tags[arn] = tags;
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["graphqlApi"] = record });
        }
    }

    private ServiceResponse GetGraphqlApi(string apiId)
    {
        lock (_lock)
        {
            if (!_apis.TryGetValue(apiId, out var api))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["graphqlApi"] = api });
        }
    }

    private ServiceResponse ListGraphqlApis()
    {
        lock (_lock)
        {
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["graphqlApis"] = _apis.Values.ToList(),
            });
        }
    }

    private ServiceResponse UpdateGraphqlApi(string apiId, Dictionary<string, object?> body)
    {
        lock (_lock)
        {
            if (!_apis.TryGetValue(apiId, out var api))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            foreach (var key in new[] { "name", "authenticationType", "additionalAuthenticationProviders",
                                         "logConfig", "userPoolConfig", "openIDConnectConfig",
                                         "xrayEnabled", "lambdaAuthorizerConfig" })
            {
                if (body.ContainsKey(key))
                {
                    api[key] = body[key];
                }
            }

            api["lastUpdatedAt"] = NowEpoch();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["graphqlApi"] = api });
        }
    }

    private ServiceResponse DeleteGraphqlApi(string apiId)
    {
        lock (_lock)
        {
            if (!_apis.TryGetValue(apiId, out var api))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            var arn = api["arn"]?.ToString() ?? "";
            _apis.TryRemove(apiId, out _);
            _apiKeys.TryRemove(apiId, out _);
            _dataSources.TryRemove(apiId, out _);
            _resolvers.TryRemove(apiId, out _);
            _types.TryRemove(apiId, out _);
            _tags.TryRemove(arn, out _);

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // ── API Keys ─────────────────────────────────────────────────────────────

    private ServiceResponse CreateApiKey(string apiId, Dictionary<string, object?> body)
    {
        lock (_lock)
        {
            if (!_apis.ContainsKey(apiId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            var keyId = "da2-" + HashHelpers.NewUuid()[..26];
            var now = NowEpoch();
            var expires = body.TryGetValue("expires", out var exp) && exp is long el ? el : now + 604800;
            var description = GetStringOrDefault(body, "description");

            var record = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["id"] = keyId,
                ["description"] = description,
                ["expires"] = expires,
                ["createdAt"] = now,
                ["lastUpdatedAt"] = now,
                ["deletes"] = expires + 5184000,
            };

            if (!_apiKeys.TryGetValue(apiId, out var keys))
            {
                keys = new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal);
                _apiKeys[apiId] = keys;
            }

            keys[keyId] = record;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["apiKey"] = record });
        }
    }

    private ServiceResponse ListApiKeys(string apiId)
    {
        lock (_lock)
        {
            if (!_apis.ContainsKey(apiId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            var keys = _apiKeys.TryGetValue(apiId, out var k) ? k.Values.ToList() : [];
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["apiKeys"] = keys });
        }
    }

    private ServiceResponse DeleteApiKey(string apiId, string keyId)
    {
        lock (_lock)
        {
            if (!_apis.ContainsKey(apiId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            if (!_apiKeys.TryGetValue(apiId, out var keys) || !keys.ContainsKey(keyId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"API key {keyId} not found", 404);
            }

            keys.Remove(keyId);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // ── Data Sources ─────────────────────────────────────────────────────────

    private ServiceResponse CreateDataSource(string apiId, Dictionary<string, object?> body)
    {
        lock (_lock)
        {
            if (!_apis.TryGetValue(apiId, out var api))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            var name = GetStringOrDefault(body, "name");
            var dsType = GetStringOrDefault(body, "type", "NONE");
            var arn = api["arn"]?.ToString() + $"/datasources/{name}";
            var now = NowEpoch();

            var record = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["dataSourceArn"] = arn,
                ["name"] = name,
                ["type"] = dsType,
                ["description"] = GetStringOrDefault(body, "description"),
                ["serviceRoleArn"] = GetStringOrDefault(body, "serviceRoleArn"),
                ["createdAt"] = now,
                ["lastUpdatedAt"] = now,
            };

            switch (dsType)
            {
                case "AMAZON_DYNAMODB":
                    if (body.TryGetValue("dynamodbConfig", out var ddb))
                    {
                        record["dynamodbConfig"] = ddb;
                    }

                    break;
                case "AWS_LAMBDA":
                    if (body.TryGetValue("lambdaConfig", out var lc))
                    {
                        record["lambdaConfig"] = lc;
                    }

                    break;
                case "AMAZON_ELASTICSEARCH" or "AMAZON_OPENSEARCH_SERVICE":
                    if (body.TryGetValue("elasticsearchConfig", out var es))
                    {
                        record["elasticsearchConfig"] = es;
                    }

                    break;
                case "HTTP":
                    if (body.TryGetValue("httpConfig", out var hc))
                    {
                        record["httpConfig"] = hc;
                    }

                    break;
                case "RELATIONAL_DATABASE":
                    if (body.TryGetValue("relationalDatabaseConfig", out var rdb))
                    {
                        record["relationalDatabaseConfig"] = rdb;
                    }

                    break;
            }

            if (!_dataSources.TryGetValue(apiId, out var sources))
            {
                sources = new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal);
                _dataSources[apiId] = sources;
            }

            sources[name] = record;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["dataSource"] = record });
        }
    }

    private ServiceResponse GetDataSource(string apiId, string name)
    {
        lock (_lock)
        {
            if (!_apis.ContainsKey(apiId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            if (!_dataSources.TryGetValue(apiId, out var sources) || !sources.TryGetValue(name, out var ds))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Data source {name} not found", 404);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["dataSource"] = ds });
        }
    }

    private ServiceResponse ListDataSources(string apiId)
    {
        lock (_lock)
        {
            if (!_apis.ContainsKey(apiId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            var sources = _dataSources.TryGetValue(apiId, out var s) ? s.Values.ToList() : [];
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["dataSources"] = sources });
        }
    }

    private ServiceResponse DeleteDataSource(string apiId, string name)
    {
        lock (_lock)
        {
            if (!_apis.ContainsKey(apiId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            if (!_dataSources.TryGetValue(apiId, out var sources) || !sources.ContainsKey(name))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Data source {name} not found", 404);
            }

            sources.Remove(name);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // ── Resolvers ────────────────────────────────────────────────────────────

    private ServiceResponse CreateResolver(string apiId, string typeName, Dictionary<string, object?> body)
    {
        lock (_lock)
        {
            if (!_apis.TryGetValue(apiId, out var api))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            var fieldName = GetStringOrDefault(body, "fieldName");
            var arn = api["arn"]?.ToString() + $"/types/{typeName}/resolvers/{fieldName}";
            var now = NowEpoch();

            var record = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["typeName"] = typeName,
                ["fieldName"] = fieldName,
                ["dataSourceName"] = body.TryGetValue("dataSourceName", out var dsn) ? dsn : null,
                ["resolverArn"] = arn,
                ["requestMappingTemplate"] = GetStringOrDefault(body, "requestMappingTemplate"),
                ["responseMappingTemplate"] = GetStringOrDefault(body, "responseMappingTemplate"),
                ["kind"] = GetStringOrDefault(body, "kind", "UNIT"),
                ["createdAt"] = now,
                ["lastUpdatedAt"] = now,
            };

            foreach (var key in new[] { "pipelineConfig", "cachingConfig", "runtime", "code" })
            {
                if (body.TryGetValue(key, out var val))
                {
                    record[key] = val;
                }
            }

            if (!_resolvers.TryGetValue(apiId, out var apiResolvers))
            {
                apiResolvers = new Dictionary<string, Dictionary<string, Dictionary<string, object?>>>(StringComparer.Ordinal);
                _resolvers[apiId] = apiResolvers;
            }

            if (!apiResolvers.TryGetValue(typeName, out var typeResolvers))
            {
                typeResolvers = new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal);
                apiResolvers[typeName] = typeResolvers;
            }

            typeResolvers[fieldName] = record;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["resolver"] = record });
        }
    }

    private ServiceResponse GetResolver(string apiId, string typeName, string fieldName)
    {
        lock (_lock)
        {
            if (!_apis.ContainsKey(apiId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            if (_resolvers.TryGetValue(apiId, out var ar) && ar.TryGetValue(typeName, out var tr) && tr.TryGetValue(fieldName, out var resolver))
            {
                return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["resolver"] = resolver });
            }

            return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Resolver {typeName}.{fieldName} not found", 404);
        }
    }

    private ServiceResponse ListResolvers(string apiId, string typeName)
    {
        lock (_lock)
        {
            if (!_apis.ContainsKey(apiId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            var resolvers = _resolvers.TryGetValue(apiId, out var ar) && ar.TryGetValue(typeName, out var tr)
                ? tr.Values.ToList()
                : new List<Dictionary<string, object?>>();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["resolvers"] = resolvers });
        }
    }

    private ServiceResponse DeleteResolver(string apiId, string typeName, string fieldName)
    {
        lock (_lock)
        {
            if (!_apis.ContainsKey(apiId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            if (!_resolvers.TryGetValue(apiId, out var ar) || !ar.TryGetValue(typeName, out var tr) || !tr.ContainsKey(fieldName))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Resolver {typeName}.{fieldName} not found", 404);
            }

            tr.Remove(fieldName);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // ── Types ────────────────────────────────────────────────────────────────

    private ServiceResponse CreateType(string apiId, Dictionary<string, object?> body)
    {
        lock (_lock)
        {
            if (!_apis.TryGetValue(apiId, out var api))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            var definition = GetStringOrDefault(body, "definition");
            var fmt = GetStringOrDefault(body, "format", "SDL");

            var nameMatch = Regex.Match(definition, @"(?:type|input|enum|interface|union|scalar)\s+(\w+)");
            var typeName = nameMatch.Success ? nameMatch.Groups[1].Value : "Unknown";

            var arn = api["arn"]?.ToString() + $"/types/{typeName}";
            var now = NowEpoch();

            var record = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["name"] = typeName,
                ["description"] = GetStringOrDefault(body, "description"),
                ["arn"] = arn,
                ["definition"] = definition,
                ["format"] = fmt,
                ["createdAt"] = now,
                ["lastUpdatedAt"] = now,
            };

            if (!_types.TryGetValue(apiId, out var typeMap))
            {
                typeMap = new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal);
                _types[apiId] = typeMap;
            }

            typeMap[typeName] = record;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["type"] = record });
        }
    }

    private ServiceResponse GetType(string apiId, string typeName)
    {
        lock (_lock)
        {
            if (!_apis.ContainsKey(apiId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            if (!_types.TryGetValue(apiId, out var typeMap) || !typeMap.TryGetValue(typeName, out var t))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"Type {typeName} not found", 404);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["type"] = t });
        }
    }

    private ServiceResponse ListTypes(string apiId)
    {
        lock (_lock)
        {
            if (!_apis.ContainsKey(apiId))
            {
                return AwsResponseHelpers.ErrorResponseJson("NotFoundException", $"GraphQL API {apiId} not found", 404);
            }

            var types = _types.TryGetValue(apiId, out var tm) ? tm.Values.ToList() : [];
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["types"] = types });
        }
    }

    // ── Tags ─────────────────────────────────────────────────────────────────

    private ServiceResponse TagResource(string arn, Dictionary<string, object?> data)
    {
        lock (_lock)
        {
            var tags = data.TryGetValue("tags", out var tObj) && tObj is Dictionary<string, object?> td
                ? td.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? "", StringComparer.Ordinal)
                : new Dictionary<string, string>(StringComparer.Ordinal);

            if (!_tags.TryGetValue(arn, out var existing))
            {
                existing = new Dictionary<string, string>(StringComparer.Ordinal);
                _tags[arn] = existing;
            }

            foreach (var kv in tags)
            {
                existing[kv.Key] = kv.Value;
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse UntagResource(string arn, IReadOnlyDictionary<string, string[]> queryParams)
    {
        lock (_lock)
        {
            if (_tags.TryGetValue(arn, out var existing))
            {
                if (queryParams.TryGetValue("tagKeys", out var tagKeys))
                {
                    foreach (var key in tagKeys)
                    {
                        existing.Remove(key);
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ListTagsForResource(string arn)
    {
        lock (_lock)
        {
            var tags = _tags.TryGetValue(arn, out var t)
                ? (object)t
                : new Dictionary<string, string>(StringComparer.Ordinal);
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["tags"] = tags });
        }
    }
}
