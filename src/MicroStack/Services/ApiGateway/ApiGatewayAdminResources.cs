using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.ApiGateway;

internal sealed partial class ApiGatewayV2ServiceHandler
{
    private static readonly AdminResourceKind[] V2Kinds =
    [
        new("apis", "APIs"), new("routes", "Routes"), new("integrations", "Integrations"),
        new("stages", "Stages"), new("deployments", "Deployments"), new("authorizers", "Authorizers"),
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId switch
        {
            "apigateway" => _v1Handler.GetAdminResourceKinds(),
            "apigatewayv2" => V2Kinds,
            _ => [],
        };

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        lock (_lock)
        {
            return serviceId switch
            {
                "apigateway" => _v1Handler.GetAdminResources(),
                "apigatewayv2" => _apis.Items.ToArray().Select(CreateV2ApiNode).ToArray(),
                _ => [],
            };
        }
    }

    private AdminNode CreateV2ApiNode(KeyValuePair<string, Dictionary<string, object?>> pair)
    {
        var (id, api) = pair;
        return AdminData.Node("apis", id, NetworkingAdminData.Get(api, "name", id),
                ApiArn(id), NetworkingAdminData.Get(api, "protocolType"))
            with
            {
                ReadFields = () => NetworkingAdminData.Fields(api),
                ReadContent = () =>
                {
                    var value = new Dictionary<string, object?>(api);
                    if (_apiTags.TryGetValue(ApiArn(id), out var tags))
                        value["tags"] = tags;
                    return NetworkingAdminData.Json(value);
                },
                ReadChildren = () =>
                [
                    .. ChildNodes(_routes, id, "routes", "routeId", "routeKey"),
                    .. ChildNodes(_integrations, id, "integrations", "integrationId", "integrationType"),
                    .. ChildNodes(_stages, id, "stages", "stageName", "stageName"),
                    .. ChildNodes(_deployments, id, "deployments", "deploymentId", "deploymentId"),
                    .. ChildNodes(_authorizers, id, "authorizers", "authorizerId", "name"),
                ],
            };
    }

    private static IEnumerable<AdminNode> ChildNodes(
        AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> source,
        string apiId, string kind, string idName, string nameName)
    {
        if (!source.TryGetValue(apiId, out var values))
            return [];
        return values.ToArray().Select(pair =>
        {
            var item = pair.Value;
            var id = NetworkingAdminData.Get(item, idName, pair.Key);
            var node = AdminData.Node(kind, id, NetworkingAdminData.Get(item, nameName, id));
            var connections = V2Connections(kind, item);
            return node with
            {
                ReadFields = () => NetworkingAdminData.Fields(item, "authorizerCredentialsArn"),
                ReadContent = () => NetworkingAdminData.Json(item, "authorizerCredentialsArn"),
                ReadConnections = connections.Count == 0 ? null : () => connections,
            };
        }).ToArray();
    }

    private static IReadOnlyList<AdminConnection> V2Connections(
        string kind, Dictionary<string, object?> item)
    {
        var connections = new List<AdminConnection>();
        if (kind == "routes")
        {
            var target = NetworkingAdminData.Get(item, "target");
            if (target.StartsWith("integrations/", StringComparison.Ordinal))
                connections.Add(new("Integration", "targets", "apigatewayv2",
                    [new("integrations", target["integrations/".Length..])]));
            var authorizerId = NetworkingAdminData.Get(item, "authorizerId");
            if (!string.IsNullOrEmpty(authorizerId))
                connections.Add(new("Authorizer", "authorized-by", "apigatewayv2",
                    [new("authorizers", authorizerId)]));
        }
        else if (kind == "stages")
        {
            var deploymentId = NetworkingAdminData.Get(item, "deploymentId");
            if (!string.IsNullOrEmpty(deploymentId))
                connections.Add(new("Deployment", "deploys", "apigatewayv2",
                    [new("deployments", deploymentId)]));
        }
        return connections;
    }
}

internal sealed partial class ApiGatewayV1ServiceHandler
{
    private static readonly AdminResourceKind[] V1Kinds =
    [
        new("rest-apis", "REST APIs"), new("resources", "Resources"), new("methods", "Methods"),
        new("method-responses", "Method responses"), new("integrations", "Integrations"),
        new("integration-responses", "Integration responses"),
        new("stages", "Stages"), new("deployments", "Deployments"),
        new("authorizers", "Authorizers"), new("models", "Models"), new("api-keys", "API keys"),
        new("usage-plans", "Usage plans"), new("usage-plan-keys", "Usage plan keys"),
        new("domain-names", "Domain names"), new("base-path-mappings", "Base path mappings"),
    ];

    internal IReadOnlyList<AdminResourceKind> GetAdminResourceKinds() => V1Kinds;

    internal IEnumerable<AdminNode> GetAdminResources() =>
    [
        .. _restApis.Items.ToArray().Select(CreateRestApiNode),
        .. _apiKeys.Items.ToArray().Select(pair => DictionaryNode("api-keys", pair.Key,
            NetworkingAdminData.Get(pair.Value, "name", pair.Key), pair.Value, sensitive: true)),
        .. _usagePlans.Items.ToArray().Select(CreateUsagePlanNode),
        .. _domainNames.Items.ToArray().Select(CreateDomainNode),
    ];

    private AdminNode CreateRestApiNode(KeyValuePair<string, Dictionary<string, object?>> pair)
    {
        var (id, api) = pair;
        var node = DictionaryNode("rest-apis", id, NetworkingAdminData.Get(api, "name", id), api,
            RestApiArn(id));
        return node with
        {
            ReadContent = () =>
            {
                var value = new Dictionary<string, object?>(api);
                if (_v1Tags.TryGetValue(RestApiArn(id), out var tags))
                    value["tags"] = tags;
                return NetworkingAdminData.Json(value);
            },
            ReadChildren = () =>
            [
                .. ResourceNodes(id),
                .. V1Children(_stages, id, "stages", "stageName", "stageName"),
                .. V1Children(_deployments, id, "deployments", "id", "id"),
                .. V1Children(_authorizers, id, "authorizers", "id", "name"),
                .. V1Children(_models, id, "models", "id", "name"),
            ],
        };
    }

    private IEnumerable<AdminNode> ResourceNodes(string apiId)
    {
        if (!_resources.TryGetValue(apiId, out var resources))
            return [];
        return resources.ToArray().Select(pair =>
        {
            var resource = pair.Value;
            return DictionaryNode("resources", pair.Key,
                NetworkingAdminData.Get(resource, "path", pair.Key), resource) with
            {
                ReadChildren = () => MethodNodes(resource),
            };
        }).ToArray();
    }

    private static IEnumerable<AdminNode> MethodNodes(Dictionary<string, object?> resource)
    {
        if (!resource.TryGetValue("resourceMethods", out var methodsValue)
            || methodsValue is not Dictionary<string, object?> methods)
            return [];
        return methods.ToArray().Where(x => x.Value is Dictionary<string, object?>)
            .Select(pair =>
            {
                var method = (Dictionary<string, object?>)pair.Value!;
                var children = new List<AdminNode>();
                if (method.TryGetValue("methodResponses", out var responsesValue)
                    && responsesValue is Dictionary<string, object?> responses)
                {
                    children.AddRange(responses.ToArray()
                        .Where(x => x.Value is Dictionary<string, object?>)
                        .Select(x => DictionaryNode("method-responses", x.Key, x.Key,
                            (Dictionary<string, object?>)x.Value!)));
                }
                if (method.TryGetValue("methodIntegration", out var integrationValue)
                    && integrationValue is Dictionary<string, object?> integration)
                {
                    children.Add(DictionaryNode("integrations", pair.Key, pair.Key, integration)
                        with
                        {
                            ReadChildren = () =>
                            {
                                if (!integration.TryGetValue("integrationResponses", out var value)
                                    || value is not Dictionary<string, object?> integrationResponses)
                                    return [];
                                return integrationResponses.ToArray()
                                    .Where(x => x.Value is Dictionary<string, object?>)
                                    .Select(x => DictionaryNode("integration-responses", x.Key, x.Key,
                                        (Dictionary<string, object?>)x.Value!))
                                    .ToArray();
                            },
                        });
                }
                var authorizerId = NetworkingAdminData.Get(method, "authorizerId");
                return DictionaryNode("methods", pair.Key, pair.Key, method) with
                {
                    ReadChildren = children.Count == 0 ? null : () => children,
                    ReadConnections = string.IsNullOrEmpty(authorizerId) ? null : () =>
                    [
                        new("Authorizer", "authorized-by", "apigateway",
                            [new("authorizers", authorizerId)]),
                    ],
                };
            }).ToArray();
    }

    private AdminNode CreateUsagePlanNode(KeyValuePair<string, Dictionary<string, object?>> pair) =>
        DictionaryNode("usage-plans", pair.Key, NetworkingAdminData.Get(pair.Value, "name", pair.Key), pair.Value)
        with
        {
            ReadChildren = () => V1Children(_usagePlanKeys, pair.Key, "usage-plan-keys", "id", "name", true),
        };

    private AdminNode CreateDomainNode(KeyValuePair<string, Dictionary<string, object?>> pair) =>
        DictionaryNode("domain-names", pair.Key,
            NetworkingAdminData.Get(pair.Value, "domainName", pair.Key), pair.Value) with
        {
            ReadChildren = () => V1Children(_basePathMappings, pair.Key,
                "base-path-mappings", "basePath", "basePath"),
        };

    private static IEnumerable<AdminNode> V1Children(
        AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> source,
        string parentId, string kind, string idName, string nameName, bool sensitive = false)
    {
        if (!source.TryGetValue(parentId, out var values))
            return [];
        return values.ToArray().Select(pair =>
        {
            var node = DictionaryNode(kind,
                NetworkingAdminData.Get(pair.Value, idName, pair.Key),
                NetworkingAdminData.Get(pair.Value, nameName, pair.Key), pair.Value, sensitive: sensitive);
            var deploymentId = kind == "stages"
                ? NetworkingAdminData.Get(pair.Value, "deploymentId")
                : "";
            return node with
            {
                ReadConnections = string.IsNullOrEmpty(deploymentId) ? node.ReadConnections : () =>
                [
                    new("Deployment", "deploys", "apigateway",
                        [new("deployments", deploymentId)]),
                ],
            };
        }).ToArray();
    }

    private static AdminNode DictionaryNode(
        string kind, string id, string name, Dictionary<string, object?> value,
        string? arn = null, bool sensitive = false)
    {
        var sensitiveNames = sensitive
            ? new[] { "value", "id", "credentials", "authorizerCredentialsArn" }
            : new[] { "credentials", "authorizerCredentialsArn" };
        return AdminData.Node(kind, id, name, arn) with
        {
            ReadFields = () => NetworkingAdminData.Fields(value, sensitiveNames),
            ReadContent = sensitive ? null : () => NetworkingAdminData.Json(value, sensitiveNames),
        };
    }
}
