using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;
using MicroStack.Services.ApiGateway;

namespace MicroStack.Services.AppSync;

internal sealed partial class AppSyncServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("graphql-apis", "GraphQL APIs"), new("api-keys", "API keys"),
        new("data-sources", "Data sources"), new("types", "Schema types"),
        new("resolvers", "Resolvers"),
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == ServiceName ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != ServiceName)
            return [];
        lock (_lock)
            return _apis.Items.ToArray().Select(CreateApiNode).ToArray();
    }

    private AdminNode CreateApiNode(KeyValuePair<string, Dictionary<string, object?>> pair)
    {
        var (apiId, api) = pair;
        return DictionaryNode("graphql-apis", apiId,
            NetworkingAdminData.Get(api, "name", apiId), api,
            NetworkingAdminData.Get(api, "arn")) with
        {
            ReadChildren = () =>
            {
                lock (_lock)
                {
                    return
                    [
                        .. ApiKeyNodes(apiId),
                        .. DictionaryChildren(_dataSources, apiId, "data-sources", "name", "name"),
                        .. TypeNodes(apiId),
                        .. ResolverNodes(apiId),
                    ];
                }
            },
        };
    }

    private IEnumerable<AdminNode> ApiKeyNodes(string apiId)
    {
        if (!_apiKeys.TryGetValue(apiId, out var values))
            return [];
        return values.ToArray().Select(pair =>
            AdminData.Node("api-keys", NetworkingAdminData.SecretId(pair.Key), "API key") with
            {
                ReadFields = () => NetworkingAdminData.Fields(pair.Value, "id"),
            }).ToArray();
    }

    private IEnumerable<AdminNode> TypeNodes(string apiId)
    {
        if (!_types.TryGetValue(apiId, out var values))
            return [];
        return values.ToArray().Select(pair =>
        {
            var type = pair.Value;
            return DictionaryNode("types", pair.Key,
                NetworkingAdminData.Get(type, "name", pair.Key), type,
                NetworkingAdminData.Get(type, "arn")) with
            {
                ReadContent = () => AdminData.Text(
                    NetworkingAdminData.Get(type, "definition"), "application/graphql"),
            };
        }).ToArray();
    }

    private IEnumerable<AdminNode> ResolverNodes(string apiId)
    {
        if (!_resolvers.TryGetValue(apiId, out var typeMap))
            return [];
        return typeMap.ToArray().SelectMany(typePair =>
            typePair.Value.ToArray().Select(pair =>
            {
                var resolver = pair.Value;
                var id = NetworkingAdminData.Composite(typePair.Key, pair.Key);
                var dataSource = NetworkingAdminData.Get(resolver, "dataSourceName");
                return DictionaryNode("resolvers", id, $"{typePair.Key}.{pair.Key}", resolver,
                    NetworkingAdminData.Get(resolver, "resolverArn")) with
                {
                    ReadConnections = string.IsNullOrEmpty(dataSource) ? null : () =>
                    [
                        new("Data source", "uses", ServiceName,
                            [new("data-sources", dataSource)]),
                    ],
                };
            })).ToArray();
    }

    private static IEnumerable<AdminNode> DictionaryChildren(
        AccountScopedDictionary<string, Dictionary<string, Dictionary<string, object?>>> source,
        string apiId, string kind, string idName, string nameName)
    {
        if (!source.TryGetValue(apiId, out var values))
            return [];
        return values.ToArray().Select(pair => DictionaryNode(kind,
            NetworkingAdminData.Get(pair.Value, idName, pair.Key),
            NetworkingAdminData.Get(pair.Value, nameName, pair.Key), pair.Value,
            NetworkingAdminData.Get(pair.Value, "dataSourceArn"))).ToArray();
    }

    private static AdminNode DictionaryNode(
        string kind, string id, string name, Dictionary<string, object?> value, string? arn = null) =>
        AdminData.Node(kind, id, name, string.IsNullOrEmpty(arn) ? null : arn) with
        {
            ReadFields = () => NetworkingAdminData.Fields(value,
                "apiKey", "clientSecret", "secret", "token", "password"),
            ReadContent = () => NetworkingAdminData.Json(value,
                "apiKey", "clientSecret", "secret", "token", "password"),
        };
}
