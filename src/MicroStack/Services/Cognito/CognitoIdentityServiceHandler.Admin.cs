using System.Globalization;
using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Cognito;

internal sealed partial class CognitoIdentityServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("identity-pool", "Identity pools"),
        new("identity", "Identities") { IsRoot = false }
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) => AdminKinds;

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        lock (_lock)
        {
            return _identityPools.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => IdentityPoolNode(x.Key, x.Value)).ToArray();
        }
    }

    private AdminNode IdentityPoolNode(string poolId, Dictionary<string, object?> snapshot) =>
        AdminData.Node("identity-pool", poolId,
            Text(snapshot, "IdentityPoolName") ?? poolId, status: "Active") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_identityPools.TryGetValue(poolId, out var pool)) return [];
                    return
                    [
                        AdminData.Field("Allow unauthenticated identities", Scalar(pool, "AllowUnauthenticatedIdentities")),
                        AdminData.Field("Allow classic flow", Scalar(pool, "AllowClassicFlow")),
                        AdminData.Field("Developer provider name", Text(pool, "DeveloperProviderName")),
                        AdminData.Field("Supported login providers", Map(pool, "SupportedLoginProviders")),
                        AdminData.Field("OpenID Connect provider ARNs", Strings(pool, "OpenIdConnectProviderARNs")),
                        AdminData.Field("SAML provider ARNs", Strings(pool, "SamlProviderARNs")),
                        AdminData.Field("Tags", Map(pool, "IdentityPoolTags")),
                        AdminData.Field("Roles", Map(pool, "_roles"))
                    ];
                }
            },
            ChildKinds = [AdminKinds[1]],
            ReadChildren = () =>
            {
                lock (_lock)
                {
                    if (!_identityPools.TryGetValue(poolId, out var pool) ||
                        pool.GetValueOrDefault("_identities") is not
                            Dictionary<string, Dictionary<string, object?>> identities)
                        return [];
                    return identities.OrderBy(x => x.Key, StringComparer.Ordinal)
                        .Select(x => IdentityNode(poolId, x.Key, x.Value)).ToArray();
                }
            },
            ReadConnections = () =>
            {
                lock (_lock)
                {
                    if (!_identityPools.TryGetValue(poolId, out var pool)) return [];
                    var ids = new HashSet<string>(StringComparer.Ordinal);
                    if (pool.GetValueOrDefault("CognitoIdentityProviders") is List<object?> providers)
                    {
                        foreach (var provider in providers.OfType<Dictionary<string, object?>>())
                            if (provider.GetValueOrDefault("ProviderName") is string name)
                                foreach (var id in _idp.UserPools.Keys.Where(name.Contains))
                                    ids.Add(id);
                    }
                    var links = ids.Where(_idp.UserPools.ContainsKey).OrderBy(x => x, StringComparer.Ordinal)
                        .Select(id => new AdminConnection(id, "identity-provider", "cognitoidp",
                            [new AdminKey("user-pool", id)])).ToList();
                    if (pool.GetValueOrDefault("_roles") is Dictionary<string, string> roles)
                    {
                        links.AddRange(roles.OrderBy(x => x.Key, StringComparer.Ordinal).Select(role =>
                        {
                            var roleName = role.Value[(role.Value.LastIndexOf('/') + 1)..];
                            return new AdminConnection($"{role.Key} role", "role", "iam",
                                [new AdminKey("role", roleName)]);
                        }));
                    }
                    return links;
                }
            }
        };

    private AdminNode IdentityNode(string poolId, string identityId, Dictionary<string, object?> snapshot) =>
        AdminData.Node("identity", identityId, identityId, status: "Active") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!TryIdentity(poolId, identityId, out var identity)) return [];
                    return
                    [
                        AdminData.Field("Created", Epoch(identity, "CreationDate"), format: "datetime"),
                        AdminData.Field("Last modified", Epoch(identity, "LastModifiedDate"), format: "datetime"),
                        AdminData.Field("Logins", Map(identity, "Logins"))
                    ];
                }
            }
        };

    private bool TryIdentity(string poolId, string identityId,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Dictionary<string, object?>? identity)
    {
        identity = null;
        return _identityPools.TryGetValue(poolId, out var pool) &&
               pool.GetValueOrDefault("_identities") is
                   Dictionary<string, Dictionary<string, object?>> identities &&
               identities.TryGetValue(identityId, out identity);
    }

    private static string? Text(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) ? item as string : null;

    private static string? Scalar(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) && item is not null
            ? Convert.ToString(item, CultureInfo.InvariantCulture)
            : null;

    private static string? Epoch(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) && item is double seconds
            ? AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds((long)(seconds * 1000)))
            : null;

    private static string Strings(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) && item is List<string> values
            ? string.Join(", ", values)
            : "";

    private static string Map(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) && item is Dictionary<string, string> map
            ? string.Join(", ", map.Select(x => $"{x.Key}={x.Value}"))
            : "";
}
