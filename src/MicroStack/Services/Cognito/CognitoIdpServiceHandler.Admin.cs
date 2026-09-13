using System.Globalization;
using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Cognito;

internal sealed partial class CognitoIdpServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("user-pool", "User pools"),
        new("user-pool-client", "User pool clients"),
        new("user", "Users"),
        new("group", "Groups")
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) => AdminKinds;

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        lock (_lock)
        {
            return UserPools.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => UserPoolNode(x.Key, x.Value)).ToArray();
        }
    }

    private AdminNode UserPoolNode(string poolId, Dictionary<string, object?> snapshot) =>
        AdminData.Node("user-pool", poolId, String(snapshot, "Name") ?? poolId,
            String(snapshot, "Arn"), "Active") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!UserPools.TryGetValue(poolId, out var pool)) return [];
                    return
                    [
                        AdminData.Field("Created", Epoch(pool, "CreationDate"), format: "datetime"),
                        AdminData.Field("Last modified", Epoch(pool, "LastModifiedDate"), format: "datetime"),
                        AdminData.Field("MFA configuration", String(pool, "MfaConfiguration")),
                        AdminData.Field("Estimated users", Scalar(pool, "EstimatedNumberOfUsers")),
                        AdminData.Field("Domain", String(pool, "Domain")),
                        AdminData.Field("Auto-verified attributes", Strings(pool, "AutoVerifiedAttributes")),
                        AdminData.Field("Alias attributes", Strings(pool, "AliasAttributes")),
                        AdminData.Field("Username attributes", Strings(pool, "UsernameAttributes")),
                        AdminData.Field("Tags", StringMap(pool, "UserPoolTags"))
                    ];
                }
            },
            ReadChildren = () =>
            {
                lock (_lock)
                {
                    if (!UserPools.TryGetValue(poolId, out var pool)) return [];
                    var children = GetClients(pool).OrderBy(x => x.Key, StringComparer.Ordinal)
                        .Select(x => ClientNode(poolId, x.Key, x.Value)).ToList();
                    children.AddRange(GetUsers(pool).OrderBy(x => x.Key, StringComparer.Ordinal)
                        .Select(x => UserNode(poolId, x.Key, x.Value)));
                    children.AddRange(GetGroups(pool).OrderBy(x => x.Key, StringComparer.Ordinal)
                        .Select(x => GroupNode(poolId, x.Key, x.Value)));
                    return children;
                }
            }
        };

    private AdminNode ClientNode(string poolId, string clientId, Dictionary<string, object?> snapshot)
    {
        var hasSecret = snapshot.TryGetValue("ClientSecret", out var secret) && secret is string;
        return AdminData.Node("user-pool-client", clientId,
            String(snapshot, "ClientName") ?? clientId, status: "Active") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!TryChild(poolId, "_clients", clientId, out var client)) return [];
                    var secretExists = client.TryGetValue("ClientSecret", out var value) && value is string;
                    return
                    [
                        AdminData.Field("Client ID", clientId),
                        AdminData.Field("Created", Epoch(client, "CreationDate"), format: "datetime"),
                        AdminData.Field("Last modified", Epoch(client, "LastModifiedDate"), format: "datetime"),
                        AdminData.Field("Refresh token validity", Scalar(client, "RefreshTokenValidity")),
                        AdminData.Field("Access token validity", Scalar(client, "AccessTokenValidity")),
                        AdminData.Field("ID token validity", Scalar(client, "IdTokenValidity")),
                        AdminData.Field("Client secret", value as string, sensitive: true, canReveal: secretExists),
                        AdminData.Field("Explicit auth flows", Strings(client, "ExplicitAuthFlows")),
                        AdminData.Field("OAuth flows", Strings(client, "AllowedOAuthFlows")),
                        AdminData.Field("OAuth scopes", Strings(client, "AllowedOAuthScopes"))
                    ];
                }
            },
            RevealableFields = hasSecret ? ["ClientSecret"] : [],
            RevealField = field =>
            {
                lock (_lock)
                {
                    return field == "ClientSecret" &&
                           TryChild(poolId, "_clients", clientId, out var client) &&
                           client.TryGetValue("ClientSecret", out var value) &&
                           value is string text
                        ? AdminData.Text(text, sensitive: true)
                        : AdminData.Unavailable("The requested value is not retained or revealable.", sensitive: true);
                }
            }
        };
    }

    private AdminNode UserNode(string poolId, string username, Dictionary<string, object?> snapshot) =>
        AdminData.Node("user", username, username, status: String(snapshot, "UserStatus")) with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!TryChild(poolId, "_users", username, out var user)) return [];
                    return
                    [
                        AdminData.Field("Enabled", Scalar(user, "Enabled")),
                        AdminData.Field("Created", Epoch(user, "UserCreateDate"), format: "datetime"),
                        AdminData.Field("Last modified", Epoch(user, "UserLastModifiedDate"), format: "datetime"),
                        AdminData.Field("Groups", Strings(user, "_groups")),
                        AdminData.Field("Password", null, sensitive: true)
                    ];
                }
            },
            ReadConnections = () =>
            {
                lock (_lock)
                {
                    if (!TryChild(poolId, "_users", username, out var user)) return [];
                    return List(user, "_groups").Where(group =>
                            TryChild(poolId, "_groups", group, out _))
                        .Select(group => new AdminConnection(group, "member-of", "cognitoidp",
                            [new AdminKey("user-pool", poolId), new AdminKey("group", group)]))
                        .ToArray();
                }
            }
        };

    private AdminNode GroupNode(string poolId, string name, Dictionary<string, object?> snapshot) =>
        AdminData.Node("group", name, name, status: "Active") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!TryChild(poolId, "_groups", name, out var group)) return [];
                    return
                    [
                        AdminData.Field("Description", String(group, "Description")),
                        AdminData.Field("Role ARN", String(group, "RoleArn")),
                        AdminData.Field("Precedence", Scalar(group, "Precedence")),
                        AdminData.Field("Created", Epoch(group, "CreationDate"), format: "datetime"),
                        AdminData.Field("Last modified", Epoch(group, "LastModifiedDate"), format: "datetime"),
                        AdminData.Field("Members", Strings(group, "_members"))
                    ];
                }
            },
            ReadConnections = () =>
            {
                lock (_lock)
                {
                    if (!TryChild(poolId, "_groups", name, out var group)) return [];
                    var links = List(group, "_members").Where(user =>
                            TryChild(poolId, "_users", user, out _))
                        .Select(user => new AdminConnection(user, "member", "cognitoidp",
                            [new AdminKey("user-pool", poolId), new AdminKey("user", user)]))
                        .ToList();
                    if (String(group, "RoleArn") is { Length: > 0 } arn)
                    {
                        var roleName = arn[(arn.LastIndexOf('/') + 1)..];
                        links.Add(new AdminConnection(roleName, "role", "iam",
                            [new AdminKey("role", roleName)]));
                    }
                    return links;
                }
            }
        };

    private bool TryChild(string poolId, string collection, string key,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Dictionary<string, object?>? value)
    {
        value = null;
        return UserPools.TryGetValue(poolId, out var pool) &&
               pool.TryGetValue(collection, out var raw) &&
               raw is Dictionary<string, Dictionary<string, object?>> children &&
               children.TryGetValue(key, out value);
    }

    private static string? String(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) ? item as string : null;

    private static string? Scalar(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) && item is not null
            ? Convert.ToString(item, CultureInfo.InvariantCulture)
            : null;

    private static string? Epoch(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) && item is double seconds
            ? AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds((long)(seconds * 1000)))
            : null;

    private static List<string> List(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) && item is List<string> list ? list : [];

    private static string Strings(IReadOnlyDictionary<string, object?> value, string key) =>
        string.Join(", ", List(value, key));

    private static string StringMap(IReadOnlyDictionary<string, object?> value, string key) =>
        value.TryGetValue(key, out var item) && item is Dictionary<string, string> map
            ? string.Join(", ", map.Select(x => $"{x.Key}={x.Value}"))
            : "";
}
