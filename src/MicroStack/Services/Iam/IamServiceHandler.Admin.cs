using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Iam;

internal sealed partial class IamServiceHandler
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("user", "Users"),
        new("access-key", "Access keys"),
        new("user-inline-policy", "User inline policies"),
        new("role", "Roles"),
        new("role-inline-policy", "Role inline policies"),
        new("policy", "Policies"),
        new("policy-version", "Policy versions"),
        new("group", "Groups"),
        new("instance-profile", "Instance profiles"),
        new("oidc-provider", "OpenID Connect providers")
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) => AdminKinds;

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        lock (_lock)
        {
            var nodes = new List<AdminNode>();
            nodes.AddRange(_users.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => UserNode(x.Key, x.Value)));
            nodes.AddRange(_roles.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => RoleNode(x.Key, x.Value)));
            nodes.AddRange(_policies.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => PolicyNode(x.Key, x.Value)));
            nodes.AddRange(_groups.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => GroupNode(x.Key, x.Value)));
            nodes.AddRange(_instanceProfiles.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => InstanceProfileNode(x.Key, x.Value)));
            nodes.AddRange(_oidcProviders.Items.OrderBy(x => x.Key, StringComparer.Ordinal)
                .Select(x => OidcNode(x.Key, x.Value)));
            return nodes;
        }
    }

    private AdminNode UserNode(string name, IamUser snapshot) =>
        AdminData.Node("user", name, name, snapshot.Arn, "Active", "global") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_users.TryGetValue(name, out var user)) return [];
                    return
                    [
                        AdminData.Field("User ID", user.UserId),
                        AdminData.Field("Path", user.Path),
                        AdminData.Field("Created", user.CreateDate, format: "datetime"),
                        AdminData.Field("Attached policies", string.Join(", ", user.AttachedPolicies)),
                        AdminData.Field("Tags", Tags(user.Tags))
                    ];
                }
            },
            ReadChildren = () =>
            {
                lock (_lock)
                {
                    if (!_users.ContainsKey(name)) return [];
                    var children = _accessKeys.Items.Where(x => x.Value.UserName == name)
                        .OrderBy(x => x.Key, StringComparer.Ordinal)
                        .Select(x => AccessKeyNode(x.Key, x.Value)).ToList();
                    children.AddRange(_userInlinePolicies.Items
                        .Where(x => x.Key.StartsWith(name + "\0", StringComparison.Ordinal))
                        .OrderBy(x => x.Key, StringComparer.Ordinal)
                        .Select(x => InlinePolicyNode("user-inline-policy", name,
                            x.Key[(name.Length + 1)..])));
                    return children;
                }
            },
            ReadConnections = () =>
            {
                lock (_lock)
                {
                    if (!_users.TryGetValue(name, out var user)) return [];
                    var links = new List<AdminConnection>();
                    links.AddRange(user.AttachedPolicies.Where(_policies.ContainsKey)
                        .Select(arn => new AdminConnection(arn, "attached-policy", "iam",
                            [new AdminKey("policy", arn)])));
                    links.AddRange(_groups.Items.Where(x => x.Value.Users.Contains(name))
                        .Select(x => new AdminConnection(x.Key, "member-of", "iam",
                            [new AdminKey("group", x.Key)])));
                    return links;
                }
            }
        };

    private AdminNode AccessKeyNode(string keyId, IamAccessKey snapshot) =>
        AdminData.Node("access-key", keyId, keyId, status: snapshot.Status, scope: "global") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_accessKeys.TryGetValue(keyId, out var key)) return [];
                    return
                    [
                        AdminData.Field("User name", key.UserName),
                        AdminData.Field("Created", key.CreateDate, format: "datetime"),
                        AdminData.Field("Secret access key", null, sensitive: true)
                    ];
                }
            }
        };

    private AdminNode RoleNode(string name, IamRole snapshot) =>
        AdminData.Node("role", name, name, snapshot.Arn, "Active", "global") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_roles.TryGetValue(name, out var role)) return [];
                    return
                    [
                        AdminData.Field("Role ID", role.RoleId),
                        AdminData.Field("Path", role.Path),
                        AdminData.Field("Description", role.Description),
                        AdminData.Field("Created", role.CreateDate, format: "datetime"),
                        AdminData.Field("Maximum session duration", role.MaxSessionDuration.ToString()),
                        AdminData.Field("Tags", Tags(role.Tags))
                    ];
                }
            },
            ReadChildren = () =>
            {
                lock (_lock)
                {
                    if (!_roles.TryGetValue(name, out var role)) return [];
                    return role.InlinePolicies.OrderBy(x => x.Key, StringComparer.Ordinal)
                        .Select(x => InlinePolicyNode("role-inline-policy", name, x.Key)).ToArray();
                }
            },
            ReadContent = () =>
            {
                lock (_lock)
                {
                    return _roles.TryGetValue(name, out var role) &&
                           !string.IsNullOrEmpty(role.AssumeRolePolicyDocument)
                        ? PolicyContent(role.AssumeRolePolicyDocument)
                        : AdminData.Unavailable("No assume-role policy is retained.", "application/json");
                }
            },
            ReadConnections = () =>
            {
                lock (_lock)
                {
                    if (!_roles.TryGetValue(name, out var role)) return [];
                    var links = new List<AdminConnection>();
                    links.AddRange(role.AttachedPolicies.Where(_policies.ContainsKey)
                        .Select(arn => new AdminConnection(arn, "attached-policy", "iam",
                            [new AdminKey("policy", arn)])));
                    links.AddRange(_instanceProfiles.Items.Where(x => x.Value.Roles.Contains(name))
                        .Select(x => new AdminConnection(x.Key, "instance-profile", "iam",
                            [new AdminKey("instance-profile", x.Key)])));
                    return links;
                }
            }
        };

    private AdminNode InlinePolicyNode(string kind, string owner, string name) =>
        AdminData.Node(kind, name, name, status: "Inline", scope: "global") with
        {
            ReadFields = () => [AdminData.Field("Owner", owner)],
            ReadContent = () =>
            {
                lock (_lock)
                {
                    string? document = null;
                    if (kind == "role-inline-policy" &&
                        _roles.TryGetValue(owner, out var role))
                        role.InlinePolicies.TryGetValue(name, out document);
                    else if (kind == "user-inline-policy")
                        _userInlinePolicies.TryGetValue($"{owner}\0{name}", out document);
                    return document is not null
                        ? PolicyContent(document)
                        : AdminData.Unavailable("The inline policy no longer exists.", "application/json");
                }
            }
        };

    private AdminNode PolicyNode(string arn, IamPolicy snapshot) =>
        AdminData.Node("policy", arn, snapshot.PolicyName, arn,
            snapshot.IsAttachable ? "Attachable" : "Not attachable", "global") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_policies.TryGetValue(arn, out var policy)) return [];
                    return
                    [
                        AdminData.Field("Policy ID", policy.PolicyId),
                        AdminData.Field("Path", policy.Path),
                        AdminData.Field("Default version", policy.DefaultVersionId),
                        AdminData.Field("Attachment count", policy.AttachmentCount.ToString()),
                        AdminData.Field("Created", policy.CreateDate, format: "datetime"),
                        AdminData.Field("Updated", policy.UpdateDate, format: "datetime"),
                        AdminData.Field("Tags", Tags(policy.Tags))
                    ];
                }
            },
            ReadChildren = () =>
            {
                lock (_lock)
                {
                    if (!_policies.TryGetValue(arn, out var policy)) return [];
                    return policy.Versions.OrderBy(x => x.Key, StringComparer.Ordinal)
                        .Select(x => PolicyVersionNode(arn, x.Key, x.Value)).ToArray();
                }
            },
            ReadConnections = () => PolicyConnections(arn)
        };

    private AdminNode PolicyVersionNode(string policyArn, string versionId, IamPolicyVersion snapshot) =>
        AdminData.Node("policy-version", versionId, versionId,
            status: snapshot.IsDefaultVersion ? "Default" : "Active", scope: "global") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    return _policies.TryGetValue(policyArn, out var policy) &&
                           policy.Versions.TryGetValue(versionId, out var version)
                        ?
                        [
                            AdminData.Field("Created", version.CreateDate, format: "datetime"),
                            AdminData.Field("Default", version.IsDefaultVersion.ToString())
                        ]
                        : [];
                }
            },
            ReadContent = () =>
            {
                lock (_lock)
                {
                    return _policies.TryGetValue(policyArn, out var policy) &&
                           policy.Versions.TryGetValue(versionId, out var version)
                        ? PolicyContent(version.Document)
                        : AdminData.Unavailable("The policy version no longer exists.", "application/json");
                }
            }
        };

    private IReadOnlyList<AdminConnection> PolicyConnections(string arn)
    {
        lock (_lock)
        {
            if (!_policies.ContainsKey(arn)) return [];
            var links = new List<AdminConnection>();
            links.AddRange(_users.Items.Where(x => x.Value.AttachedPolicies.Contains(arn))
                .Select(x => new AdminConnection(x.Key, "attached-user", "iam",
                    [new AdminKey("user", x.Key)])));
            links.AddRange(_roles.Items.Where(x => x.Value.AttachedPolicies.Contains(arn))
                .Select(x => new AdminConnection(x.Key, "attached-role", "iam",
                    [new AdminKey("role", x.Key)])));
            links.AddRange(_groups.Items.Where(x => x.Value.AttachedPolicies.Contains(arn))
                .Select(x => new AdminConnection(x.Key, "attached-group", "iam",
                    [new AdminKey("group", x.Key)])));
            return links;
        }
    }

    private AdminNode GroupNode(string name, IamGroup snapshot) =>
        AdminData.Node("group", name, name, snapshot.Arn, "Active", "global") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_groups.TryGetValue(name, out var group)) return [];
                    return
                    [
                        AdminData.Field("Group ID", group.GroupId),
                        AdminData.Field("Path", group.Path),
                        AdminData.Field("Created", group.CreateDate, format: "datetime")
                    ];
                }
            },
            ReadConnections = () =>
            {
                lock (_lock)
                {
                    if (!_groups.TryGetValue(name, out var group)) return [];
                    var links = group.Users.Where(_users.ContainsKey)
                        .Select(user => new AdminConnection(user, "member", "iam",
                            [new AdminKey("user", user)])).ToList();
                    links.AddRange(group.AttachedPolicies.Where(_policies.ContainsKey)
                        .Select(arn => new AdminConnection(arn, "attached-policy", "iam",
                            [new AdminKey("policy", arn)])));
                    return links;
                }
            }
        };

    private AdminNode InstanceProfileNode(string name, IamInstanceProfile snapshot) =>
        AdminData.Node("instance-profile", name, name, snapshot.Arn, "Active", "global") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_instanceProfiles.TryGetValue(name, out var profile)) return [];
                    return
                    [
                        AdminData.Field("Instance profile ID", profile.InstanceProfileId),
                        AdminData.Field("Path", profile.Path),
                        AdminData.Field("Created", profile.CreateDate, format: "datetime")
                    ];
                }
            },
            ReadConnections = () =>
            {
                lock (_lock)
                {
                    return _instanceProfiles.TryGetValue(name, out var profile)
                        ? profile.Roles.Where(_roles.ContainsKey)
                            .Select(role => new AdminConnection(role, "role", "iam",
                                [new AdminKey("role", role)])).ToArray()
                        : [];
                }
            }
        };

    private AdminNode OidcNode(string arn, IamOidcProvider snapshot) =>
        AdminData.Node("oidc-provider", arn, snapshot.Url, arn, "Active", "global") with
        {
            ReadFields = () =>
            {
                lock (_lock)
                {
                    if (!_oidcProviders.TryGetValue(arn, out var provider)) return [];
                    return
                    [
                        AdminData.Field("URL", provider.Url),
                        AdminData.Field("Created", provider.CreateDate, format: "datetime"),
                        AdminData.Field("Client IDs", string.Join(", ", provider.ClientIdList)),
                        AdminData.Field("Thumbprints", string.Join(", ", provider.ThumbprintList)),
                        AdminData.Field("Tags", Tags(provider.Tags))
                    ];
                }
            }
        };

    private static string Tags(IEnumerable<IamTag> tags) =>
        string.Join(", ", tags.Select(t => $"{t.Key}={t.Value}"));

    private static AdminContent PolicyContent(string document)
    {
        var decoded = System.Net.WebUtility.UrlDecode(document);
        try { return AdminData.JsonText(decoded); }
        catch (System.Text.Json.JsonException) { return AdminData.Text(decoded); }
    }
}
