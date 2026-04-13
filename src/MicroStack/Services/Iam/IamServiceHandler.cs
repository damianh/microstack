using System.Text;
using System.Text.Json;
using System.Web;
using MicroStack.Internal;

namespace MicroStack.Services.Iam;

/// <summary>
/// IAM service handler — supports Query/XML protocol (Action= form params) and
/// JSON protocol (X-Amz-Target: IAMService.ActionName).
///
/// Port of ministack/services/iam_sts.py (IAM portion).
///
/// Supports: CreateUser, GetUser, ListUsers, DeleteUser,
///           CreateRole, GetRole, ListRoles, DeleteRole, UpdateRole,
///           UpdateAssumeRolePolicy,
///           CreatePolicy, GetPolicy, GetPolicyVersion, ListPolicyVersions,
///           CreatePolicyVersion, DeletePolicyVersion, ListPolicies, DeletePolicy,
///           ListEntitiesForPolicy,
///           AttachRolePolicy, DetachRolePolicy, ListAttachedRolePolicies,
///           PutRolePolicy, GetRolePolicy, DeleteRolePolicy, ListRolePolicies,
///           AttachUserPolicy, DetachUserPolicy, ListAttachedUserPolicies,
///           CreateAccessKey, ListAccessKeys, DeleteAccessKey,
///           CreateInstanceProfile, DeleteInstanceProfile, GetInstanceProfile,
///           AddRoleToInstanceProfile, RemoveRoleFromInstanceProfile,
///           ListInstanceProfiles, ListInstanceProfilesForRole,
///           TagRole, UntagRole, ListRoleTags,
///           TagUser, UntagUser, ListUserTags,
///           TagPolicy, UntagPolicy, ListPolicyTags,
///           SimulatePrincipalPolicy, SimulateCustomPolicy,
///           CreateGroup, GetGroup, DeleteGroup, ListGroups,
///           AddUserToGroup, RemoveUserFromGroup, ListGroupsForUser,
///           PutUserPolicy, GetUserPolicy, DeleteUserPolicy, ListUserPolicies,
///           CreateServiceLinkedRole, DeleteServiceLinkedRole, GetServiceLinkedRoleDeletionStatus,
///           CreateOpenIDConnectProvider, GetOpenIDConnectProvider, DeleteOpenIDConnectProvider.
/// </summary>
internal sealed class IamServiceHandler : IServiceHandler
{
    private readonly AccountScopedDictionary<string, IamUser> _users = new();
    private readonly AccountScopedDictionary<string, IamRole> _roles = new();
    private readonly AccountScopedDictionary<string, IamPolicy> _policies = new(); // keyed by ARN
    private readonly AccountScopedDictionary<string, IamAccessKey> _accessKeys = new(); // keyed by key ID
    private readonly AccountScopedDictionary<string, IamInstanceProfile> _instanceProfiles = new();
    private readonly AccountScopedDictionary<string, IamGroup> _groups = new();
    private readonly AccountScopedDictionary<string, string> _userInlinePolicies = new(); // key: "{userName}\0{policyName}"
    private readonly AccountScopedDictionary<string, IamOidcProvider> _oidcProviders = new(); // keyed by ARN
    private readonly AccountScopedDictionary<string, IamDeletionTask> _serviceLinkDeletionTasks = new();

    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    private const string IamXmlNs = "https://iam.amazonaws.com/doc/2010-05-08/";

    // These allow StsServiceHandler to access shared state
    internal AccountScopedDictionary<string, IamRole> Roles => _roles;
    internal AccountScopedDictionary<string, IamAccessKey> AccessKeys => _accessKeys;

    // ── IServiceHandler ─────────────────────────────────────────────────────────

    public string ServiceName => "iam";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var formParams = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (k, v) in request.QueryParams)
        {
            if (v.Length > 0)
            {
                formParams[k] = v[0];
            }
        }

        var contentType = request.GetHeader("content-type") ?? "";
        var target = request.GetHeader("x-amz-target") ?? "";

        // JSON protocol (newer SDKs): X-Amz-Target: IAMService.ActionName
        if (contentType.Contains("amz-json", StringComparison.Ordinal) && target.Contains('.', StringComparison.Ordinal))
        {
            var actionName = target[(target.LastIndexOf('.') + 1)..];
            formParams["Action"] = actionName;
            if (request.Body.Length > 0)
            {
                try
                {
                    var jsonDoc = JsonDocument.Parse(request.Body);
                    foreach (var prop in jsonDoc.RootElement.EnumerateObject())
                    {
                        formParams[prop.Name] = prop.Value.ValueKind == JsonValueKind.Array
                            ? prop.Value[0].ToString()
                            : prop.Value.ToString();
                    }
                }
                catch (JsonException)
                {
                    // Ignore malformed JSON
                }
            }
        }
        else if (request.Method == "POST" && request.Body.Length > 0)
        {
            var formStr = Encoding.UTF8.GetString(request.Body);
            foreach (var pair in formStr.Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                var eq = pair.IndexOf('=');
                if (eq < 0)
                {
                    continue;
                }

                var key = HttpUtility.UrlDecode(pair[..eq]);
                var val = HttpUtility.UrlDecode(pair[(eq + 1)..]);
                formParams[key] = val;
            }
        }

        var action = P(formParams, "Action");
        var response = action switch
        {
            "CreateUser" => ActCreateUser(formParams),
            "GetUser" => ActGetUser(formParams),
            "ListUsers" => ActListUsers(formParams),
            "DeleteUser" => ActDeleteUser(formParams),
            "CreateRole" => ActCreateRole(formParams),
            "GetRole" => ActGetRole(formParams),
            "ListRoles" => ActListRoles(formParams),
            "DeleteRole" => ActDeleteRole(formParams),
            "UpdateRole" => ActUpdateRole(formParams),
            "UpdateAssumeRolePolicy" => ActUpdateAssumeRolePolicy(formParams),
            "CreatePolicy" => ActCreatePolicy(formParams),
            "GetPolicy" => ActGetPolicy(formParams),
            "GetPolicyVersion" => ActGetPolicyVersion(formParams),
            "ListPolicyVersions" => ActListPolicyVersions(formParams),
            "CreatePolicyVersion" => ActCreatePolicyVersion(formParams),
            "DeletePolicyVersion" => ActDeletePolicyVersion(formParams),
            "ListPolicies" => ActListPolicies(formParams),
            "DeletePolicy" => ActDeletePolicy(formParams),
            "ListEntitiesForPolicy" => ActListEntitiesForPolicy(formParams),
            "AttachRolePolicy" => ActAttachRolePolicy(formParams),
            "DetachRolePolicy" => ActDetachRolePolicy(formParams),
            "ListAttachedRolePolicies" => ActListAttachedRolePolicies(formParams),
            "PutRolePolicy" => ActPutRolePolicy(formParams),
            "GetRolePolicy" => ActGetRolePolicy(formParams),
            "DeleteRolePolicy" => ActDeleteRolePolicy(formParams),
            "ListRolePolicies" => ActListRolePolicies(formParams),
            "AttachUserPolicy" => ActAttachUserPolicy(formParams),
            "DetachUserPolicy" => ActDetachUserPolicy(formParams),
            "ListAttachedUserPolicies" => ActListAttachedUserPolicies(formParams),
            "CreateAccessKey" => ActCreateAccessKey(formParams),
            "ListAccessKeys" => ActListAccessKeys(formParams),
            "DeleteAccessKey" => ActDeleteAccessKey(formParams),
            "CreateInstanceProfile" => ActCreateInstanceProfile(formParams),
            "DeleteInstanceProfile" => ActDeleteInstanceProfile(formParams),
            "GetInstanceProfile" => ActGetInstanceProfile(formParams),
            "AddRoleToInstanceProfile" => ActAddRoleToInstanceProfile(formParams),
            "RemoveRoleFromInstanceProfile" => ActRemoveRoleFromInstanceProfile(formParams),
            "ListInstanceProfiles" => ActListInstanceProfiles(formParams),
            "ListInstanceProfilesForRole" => ActListInstanceProfilesForRole(formParams),
            "TagRole" => ActTagRole(formParams),
            "UntagRole" => ActUntagRole(formParams),
            "ListRoleTags" => ActListRoleTags(formParams),
            "TagUser" => ActTagUser(formParams),
            "UntagUser" => ActUntagUser(formParams),
            "ListUserTags" => ActListUserTags(formParams),
            "TagPolicy" => ActTagPolicy(formParams),
            "UntagPolicy" => ActUntagPolicy(formParams),
            "ListPolicyTags" => ActListPolicyTags(formParams),
            "SimulatePrincipalPolicy" => ActSimulatePrincipalPolicy(formParams),
            "SimulateCustomPolicy" => ActSimulateCustomPolicy(formParams),
            "CreateGroup" => ActCreateGroup(formParams),
            "GetGroup" => ActGetGroup(formParams),
            "DeleteGroup" => ActDeleteGroup(formParams),
            "ListGroups" => ActListGroups(formParams),
            "AddUserToGroup" => ActAddUserToGroup(formParams),
            "RemoveUserFromGroup" => ActRemoveUserFromGroup(formParams),
            "ListGroupsForUser" => ActListGroupsForUser(formParams),
            "PutUserPolicy" => ActPutUserPolicy(formParams),
            "GetUserPolicy" => ActGetUserPolicy(formParams),
            "DeleteUserPolicy" => ActDeleteUserPolicy(formParams),
            "ListUserPolicies" => ActListUserPolicies(formParams),
            "CreateServiceLinkedRole" => ActCreateServiceLinkedRole(formParams),
            "DeleteServiceLinkedRole" => ActDeleteServiceLinkedRole(formParams),
            "GetServiceLinkedRoleDeletionStatus" => ActGetServiceLinkedRoleDeletionStatus(formParams),
            "CreateOpenIDConnectProvider" => ActCreateOpenIdConnectProvider(formParams),
            "GetOpenIDConnectProvider" => ActGetOpenIdConnectProvider(formParams),
            "DeleteOpenIDConnectProvider" => ActDeleteOpenIdConnectProvider(formParams),
            _ => XmlError(400, "InvalidAction", $"Unknown IAM action: {action}"),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _users.Clear();
            _roles.Clear();
            _policies.Clear();
            _accessKeys.Clear();
            _instanceProfiles.Clear();
            _groups.Clear();
            _userInlinePolicies.Clear();
            _oidcProviders.Clear();
            _serviceLinkDeletionTasks.Clear();
        }
    }

    public JsonElement? GetState() => null;

    public void RestoreState(JsonElement state) { }

    // ── User management ─────────────────────────────────────────────────────────

    private ServiceResponse ActCreateUser(Dictionary<string, string> p)
    {
        var name = P(p, "UserName");
        lock (_lock)
        {
            if (_users.ContainsKey(name))
            {
                return XmlError(409, "EntityAlreadyExists", $"User with name {name} already exists.");
            }

            var path = P(p, "Path");
            if (string.IsNullOrEmpty(path))
            {
                path = "/";
            }

            var accountId = AccountContext.GetAccountId();
            _users[name] = new IamUser
            {
                UserName = name,
                Arn = path != "/"
                    ? $"arn:aws:iam::{accountId}:user{path}{name}"
                    : $"arn:aws:iam::{accountId}:user/{name}",
                UserId = GenId("AIDA"),
                CreateDate = Now(),
                Path = path,
                Tags = ExtractTags(p),
            };
        }

        return Xml(200, "CreateUserResponse",
            $"<CreateUserResult><User>{UserXml(name)}</User></CreateUserResult>");
    }

    private ServiceResponse ActGetUser(Dictionary<string, string> p)
    {
        var name = P(p, "UserName");
        if (string.IsNullOrEmpty(name))
        {
            var accountId = AccountContext.GetAccountId();
            return Xml(200, "GetUserResponse",
                "<GetUserResult><User>"
                + "<UserName>root</UserName>"
                + $"<UserId>{accountId}</UserId>"
                + $"<Arn>arn:aws:iam::{accountId}:root</Arn>"
                + "<Path>/</Path>"
                + $"<CreateDate>{Now()}</CreateDate>"
                + "</User></GetUserResult>");
        }

        lock (_lock)
        {
            if (!_users.ContainsKey(name))
            {
                return XmlError(404, "NoSuchEntity", $"The user with name {name} cannot be found.");
            }
        }

        return Xml(200, "GetUserResponse",
            $"<GetUserResult><User>{UserXml(name)}</User></GetUserResult>");
    }

    private ServiceResponse ActListUsers(Dictionary<string, string> p)
    {
        var prefix = P(p, "PathPrefix");
        if (string.IsNullOrEmpty(prefix))
        {
            prefix = "/";
        }

        var sb = new StringBuilder();
        lock (_lock)
        {
            foreach (var (n, u) in _users.Items)
            {
                if (u.Path.StartsWith(prefix, StringComparison.Ordinal))
                {
                    sb.Append($"<member>{UserXml(n)}</member>");
                }
            }
        }

        return Xml(200, "ListUsersResponse",
            $"<ListUsersResult><Users>{sb}</Users>"
            + "<IsTruncated>false</IsTruncated></ListUsersResult>");
    }

    private ServiceResponse ActDeleteUser(Dictionary<string, string> p)
    {
        var name = P(p, "UserName");
        lock (_lock)
        {
            if (!_users.TryGetValue(name, out var user))
            {
                return XmlError(404, "NoSuchEntity", $"The user with name {name} cannot be found.");
            }

            if (user.AttachedPolicies.Count > 0)
            {
                return XmlError(409, "DeleteConflict",
                    "Cannot delete entity, must detach all policies first.");
            }

            foreach (var (_, v) in _accessKeys.Items)
            {
                if (v.UserName == name)
                {
                    return XmlError(409, "DeleteConflict",
                        "Cannot delete entity, must delete access keys first.");
                }
            }

            _users.TryRemove(name, out _);
        }

        return Xml(200, "DeleteUserResponse", "");
    }

    // ── Role management ─────────────────────────────────────────────────────────

    private ServiceResponse ActCreateRole(Dictionary<string, string> p)
    {
        var name = P(p, "RoleName");
        lock (_lock)
        {
            if (_roles.ContainsKey(name))
            {
                return XmlError(409, "EntityAlreadyExists", $"Role with name {name} already exists.");
            }

            var path = P(p, "Path");
            if (string.IsNullOrEmpty(path))
            {
                path = "/";
            }

            var maxSessionStr = P(p, "MaxSessionDuration");
            var maxSession = string.IsNullOrEmpty(maxSessionStr)
                ? 3600
                : int.Parse(maxSessionStr);

            var accountId = AccountContext.GetAccountId();
            _roles[name] = new IamRole
            {
                RoleName = name,
                Arn = path != "/"
                    ? $"arn:aws:iam::{accountId}:role{path}{name}"
                    : $"arn:aws:iam::{accountId}:role/{name}",
                RoleId = GenId("AROA"),
                CreateDate = Now(),
                Path = path,
                AssumeRolePolicyDocument = P(p, "AssumeRolePolicyDocument"),
                Description = P(p, "Description"),
                MaxSessionDuration = maxSession,
                Tags = ExtractTags(p),
            };
        }

        return Xml(200, "CreateRoleResponse",
            $"<CreateRoleResult><Role>{RoleXml(name)}</Role></CreateRoleResult>");
    }

    private ServiceResponse ActGetRole(Dictionary<string, string> p)
    {
        var name = P(p, "RoleName");
        lock (_lock)
        {
            if (!_roles.ContainsKey(name))
            {
                return XmlError(404, "NoSuchEntity", $"Role {name} not found.");
            }
        }

        return Xml(200, "GetRoleResponse",
            $"<GetRoleResult><Role>{RoleXml(name)}</Role></GetRoleResult>");
    }

    private ServiceResponse ActListRoles(Dictionary<string, string> p)
    {
        var prefix = P(p, "PathPrefix");
        if (string.IsNullOrEmpty(prefix))
        {
            prefix = "/";
        }

        var sb = new StringBuilder();
        lock (_lock)
        {
            foreach (var (n, r) in _roles.Items)
            {
                if (r.Path.StartsWith(prefix, StringComparison.Ordinal))
                {
                    sb.Append($"<member>{RoleXml(n)}</member>");
                }
            }
        }

        return Xml(200, "ListRolesResponse",
            $"<ListRolesResult><Roles>{sb}</Roles>"
            + "<IsTruncated>false</IsTruncated></ListRolesResult>");
    }

    private ServiceResponse ActDeleteRole(Dictionary<string, string> p)
    {
        var name = P(p, "RoleName");
        lock (_lock)
        {
            if (!_roles.TryGetValue(name, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {name} not found.");
            }

            if (role.AttachedPolicies.Count > 0)
            {
                return XmlError(409, "DeleteConflict",
                    "Cannot delete entity, must detach all policies first.");
            }

            if (role.InlinePolicies.Count > 0)
            {
                return XmlError(409, "DeleteConflict",
                    "Cannot delete entity, must delete all inline policies first.");
            }

            foreach (var ip in _instanceProfiles.Values)
            {
                if (ip.Roles.Contains(name))
                {
                    return XmlError(409, "DeleteConflict",
                        "Cannot delete entity, must remove role from all instance profiles first.");
                }
            }

            _roles.TryRemove(name, out _);
        }

        return Xml(200, "DeleteRoleResponse", "");
    }

    private ServiceResponse ActUpdateRole(Dictionary<string, string> p)
    {
        var name = P(p, "RoleName");
        lock (_lock)
        {
            if (!_roles.TryGetValue(name, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {name} not found.");
            }

            if (p.ContainsKey("Description"))
            {
                role.Description = P(p, "Description");
            }

            if (p.ContainsKey("MaxSessionDuration"))
            {
                var maxStr = P(p, "MaxSessionDuration");
                role.MaxSessionDuration = string.IsNullOrEmpty(maxStr) ? 3600 : int.Parse(maxStr);
            }
        }

        return Xml(200, "UpdateRoleResponse", "<UpdateRoleResult></UpdateRoleResult>");
    }

    private ServiceResponse ActUpdateAssumeRolePolicy(Dictionary<string, string> p)
    {
        var name = P(p, "RoleName");
        lock (_lock)
        {
            if (!_roles.TryGetValue(name, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {name} not found.");
            }

            role.AssumeRolePolicyDocument = P(p, "PolicyDocument");
        }

        return Xml(200, "UpdateAssumeRolePolicyResponse", "");
    }

    // ── Managed policy management ───────────────────────────────────────────────

    private ServiceResponse ActCreatePolicy(Dictionary<string, string> p)
    {
        var name = P(p, "PolicyName");
        var path = P(p, "Path");
        if (string.IsNullOrEmpty(path))
        {
            path = "/";
        }

        var accountId = AccountContext.GetAccountId();
        var arn = path != "/"
            ? $"arn:aws:iam::{accountId}:policy{path}{name}"
            : $"arn:aws:iam::{accountId}:policy/{name}";

        lock (_lock)
        {
            if (_policies.ContainsKey(arn))
            {
                return XmlError(409, "EntityAlreadyExists", $"A policy called {name} already exists.");
            }

            var doc = P(p, "PolicyDocument");
            var policyId = GenId("ANPA");
            var versionId = "v1";
            var now = Now();
            _policies[arn] = new IamPolicy
            {
                PolicyName = name,
                Arn = arn,
                PolicyId = policyId,
                CreateDate = now,
                UpdateDate = now,
                DefaultVersionId = versionId,
                AttachmentCount = 0,
                IsAttachable = true,
                Path = path,
                Versions =
                {
                    [versionId] = new IamPolicyVersion
                    {
                        Document = doc,
                        VersionId = versionId,
                        IsDefaultVersion = true,
                        CreateDate = now,
                    },
                },
            };
        }

        return Xml(200, "CreatePolicyResponse",
            $"<CreatePolicyResult><Policy>{ManagedPolicyXml(arn)}</Policy></CreatePolicyResult>");
    }

    private ServiceResponse ActGetPolicy(Dictionary<string, string> p)
    {
        var arn = P(p, "PolicyArn");
        lock (_lock)
        {
            if (!_policies.ContainsKey(arn))
            {
                return XmlError(404, "NoSuchEntity", $"Policy {arn} not found.");
            }
        }

        return Xml(200, "GetPolicyResponse",
            $"<GetPolicyResult><Policy>{ManagedPolicyXml(arn)}</Policy></GetPolicyResult>");
    }

    private ServiceResponse ActGetPolicyVersion(Dictionary<string, string> p)
    {
        var arn = P(p, "PolicyArn");
        var vid = P(p, "VersionId");
        lock (_lock)
        {
            if (!_policies.TryGetValue(arn, out var pol))
            {
                return XmlError(404, "NoSuchEntity", "Policy not found.");
            }

            if (!pol.Versions.TryGetValue(vid, out var ver))
            {
                return XmlError(404, "NoSuchEntity", $"Policy version {vid} not found.");
            }

            var doc = Uri.EscapeDataString(ver.Document ?? "{}");
            var isDefault = ver.IsDefaultVersion ? "true" : "false";
            return Xml(200, "GetPolicyVersionResponse",
                "<GetPolicyVersionResult><PolicyVersion>"
                + $"<Document>{doc}</Document>"
                + $"<VersionId>{vid}</VersionId>"
                + $"<IsDefaultVersion>{isDefault}</IsDefaultVersion>"
                + $"<CreateDate>{ver.CreateDate}</CreateDate>"
                + "</PolicyVersion></GetPolicyVersionResult>");
        }
    }

    private ServiceResponse ActListPolicyVersions(Dictionary<string, string> p)
    {
        var arn = P(p, "PolicyArn");
        lock (_lock)
        {
            if (!_policies.TryGetValue(arn, out var pol))
            {
                return XmlError(404, "NoSuchEntity", "Policy not found.");
            }

            var sb = new StringBuilder();
            foreach (var (vid, ver) in pol.Versions)
            {
                var isDefault = ver.IsDefaultVersion ? "true" : "false";
                sb.Append($"<member><VersionId>{vid}</VersionId>"
                          + $"<IsDefaultVersion>{isDefault}</IsDefaultVersion>"
                          + $"<CreateDate>{ver.CreateDate}</CreateDate></member>");
            }

            return Xml(200, "ListPolicyVersionsResponse",
                $"<ListPolicyVersionsResult><Versions>{sb}</Versions>"
                + "<IsTruncated>false</IsTruncated></ListPolicyVersionsResult>");
        }
    }

    private ServiceResponse ActCreatePolicyVersion(Dictionary<string, string> p)
    {
        var arn = P(p, "PolicyArn");
        lock (_lock)
        {
            if (!_policies.TryGetValue(arn, out var pol))
            {
                return XmlError(404, "NoSuchEntity", "Policy not found.");
            }

            if (pol.Versions.Count >= 5)
            {
                return XmlError(409, "LimitExceeded",
                    "A managed policy can have at most 5 versions.");
            }

            var doc = P(p, "PolicyDocument");
            var setDefaultStr = P(p, "SetAsDefault");
            var setDefault = !string.IsNullOrEmpty(setDefaultStr)
                             && (setDefaultStr.Equals("true", StringComparison.OrdinalIgnoreCase)
                                 || setDefaultStr == "1");

            var nextV = 0;
            foreach (var v in pol.Versions.Keys)
            {
                if (int.TryParse(v.AsSpan(1), out var num) && num > nextV)
                {
                    nextV = num;
                }
            }

            nextV++;
            var vid = $"v{nextV}";
            var now = Now();

            pol.Versions[vid] = new IamPolicyVersion
            {
                Document = doc,
                VersionId = vid,
                IsDefaultVersion = setDefault,
                CreateDate = now,
            };

            if (setDefault)
            {
                foreach (var ver in pol.Versions.Values)
                {
                    ver.IsDefaultVersion = ver.VersionId == vid;
                }

                pol.DefaultVersionId = vid;
            }

            pol.UpdateDate = now;
            var isDefaultStr = setDefault ? "true" : "false";
            return Xml(200, "CreatePolicyVersionResponse",
                "<CreatePolicyVersionResult><PolicyVersion>"
                + $"<VersionId>{vid}</VersionId>"
                + $"<IsDefaultVersion>{isDefaultStr}</IsDefaultVersion>"
                + $"<CreateDate>{now}</CreateDate>"
                + "</PolicyVersion></CreatePolicyVersionResult>");
        }
    }

    private ServiceResponse ActDeletePolicyVersion(Dictionary<string, string> p)
    {
        var arn = P(p, "PolicyArn");
        var vid = P(p, "VersionId");
        lock (_lock)
        {
            if (!_policies.TryGetValue(arn, out var pol))
            {
                return XmlError(404, "NoSuchEntity", "Policy not found.");
            }

            if (!pol.Versions.TryGetValue(vid, out var ver))
            {
                return XmlError(404, "NoSuchEntity", $"Policy version {vid} not found.");
            }

            if (ver.IsDefaultVersion)
            {
                return XmlError(409, "DeleteConflict",
                    "Cannot delete the default version of a policy.");
            }

            pol.Versions.Remove(vid);
        }

        return Xml(200, "DeletePolicyVersionResponse", "");
    }

    private ServiceResponse ActListPolicies(Dictionary<string, string> p)
    {
        var scope = P(p, "Scope");
        if (string.IsNullOrEmpty(scope))
        {
            scope = "All";
        }

        var prefix = P(p, "PathPrefix");
        if (string.IsNullOrEmpty(prefix))
        {
            prefix = "/";
        }

        var sb = new StringBuilder();
        lock (_lock)
        {
            foreach (var (arn, pol) in _policies.Items)
            {
                if (!pol.Path.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                if (scope == "Local" && arn.StartsWith("arn:aws:iam::aws:", StringComparison.Ordinal))
                {
                    continue;
                }

                sb.Append($"<member>{ManagedPolicyXml(arn)}</member>");
            }
        }

        return Xml(200, "ListPoliciesResponse",
            $"<ListPoliciesResult><Policies>{sb}</Policies>"
            + "<IsTruncated>false</IsTruncated></ListPoliciesResult>");
    }

    private ServiceResponse ActDeletePolicy(Dictionary<string, string> p)
    {
        var arn = P(p, "PolicyArn");
        lock (_lock)
        {
            if (!_policies.TryGetValue(arn, out var pol))
            {
                return XmlError(404, "NoSuchEntity", $"Policy {arn} not found.");
            }

            if (pol.AttachmentCount > 0)
            {
                return XmlError(409, "DeleteConflict",
                    "Cannot delete a policy attached to entities.");
            }

            _policies.TryRemove(arn, out _);
        }

        return Xml(200, "DeletePolicyResponse", "");
    }

    // ── List entities for policy ────────────────────────────────────────────────

    private ServiceResponse ActListEntitiesForPolicy(Dictionary<string, string> p)
    {
        var arn = P(p, "PolicyArn");
        var entityFilter = P(p, "EntityFilter");
        var pathPrefix = P(p, "PathPrefix");
        if (string.IsNullOrEmpty(pathPrefix))
        {
            pathPrefix = "/";
        }

        lock (_lock)
        {
            if (!_policies.ContainsKey(arn))
            {
                return XmlError(404, "NoSuchEntity", $"Policy {arn} not found.");
            }

            var groupsXml = new StringBuilder();
            if (entityFilter is "" or "Group")
            {
                foreach (var g in _groups.Values)
                {
                    if (!g.Path.StartsWith(pathPrefix, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (g.AttachedPolicies.Contains(arn))
                    {
                        groupsXml.Append($"<member><GroupName>{g.GroupName}</GroupName>"
                                         + $"<GroupId>{g.GroupId}</GroupId></member>");
                    }
                }
            }

            var rolesXml = new StringBuilder();
            if (entityFilter is "" or "Role")
            {
                foreach (var r in _roles.Values)
                {
                    if (!r.Path.StartsWith(pathPrefix, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (r.AttachedPolicies.Contains(arn))
                    {
                        rolesXml.Append($"<member><RoleName>{r.RoleName}</RoleName>"
                                        + $"<RoleId>{r.RoleId}</RoleId></member>");
                    }
                }
            }

            var usersXml = new StringBuilder();
            if (entityFilter is "" or "User")
            {
                foreach (var u in _users.Values)
                {
                    if (!u.Path.StartsWith(pathPrefix, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (u.AttachedPolicies.Contains(arn))
                    {
                        usersXml.Append($"<member><UserName>{u.UserName}</UserName>"
                                        + $"<UserId>{u.UserId}</UserId></member>");
                    }
                }
            }

            return Xml(200, "ListEntitiesForPolicyResponse",
                "<ListEntitiesForPolicyResult>"
                + $"<PolicyGroups>{groupsXml}</PolicyGroups>"
                + $"<PolicyRoles>{rolesXml}</PolicyRoles>"
                + $"<PolicyUsers>{usersXml}</PolicyUsers>"
                + "<IsTruncated>false</IsTruncated>"
                + "</ListEntitiesForPolicyResult>");
        }
    }

    // ── Attached role policies ──────────────────────────────────────────────────

    private ServiceResponse ActAttachRolePolicy(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        var policyArn = P(p, "PolicyArn");
        lock (_lock)
        {
            if (!_roles.TryGetValue(roleName, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            if (!role.AttachedPolicies.Contains(policyArn))
            {
                role.AttachedPolicies.Add(policyArn);
                if (_policies.TryGetValue(policyArn, out var pol))
                {
                    pol.AttachmentCount++;
                }
            }
        }

        return Xml(200, "AttachRolePolicyResponse", "");
    }

    private ServiceResponse ActDetachRolePolicy(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        var policyArn = P(p, "PolicyArn");
        lock (_lock)
        {
            if (!_roles.TryGetValue(roleName, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            if (!role.AttachedPolicies.Contains(policyArn))
            {
                return XmlError(404, "NoSuchEntity",
                    $"Policy {policyArn} is not attached to role {roleName}.");
            }

            role.AttachedPolicies.Remove(policyArn);
            if (_policies.TryGetValue(policyArn, out var pol))
            {
                pol.AttachmentCount = Math.Max(pol.AttachmentCount - 1, 0);
            }
        }

        return Xml(200, "DetachRolePolicyResponse", "");
    }

    private ServiceResponse ActListAttachedRolePolicies(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        lock (_lock)
        {
            if (!_roles.TryGetValue(roleName, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            var sb = new StringBuilder();
            foreach (var arn in role.AttachedPolicies)
            {
                var pname = _policies.TryGetValue(arn, out var pol)
                    ? pol.PolicyName
                    : arn[(arn.LastIndexOf('/') + 1)..];
                sb.Append($"<member><PolicyName>{pname}</PolicyName>"
                          + $"<PolicyArn>{arn}</PolicyArn></member>");
            }

            return Xml(200, "ListAttachedRolePoliciesResponse",
                $"<ListAttachedRolePoliciesResult><AttachedPolicies>{sb}</AttachedPolicies>"
                + "<IsTruncated>false</IsTruncated></ListAttachedRolePoliciesResult>");
        }
    }

    // ── Inline role policies ────────────────────────────────────────────────────

    private ServiceResponse ActPutRolePolicy(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        var policyName = P(p, "PolicyName");
        var policyDoc = P(p, "PolicyDocument");
        lock (_lock)
        {
            if (!_roles.TryGetValue(roleName, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            role.InlinePolicies[policyName] = policyDoc;
        }

        return Xml(200, "PutRolePolicyResponse", "");
    }

    private ServiceResponse ActGetRolePolicy(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        var policyName = P(p, "PolicyName");
        lock (_lock)
        {
            if (!_roles.TryGetValue(roleName, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            if (!role.InlinePolicies.TryGetValue(policyName, out var doc))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The role policy with name {policyName} cannot be found.");
            }

            var encodedDoc = Uri.EscapeDataString(doc);
            return Xml(200, "GetRolePolicyResponse",
                "<GetRolePolicyResult>"
                + $"<RoleName>{roleName}</RoleName>"
                + $"<PolicyName>{policyName}</PolicyName>"
                + $"<PolicyDocument>{encodedDoc}</PolicyDocument>"
                + "</GetRolePolicyResult>");
        }
    }

    private ServiceResponse ActDeleteRolePolicy(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        var policyName = P(p, "PolicyName");
        lock (_lock)
        {
            if (!_roles.TryGetValue(roleName, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            if (!role.InlinePolicies.ContainsKey(policyName))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The role policy with name {policyName} cannot be found.");
            }

            role.InlinePolicies.Remove(policyName);
        }

        return Xml(200, "DeleteRolePolicyResponse", "");
    }

    private ServiceResponse ActListRolePolicies(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        lock (_lock)
        {
            if (!_roles.TryGetValue(roleName, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            var sb = new StringBuilder();
            foreach (var name in role.InlinePolicies.Keys)
            {
                sb.Append($"<member>{name}</member>");
            }

            return Xml(200, "ListRolePoliciesResponse",
                $"<ListRolePoliciesResult><PolicyNames>{sb}</PolicyNames>"
                + "<IsTruncated>false</IsTruncated></ListRolePoliciesResult>");
        }
    }

    // ── Attached user policies ──────────────────────────────────────────────────

    private ServiceResponse ActAttachUserPolicy(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        var policyArn = P(p, "PolicyArn");
        lock (_lock)
        {
            if (!_users.TryGetValue(userName, out var user))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            if (!user.AttachedPolicies.Contains(policyArn))
            {
                user.AttachedPolicies.Add(policyArn);
                if (_policies.TryGetValue(policyArn, out var pol))
                {
                    pol.AttachmentCount++;
                }
            }
        }

        return Xml(200, "AttachUserPolicyResponse", "");
    }

    private ServiceResponse ActDetachUserPolicy(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        var policyArn = P(p, "PolicyArn");
        lock (_lock)
        {
            if (!_users.TryGetValue(userName, out var user))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            if (!user.AttachedPolicies.Contains(policyArn))
            {
                return XmlError(404, "NoSuchEntity",
                    $"Policy {policyArn} is not attached to user {userName}.");
            }

            user.AttachedPolicies.Remove(policyArn);
            if (_policies.TryGetValue(policyArn, out var pol))
            {
                pol.AttachmentCount = Math.Max(pol.AttachmentCount - 1, 0);
            }
        }

        return Xml(200, "DetachUserPolicyResponse", "");
    }

    private ServiceResponse ActListAttachedUserPolicies(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        lock (_lock)
        {
            if (!_users.TryGetValue(userName, out var user))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            var sb = new StringBuilder();
            foreach (var arn in user.AttachedPolicies)
            {
                var pname = _policies.TryGetValue(arn, out var pol)
                    ? pol.PolicyName
                    : arn[(arn.LastIndexOf('/') + 1)..];
                sb.Append($"<member><PolicyName>{pname}</PolicyName>"
                          + $"<PolicyArn>{arn}</PolicyArn></member>");
            }

            return Xml(200, "ListAttachedUserPoliciesResponse",
                $"<ListAttachedUserPoliciesResult><AttachedPolicies>{sb}</AttachedPolicies>"
                + "<IsTruncated>false</IsTruncated></ListAttachedUserPoliciesResult>");
        }
    }

    // ── Access keys ─────────────────────────────────────────────────────────────

    private ServiceResponse ActCreateAccessKey(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        if (string.IsNullOrEmpty(userName))
        {
            userName = "default";
        }

        lock (_lock)
        {
            if (userName != "default" && !_users.ContainsKey(userName))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            var keyId = GenAccessKeyId();
            var secret = Guid.NewGuid().ToString("N") + Guid.NewGuid().ToString("N")[..8];
            var now = Now();
            _accessKeys[keyId] = new IamAccessKey
            {
                UserName = userName,
                AccessKeyId = keyId,
                SecretAccessKey = secret,
                Status = "Active",
                CreateDate = now,
            };

            return Xml(200, "CreateAccessKeyResponse",
                "<CreateAccessKeyResult><AccessKey>"
                + $"<UserName>{userName}</UserName>"
                + $"<AccessKeyId>{keyId}</AccessKeyId>"
                + $"<SecretAccessKey>{secret}</SecretAccessKey>"
                + "<Status>Active</Status>"
                + $"<CreateDate>{now}</CreateDate>"
                + "</AccessKey></CreateAccessKeyResult>");
        }
    }

    private ServiceResponse ActListAccessKeys(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        if (string.IsNullOrEmpty(userName))
        {
            userName = "default";
        }

        var sb = new StringBuilder();
        lock (_lock)
        {
            foreach (var (kid, v) in _accessKeys.Items)
            {
                if (v.UserName == userName)
                {
                    sb.Append($"<member><AccessKeyId>{kid}</AccessKeyId>"
                              + $"<Status>{v.Status}</Status>"
                              + $"<UserName>{userName}</UserName>"
                              + $"<CreateDate>{v.CreateDate}</CreateDate>"
                              + "</member>");
                }
            }
        }

        return Xml(200, "ListAccessKeysResponse",
            $"<ListAccessKeysResult><AccessKeyMetadata>{sb}</AccessKeyMetadata>"
            + "<IsTruncated>false</IsTruncated></ListAccessKeysResult>");
    }

    private ServiceResponse ActDeleteAccessKey(Dictionary<string, string> p)
    {
        var keyId = P(p, "AccessKeyId");
        lock (_lock)
        {
            if (!_accessKeys.ContainsKey(keyId))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The Access Key with id {keyId} cannot be found.");
            }

            _accessKeys.TryRemove(keyId, out _);
        }

        return Xml(200, "DeleteAccessKeyResponse", "");
    }

    // ── Instance profiles ───────────────────────────────────────────────────────

    private ServiceResponse ActCreateInstanceProfile(Dictionary<string, string> p)
    {
        var name = P(p, "InstanceProfileName");
        lock (_lock)
        {
            if (_instanceProfiles.ContainsKey(name))
            {
                return XmlError(409, "EntityAlreadyExists",
                    $"Instance profile {name} already exists.");
            }

            var path = P(p, "Path");
            if (string.IsNullOrEmpty(path))
            {
                path = "/";
            }

            var ipId = GenId("AIPA");
            var accountId = AccountContext.GetAccountId();
            var arn = path != "/"
                ? $"arn:aws:iam::{accountId}:instance-profile{path}{name}"
                : $"arn:aws:iam::{accountId}:instance-profile/{name}";

            _instanceProfiles[name] = new IamInstanceProfile
            {
                InstanceProfileName = name,
                InstanceProfileId = ipId,
                Arn = arn,
                Path = path,
                CreateDate = Now(),
            };
        }

        return Xml(200, "CreateInstanceProfileResponse",
            "<CreateInstanceProfileResult>"
            + $"<InstanceProfile>{InstanceProfileXml(name)}</InstanceProfile>"
            + "</CreateInstanceProfileResult>");
    }

    private ServiceResponse ActDeleteInstanceProfile(Dictionary<string, string> p)
    {
        var name = P(p, "InstanceProfileName");
        lock (_lock)
        {
            if (!_instanceProfiles.TryGetValue(name, out var ip))
            {
                return XmlError(404, "NoSuchEntity",
                    $"Instance profile {name} not found.");
            }

            if (ip.Roles.Count > 0)
            {
                return XmlError(409, "DeleteConflict",
                    "Cannot delete entity, must remove all roles first.");
            }

            _instanceProfiles.TryRemove(name, out _);
        }

        return Xml(200, "DeleteInstanceProfileResponse", "");
    }

    private ServiceResponse ActGetInstanceProfile(Dictionary<string, string> p)
    {
        var name = P(p, "InstanceProfileName");
        lock (_lock)
        {
            if (!_instanceProfiles.ContainsKey(name))
            {
                return XmlError(404, "NoSuchEntity",
                    $"Instance profile {name} not found.");
            }
        }

        return Xml(200, "GetInstanceProfileResponse",
            "<GetInstanceProfileResult>"
            + $"<InstanceProfile>{InstanceProfileXml(name)}</InstanceProfile>"
            + "</GetInstanceProfileResult>");
    }

    private ServiceResponse ActAddRoleToInstanceProfile(Dictionary<string, string> p)
    {
        var ipName = P(p, "InstanceProfileName");
        var roleName = P(p, "RoleName");
        lock (_lock)
        {
            if (!_instanceProfiles.TryGetValue(ipName, out var ip))
            {
                return XmlError(404, "NoSuchEntity",
                    $"Instance profile {ipName} not found.");
            }

            if (!_roles.ContainsKey(roleName))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            if (ip.Roles.Contains(roleName))
            {
                return XmlError(409, "LimitExceeded",
                    $"Role {roleName} is already associated with instance profile {ipName}.");
            }

            if (ip.Roles.Count >= 1)
            {
                return XmlError(409, "LimitExceeded",
                    "An instance profile can have only one role.");
            }

            ip.Roles.Add(roleName);
        }

        return Xml(200, "AddRoleToInstanceProfileResponse", "");
    }

    private ServiceResponse ActRemoveRoleFromInstanceProfile(Dictionary<string, string> p)
    {
        var ipName = P(p, "InstanceProfileName");
        var roleName = P(p, "RoleName");
        lock (_lock)
        {
            if (!_instanceProfiles.TryGetValue(ipName, out var ip))
            {
                return XmlError(404, "NoSuchEntity",
                    $"Instance profile {ipName} not found.");
            }

            if (!ip.Roles.Contains(roleName))
            {
                return XmlError(404, "NoSuchEntity",
                    $"Role {roleName} is not associated with instance profile {ipName}.");
            }

            ip.Roles.Remove(roleName);
        }

        return Xml(200, "RemoveRoleFromInstanceProfileResponse", "");
    }

    private ServiceResponse ActListInstanceProfiles(Dictionary<string, string> p)
    {
        var prefix = P(p, "PathPrefix");
        if (string.IsNullOrEmpty(prefix))
        {
            prefix = "/";
        }

        var sb = new StringBuilder();
        lock (_lock)
        {
            foreach (var (name, ip) in _instanceProfiles.Items)
            {
                if (ip.Path.StartsWith(prefix, StringComparison.Ordinal))
                {
                    sb.Append($"<member>{InstanceProfileXml(name)}</member>");
                }
            }
        }

        return Xml(200, "ListInstanceProfilesResponse",
            $"<ListInstanceProfilesResult><InstanceProfiles>{sb}</InstanceProfiles>"
            + "<IsTruncated>false</IsTruncated></ListInstanceProfilesResult>");
    }

    private ServiceResponse ActListInstanceProfilesForRole(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        lock (_lock)
        {
            if (!_roles.ContainsKey(roleName))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            var sb = new StringBuilder();
            foreach (var (name, ip) in _instanceProfiles.Items)
            {
                if (ip.Roles.Contains(roleName))
                {
                    sb.Append($"<member>{InstanceProfileXml(name)}</member>");
                }
            }

            return Xml(200, "ListInstanceProfilesForRoleResponse",
                $"<ListInstanceProfilesForRoleResult><InstanceProfiles>{sb}</InstanceProfiles>"
                + "<IsTruncated>false</IsTruncated></ListInstanceProfilesForRoleResult>");
        }
    }

    // ── Tags: roles ─────────────────────────────────────────────────────────────

    private ServiceResponse ActTagRole(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        lock (_lock)
        {
            if (!_roles.TryGetValue(roleName, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            var newTags = ExtractTags(p);
            var existing = new Dictionary<string, IamTag>(StringComparer.Ordinal);
            foreach (var t in role.Tags)
            {
                existing[t.Key] = t;
            }

            foreach (var t in newTags)
            {
                existing[t.Key] = t;
            }

            role.Tags = [.. existing.Values];
        }

        return Xml(200, "TagRoleResponse", "");
    }

    private ServiceResponse ActUntagRole(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        lock (_lock)
        {
            if (!_roles.TryGetValue(roleName, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            var keysToRemove = ExtractTagKeys(p);
            role.Tags = role.Tags.Where(t => !keysToRemove.Contains(t.Key)).ToList();
        }

        return Xml(200, "UntagRoleResponse", "");
    }

    private ServiceResponse ActListRoleTags(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        lock (_lock)
        {
            if (!_roles.TryGetValue(roleName, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            var sb = new StringBuilder();
            foreach (var t in role.Tags)
            {
                sb.Append($"<member><Key>{t.Key}</Key><Value>{t.Value}</Value></member>");
            }

            return Xml(200, "ListRoleTagsResponse",
                $"<ListRoleTagsResult><Tags>{sb}</Tags>"
                + "<IsTruncated>false</IsTruncated></ListRoleTagsResult>");
        }
    }

    // ── Tags: users ─────────────────────────────────────────────────────────────

    private ServiceResponse ActTagUser(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        lock (_lock)
        {
            if (!_users.TryGetValue(userName, out var user))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            var newTags = ExtractTags(p);
            var existing = new Dictionary<string, IamTag>(StringComparer.Ordinal);
            foreach (var t in user.Tags)
            {
                existing[t.Key] = t;
            }

            foreach (var t in newTags)
            {
                existing[t.Key] = t;
            }

            user.Tags = [.. existing.Values];
        }

        return Xml(200, "TagUserResponse", "");
    }

    private ServiceResponse ActUntagUser(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        lock (_lock)
        {
            if (!_users.TryGetValue(userName, out var user))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            var keysToRemove = ExtractTagKeys(p);
            user.Tags = user.Tags.Where(t => !keysToRemove.Contains(t.Key)).ToList();
        }

        return Xml(200, "UntagUserResponse", "");
    }

    private ServiceResponse ActListUserTags(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        lock (_lock)
        {
            if (!_users.TryGetValue(userName, out var user))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            var sb = new StringBuilder();
            foreach (var t in user.Tags)
            {
                sb.Append($"<member><Key>{t.Key}</Key><Value>{t.Value}</Value></member>");
            }

            return Xml(200, "ListUserTagsResponse",
                $"<ListUserTagsResult><Tags>{sb}</Tags>"
                + "<IsTruncated>false</IsTruncated></ListUserTagsResult>");
        }
    }

    // ── Tags: policies ──────────────────────────────────────────────────────────

    private ServiceResponse ActTagPolicy(Dictionary<string, string> p)
    {
        var arn = P(p, "PolicyArn");
        lock (_lock)
        {
            if (!_policies.TryGetValue(arn, out var pol))
            {
                return XmlError(404, "NoSuchEntity", $"Policy {arn} not found.");
            }

            var newTags = ExtractTags(p);
            var existing = new Dictionary<string, IamTag>(StringComparer.Ordinal);
            foreach (var t in pol.Tags)
            {
                existing[t.Key] = t;
            }

            foreach (var t in newTags)
            {
                existing[t.Key] = t;
            }

            pol.Tags = [.. existing.Values];
        }

        return Xml(200, "TagPolicyResponse", "");
    }

    private ServiceResponse ActUntagPolicy(Dictionary<string, string> p)
    {
        var arn = P(p, "PolicyArn");
        lock (_lock)
        {
            if (!_policies.TryGetValue(arn, out var pol))
            {
                return XmlError(404, "NoSuchEntity", $"Policy {arn} not found.");
            }

            var keysToRemove = ExtractTagKeys(p);
            pol.Tags = pol.Tags.Where(t => !keysToRemove.Contains(t.Key)).ToList();
        }

        return Xml(200, "UntagPolicyResponse", "");
    }

    private ServiceResponse ActListPolicyTags(Dictionary<string, string> p)
    {
        var arn = P(p, "PolicyArn");
        lock (_lock)
        {
            if (!_policies.TryGetValue(arn, out var pol))
            {
                return XmlError(404, "NoSuchEntity", $"Policy {arn} not found.");
            }

            var sb = new StringBuilder();
            foreach (var t in pol.Tags)
            {
                sb.Append($"<member><Key>{t.Key}</Key><Value>{t.Value}</Value></member>");
            }

            return Xml(200, "ListPolicyTagsResponse",
                $"<ListPolicyTagsResult><Tags>{sb}</Tags>"
                + "<IsTruncated>false</IsTruncated></ListPolicyTagsResult>");
        }
    }

    // ── Simulate (stubs) ────────────────────────────────────────────────────────

    private ServiceResponse ActSimulatePrincipalPolicy(Dictionary<string, string> p)
    {
        var results = BuildSimulateResults(p);
        return Xml(200, "SimulatePrincipalPolicyResponse",
            "<SimulatePrincipalPolicyResult>"
            + $"<EvaluationResults>{results}</EvaluationResults>"
            + "<IsTruncated>false</IsTruncated>"
            + "</SimulatePrincipalPolicyResult>");
    }

    private ServiceResponse ActSimulateCustomPolicy(Dictionary<string, string> p)
    {
        var results = BuildSimulateResults(p);
        return Xml(200, "SimulateCustomPolicyResponse",
            "<SimulateCustomPolicyResult>"
            + $"<EvaluationResults>{results}</EvaluationResults>"
            + "<IsTruncated>false</IsTruncated>"
            + "</SimulateCustomPolicyResult>");
    }

    // ── Group management ────────────────────────────────────────────────────────

    private ServiceResponse ActCreateGroup(Dictionary<string, string> p)
    {
        var name = P(p, "GroupName");
        lock (_lock)
        {
            if (_groups.ContainsKey(name))
            {
                return XmlError(409, "EntityAlreadyExists",
                    $"Group with name {name} already exists.");
            }

            var path = P(p, "Path");
            if (string.IsNullOrEmpty(path))
            {
                path = "/";
            }

            var accountId = AccountContext.GetAccountId();
            _groups[name] = new IamGroup
            {
                GroupName = name,
                GroupId = GenId("AGPA"),
                Arn = path != "/"
                    ? $"arn:aws:iam::{accountId}:group{path}{name}"
                    : $"arn:aws:iam::{accountId}:group/{name}",
                Path = path,
                CreateDate = Now(),
            };
        }

        return Xml(200, "CreateGroupResponse",
            $"<CreateGroupResult><Group>{GroupXml(name)}</Group></CreateGroupResult>");
    }

    private ServiceResponse ActGetGroup(Dictionary<string, string> p)
    {
        var name = P(p, "GroupName");
        lock (_lock)
        {
            if (!_groups.TryGetValue(name, out var g))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The group with name {name} cannot be found.");
            }

            var userMembers = new StringBuilder();
            foreach (var uname in g.Users)
            {
                if (_users.ContainsKey(uname))
                {
                    userMembers.Append($"<member>{UserXml(uname)}</member>");
                }
            }

            return Xml(200, "GetGroupResponse",
                "<GetGroupResult>"
                + $"<Group>{GroupXml(name)}</Group>"
                + $"<Users>{userMembers}</Users>"
                + "<IsTruncated>false</IsTruncated>"
                + "</GetGroupResult>");
        }
    }

    private ServiceResponse ActDeleteGroup(Dictionary<string, string> p)
    {
        var name = P(p, "GroupName");
        lock (_lock)
        {
            if (!_groups.ContainsKey(name))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The group with name {name} cannot be found.");
            }

            _groups.TryRemove(name, out _);
        }

        return Xml(200, "DeleteGroupResponse", "");
    }

    private ServiceResponse ActListGroups(Dictionary<string, string> p)
    {
        var prefix = P(p, "PathPrefix");
        if (string.IsNullOrEmpty(prefix))
        {
            prefix = "/";
        }

        var sb = new StringBuilder();
        lock (_lock)
        {
            foreach (var (n, g) in _groups.Items)
            {
                if (g.Path.StartsWith(prefix, StringComparison.Ordinal))
                {
                    sb.Append($"<member>{GroupXml(n)}</member>");
                }
            }
        }

        return Xml(200, "ListGroupsResponse",
            $"<ListGroupsResult><Groups>{sb}</Groups>"
            + "<IsTruncated>false</IsTruncated></ListGroupsResult>");
    }

    private ServiceResponse ActAddUserToGroup(Dictionary<string, string> p)
    {
        var groupName = P(p, "GroupName");
        var userName = P(p, "UserName");
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupName, out var g))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The group with name {groupName} cannot be found.");
            }

            if (!_users.ContainsKey(userName))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            if (!g.Users.Contains(userName))
            {
                g.Users.Add(userName);
            }
        }

        return Xml(200, "AddUserToGroupResponse", "");
    }

    private ServiceResponse ActRemoveUserFromGroup(Dictionary<string, string> p)
    {
        var groupName = P(p, "GroupName");
        var userName = P(p, "UserName");
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupName, out var g))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The group with name {groupName} cannot be found.");
            }

            if (!g.Users.Contains(userName))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} is not in group {groupName}.");
            }

            g.Users.Remove(userName);
        }

        return Xml(200, "RemoveUserFromGroupResponse", "");
    }

    private ServiceResponse ActListGroupsForUser(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        lock (_lock)
        {
            if (!_users.ContainsKey(userName))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            var sb = new StringBuilder();
            foreach (var (n, g) in _groups.Items)
            {
                if (g.Users.Contains(userName))
                {
                    sb.Append($"<member>{GroupXml(n)}</member>");
                }
            }

            return Xml(200, "ListGroupsForUserResponse",
                $"<ListGroupsForUserResult><Groups>{sb}</Groups>"
                + "<IsTruncated>false</IsTruncated></ListGroupsForUserResult>");
        }
    }

    // ── Inline user policies ────────────────────────────────────────────────────

    private ServiceResponse ActPutUserPolicy(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        var policyName = P(p, "PolicyName");
        var policyDoc = P(p, "PolicyDocument");
        lock (_lock)
        {
            if (!_users.ContainsKey(userName))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            _userInlinePolicies[$"{userName}\0{policyName}"] = policyDoc;
        }

        return Xml(200, "PutUserPolicyResponse", "");
    }

    private ServiceResponse ActGetUserPolicy(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        var policyName = P(p, "PolicyName");
        lock (_lock)
        {
            if (!_users.ContainsKey(userName))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            var compositeKey = $"{userName}\0{policyName}";
            if (!_userInlinePolicies.TryGetValue(compositeKey, out var doc))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user policy with name {policyName} cannot be found.");
            }

            var encodedDoc = Uri.EscapeDataString(doc);
            return Xml(200, "GetUserPolicyResponse",
                "<GetUserPolicyResult>"
                + $"<UserName>{userName}</UserName>"
                + $"<PolicyName>{policyName}</PolicyName>"
                + $"<PolicyDocument>{encodedDoc}</PolicyDocument>"
                + "</GetUserPolicyResult>");
        }
    }

    private ServiceResponse ActDeleteUserPolicy(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        var policyName = P(p, "PolicyName");
        lock (_lock)
        {
            if (!_users.ContainsKey(userName))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            var compositeKey = $"{userName}\0{policyName}";
            if (!_userInlinePolicies.ContainsKey(compositeKey))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user policy with name {policyName} cannot be found.");
            }

            _userInlinePolicies.TryRemove(compositeKey, out _);
        }

        return Xml(200, "DeleteUserPolicyResponse", "");
    }

    private ServiceResponse ActListUserPolicies(Dictionary<string, string> p)
    {
        var userName = P(p, "UserName");
        lock (_lock)
        {
            if (!_users.ContainsKey(userName))
            {
                return XmlError(404, "NoSuchEntity",
                    $"The user with name {userName} cannot be found.");
            }

            var sb = new StringBuilder();
            foreach (var key in _userInlinePolicies.Keys)
            {
                var separatorIndex = key.IndexOf('\0');
                if (separatorIndex < 0)
                {
                    continue;
                }

                var uname = key[..separatorIndex];
                var pname = key[(separatorIndex + 1)..];
                if (uname == userName)
                {
                    sb.Append($"<member>{pname}</member>");
                }
            }

            return Xml(200, "ListUserPoliciesResponse",
                $"<ListUserPoliciesResult><PolicyNames>{sb}</PolicyNames>"
                + "<IsTruncated>false</IsTruncated></ListUserPoliciesResult>");
        }
    }

    // ── Service-linked roles ────────────────────────────────────────────────────

    private ServiceResponse ActCreateServiceLinkedRole(Dictionary<string, string> p)
    {
        var serviceName = P(p, "AWSServiceName");
        var suffix = serviceName.Contains('.', StringComparison.Ordinal)
            ? serviceName[..serviceName.IndexOf('.')]
            : serviceName;
        var roleName = $"AWSServiceRoleFor{char.ToUpperInvariant(suffix[0])}{suffix[1..]}";
        var path = $"/aws-service-role/{serviceName}/";

        lock (_lock)
        {
            if (_roles.ContainsKey(roleName))
            {
                return XmlError(409, "EntityAlreadyExists",
                    $"Role with name {roleName} already exists.");
            }

            var trustPolicy = DictionaryObjectJsonConverter.SerializeValue(new Dictionary<string, object?>
            {
                ["Version"] = "2012-10-17",
                ["Statement"] = new List<object?>
                {
                    new Dictionary<string, object?>
                    {
                        ["Effect"] = "Allow",
                        ["Principal"] = new Dictionary<string, object?> { ["Service"] = serviceName },
                        ["Action"] = "sts:AssumeRole",
                    },
                },
            });

            var accountId = AccountContext.GetAccountId();
            _roles[roleName] = new IamRole
            {
                RoleName = roleName,
                Arn = $"arn:aws:iam::{accountId}:role{path}{roleName}",
                RoleId = GenId("AROA"),
                CreateDate = Now(),
                Path = path,
                AssumeRolePolicyDocument = trustPolicy,
                Description = $"Service-linked role for {serviceName}",
                MaxSessionDuration = 3600,
            };
        }

        return Xml(200, "CreateServiceLinkedRoleResponse",
            $"<CreateServiceLinkedRoleResult><Role>{RoleXml(roleName)}</Role></CreateServiceLinkedRoleResult>");
    }

    private ServiceResponse ActDeleteServiceLinkedRole(Dictionary<string, string> p)
    {
        var roleName = P(p, "RoleName");
        string taskId;
        lock (_lock)
        {
            if (!_roles.TryGetValue(roleName, out var role))
            {
                return XmlError(404, "NoSuchEntity", $"Role {roleName} not found.");
            }

            if (!role.Path.StartsWith("/aws-service-role/", StringComparison.Ordinal))
            {
                return XmlError(400, "InvalidInput",
                    $"Role {roleName} is not a service-linked role.");
            }

            taskId = HashHelpers.NewUuid();
            _serviceLinkDeletionTasks[taskId] = new IamDeletionTask
            {
                Status = "SUCCEEDED",
                RoleName = roleName,
            };
            _roles.TryRemove(roleName, out _);
        }

        return Xml(200, "DeleteServiceLinkedRoleResponse",
            "<DeleteServiceLinkedRoleResult>"
            + $"<DeletionTaskId>{taskId}</DeletionTaskId>"
            + "</DeleteServiceLinkedRoleResult>");
    }

    private ServiceResponse ActGetServiceLinkedRoleDeletionStatus(Dictionary<string, string> p)
    {
        var taskId = P(p, "DeletionTaskId");
        lock (_lock)
        {
            if (!_serviceLinkDeletionTasks.TryGetValue(taskId, out var task))
            {
                return XmlError(404, "NoSuchEntity", $"Deletion task {taskId} not found.");
            }

            var reason = "";
            if (task.Status == "FAILED")
            {
                reason = $"<Reason>{task.Reason}</Reason>";
            }

            return Xml(200, "GetServiceLinkedRoleDeletionStatusResponse",
                "<GetServiceLinkedRoleDeletionStatusResult>"
                + $"<Status>{task.Status}</Status>"
                + reason
                + "</GetServiceLinkedRoleDeletionStatusResult>");
        }
    }

    // ── OIDC providers ──────────────────────────────────────────────────────────

    private ServiceResponse ActCreateOpenIdConnectProvider(Dictionary<string, string> p)
    {
        var url = P(p, "Url");
        var clientIds = new List<string>();
        for (var idx = 1; ; idx++)
        {
            var cid = P(p, $"ClientIDList.member.{idx}");
            if (string.IsNullOrEmpty(cid))
            {
                break;
            }

            clientIds.Add(cid);
        }

        var thumbprints = new List<string>();
        for (var idx = 1; ; idx++)
        {
            var tp = P(p, $"ThumbprintList.member.{idx}");
            if (string.IsNullOrEmpty(tp))
            {
                break;
            }

            thumbprints.Add(tp);
        }

        var host = url.Replace("https://", "", StringComparison.OrdinalIgnoreCase)
            .Replace("http://", "", StringComparison.OrdinalIgnoreCase)
            .TrimEnd('/');
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:iam::{accountId}:oidc-provider/{host}";

        lock (_lock)
        {
            if (_oidcProviders.ContainsKey(arn))
            {
                return XmlError(409, "EntityAlreadyExists",
                    $"OIDC provider with url {url} already exists.");
            }

            _oidcProviders[arn] = new IamOidcProvider
            {
                Url = url,
                ClientIdList = clientIds,
                ThumbprintList = thumbprints,
                Arn = arn,
                CreateDate = Now(),
                Tags = ExtractTags(p),
            };
        }

        return Xml(200, "CreateOpenIDConnectProviderResponse",
            "<CreateOpenIDConnectProviderResult>"
            + $"<OpenIDConnectProviderArn>{arn}</OpenIDConnectProviderArn>"
            + "</CreateOpenIDConnectProviderResult>");
    }

    private ServiceResponse ActGetOpenIdConnectProvider(Dictionary<string, string> p)
    {
        var arn = P(p, "OpenIDConnectProviderArn");
        lock (_lock)
        {
            if (!_oidcProviders.TryGetValue(arn, out var prov))
            {
                return XmlError(404, "NoSuchEntity", $"OIDC provider {arn} not found.");
            }

            var clientMembers = new StringBuilder();
            foreach (var c in prov.ClientIdList)
            {
                clientMembers.Append($"<member>{c}</member>");
            }

            var thumbMembers = new StringBuilder();
            foreach (var t in prov.ThumbprintList)
            {
                thumbMembers.Append($"<member>{t}</member>");
            }

            var tagMembers = new StringBuilder();
            foreach (var t in prov.Tags)
            {
                tagMembers.Append($"<member><Key>{t.Key}</Key><Value>{t.Value}</Value></member>");
            }

            return Xml(200, "GetOpenIDConnectProviderResponse",
                "<GetOpenIDConnectProviderResult>"
                + $"<Url>{prov.Url}</Url>"
                + $"<ClientIDList>{clientMembers}</ClientIDList>"
                + $"<ThumbprintList>{thumbMembers}</ThumbprintList>"
                + $"<CreateDate>{prov.CreateDate}</CreateDate>"
                + $"<Tags>{tagMembers}</Tags>"
                + "</GetOpenIDConnectProviderResult>");
        }
    }

    private ServiceResponse ActDeleteOpenIdConnectProvider(Dictionary<string, string> p)
    {
        var arn = P(p, "OpenIDConnectProviderArn");
        lock (_lock)
        {
            if (!_oidcProviders.ContainsKey(arn))
            {
                return XmlError(404, "NoSuchEntity", $"OIDC provider {arn} not found.");
            }

            _oidcProviders.TryRemove(arn, out _);
        }

        return Xml(200, "DeleteOpenIDConnectProviderResponse", "");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    private static string P(Dictionary<string, string> p, string key) =>
        p.TryGetValue(key, out var v) ? v : "";

    private static string GenId(string prefix) =>
        prefix + Guid.NewGuid().ToString("N")[..17].ToUpperInvariant();

    private static string GenAccessKeyId() =>
        "AKIA" + Guid.NewGuid().ToString("N")[..16].ToUpperInvariant();

    private static string Now() =>
        DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

    private static List<IamTag> ExtractTags(Dictionary<string, string> p)
    {
        var tags = new List<IamTag>();
        for (var idx = 1; ; idx++)
        {
            var key = P(p, $"Tags.member.{idx}.Key");
            if (string.IsNullOrEmpty(key))
            {
                break;
            }

            var value = P(p, $"Tags.member.{idx}.Value");
            tags.Add(new IamTag { Key = key, Value = value });
        }

        return tags;
    }

    private static HashSet<string> ExtractTagKeys(Dictionary<string, string> p)
    {
        var keys = new HashSet<string>(StringComparer.Ordinal);
        for (var idx = 1; ; idx++)
        {
            var key = P(p, $"TagKeys.member.{idx}");
            if (string.IsNullOrEmpty(key))
            {
                break;
            }

            keys.Add(key);
        }

        return keys;
    }

    private static string BuildSimulateResults(Dictionary<string, string> p)
    {
        var actions = new List<string>();
        for (var idx = 1; ; idx++)
        {
            var a = P(p, $"ActionNames.member.{idx}");
            if (string.IsNullOrEmpty(a))
            {
                break;
            }

            actions.Add(a);
        }

        if (actions.Count == 0)
        {
            actions.Add("sts:AssumeRole");
        }

        var resourceArn = P(p, "ResourceArns.member.1");
        if (string.IsNullOrEmpty(resourceArn))
        {
            resourceArn = "*";
        }

        var sb = new StringBuilder();
        foreach (var action in actions)
        {
            sb.Append("<member>"
                      + $"<EvalActionName>{action}</EvalActionName>"
                      + $"<EvalResourceName>{resourceArn}</EvalResourceName>"
                      + "<EvalDecision>allowed</EvalDecision>"
                      + "<MatchedStatements></MatchedStatements>"
                      + "<MissingContextValues></MissingContextValues>"
                      + "</member>");
        }

        return sb.ToString();
    }

    // ── XML builders ────────────────────────────────────────────────────────────

    private string UserXml(string name)
    {
        var u = _users[name];
        return $"<UserName>{u.UserName}</UserName>"
               + $"<UserId>{u.UserId}</UserId>"
               + $"<Arn>{u.Arn}</Arn>"
               + $"<Path>{u.Path}</Path>"
               + $"<CreateDate>{u.CreateDate}</CreateDate>";
    }

    private string RoleXml(string name)
    {
        var r = _roles[name];
        var assumeDoc = Uri.EscapeDataString(r.AssumeRolePolicyDocument ?? "{}");
        var desc = r.Description ?? "";
        var maxDur = r.MaxSessionDuration;
        var tagsXml = "";
        if (r.Tags.Count > 0)
        {
            var sb = new StringBuilder();
            foreach (var t in r.Tags)
            {
                sb.Append($"<member><Key>{t.Key}</Key><Value>{t.Value}</Value></member>");
            }

            tagsXml = $"<Tags>{sb}</Tags>";
        }

        return $"<RoleName>{r.RoleName}</RoleName>"
               + $"<RoleId>{r.RoleId}</RoleId>"
               + $"<Arn>{r.Arn}</Arn>"
               + $"<Path>{r.Path}</Path>"
               + $"<CreateDate>{r.CreateDate}</CreateDate>"
               + $"<AssumeRolePolicyDocument>{assumeDoc}</AssumeRolePolicyDocument>"
               + $"<Description>{desc}</Description>"
               + $"<MaxSessionDuration>{maxDur}</MaxSessionDuration>"
               + tagsXml;
    }

    private string ManagedPolicyXml(string arn)
    {
        var pol = _policies[arn];
        return $"<PolicyName>{pol.PolicyName}</PolicyName>"
               + $"<Arn>{arn}</Arn>"
               + $"<PolicyId>{pol.PolicyId}</PolicyId>"
               + $"<DefaultVersionId>{pol.DefaultVersionId}</DefaultVersionId>"
               + $"<AttachmentCount>{pol.AttachmentCount}</AttachmentCount>"
               + "<IsAttachable>true</IsAttachable>"
               + $"<CreateDate>{pol.CreateDate}</CreateDate>"
               + $"<UpdateDate>{pol.UpdateDate}</UpdateDate>"
               + $"<Path>{pol.Path}</Path>";
    }

    private string GroupXml(string name)
    {
        var g = _groups[name];
        return $"<GroupName>{g.GroupName}</GroupName>"
               + $"<GroupId>{g.GroupId}</GroupId>"
               + $"<Arn>{g.Arn}</Arn>"
               + $"<Path>{g.Path}</Path>"
               + $"<CreateDate>{g.CreateDate}</CreateDate>";
    }

    private string InstanceProfileXml(string name)
    {
        var ip = _instanceProfiles[name];
        var rolesXml = new StringBuilder();
        foreach (var rname in ip.Roles)
        {
            if (_roles.ContainsKey(rname))
            {
                rolesXml.Append($"<member>{RoleXml(rname)}</member>");
            }
        }

        return $"<InstanceProfileName>{ip.InstanceProfileName}</InstanceProfileName>"
               + $"<InstanceProfileId>{ip.InstanceProfileId}</InstanceProfileId>"
               + $"<Arn>{ip.Arn}</Arn>"
               + $"<Path>{ip.Path}</Path>"
               + $"<CreateDate>{ip.CreateDate}</CreateDate>"
               + $"<Roles>{rolesXml}</Roles>";
    }

    // ── XML response helpers ────────────────────────────────────────────────────

    private static ServiceResponse Xml(int status, string rootTag, string inner)
    {
        var body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                   + $"<{rootTag} xmlns=\"{IamXmlNs}\">"
                   + inner
                   + $"<ResponseMetadata><RequestId>{Guid.NewGuid()}</RequestId></ResponseMetadata>"
                   + $"</{rootTag}>";
        return new ServiceResponse(status, XmlContentType, Encoding.UTF8.GetBytes(body));
    }

    private static ServiceResponse XmlError(int status, string code, string message)
    {
        var body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                   + $"<ErrorResponse xmlns=\"{IamXmlNs}\">"
                   + $"<Error><Code>{code}</Code><Message>{message}</Message></Error>"
                   + $"<RequestId>{Guid.NewGuid()}</RequestId>"
                   + "</ErrorResponse>";
        return new ServiceResponse(status, XmlContentType, Encoding.UTF8.GetBytes(body));
    }

    private static readonly Dictionary<string, string> XmlContentType = new(StringComparer.Ordinal)
    {
        ["Content-Type"] = "application/xml",
    };
}

// ── Data models ─────────────────────────────────────────────────────────────────

internal sealed class IamUser
{
    public required string UserName { get; set; }
    public required string Arn { get; set; }
    public required string UserId { get; set; }
    public required string CreateDate { get; set; }
    public required string Path { get; set; }
    public List<string> AttachedPolicies { get; set; } = [];
    public List<IamTag> Tags { get; set; } = [];
}

internal sealed class IamRole
{
    public required string RoleName { get; set; }
    public required string Arn { get; set; }
    public required string RoleId { get; set; }
    public required string CreateDate { get; set; }
    public required string Path { get; set; }
    public string AssumeRolePolicyDocument { get; set; } = "";
    public string Description { get; set; } = "";
    public int MaxSessionDuration { get; set; } = 3600;
    public List<string> AttachedPolicies { get; set; } = [];
    public Dictionary<string, string> InlinePolicies { get; set; } = new(StringComparer.Ordinal);
    public List<IamTag> Tags { get; set; } = [];
}

internal sealed class IamPolicy
{
    public required string PolicyName { get; set; }
    public required string Arn { get; set; }
    public required string PolicyId { get; set; }
    public required string CreateDate { get; set; }
    public string UpdateDate { get; set; } = "";
    public string DefaultVersionId { get; set; } = "v1";
    public int AttachmentCount { get; set; }
    public bool IsAttachable { get; set; } = true;
    public string Path { get; set; } = "/";
    public List<IamTag> Tags { get; set; } = [];
    public Dictionary<string, IamPolicyVersion> Versions { get; set; } = new(StringComparer.Ordinal);
}

internal sealed class IamPolicyVersion
{
    public string Document { get; set; } = "";
    public required string VersionId { get; set; }
    public bool IsDefaultVersion { get; set; }
    public required string CreateDate { get; set; }
}

internal sealed class IamAccessKey
{
    public required string UserName { get; set; }
    public required string AccessKeyId { get; set; }
    public required string SecretAccessKey { get; set; }
    public required string Status { get; set; }
    public required string CreateDate { get; set; }
}

internal sealed class IamInstanceProfile
{
    public required string InstanceProfileName { get; set; }
    public required string InstanceProfileId { get; set; }
    public required string Arn { get; set; }
    public required string Path { get; set; }
    public required string CreateDate { get; set; }
    public List<string> Roles { get; set; } = [];
}

internal sealed class IamGroup
{
    public required string GroupName { get; set; }
    public required string GroupId { get; set; }
    public required string Arn { get; set; }
    public required string Path { get; set; }
    public required string CreateDate { get; set; }
    public List<string> Users { get; set; } = [];
    public List<string> AttachedPolicies { get; set; } = [];
}

internal sealed class IamTag
{
    public required string Key { get; set; }
    public required string Value { get; set; }
}

internal sealed class IamOidcProvider
{
    public required string Url { get; set; }
    public List<string> ClientIdList { get; set; } = [];
    public List<string> ThumbprintList { get; set; } = [];
    public required string Arn { get; set; }
    public required string CreateDate { get; set; }
    public List<IamTag> Tags { get; set; } = [];
}

internal sealed class IamDeletionTask
{
    public required string Status { get; set; }
    public required string RoleName { get; set; }
    public string Reason { get; set; } = "";
}
