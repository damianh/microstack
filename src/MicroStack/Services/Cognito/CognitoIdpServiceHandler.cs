using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using MicroStack.Internal;

namespace MicroStack.Services.Cognito;

/// <summary>
/// Cognito Identity Provider (User Pools) service handler.
/// X-Amz-Target: AWSCognitoIdentityProviderService.*
///
/// Port of ministack/services/cognito.py (IDP portion).
/// </summary>
internal sealed partial class CognitoIdpServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // ── Shared state (also read by CognitoIdentityServiceHandler) ──────────
    internal readonly AccountScopedDictionary<string, Dictionary<string, object?>> UserPools = new();
    internal readonly AccountScopedDictionary<string, string> PoolDomainMap = new(); // domain -> pool_id

    // ── RSA key pair for JWKS / token signing ──────────────────────────────
    private readonly RSA _rsaKey = RSA.Create(2048);
    private readonly string _jwksN;
    private readonly string _jwksE;

    internal CognitoIdpServiceHandler()
    {
        var pubParams = _rsaKey.ExportParameters(false);
        _jwksN = Base64UrlEncode(pubParams.Modulus!);
        _jwksE = Base64UrlEncode(pubParams.Exponent!);
    }

    // ── IServiceHandler ────────────────────────────────────────────────────
    public string ServiceName => "cognito-idp";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var path = request.Path;

        // ── Well-known endpoints ───────────────────────────────────────────
        if (path.Contains("/.well-known/jwks.json", StringComparison.OrdinalIgnoreCase)
            && request.Method == "GET")
        {
            return Task.FromResult(HandleJwks());
        }

        if (path.Contains("/.well-known/openid-configuration", StringComparison.OrdinalIgnoreCase)
            && request.Method == "GET")
        {
            var poolId = ExtractPoolIdFromPath(path);
            return Task.FromResult(HandleOpenIdConfiguration(poolId));
        }

        // ── OAuth2 token endpoint ──────────────────────────────────────────
        if (path.StartsWith("/oauth2/token", StringComparison.OrdinalIgnoreCase))
        {
            return Task.FromResult(HandleOAuth2Token(request));
        }

        // ── X-Amz-Target dispatch ──────────────────────────────────────────
        var target = request.GetHeader("x-amz-target") ?? "";

        JsonElement data;
        if (request.Body.Length > 0)
        {
            try
            {
                using var doc = JsonDocument.Parse(request.Body);
                data = doc.RootElement.Clone();
            }
            catch (JsonException)
            {
                return Task.FromResult(
                    AwsResponseHelpers.ErrorResponseJson("SerializationException", "Invalid JSON", 400));
            }
        }
        else
        {
            data = JsonDocument.Parse("{}"u8.ToArray()).RootElement.Clone();
        }

        if (!target.StartsWith("AWSCognitoIdentityProviderService.", StringComparison.Ordinal))
        {
            return Task.FromResult(
                AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown Cognito target: {target}", 400));
        }

        var action = target[(target.LastIndexOf('.') + 1)..];
        var response = DispatchIdp(action, data);
        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            UserPools.Clear();
            PoolDomainMap.Clear();
        }
    }

    public object? GetState() => null;
    public void RestoreState(object state) { }

    // ── IDP Dispatcher ─────────────────────────────────────────────────────
    private ServiceResponse DispatchIdp(string action, JsonElement data)
    {
        return action switch
        {
            // User Pool CRUD
            "CreateUserPool" => ActCreateUserPool(data),
            "DeleteUserPool" => ActDeleteUserPool(data),
            "DescribeUserPool" => ActDescribeUserPool(data),
            "ListUserPools" => ActListUserPools(data),
            "UpdateUserPool" => ActUpdateUserPool(data),
            // User Pool Client CRUD
            "CreateUserPoolClient" => ActCreateUserPoolClient(data),
            "DeleteUserPoolClient" => ActDeleteUserPoolClient(data),
            "DescribeUserPoolClient" => ActDescribeUserPoolClient(data),
            "ListUserPoolClients" => ActListUserPoolClients(data),
            "UpdateUserPoolClient" => ActUpdateUserPoolClient(data),
            // User management
            "AdminCreateUser" => ActAdminCreateUser(data),
            "AdminDeleteUser" => ActAdminDeleteUser(data),
            "AdminGetUser" => ActAdminGetUser(data),
            "ListUsers" => ActListUsers(data),
            "AdminSetUserPassword" => ActAdminSetUserPassword(data),
            "AdminUpdateUserAttributes" => ActAdminUpdateUserAttributes(data),
            "AdminConfirmSignUp" => ActAdminConfirmSignUp(data),
            "AdminDisableUser" => ActAdminDisableUser(data),
            "AdminEnableUser" => ActAdminEnableUser(data),
            "AdminResetUserPassword" => ActAdminResetUserPassword(data),
            "AdminUserGlobalSignOut" => ActAdminUserGlobalSignOut(data),
            "AdminListGroupsForUser" => ActAdminListGroupsForUser(data),
            "AdminListUserAuthEvents" => ActAdminListUserAuthEvents(data),
            "AdminAddUserToGroup" => ActAdminAddUserToGroup(data),
            "AdminRemoveUserFromGroup" => ActAdminRemoveUserFromGroup(data),
            // Auth flows
            "AdminInitiateAuth" => ActAdminInitiateAuth(data),
            "AdminRespondToAuthChallenge" => ActAdminRespondToAuthChallenge(data),
            "InitiateAuth" => ActInitiateAuth(data),
            "RespondToAuthChallenge" => ActRespondToAuthChallenge(data),
            "GlobalSignOut" => ActGlobalSignOut(),
            "RevokeToken" => ActRevokeToken(),
            // Self-service
            "SignUp" => ActSignUp(data),
            "ConfirmSignUp" => ActConfirmSignUp(data),
            "ForgotPassword" => ActForgotPassword(data),
            "ConfirmForgotPassword" => ActConfirmForgotPassword(data),
            "ChangePassword" => ActChangePassword(data),
            "GetUser" => ActGetUser(data),
            "UpdateUserAttributes" => ActUpdateUserAttributes(data),
            "DeleteUser" => ActDeleteUser(data),
            // Groups
            "CreateGroup" => ActCreateGroup(data),
            "DeleteGroup" => ActDeleteGroup(data),
            "GetGroup" => ActGetGroup(data),
            "ListGroups" => ActListGroups(data),
            "ListUsersInGroup" => ActListUsersInGroup(data),
            // Domain
            "CreateUserPoolDomain" => ActCreateUserPoolDomain(data),
            "DeleteUserPoolDomain" => ActDeleteUserPoolDomain(data),
            "DescribeUserPoolDomain" => ActDescribeUserPoolDomain(data),
            // MFA
            "GetUserPoolMfaConfig" => ActGetUserPoolMfaConfig(data),
            "SetUserPoolMfaConfig" => ActSetUserPoolMfaConfig(data),
            "AssociateSoftwareToken" => ActAssociateSoftwareToken(),
            "VerifySoftwareToken" => ActVerifySoftwareToken(data),
            "AdminSetUserMFAPreference" => ActAdminSetUserMfaPreference(data),
            "SetUserMFAPreference" => ActSetUserMfaPreference(data),
            // Tags
            "TagResource" => ActTagResource(data),
            "UntagResource" => ActUntagResource(data),
            "ListTagsForResource" => ActListTagsForResource(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown Cognito IDP action: {action}", 400),
        };
    }

    // ═══════════════════════════════════════════════════════════════════════
    // USER POOL CRUD
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActCreateUserPool(JsonElement data)
    {
        var name = data.GetStringOrDefault("PoolName");
        if (string.IsNullOrEmpty(name))
        {
            return Error("InvalidParameterException", "PoolName is required.");
        }

        lock (_lock)
        {
            var pid = GeneratePoolId();
            var now = NowEpoch();
            var pool = new Dictionary<string, object?>
            {
                ["Id"] = pid,
                ["Name"] = name,
                ["Arn"] = PoolArn(pid),
                ["CreationDate"] = now,
                ["LastModifiedDate"] = now,
                ["Policies"] = data.GetPropertyOrNull("Policies")?.ToObject() ?? DefaultPasswordPolicy(),
                ["Schema"] = data.GetPropertyOrNull("Schema")?.ToObjectList() ?? new List<object?>(),
                ["AutoVerifiedAttributes"] = data.GetPropertyOrNull("AutoVerifiedAttributes")?.ToStringList() ?? new List<string>(),
                ["AliasAttributes"] = data.GetPropertyOrNull("AliasAttributes")?.ToStringList() ?? new List<string>(),
                ["UsernameAttributes"] = data.GetPropertyOrNull("UsernameAttributes")?.ToStringList() ?? new List<string>(),
                ["MfaConfiguration"] = data.GetStringOrDefault("MfaConfiguration", "OFF"),
                ["EstimatedNumberOfUsers"] = 0,
                ["UserPoolTags"] = data.GetPropertyOrNull("UserPoolTags")?.ToStringDictionary() ?? new Dictionary<string, string>(),
                ["AdminCreateUserConfig"] = data.GetPropertyOrNull("AdminCreateUserConfig")?.ToObject()
                    ?? new Dictionary<string, object?> { ["AllowAdminCreateUserOnly"] = false, ["UnusedAccountValidityDays"] = 7 },
                ["UsernameConfiguration"] = data.GetPropertyOrNull("UsernameConfiguration")?.ToObject()
                    ?? new Dictionary<string, object?> { ["CaseSensitive"] = false },
                ["AccountRecoverySetting"] = data.GetPropertyOrNull("AccountRecoverySetting")?.ToObject() ?? new Dictionary<string, object?>(),
                ["Domain"] = null,
                // Internal state
                ["_clients"] = new Dictionary<string, Dictionary<string, object?>>(),
                ["_users"] = new Dictionary<string, Dictionary<string, object?>>(),
                ["_groups"] = new Dictionary<string, Dictionary<string, object?>>(),
            };
            UserPools[pid] = pool;
            return Json(new Dictionary<string, object?> { ["UserPool"] = PoolOut(pool) });
        }
    }

    private ServiceResponse ActDeleteUserPool(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var domain = pool!["Domain"] as string;
            if (!string.IsNullOrEmpty(domain))
            {
                PoolDomainMap.TryRemove(domain, out _);
            }
            UserPools.TryRemove(pid, out _);
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDescribeUserPool(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        return Json(new Dictionary<string, object?> { ["UserPool"] = PoolOut(pool!) });
    }

    private ServiceResponse ActListUserPools(JsonElement data)
    {
        var maxResults = Math.Min(data.GetIntOrDefault("MaxResults", 60), 60);
        var nextToken = data.GetStringOrDefault("NextToken");
        var pools = UserPools.Values
            .OrderBy(p => (double)p["CreationDate"]!)
            .ToList();
        var start = string.IsNullOrEmpty(nextToken) ? 0 : int.Parse(nextToken);
        var page = pools.Skip(start).Take(maxResults).ToList();
        var resp = new Dictionary<string, object?>
        {
            ["UserPools"] = page.Select(p => new Dictionary<string, object?>
            {
                ["Id"] = p["Id"],
                ["Name"] = p["Name"],
                ["LastModifiedDate"] = p["LastModifiedDate"],
                ["CreationDate"] = p["CreationDate"],
            }).ToList(),
        };
        if (start + maxResults < pools.Count)
        {
            resp["NextToken"] = (start + maxResults).ToString();
        }
        return Json(resp);
    }

    private ServiceResponse ActUpdateUserPool(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var updatable = new[]
            {
                "Policies", "AutoVerifiedAttributes", "SmsVerificationMessage",
                "EmailVerificationMessage", "EmailVerificationSubject",
                "SmsAuthenticationMessage", "MfaConfiguration", "DeviceConfiguration",
                "EmailConfiguration", "SmsConfiguration", "UserPoolTags",
                "AdminCreateUserConfig", "UserPoolAddOns", "VerificationMessageTemplate",
                "AccountRecoverySetting",
            };
            foreach (var key in updatable)
            {
                var prop = data.GetPropertyOrNull(key);
                if (prop is not null)
                {
                    pool![key] = prop.Value.ToObject();
                }
            }
            pool!["LastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?>());
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // USER POOL CLIENT CRUD
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActCreateUserPoolClient(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var clients = GetClients(pool!);
            var cid = GenerateClientId();
            var now = NowEpoch();
            var generateSecret = data.GetBoolOrDefault("GenerateSecret");
            var client = new Dictionary<string, object?>
            {
                ["UserPoolId"] = pid,
                ["ClientName"] = data.GetStringOrDefault("ClientName"),
                ["ClientId"] = cid,
                ["ClientSecret"] = generateSecret ? GenerateClientSecret() : null,
                ["CreationDate"] = now,
                ["LastModifiedDate"] = now,
                ["RefreshTokenValidity"] = data.GetIntOrDefault("RefreshTokenValidity", 30),
                ["AccessTokenValidity"] = data.GetIntOrDefault("AccessTokenValidity", 60),
                ["IdTokenValidity"] = data.GetIntOrDefault("IdTokenValidity", 60),
                ["ExplicitAuthFlows"] = data.GetPropertyOrNull("ExplicitAuthFlows")?.ToStringList() ?? new List<string>(),
                ["AllowedOAuthFlows"] = data.GetPropertyOrNull("AllowedOAuthFlows")?.ToStringList() ?? new List<string>(),
                ["AllowedOAuthScopes"] = data.GetPropertyOrNull("AllowedOAuthScopes")?.ToStringList() ?? new List<string>(),
                ["CallbackURLs"] = data.GetPropertyOrNull("CallbackURLs")?.ToStringList() ?? new List<string>(),
                ["LogoutURLs"] = data.GetPropertyOrNull("LogoutURLs")?.ToStringList() ?? new List<string>(),
                ["SupportedIdentityProviders"] = data.GetPropertyOrNull("SupportedIdentityProviders")?.ToStringList() ?? new List<string>(),
                ["PreventUserExistenceErrors"] = data.GetStringOrDefault("PreventUserExistenceErrors", "ENABLED"),
                ["EnableTokenRevocation"] = data.GetBoolOrDefault("EnableTokenRevocation", true),
            };
            clients[cid] = client;
            return Json(new Dictionary<string, object?> { ["UserPoolClient"] = ClientOut(client) });
        }
    }

    private ServiceResponse ActDeleteUserPoolClient(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var cid = data.GetStringOrDefault("ClientId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var clients = GetClients(pool!);
            if (!clients.ContainsKey(cid))
            {
                return Error("ResourceNotFoundException", $"Client {cid} not found.");
            }
            clients.Remove(cid);
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDescribeUserPoolClient(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var cid = data.GetStringOrDefault("ClientId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        var clients = GetClients(pool!);
        if (!clients.TryGetValue(cid, out var client))
        {
            return Error("ResourceNotFoundException", $"Client {cid} not found.");
        }
        return Json(new Dictionary<string, object?> { ["UserPoolClient"] = ClientOut(client) });
    }

    private ServiceResponse ActListUserPoolClients(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        var clients = GetClients(pool!);
        var maxResults = Math.Min(data.GetIntOrDefault("MaxResults", 60), 60);
        var nextToken = data.GetStringOrDefault("NextToken");
        var sorted = clients.Values.OrderBy(c => (double)c["CreationDate"]!).ToList();
        var start = string.IsNullOrEmpty(nextToken) ? 0 : int.Parse(nextToken);
        var page = sorted.Skip(start).Take(maxResults).ToList();
        var resp = new Dictionary<string, object?>
        {
            ["UserPoolClients"] = page.Select(c => new Dictionary<string, object?>
            {
                ["ClientId"] = c["ClientId"],
                ["UserPoolId"] = pid,
                ["ClientName"] = c["ClientName"],
            }).ToList(),
        };
        if (start + maxResults < sorted.Count)
        {
            resp["NextToken"] = (start + maxResults).ToString();
        }
        return Json(resp);
    }

    private ServiceResponse ActUpdateUserPoolClient(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var cid = data.GetStringOrDefault("ClientId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var clients = GetClients(pool!);
            if (!clients.TryGetValue(cid, out var client))
            {
                return Error("ResourceNotFoundException", $"Client {cid} not found.");
            }
            var updatable = new[]
            {
                "ClientName", "RefreshTokenValidity", "AccessTokenValidity", "IdTokenValidity",
                "ExplicitAuthFlows", "SupportedIdentityProviders", "CallbackURLs", "LogoutURLs",
                "AllowedOAuthFlows", "AllowedOAuthScopes", "PreventUserExistenceErrors",
                "EnableTokenRevocation",
            };
            foreach (var key in updatable)
            {
                var prop = data.GetPropertyOrNull(key);
                if (prop is not null)
                {
                    client[key] = prop.Value.ToObject();
                }
            }
            client["LastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?> { ["UserPoolClient"] = ClientOut(client) });
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // USER MANAGEMENT
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActAdminCreateUser(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var users = GetUsers(pool!);
            var username = data.GetStringOrDefault("Username");
            if (string.IsNullOrEmpty(username))
            {
                return Error("InvalidParameterException", "Username is required.");
            }
            if (users.ContainsKey(username))
            {
                return Error("UsernameExistsException", "User account already exists.");
            }
            var now = NowEpoch();
            var attrs = data.GetPropertyOrNull("UserAttributes")?.ToAttributeList() ?? [];
            var attrDict = AttrListToDict(attrs);
            if (!attrDict.ContainsKey("sub"))
            {
                attrDict["sub"] = HashHelpers.NewUuid();
            }
            attrs = DictToAttrList(attrDict);
            var user = new Dictionary<string, object?>
            {
                ["Username"] = username,
                ["Attributes"] = attrs,
                ["UserCreateDate"] = now,
                ["UserLastModifiedDate"] = now,
                ["Enabled"] = true,
                ["UserStatus"] = "FORCE_CHANGE_PASSWORD",
                ["MFAOptions"] = new List<object?>(),
                ["_password"] = data.GetStringOrDefault("TemporaryPassword", GenerateTempPassword()),
                ["_groups"] = new List<string>(),
                ["_tokens"] = new List<string>(),
                ["_mfa_enabled"] = new List<string>(),
                ["_preferred_mfa"] = "",
            };
            users[username] = user;
            pool!["EstimatedNumberOfUsers"] = users.Count;
            return Json(new Dictionary<string, object?> { ["User"] = UserOut(user) });
        }
    }

    private ServiceResponse ActAdminDeleteUser(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var users = GetUsers(pool!);
            var username = data.GetStringOrDefault("Username");
            if (!users.ContainsKey(username))
            {
                return Error("UserNotFoundException", $"User {username} does not exist.");
            }
            users.Remove(username);
            pool!["EstimatedNumberOfUsers"] = users.Count;
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActAdminGetUser(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        var username = data.GetStringOrDefault("Username");
        var (user, uerr) = ResolveUser(pool!, username);
        if (uerr is not null) return uerr;
        var out_ = UserOut(user!);
        out_["UserAttributes"] = out_["Attributes"];
        out_.Remove("Attributes");
        out_["UserMFASettingList"] = GetStringList(user!, "_mfa_enabled");
        out_["PreferredMfaSetting"] = GetString(user!, "_preferred_mfa");
        return Json(out_);
    }

    private ServiceResponse ActListUsers(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        var users = GetUsers(pool!);
        var limit = Math.Min(data.GetIntOrDefault("Limit", 60), 60);
        var paginationToken = data.GetStringOrDefault("PaginationToken");
        var filterStr = data.GetStringOrDefault("Filter");
        var userList = users.Values.ToList();
        if (!string.IsNullOrEmpty(filterStr))
        {
            userList = ApplyUserFilter(userList, filterStr);
        }
        var start = string.IsNullOrEmpty(paginationToken) ? 0 : int.Parse(paginationToken);
        var page = userList.Skip(start).Take(limit).ToList();
        var resp = new Dictionary<string, object?>
        {
            ["Users"] = page.Select(UserOut).ToList<object?>(),
        };
        if (start + limit < userList.Count)
        {
            resp["PaginationToken"] = (start + limit).ToString();
        }
        return Json(resp);
    }

    private ServiceResponse ActAdminSetUserPassword(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var username = data.GetStringOrDefault("Username");
            var (user, uerr) = ResolveUser(pool!, username);
            if (uerr is not null) return uerr;
            user!["_password"] = data.GetStringOrDefault("Password");
            var permanent = data.GetBoolOrDefault("Permanent");
            user["UserStatus"] = permanent ? "CONFIRMED" : "FORCE_CHANGE_PASSWORD";
            user["UserLastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActAdminUpdateUserAttributes(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var username = data.GetStringOrDefault("Username");
            var (user, uerr) = ResolveUser(pool!, username);
            if (uerr is not null) return uerr;
            var existing = GetAttributeList(user!);
            var updates = data.GetPropertyOrNull("UserAttributes")?.ToAttributeList() ?? [];
            user!["Attributes"] = MergeAttributes(existing, updates);
            user["UserLastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActAdminConfirmSignUp(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var username = data.GetStringOrDefault("Username");
            var (user, uerr) = ResolveUser(pool!, username);
            if (uerr is not null) return uerr;
            user!["UserStatus"] = "CONFIRMED";
            user["UserLastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActAdminDisableUser(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var username = data.GetStringOrDefault("Username");
            var (user, uerr) = ResolveUser(pool!, username);
            if (uerr is not null) return uerr;
            user!["Enabled"] = false;
            user["UserLastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActAdminEnableUser(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var username = data.GetStringOrDefault("Username");
            var (user, uerr) = ResolveUser(pool!, username);
            if (uerr is not null) return uerr;
            user!["Enabled"] = true;
            user["UserLastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActAdminResetUserPassword(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var username = data.GetStringOrDefault("Username");
            var (user, uerr) = ResolveUser(pool!, username);
            if (uerr is not null) return uerr;
            user!["UserStatus"] = "RESET_REQUIRED";
            user["UserLastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActAdminUserGlobalSignOut(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var username = data.GetStringOrDefault("Username");
            var (user, uerr) = ResolveUser(pool!, username);
            if (uerr is not null) return uerr;
            user!["_tokens"] = new List<string>();
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActAdminListGroupsForUser(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        var username = data.GetStringOrDefault("Username");
        var (user, uerr) = ResolveUser(pool!, username);
        if (uerr is not null) return uerr;
        var groups = GetGroups(pool!);
        var userGroups = GetStringList(user!, "_groups");
        var result = userGroups
            .Where(g => groups.ContainsKey(g))
            .Select(g => GroupOut(groups[g]))
            .ToList<object?>();
        return Json(new Dictionary<string, object?> { ["Groups"] = result });
    }

    private ServiceResponse ActAdminListUserAuthEvents(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        var username = data.GetStringOrDefault("Username");
        var (_, uerr) = ResolveUser(pool!, username);
        if (uerr is not null) return uerr;
        return Json(new Dictionary<string, object?> { ["AuthEvents"] = new List<object?>() });
    }

    private ServiceResponse ActAdminAddUserToGroup(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var username = data.GetStringOrDefault("Username");
            var groupName = data.GetStringOrDefault("GroupName");
            var (user, uerr) = ResolveUser(pool!, username);
            if (uerr is not null) return uerr;
            var groups = GetGroups(pool!);
            if (!groups.ContainsKey(groupName))
            {
                return Error("ResourceNotFoundException", $"Group {groupName} not found.");
            }
            var userGroupList = GetStringList(user!, "_groups");
            if (!userGroupList.Contains(groupName))
            {
                userGroupList.Add(groupName);
                GetGroupMembers(groups[groupName]).Add(username);
            }
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActAdminRemoveUserFromGroup(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var username = data.GetStringOrDefault("Username");
            var groupName = data.GetStringOrDefault("GroupName");
            var (user, uerr) = ResolveUser(pool!, username);
            if (uerr is not null) return uerr;
            var userGroupList = GetStringList(user!, "_groups");
            userGroupList.Remove(groupName);
            var groups = GetGroups(pool!);
            if (groups.TryGetValue(groupName, out var group))
            {
                GetGroupMembers(group).Remove(username);
            }
            return Json(new Dictionary<string, object?>());
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // AUTH FLOWS
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActAdminInitiateAuth(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var cid = data.GetStringOrDefault("ClientId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        var clients = GetClients(pool!);
        if (!clients.ContainsKey(cid))
        {
            return Error("ResourceNotFoundException", $"Client {cid} not found.");
        }

        var authFlow = data.GetStringOrDefault("AuthFlow");
        var authParams = data.GetPropertyOrNull("AuthParameters")?.ToStringDictionary()
                         ?? new Dictionary<string, string>();

        if (authFlow is "ADMIN_USER_PASSWORD_AUTH" or "ADMIN_NO_SRP_AUTH")
        {
            var username = authParams.GetValueOrDefault("USERNAME", "");
            var password = authParams.GetValueOrDefault("PASSWORD", "");
            var users = GetUsers(pool!);
            if (!users.TryGetValue(username, out var user))
            {
                return Error("UserNotFoundException", "User does not exist.");
            }
            if (user["Enabled"] is false)
            {
                return Error("NotAuthorizedException", "User is disabled.");
            }
            var storedPassword = GetString(user, "_password");
            if (!string.IsNullOrEmpty(storedPassword) && storedPassword != password)
            {
                return Error("NotAuthorizedException", "Incorrect username or password.");
            }
            if (GetString(user, "UserStatus") == "FORCE_CHANGE_PASSWORD")
            {
                return Json(new Dictionary<string, object?>
                {
                    ["ChallengeName"] = "NEW_PASSWORD_REQUIRED",
                    ["Session"] = GenerateSession(),
                    ["ChallengeParameters"] = new Dictionary<string, object?>
                    {
                        ["USER_ID_FOR_SRP"] = username,
                        ["requiredAttributes"] = "[]",
                        ["userAttributes"] = JsonSerializer.Serialize(AttrListToDict(GetAttributeList(user))),
                    },
                });
            }
            var mfaChallenge = MfaChallengeForUser(pool!, user, username);
            if (mfaChallenge is not null)
            {
                return Json(mfaChallenge);
            }
            return Json(new Dictionary<string, object?> { ["AuthenticationResult"] = BuildAuthResult(pid, cid, user) });
        }

        if (authFlow is "REFRESH_TOKEN_AUTH" or "REFRESH_TOKEN")
        {
            var refreshToken = authParams.GetValueOrDefault("REFRESH_TOKEN", "");
            if (string.IsNullOrEmpty(refreshToken))
            {
                return Error("NotAuthorizedException", "Refresh token is missing.");
            }
            var user = UserFromToken(refreshToken, pool!);
            if (user is null)
            {
                var users = GetUsers(pool!);
                if (users.Count == 0)
                {
                    return Error("NotAuthorizedException", "No users in pool.");
                }
                user = users.Values.First();
            }
            var result = BuildAuthResult(pid, cid, user);
            result.Remove("RefreshToken");
            return Json(new Dictionary<string, object?> { ["AuthenticationResult"] = result });
        }

        return Error("InvalidParameterException", $"Unsupported AuthFlow: {authFlow}");
    }

    private ServiceResponse ActAdminRespondToAuthChallenge(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var cid = data.GetStringOrDefault("ClientId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        var challengeName = data.GetStringOrDefault("ChallengeName");
        var responses = data.GetPropertyOrNull("ChallengeResponses")?.ToStringDictionary()
                        ?? new Dictionary<string, string>();

        if (challengeName == "NEW_PASSWORD_REQUIRED")
        {
            var username = responses.GetValueOrDefault("USERNAME", "");
            var newPassword = responses.GetValueOrDefault("NEW_PASSWORD", "");
            var users = GetUsers(pool!);
            if (!users.TryGetValue(username, out var user))
            {
                return Error("UserNotFoundException", "User does not exist.");
            }
            if (!string.IsNullOrEmpty(newPassword))
            {
                user["_password"] = newPassword;
            }
            user["UserStatus"] = "CONFIRMED";
            user["UserLastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?> { ["AuthenticationResult"] = BuildAuthResult(pid, cid, user) });
        }

        if (challengeName is "SMS_MFA" or "SOFTWARE_TOKEN_MFA" or "MFA_SETUP")
        {
            var username = responses.GetValueOrDefault("USERNAME", "");
            var users = GetUsers(pool!);
            if (!users.TryGetValue(username, out var user))
            {
                return Error("UserNotFoundException", "User does not exist.");
            }
            return Json(new Dictionary<string, object?> { ["AuthenticationResult"] = BuildAuthResult(pid, cid, user) });
        }

        return Error("InvalidParameterException", $"Unsupported challenge: {challengeName}");
    }

    private ServiceResponse ActInitiateAuth(JsonElement data)
    {
        var cid = data.GetStringOrDefault("ClientId");
        var authFlow = data.GetStringOrDefault("AuthFlow");
        var authParams = data.GetPropertyOrNull("AuthParameters")?.ToStringDictionary()
                         ?? new Dictionary<string, string>();

        var (pool, pid) = FindPoolByClientId(cid);
        if (pool is null)
        {
            return Error("ResourceNotFoundException", $"Client {cid} not found.");
        }

        if (authFlow == "USER_PASSWORD_AUTH")
        {
            var username = authParams.GetValueOrDefault("USERNAME", "");
            var password = authParams.GetValueOrDefault("PASSWORD", "");
            var users = GetUsers(pool);
            if (!users.TryGetValue(username, out var user))
            {
                return Error("UserNotFoundException", "User does not exist.");
            }
            if (user["Enabled"] is false)
            {
                return Error("NotAuthorizedException", "User is disabled.");
            }
            var storedPassword = GetString(user, "_password");
            if (!string.IsNullOrEmpty(storedPassword) && storedPassword != password)
            {
                return Error("NotAuthorizedException", "Incorrect username or password.");
            }
            if (GetString(user, "UserStatus") == "FORCE_CHANGE_PASSWORD")
            {
                return Json(new Dictionary<string, object?>
                {
                    ["ChallengeName"] = "NEW_PASSWORD_REQUIRED",
                    ["Session"] = GenerateSession(),
                    ["ChallengeParameters"] = new Dictionary<string, object?>
                    {
                        ["USER_ID_FOR_SRP"] = username,
                        ["requiredAttributes"] = "[]",
                        ["userAttributes"] = JsonSerializer.Serialize(AttrListToDict(GetAttributeList(user))),
                    },
                });
            }
            var mfaChallenge = MfaChallengeForUser(pool, user, username);
            if (mfaChallenge is not null)
            {
                return Json(mfaChallenge);
            }
            return Json(new Dictionary<string, object?> { ["AuthenticationResult"] = BuildAuthResult(pid!, cid, user) });
        }

        if (authFlow is "REFRESH_TOKEN_AUTH" or "REFRESH_TOKEN")
        {
            var refreshToken = authParams.GetValueOrDefault("REFRESH_TOKEN", "");
            if (string.IsNullOrEmpty(refreshToken))
            {
                return Error("NotAuthorizedException", "Refresh token is missing.");
            }
            var user = UserFromToken(refreshToken, pool);
            if (user is null)
            {
                var users = GetUsers(pool);
                if (users.Count == 0)
                {
                    return Error("NotAuthorizedException", "No users in pool.");
                }
                user = users.Values.First();
            }
            var result = BuildAuthResult(pid!, cid, user);
            result.Remove("RefreshToken");
            return Json(new Dictionary<string, object?> { ["AuthenticationResult"] = result });
        }

        if (authFlow == "USER_SRP_AUTH")
        {
            var username = authParams.GetValueOrDefault("USERNAME", "");
            return Json(new Dictionary<string, object?>
            {
                ["ChallengeName"] = "PASSWORD_VERIFIER",
                ["Session"] = GenerateSession(),
                ["ChallengeParameters"] = new Dictionary<string, object?>
                {
                    ["USER_ID_FOR_SRP"] = username,
                    ["SRP_B"] = Convert.ToHexString(RandomNumberGenerator.GetBytes(128)),
                    ["SALT"] = Convert.ToHexString(RandomNumberGenerator.GetBytes(16)),
                    ["SECRET_BLOCK"] = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)),
                },
            });
        }

        return Error("InvalidParameterException", $"Unsupported AuthFlow: {authFlow}");
    }

    private ServiceResponse ActRespondToAuthChallenge(JsonElement data)
    {
        var cid = data.GetStringOrDefault("ClientId");
        var challengeName = data.GetStringOrDefault("ChallengeName");
        var responses = data.GetPropertyOrNull("ChallengeResponses")?.ToStringDictionary()
                        ?? new Dictionary<string, string>();

        var (pool, pid) = FindPoolByClientId(cid);
        if (pool is null)
        {
            return Error("ResourceNotFoundException", $"Client {cid} not found.");
        }

        if (challengeName is "NEW_PASSWORD_REQUIRED" or "PASSWORD_VERIFIER")
        {
            var username = responses.GetValueOrDefault("USERNAME", "");
            var newPassword = responses.GetValueOrDefault("NEW_PASSWORD", "")
                              ?? responses.GetValueOrDefault("PASSWORD", "");
            var users = GetUsers(pool);
            if (!users.TryGetValue(username, out var user))
            {
                return Error("UserNotFoundException", "User does not exist.");
            }
            if (!string.IsNullOrEmpty(newPassword))
            {
                user["_password"] = newPassword;
            }
            user["UserStatus"] = "CONFIRMED";
            user["UserLastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?> { ["AuthenticationResult"] = BuildAuthResult(pid!, cid, user) });
        }

        if (challengeName is "SOFTWARE_TOKEN_MFA" or "MFA_SETUP")
        {
            var username = responses.GetValueOrDefault("USERNAME", "");
            var users = GetUsers(pool);
            if (!users.TryGetValue(username, out var user))
            {
                return Error("UserNotFoundException", "User does not exist.");
            }
            return Json(new Dictionary<string, object?> { ["AuthenticationResult"] = BuildAuthResult(pid!, cid, user) });
        }

        return Error("InvalidParameterException", $"Unsupported challenge: {challengeName}");
    }

    private static ServiceResponse ActGlobalSignOut() =>
        Json(new Dictionary<string, object?>());

    private static ServiceResponse ActRevokeToken() =>
        Json(new Dictionary<string, object?>());

    // ═══════════════════════════════════════════════════════════════════════
    // SELF-SERVICE (public-facing)
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActSignUp(JsonElement data)
    {
        var cid = data.GetStringOrDefault("ClientId");
        var username = data.GetStringOrDefault("Username");
        var password = data.GetStringOrDefault("Password");

        lock (_lock)
        {
            var (pool, pid) = FindPoolByClientId(cid);
            if (pool is null)
            {
                return Error("ResourceNotFoundException", $"Client {cid} not found.");
            }
            var users = GetUsers(pool);
            if (users.ContainsKey(username))
            {
                return Error("UsernameExistsException", "User already exists.");
            }
            var now = NowEpoch();
            var attrs = data.GetPropertyOrNull("UserAttributes")?.ToAttributeList() ?? [];
            var attrDict = AttrListToDict(attrs);
            if (!attrDict.ContainsKey("sub"))
            {
                attrDict["sub"] = HashHelpers.NewUuid();
            }
            attrs = DictToAttrList(attrDict);
            var user = new Dictionary<string, object?>
            {
                ["Username"] = username,
                ["Attributes"] = attrs,
                ["UserCreateDate"] = now,
                ["UserLastModifiedDate"] = now,
                ["Enabled"] = true,
                ["UserStatus"] = "UNCONFIRMED",
                ["MFAOptions"] = new List<object?>(),
                ["_password"] = password,
                ["_groups"] = new List<string>(),
                ["_tokens"] = new List<string>(),
                ["_confirmation_code"] = "123456",
                ["_mfa_enabled"] = new List<string>(),
                ["_preferred_mfa"] = "",
            };
            users[username] = user;
            pool["EstimatedNumberOfUsers"] = users.Count;
            var resp = new Dictionary<string, object?>
            {
                ["UserConfirmed"] = false,
                ["UserSub"] = attrDict["sub"],
            };
            if (attrDict.TryGetValue("email", out var email))
            {
                resp["CodeDeliveryDetails"] = new Dictionary<string, object?>
                {
                    ["Destination"] = email,
                    ["DeliveryMedium"] = "EMAIL",
                    ["AttributeName"] = "email",
                };
            }
            return Json(resp);
        }
    }

    private ServiceResponse ActConfirmSignUp(JsonElement data)
    {
        var cid = data.GetStringOrDefault("ClientId");
        var username = data.GetStringOrDefault("Username");

        lock (_lock)
        {
            var (pool, _) = FindPoolByClientId(cid);
            if (pool is null)
            {
                return Error("ResourceNotFoundException", $"Client {cid} not found.");
            }
            var users = GetUsers(pool);
            if (!users.TryGetValue(username, out var user))
            {
                return Error("UserNotFoundException", "User does not exist.");
            }
            user["UserStatus"] = "CONFIRMED";
            user["UserLastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActForgotPassword(JsonElement data)
    {
        var cid = data.GetStringOrDefault("ClientId");
        var username = data.GetStringOrDefault("Username");

        var (pool, _) = FindPoolByClientId(cid);
        if (pool is null)
        {
            return Error("ResourceNotFoundException", $"Client {cid} not found.");
        }
        var users = GetUsers(pool);
        if (!users.TryGetValue(username, out var user))
        {
            return Error("UserNotFoundException", "User does not exist.");
        }
        user["_reset_code"] = "654321";
        var attrs = AttrListToDict(GetAttributeList(user));
        return Json(new Dictionary<string, object?>
        {
            ["CodeDeliveryDetails"] = new Dictionary<string, object?>
            {
                ["Destination"] = attrs.GetValueOrDefault("email", ""),
                ["DeliveryMedium"] = "EMAIL",
                ["AttributeName"] = "email",
            },
        });
    }

    private ServiceResponse ActConfirmForgotPassword(JsonElement data)
    {
        var cid = data.GetStringOrDefault("ClientId");
        var username = data.GetStringOrDefault("Username");
        var newPassword = data.GetStringOrDefault("Password");

        lock (_lock)
        {
            var (pool, _) = FindPoolByClientId(cid);
            if (pool is null)
            {
                return Error("ResourceNotFoundException", $"Client {cid} not found.");
            }
            var users = GetUsers(pool);
            if (!users.TryGetValue(username, out var user))
            {
                return Error("UserNotFoundException", "User does not exist.");
            }
            user["_password"] = newPassword;
            user["UserStatus"] = "CONFIRMED";
            user["UserLastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActChangePassword(JsonElement data)
    {
        var accessToken = data.GetStringOrDefault("AccessToken");
        if (string.IsNullOrEmpty(accessToken))
        {
            return Error("NotAuthorizedException", "Access token is missing.");
        }
        var proposed = data.GetStringOrDefault("ProposedPassword");
        lock (_lock)
        {
            foreach (var pool in UserPools.Values)
            {
                var user = UserFromToken(accessToken, pool);
                if (user is not null)
                {
                    user["_password"] = proposed;
                    user["UserLastModifiedDate"] = NowEpoch();
                    return Json(new Dictionary<string, object?>());
                }
            }
        }
        return Error("NotAuthorizedException", "Invalid access token.");
    }

    private ServiceResponse ActGetUser(JsonElement data)
    {
        var accessToken = data.GetStringOrDefault("AccessToken");
        if (string.IsNullOrEmpty(accessToken))
        {
            return Error("NotAuthorizedException", "Access token is missing.");
        }
        foreach (var pool in UserPools.Values)
        {
            var user = UserFromToken(accessToken, pool);
            if (user is not null)
            {
                var out_ = UserOut(user);
                out_["UserAttributes"] = out_["Attributes"];
                out_.Remove("Attributes");
                out_["UserMFASettingList"] = GetStringList(user, "_mfa_enabled");
                out_["PreferredMfaSetting"] = GetString(user, "_preferred_mfa");
                return Json(out_);
            }
        }
        return Error("NotAuthorizedException", "Invalid access token.");
    }

    private ServiceResponse ActUpdateUserAttributes(JsonElement data)
    {
        var accessToken = data.GetStringOrDefault("AccessToken");
        if (string.IsNullOrEmpty(accessToken))
        {
            return Error("NotAuthorizedException", "Access token is missing.");
        }
        lock (_lock)
        {
            foreach (var pool in UserPools.Values)
            {
                var user = UserFromToken(accessToken, pool);
                if (user is not null)
                {
                    var existing = GetAttributeList(user);
                    var updates = data.GetPropertyOrNull("UserAttributes")?.ToAttributeList() ?? [];
                    user["Attributes"] = MergeAttributes(existing, updates);
                    user["UserLastModifiedDate"] = NowEpoch();
                    return Json(new Dictionary<string, object?> { ["CodeDeliveryDetailsList"] = new List<object?>() });
                }
            }
        }
        return Error("NotAuthorizedException", "Invalid access token.");
    }

    private ServiceResponse ActDeleteUser(JsonElement data)
    {
        var accessToken = data.GetStringOrDefault("AccessToken");
        if (string.IsNullOrEmpty(accessToken))
        {
            return Error("NotAuthorizedException", "Access token is missing.");
        }
        lock (_lock)
        {
            foreach (var pool in UserPools.Values)
            {
                var user = UserFromToken(accessToken, pool);
                if (user is not null)
                {
                    var username = GetString(user, "Username");
                    var users = GetUsers(pool);
                    users.Remove(username);
                    pool["EstimatedNumberOfUsers"] = users.Count;
                    return Json(new Dictionary<string, object?>());
                }
            }
        }
        return Error("NotAuthorizedException", "Invalid access token.");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // GROUPS
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActCreateGroup(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var groupName = data.GetStringOrDefault("GroupName");
            if (string.IsNullOrEmpty(groupName))
            {
                return Error("InvalidParameterException", "GroupName is required.");
            }
            var groups = GetGroups(pool!);
            if (groups.ContainsKey(groupName))
            {
                return Error("GroupExistsException", $"Group {groupName} already exists.");
            }
            var now = NowEpoch();
            var group = new Dictionary<string, object?>
            {
                ["GroupName"] = groupName,
                ["UserPoolId"] = pid,
                ["Description"] = data.GetStringOrDefault("Description"),
                ["RoleArn"] = data.GetStringOrDefault("RoleArn"),
                ["Precedence"] = data.GetIntOrDefault("Precedence"),
                ["CreationDate"] = now,
                ["LastModifiedDate"] = now,
                ["_members"] = new List<string>(),
            };
            groups[groupName] = group;
            return Json(new Dictionary<string, object?> { ["Group"] = GroupOut(group) });
        }
    }

    private ServiceResponse ActDeleteGroup(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var groupName = data.GetStringOrDefault("GroupName");
            var groups = GetGroups(pool!);
            if (!groups.TryGetValue(groupName, out var group))
            {
                return Error("ResourceNotFoundException", $"Group {groupName} not found.");
            }
            var members = GetGroupMembers(group);
            var users = GetUsers(pool!);
            foreach (var username in members)
            {
                if (users.TryGetValue(username, out var user))
                {
                    GetStringList(user, "_groups").Remove(groupName);
                }
            }
            groups.Remove(groupName);
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetGroup(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        var groupName = data.GetStringOrDefault("GroupName");
        var groups = GetGroups(pool!);
        if (!groups.TryGetValue(groupName, out var group))
        {
            return Error("ResourceNotFoundException", $"Group {groupName} not found.");
        }
        return Json(new Dictionary<string, object?> { ["Group"] = GroupOut(group) });
    }

    private ServiceResponse ActListGroups(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        var groups = GetGroups(pool!);
        var limit = Math.Min(data.GetIntOrDefault("Limit", 60), 60);
        var nextToken = data.GetStringOrDefault("NextToken");
        var sorted = groups.Values.OrderBy(g => GetString(g, "GroupName")).ToList();
        var start = string.IsNullOrEmpty(nextToken) ? 0 : int.Parse(nextToken);
        var page = sorted.Skip(start).Take(limit).ToList();
        var resp = new Dictionary<string, object?>
        {
            ["Groups"] = page.Select(GroupOut).ToList<object?>(),
        };
        if (start + limit < sorted.Count)
        {
            resp["NextToken"] = (start + limit).ToString();
        }
        return Json(resp);
    }

    private ServiceResponse ActListUsersInGroup(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        var groupName = data.GetStringOrDefault("GroupName");
        var groups = GetGroups(pool!);
        if (!groups.TryGetValue(groupName, out var group))
        {
            return Error("ResourceNotFoundException", $"Group {groupName} not found.");
        }
        var members = GetGroupMembers(group);
        var users = GetUsers(pool!);
        var limit = Math.Min(data.GetIntOrDefault("Limit", 60), 60);
        var nextToken = data.GetStringOrDefault("NextToken");
        var start = string.IsNullOrEmpty(nextToken) ? 0 : int.Parse(nextToken);
        var page = members.Skip(start).Take(limit)
            .Where(users.ContainsKey)
            .Select(u => UserOut(users[u]))
            .ToList<object?>();
        var resp = new Dictionary<string, object?>
        {
            ["Users"] = page,
        };
        if (start + limit < members.Count)
        {
            resp["NextToken"] = (start + limit).ToString();
        }
        return Json(resp);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DOMAIN
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActCreateUserPoolDomain(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var domain = data.GetStringOrDefault("Domain");
            if (string.IsNullOrEmpty(domain))
            {
                return Error("InvalidParameterException", "Domain is required.");
            }
            if (PoolDomainMap.ContainsKey(domain))
            {
                return Error("InvalidParameterException", $"Domain {domain} already exists.");
            }
            pool!["Domain"] = domain;
            PoolDomainMap[domain] = pid;
            return Json(new Dictionary<string, object?> { ["CloudFrontDomain"] = $"{domain}.auth.{Region}.amazoncognito.com" });
        }
    }

    private ServiceResponse ActDeleteUserPoolDomain(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var domain = data.GetStringOrDefault("Domain");
            PoolDomainMap.TryRemove(domain, out _);
            pool!["Domain"] = null;
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActDescribeUserPoolDomain(JsonElement data)
    {
        var domain = data.GetStringOrDefault("Domain");
        if (!PoolDomainMap.TryGetValue(domain, out var pid))
        {
            return Json(new Dictionary<string, object?> { ["DomainDescription"] = new Dictionary<string, object?>() });
        }
        return Json(new Dictionary<string, object?>
        {
            ["DomainDescription"] = new Dictionary<string, object?>
            {
                ["UserPoolId"] = pid,
                ["AWSAccountId"] = AccountContext.GetAccountId(),
                ["Domain"] = domain,
                ["S3Bucket"] = "",
                ["CloudFrontDistribution"] = $"{domain}.auth.{Region}.amazoncognito.com",
                ["Version"] = "1",
                ["Status"] = "ACTIVE",
                ["CustomDomainConfig"] = new Dictionary<string, object?>(),
            },
        });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // MFA CONFIG
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActGetUserPoolMfaConfig(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        var (pool, err) = ResolvePool(pid);
        if (err is not null) return err;
        return Json(new Dictionary<string, object?>
        {
            ["SmsMfaConfiguration"] = pool!.GetValueOrDefault("SmsMfaConfiguration") ?? new Dictionary<string, object?>(),
            ["SoftwareTokenMfaConfiguration"] = (pool!.TryGetValue("SoftwareTokenMfaConfiguration", out var stmc) ? stmc : null)
                ?? new Dictionary<string, object?> { ["Enabled"] = false },
            ["MfaConfiguration"] = pool.GetValueOrDefault("MfaConfiguration") ?? "OFF",
        });
    }

    private ServiceResponse ActSetUserPoolMfaConfig(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var smsProp = data.GetPropertyOrNull("SmsMfaConfiguration");
            if (smsProp is not null)
            {
                pool!["SmsMfaConfiguration"] = smsProp.Value.ToObject();
            }
            var totpProp = data.GetPropertyOrNull("SoftwareTokenMfaConfiguration");
            if (totpProp is not null)
            {
                pool!["SoftwareTokenMfaConfiguration"] = totpProp.Value.ToObject();
            }
            var mfaProp = data.GetPropertyOrNull("MfaConfiguration");
            if (mfaProp is not null)
            {
                pool!["MfaConfiguration"] = mfaProp.Value.GetString();
            }
            pool!["LastModifiedDate"] = NowEpoch();
            return Json(new Dictionary<string, object?>
            {
                ["SmsMfaConfiguration"] = pool.GetValueOrDefault("SmsMfaConfiguration") ?? new Dictionary<string, object?>(),
                ["SoftwareTokenMfaConfiguration"] = pool.GetValueOrDefault("SoftwareTokenMfaConfiguration") ?? new Dictionary<string, object?>(),
                ["MfaConfiguration"] = pool.GetValueOrDefault("MfaConfiguration") ?? "OFF",
            });
        }
    }

    private ServiceResponse ActAssociateSoftwareToken()
    {
        var secret = Convert.ToBase64String(RandomNumberGenerator.GetBytes(20))
            .Replace("+", "A").Replace("/", "B").Replace("=", ""); // base32-like
        var session = GenerateSession();
        return Json(new Dictionary<string, object?> { ["SecretCode"] = secret, ["Session"] = session });
    }

    private ServiceResponse ActVerifySoftwareToken(JsonElement data)
    {
        var accessToken = data.GetStringOrDefault("AccessToken");
        if (!string.IsNullOrEmpty(accessToken))
        {
            lock (_lock)
            {
                foreach (var pool in UserPools.Values)
                {
                    var user = UserFromToken(accessToken, pool);
                    if (user is not null)
                    {
                        var mfaEnabled = GetStringList(user, "_mfa_enabled");
                        if (!mfaEnabled.Contains("SOFTWARE_TOKEN_MFA"))
                        {
                            mfaEnabled.Add("SOFTWARE_TOKEN_MFA");
                        }
                        user["_preferred_mfa"] = "SOFTWARE_TOKEN_MFA";
                        break;
                    }
                }
            }
        }
        return Json(new Dictionary<string, object?> { ["Status"] = "SUCCESS" });
    }

    private ServiceResponse ActAdminSetUserMfaPreference(JsonElement data)
    {
        var pid = data.GetStringOrDefault("UserPoolId");
        lock (_lock)
        {
            var (pool, err) = ResolvePool(pid);
            if (err is not null) return err;
            var username = data.GetStringOrDefault("Username");
            var (user, uerr) = ResolveUser(pool!, username);
            if (uerr is not null) return uerr;
            ApplyMfaPreference(user!, data);
            return Json(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActSetUserMfaPreference(JsonElement data)
    {
        var accessToken = data.GetStringOrDefault("AccessToken");
        if (string.IsNullOrEmpty(accessToken))
        {
            return Error("NotAuthorizedException", "Missing access token.");
        }
        lock (_lock)
        {
            foreach (var pool in UserPools.Values)
            {
                var user = UserFromToken(accessToken, pool);
                if (user is not null)
                {
                    ApplyMfaPreference(user, data);
                    return Json(new Dictionary<string, object?>());
                }
            }
        }
        return Error("NotAuthorizedException", "Invalid access token.");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // IDP TAGS
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse ActTagResource(JsonElement data)
    {
        var arn = data.GetStringOrDefault("ResourceArn");
        var tags = data.GetPropertyOrNull("Tags")?.ToStringDictionary() ?? new Dictionary<string, string>();
        lock (_lock)
        {
            foreach (var pool in UserPools.Values)
            {
                if (GetString(pool, "Arn") == arn)
                {
                    var poolTags = GetStringDictionary(pool, "UserPoolTags");
                    foreach (var (k, v) in tags) poolTags[k] = v;
                    return Json(new Dictionary<string, object?>());
                }
            }
        }
        return Error("ResourceNotFoundException", $"Resource {arn} not found.");
    }

    private ServiceResponse ActUntagResource(JsonElement data)
    {
        var arn = data.GetStringOrDefault("ResourceArn");
        var tagKeys = data.GetPropertyOrNull("TagKeys")?.ToStringList() ?? [];
        lock (_lock)
        {
            foreach (var pool in UserPools.Values)
            {
                if (GetString(pool, "Arn") == arn)
                {
                    var poolTags = GetStringDictionary(pool, "UserPoolTags");
                    foreach (var k in tagKeys) poolTags.Remove(k);
                    return Json(new Dictionary<string, object?>());
                }
            }
        }
        return Error("ResourceNotFoundException", $"Resource {arn} not found.");
    }

    private ServiceResponse ActListTagsForResource(JsonElement data)
    {
        var arn = data.GetStringOrDefault("ResourceArn");
        foreach (var pool in UserPools.Values)
        {
            if (GetString(pool, "Arn") == arn)
            {
                return Json(new Dictionary<string, object?> { ["Tags"] = GetStringDictionary(pool, "UserPoolTags") });
            }
        }
        return Error("ResourceNotFoundException", $"Resource {arn} not found.");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // WELL-KNOWN & OAUTH2 ENDPOINTS
    // ═══════════════════════════════════════════════════════════════════════

    private ServiceResponse HandleJwks()
    {
        var jwks = new Dictionary<string, object?>
        {
            ["keys"] = new List<object?>
            {
                new Dictionary<string, object?>
                {
                    ["kty"] = "RSA",
                    ["alg"] = "RS256",
                    ["use"] = "sig",
                    ["kid"] = "microstack-key-1",
                    ["n"] = _jwksN,
                    ["e"] = _jwksE,
                },
            },
        };
        var body = JsonSerializer.SerializeToUtf8Bytes(jwks);
        return new ServiceResponse(200, JsonContentType, body);
    }

    private ServiceResponse HandleOpenIdConfiguration(string poolId)
    {
        var issuer = $"https://cognito-idp.{Region}.amazonaws.com/{poolId}";
        var doc = new Dictionary<string, object?>
        {
            ["issuer"] = issuer,
            ["jwks_uri"] = $"{issuer}/.well-known/jwks.json",
            ["authorization_endpoint"] = $"{issuer}/oauth2/authorize",
            ["token_endpoint"] = $"{issuer}/oauth2/token",
            ["response_types_supported"] = new List<string> { "code", "token" },
            ["subject_types_supported"] = new List<string> { "public" },
            ["id_token_signing_alg_values_supported"] = new List<string> { "RS256" },
        };
        var body = JsonSerializer.SerializeToUtf8Bytes(doc);
        return new ServiceResponse(200, JsonContentType, body);
    }

    private ServiceResponse HandleOAuth2Token(ServiceRequest request)
    {
        // Parse form-encoded body
        var form = new Dictionary<string, string>(StringComparer.Ordinal);
        if (request.Body.Length > 0)
        {
            var bodyStr = Encoding.UTF8.GetString(request.Body);
            foreach (var pair in bodyStr.Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                var eq = pair.IndexOf('=');
                if (eq < 0) continue;
                var key = Uri.UnescapeDataString(pair[..eq]);
                var val = Uri.UnescapeDataString(pair[(eq + 1)..]);
                form[key] = val;
            }
        }

        var clientId = form.GetValueOrDefault("client_id", "");
        if (string.IsNullOrEmpty(clientId))
        {
            clientId = request.GetQueryParam("client_id") ?? "";
        }
        var poolId = "";
        foreach (var (pid, pool) in UserPools.Items)
        {
            var clients = GetClients(pool);
            if (clients.ContainsKey(clientId))
            {
                poolId = pid;
                break;
            }
        }
        var sub = string.IsNullOrEmpty(clientId) ? HashHelpers.NewUuid() : clientId;
        var accessToken = GenerateJwt(sub, poolId, clientId, "access");
        var resp = new Dictionary<string, object?>
        {
            ["access_token"] = accessToken,
            ["token_type"] = "Bearer",
            ["expires_in"] = 3600,
        };
        var body = JsonSerializer.SerializeToUtf8Bytes(resp);
        return new ServiceResponse(200, JsonContentType, body);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // HELPERS
    // ═══════════════════════════════════════════════════════════════════════

    private (Dictionary<string, object?>? Pool, ServiceResponse? Error) ResolvePool(string poolId)
    {
        if (UserPools.TryGetValue(poolId, out var pool))
        {
            return (pool, null);
        }
        return (null, Error("ResourceNotFoundException", $"User pool {poolId} does not exist."));
    }

    private static (Dictionary<string, object?>? User, ServiceResponse? Error) ResolveUser(
        Dictionary<string, object?> pool, string username)
    {
        var users = GetUsers(pool);
        if (users.TryGetValue(username, out var user))
        {
            return (user, null);
        }
        return (null, Error("UserNotFoundException", $"User {username} does not exist."));
    }

    private (Dictionary<string, object?>? Pool, string? PoolId) FindPoolByClientId(string clientId)
    {
        foreach (var (pid, pool) in UserPools.Items)
        {
            var clients = GetClients(pool);
            if (clients.ContainsKey(clientId))
            {
                return (pool, pid);
            }
        }
        return (null, null);
    }

    private Dictionary<string, object?>? UserFromToken(string token, Dictionary<string, object?> pool)
    {
        try
        {
            var parts = token.Split('.');
            if (parts.Length < 2) return null;
            var payloadB64 = parts[1];
            // Add padding
            payloadB64 = payloadB64.PadRight(payloadB64.Length + (4 - payloadB64.Length % 4) % 4, '=');
            // Convert URL-safe base64 to standard
            payloadB64 = payloadB64.Replace('-', '+').Replace('_', '/');
            var payloadBytes = Convert.FromBase64String(payloadB64);
            using var doc = JsonDocument.Parse(payloadBytes);
            var sub = doc.RootElement.TryGetProperty("sub", out var subEl) ? subEl.GetString() : null;
            if (string.IsNullOrEmpty(sub)) return null;
            var users = GetUsers(pool);
            foreach (var user in users.Values)
            {
                var attrs = AttrListToDict(GetAttributeList(user));
                if (attrs.TryGetValue("sub", out var userSub) && userSub == sub)
                {
                    return user;
                }
            }
        }
        catch
        {
            // Ignore decode failures
        }
        return null;
    }

    private Dictionary<string, object?>? MfaChallengeForUser(
        Dictionary<string, object?> pool,
        Dictionary<string, object?> user,
        string username)
    {
        var mfaConfig = pool.GetValueOrDefault("MfaConfiguration") as string ?? "OFF";
        if (mfaConfig == "OFF") return null;
        var enabledMfa = GetStringList(user, "_mfa_enabled");
        if (mfaConfig == "OPTIONAL" && !enabledMfa.Contains("SOFTWARE_TOKEN_MFA"))
        {
            return null;
        }
        if (mfaConfig == "ON" && !enabledMfa.Contains("SOFTWARE_TOKEN_MFA"))
        {
            return null;
        }
        return new Dictionary<string, object?>
        {
            ["ChallengeName"] = "SOFTWARE_TOKEN_MFA",
            ["Session"] = GenerateSession(),
            ["ChallengeParameters"] = new Dictionary<string, object?>
            {
                ["USER_ID_FOR_SRP"] = username,
                ["FRIENDLY_DEVICE_NAME"] = "TOTP device",
            },
        };
    }

    private Dictionary<string, object?> BuildAuthResult(string poolId, string clientId, Dictionary<string, object?> user)
    {
        var attrs = AttrListToDict(GetAttributeList(user));
        var sub = attrs.GetValueOrDefault("sub", GetString(user, "Username"));
        var username = GetString(user, "Username");
        return new Dictionary<string, object?>
        {
            ["AccessToken"] = GenerateJwt(sub, poolId, clientId, "access", username),
            ["IdToken"] = GenerateJwt(sub, poolId, clientId, "id"),
            ["RefreshToken"] = GenerateJwt(sub, poolId, clientId, "refresh"),
            ["TokenType"] = "Bearer",
            ["ExpiresIn"] = 3600,
        };
    }

    internal string GenerateJwt(string sub, string poolId, string clientId, string tokenType, string username = "")
    {
        var header = Base64UrlEncodeJson(new Dictionary<string, object?>
        {
            ["alg"] = "RS256",
            ["kid"] = "microstack-key-1",
        });
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var claims = new Dictionary<string, object?>
        {
            ["sub"] = sub,
            ["iss"] = $"https://cognito-idp.{Region}.amazonaws.com/{poolId}",
            ["client_id"] = clientId,
            ["token_use"] = tokenType,
            ["iat"] = now,
            ["exp"] = now + 3600,
            ["jti"] = HashHelpers.NewUuid(),
        };
        if (tokenType == "access" && !string.IsNullOrEmpty(username))
        {
            claims["username"] = username;
        }
        var payload = Base64UrlEncodeJson(claims);
        var signingInput = Encoding.UTF8.GetBytes($"{header}.{payload}");
        var sigBytes = _rsaKey.SignData(signingInput, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var sig = Base64UrlEncode(sigBytes);
        return $"{header}.{payload}.{sig}";
    }

    private static void ApplyMfaPreference(Dictionary<string, object?> user, JsonElement data)
    {
        var totpSettings = data.GetPropertyOrNull("SoftwareTokenMfaSettings");
        var enabledMfa = GetStringList(user, "_mfa_enabled");

        if (totpSettings is not null)
        {
            var enabled = totpSettings.Value.GetBoolPropertyOrDefault("Enabled");
            var preferred = totpSettings.Value.GetBoolPropertyOrDefault("PreferredMfa");
            if (enabled)
            {
                if (!enabledMfa.Contains("SOFTWARE_TOKEN_MFA"))
                {
                    enabledMfa.Add("SOFTWARE_TOKEN_MFA");
                }
                if (preferred)
                {
                    user["_preferred_mfa"] = "SOFTWARE_TOKEN_MFA";
                }
            }
            else
            {
                enabledMfa.Remove("SOFTWARE_TOKEN_MFA");
                if (GetString(user, "_preferred_mfa") == "SOFTWARE_TOKEN_MFA")
                {
                    user["_preferred_mfa"] = "";
                }
            }
        }
    }

    private static List<Dictionary<string, object?>> ApplyUserFilter(
        List<Dictionary<string, object?>> users, string filterStr)
    {
        var m = FilterRegex().Match(filterStr.Trim());
        if (!m.Success) return users;
        var attrName = m.Groups[1].Value;
        var op = m.Groups[2].Value;
        var value = m.Groups[3].Value;
        return users.Where(user =>
        {
            var attrDict = AttrListToDict(GetAttributeList(user));
            string fieldVal;
            if (attrName == "username")
                fieldVal = GetString(user, "Username");
            else if (attrName == "status")
                fieldVal = GetString(user, "UserStatus");
            else
                fieldVal = attrDict.GetValueOrDefault(attrName, "");

            return op switch
            {
                "=" => fieldVal == value,
                "^=" => fieldVal.StartsWith(value, StringComparison.Ordinal),
                "!=" => fieldVal != value,
                _ => false,
            };
        }).ToList();
    }

    [GeneratedRegex(@"(\w+)\s*(=|\^=|!=)\s*""([^""]*)""")]
    private static partial Regex FilterRegex();

    // ── Data access helpers ────────────────────────────────────────────────

    private static Dictionary<string, Dictionary<string, object?>> GetClients(Dictionary<string, object?> pool) =>
        (Dictionary<string, Dictionary<string, object?>>)pool["_clients"]!;

    internal static Dictionary<string, Dictionary<string, object?>> GetUsers(Dictionary<string, object?> pool) =>
        (Dictionary<string, Dictionary<string, object?>>)pool["_users"]!;

    private static Dictionary<string, Dictionary<string, object?>> GetGroups(Dictionary<string, object?> pool) =>
        (Dictionary<string, Dictionary<string, object?>>)pool["_groups"]!;

    private static List<string> GetGroupMembers(Dictionary<string, object?> group) =>
        (List<string>)group["_members"]!;

    internal static List<string> GetStringList(Dictionary<string, object?> dict, string key) =>
        dict.TryGetValue(key, out var val) && val is List<string> list ? list : [];

    private static string GetString(Dictionary<string, object?> dict, string key) =>
        dict.TryGetValue(key, out var val) && val is string s ? s : "";

    private static Dictionary<string, string> GetStringDictionary(Dictionary<string, object?> dict, string key) =>
        dict.TryGetValue(key, out var val) && val is Dictionary<string, string> d ? d : new Dictionary<string, string>();

    private static List<Dictionary<string, string>> GetAttributeList(Dictionary<string, object?> user) =>
        user.TryGetValue("Attributes", out var val) && val is List<Dictionary<string, string>> list
            ? list
            : [];

    private static Dictionary<string, string> AttrListToDict(List<Dictionary<string, string>> attrs) =>
        attrs.Where(a => a.ContainsKey("Name"))
             .ToDictionary(a => a["Name"], a => a.GetValueOrDefault("Value", ""), StringComparer.Ordinal);

    private static List<Dictionary<string, string>> DictToAttrList(Dictionary<string, string> d) =>
        d.Select(kv => new Dictionary<string, string> { ["Name"] = kv.Key, ["Value"] = kv.Value }).ToList();

    private static List<Dictionary<string, string>> MergeAttributes(
        List<Dictionary<string, string>> existing, List<Dictionary<string, string>> updates)
    {
        var d = AttrListToDict(existing);
        foreach (var (k, v) in AttrListToDict(updates))
        {
            d[k] = v;
        }
        return DictToAttrList(d);
    }

    private static Dictionary<string, object?> UserOut(Dictionary<string, object?> user) =>
        new()
        {
            ["Username"] = user["Username"],
            ["Attributes"] = user.GetValueOrDefault("Attributes") ?? new List<Dictionary<string, string>>(),
            ["UserCreateDate"] = user.GetValueOrDefault("UserCreateDate") ?? NowEpoch(),
            ["UserLastModifiedDate"] = user.GetValueOrDefault("UserLastModifiedDate") ?? NowEpoch(),
            ["Enabled"] = user.GetValueOrDefault("Enabled") ?? true,
            ["UserStatus"] = user.GetValueOrDefault("UserStatus") ?? "CONFIRMED",
            ["MFAOptions"] = user.GetValueOrDefault("MFAOptions") ?? new List<object?>(),
        };

    private static Dictionary<string, object?> PoolOut(Dictionary<string, object?> pool)
    {
        var result = new Dictionary<string, object?>();
        foreach (var (k, v) in pool)
        {
            if (!k.StartsWith('_'))
            {
                result[k] = v;
            }
        }
        return result;
    }

    private static Dictionary<string, object?> ClientOut(Dictionary<string, object?> client)
    {
        var result = new Dictionary<string, object?>();
        foreach (var (k, v) in client)
        {
            if (v is not null) result[k] = v;
        }
        return result;
    }

    private static Dictionary<string, object?> GroupOut(Dictionary<string, object?> group)
    {
        var result = new Dictionary<string, object?>();
        foreach (var (k, v) in group)
        {
            if (!k.StartsWith('_')) result[k] = v;
        }
        return result;
    }

    // ── Generation helpers ─────────────────────────────────────────────────

    private static string GeneratePoolId()
    {
        var suffix = Guid.NewGuid().ToString("N")[..9];
        return $"{Region}_{suffix}";
    }

    private static string GenerateClientId() =>
        Guid.NewGuid().ToString("N")[..26];

    private static string GenerateClientSecret() =>
        Convert.ToBase64String(RandomNumberGenerator.GetBytes(48));

    private static string GenerateSession() =>
        Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));

    private static string GenerateTempPassword()
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%";
        var result = new char[12];
        for (var i = 0; i < result.Length; i++)
        {
            result[i] = chars[RandomNumberGenerator.GetInt32(chars.Length)];
        }
        return new string(result);
    }

    private static double NowEpoch() =>
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;

    private static string PoolArn(string poolId) =>
        $"arn:aws:cognito-idp:{Region}:{AccountContext.GetAccountId()}:userpool/{poolId}";

    private static string ExtractPoolIdFromPath(string path)
    {
        // Path format: /{poolId}/.well-known/...
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        return segments.Length > 0 ? segments[0] : "";
    }

    private static string Base64UrlEncode(byte[] data) =>
        Convert.ToBase64String(data).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private static string Base64UrlEncodeJson(Dictionary<string, object?> obj)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(obj);
        return Base64UrlEncode(json);
    }

    private static object DefaultPasswordPolicy() =>
        new Dictionary<string, object?>
        {
            ["PasswordPolicy"] = new Dictionary<string, object?>
            {
                ["MinimumLength"] = 8,
                ["RequireUppercase"] = true,
                ["RequireLowercase"] = true,
                ["RequireNumbers"] = true,
                ["RequireSymbols"] = true,
                ["TemporaryPasswordValidityDays"] = 7,
            },
        };

    private static ServiceResponse Json(Dictionary<string, object?> data) =>
        AwsResponseHelpers.JsonResponse(data);

    private static ServiceResponse Error(string code, string message) =>
        AwsResponseHelpers.ErrorResponseJson(code, message, 400);

    private static readonly Dictionary<string, string> JsonContentType = new(StringComparer.Ordinal)
    {
        ["Content-Type"] = "application/json",
    };
}

/// <summary>
/// Extension methods for JsonElement to simplify data extraction.
/// </summary>
internal static class CognitoJsonExtensions
{
    internal static string GetStringOrDefault(this JsonElement el, string propertyName, string defaultValue = "")
    {
        if (el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String)
        {
            return prop.GetString() ?? defaultValue;
        }
        return defaultValue;
    }

    internal static int GetIntOrDefault(this JsonElement el, string propertyName, int defaultValue = 0)
    {
        if (el.TryGetProperty(propertyName, out var prop))
        {
            if (prop.ValueKind == JsonValueKind.Number) return prop.GetInt32();
        }
        return defaultValue;
    }

    internal static bool GetBoolOrDefault(this JsonElement el, string propertyName, bool defaultValue = false)
    {
        if (el.TryGetProperty(propertyName, out var prop))
        {
            if (prop.ValueKind is JsonValueKind.True or JsonValueKind.False) return prop.GetBoolean();
        }
        return defaultValue;
    }

    internal static bool GetBoolPropertyOrDefault(this JsonElement el, string propertyName, bool defaultValue = false)
    {
        if (el.TryGetProperty(propertyName, out var prop))
        {
            if (prop.ValueKind is JsonValueKind.True or JsonValueKind.False) return prop.GetBoolean();
        }
        return defaultValue;
    }

    internal static JsonElement? GetPropertyOrNull(this JsonElement el, string propertyName)
    {
        if (el.TryGetProperty(propertyName, out var prop) && prop.ValueKind != JsonValueKind.Null)
        {
            return prop;
        }
        return null;
    }

    internal static object? ToObject(this JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.Object => el.EnumerateObject()
                .ToDictionary(p => p.Name, p => p.Value.ToObject(), StringComparer.Ordinal) as object,
            JsonValueKind.Array => el.EnumerateArray().Select(e => e.ToObject()).ToList(),
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? l : el.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    internal static List<object?> ToObjectList(this JsonElement el) =>
        el.ValueKind == JsonValueKind.Array
            ? el.EnumerateArray().Select(e => e.ToObject()).ToList()
            : [];

    internal static List<string> ToStringList(this JsonElement el) =>
        el.ValueKind == JsonValueKind.Array
            ? el.EnumerateArray().Where(e => e.ValueKind == JsonValueKind.String).Select(e => e.GetString()!).ToList()
            : [];

    internal static Dictionary<string, string> ToStringDictionary(this JsonElement el) =>
        el.ValueKind == JsonValueKind.Object
            ? el.EnumerateObject()
                .ToDictionary(p => p.Name, p => p.Value.GetString() ?? "", StringComparer.Ordinal)
            : new Dictionary<string, string>();

    internal static List<Dictionary<string, string>> ToAttributeList(this JsonElement el) =>
        el.ValueKind == JsonValueKind.Array
            ? el.EnumerateArray()
                .Where(e => e.ValueKind == JsonValueKind.Object)
                .Select(e => e.EnumerateObject()
                    .ToDictionary(p => p.Name, p => p.Value.GetString() ?? "", StringComparer.Ordinal))
                .ToList()
            : [];
}
