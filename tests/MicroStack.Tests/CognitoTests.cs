using Amazon;
using Amazon.CognitoIdentity;
using Amazon.CognitoIdentity.Model;
using Amazon.CognitoIdentityProvider;
using Amazon.CognitoIdentityProvider.Model;
using Amazon.Runtime;

using NotAuthorizedException = Amazon.CognitoIdentityProvider.Model.NotAuthorizedException;
using TagResourceRequest = Amazon.CognitoIdentityProvider.Model.TagResourceRequest;
using UntagResourceRequest = Amazon.CognitoIdentityProvider.Model.UntagResourceRequest;
using ListTagsForResourceRequest = Amazon.CognitoIdentityProvider.Model.ListTagsForResourceRequest;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the Cognito service handlers (IdP + Identity).
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Port of ministack tests/test_cognito.py (1102 lines, ~50 test cases).
/// </summary>
public sealed class CognitoTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonCognitoIdentityProviderClient _idp;
    private readonly AmazonCognitoIdentityClient _identity;

    public CognitoTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _idp = CreateIdpClient(fixture);
        _identity = CreateIdentityClient(fixture);
    }

    private static AmazonCognitoIdentityProviderClient CreateIdpClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        var config = new AmazonCognitoIdentityProviderConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };
        return new AmazonCognitoIdentityProviderClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    private static AmazonCognitoIdentityClient CreateIdentityClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        var config = new AmazonCognitoIdentityConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };
        return new AmazonCognitoIdentityClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _idp.Dispose();
        _identity.Dispose();
        return Task.CompletedTask;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // USER POOL CRUD
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateAndDescribeUserPool()
    {
        var resp = await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "TestPool" });
        var pool = resp.UserPool;
        Assert.Equal("TestPool", pool.Name);
        Assert.StartsWith("us-east-1_", pool.Id);

        var desc = await _idp.DescribeUserPoolAsync(new DescribeUserPoolRequest { UserPoolId = pool.Id });
        Assert.Equal(pool.Id, desc.UserPool.Id);
        Assert.Equal("TestPool", desc.UserPool.Name);
    }

    [Fact]
    public async Task ListUserPools()
    {
        await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "ListPoolA" });
        await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "ListPoolB" });

        var resp = await _idp.ListUserPoolsAsync(new ListUserPoolsRequest { MaxResults = 60 });
        var names = resp.UserPools.Select(p => p.Name).ToList();
        Assert.Contains("ListPoolA", names);
        Assert.Contains("ListPoolB", names);
    }

    [Fact]
    public async Task UpdateUserPool()
    {
        var createResp = await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "UpdatePool" });
        var pid = createResp.UserPool.Id;

        await _idp.UpdateUserPoolAsync(new UpdateUserPoolRequest
        {
            UserPoolId = pid,
            UserPoolTags = new Dictionary<string, string> { ["env"] = "test" },
        });

        var desc = await _idp.DescribeUserPoolAsync(new DescribeUserPoolRequest { UserPoolId = pid });
        Assert.Equal("test", desc.UserPool.UserPoolTags["env"]);
    }

    [Fact]
    public async Task DeleteUserPool()
    {
        var createResp = await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "DeletePool" });
        var pid = createResp.UserPool.Id;

        await _idp.DeleteUserPoolAsync(new DeleteUserPoolRequest { UserPoolId = pid });

        var pools = await _idp.ListUserPoolsAsync(new ListUserPoolsRequest { MaxResults = 60 });
        Assert.DoesNotContain(pools.UserPools, p => p.Id == pid);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // USER POOL CLIENT
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateAndDescribeUserPoolClient()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "ClientPool" })).UserPool.Id;
        var clientResp = await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "MyApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        });
        var client = clientResp.UserPoolClient;
        Assert.Equal("MyApp", client.ClientName);

        var desc = await _idp.DescribeUserPoolClientAsync(new DescribeUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientId = client.ClientId,
        });
        Assert.Equal(client.ClientId, desc.UserPoolClient.ClientId);
        Assert.Equal("MyApp", desc.UserPoolClient.ClientName);
    }

    [Fact]
    public async Task ListUserPoolClients()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "MultiClientPool" })).UserPool.Id;
        await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest { UserPoolId = pid, ClientName = "App1" });
        await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest { UserPoolId = pid, ClientName = "App2" });

        var clients = await _idp.ListUserPoolClientsAsync(new ListUserPoolClientsRequest
        {
            UserPoolId = pid,
            MaxResults = 60,
        });
        var names = clients.UserPoolClients.Select(c => c.ClientName).ToList();
        Assert.Contains("App1", names);
        Assert.Contains("App2", names);
    }

    [Fact]
    public async Task UpdateUserPoolClient()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "UpdateClientPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "OriginalName",
        })).UserPoolClient.ClientId;

        var updated = await _idp.UpdateUserPoolClientAsync(new UpdateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            ClientName = "UpdatedName",
            RefreshTokenValidity = 14,
        });
        Assert.Equal("UpdatedName", updated.UserPoolClient.ClientName);
        Assert.Equal(14, updated.UserPoolClient.RefreshTokenValidity);

        var desc = await _idp.DescribeUserPoolClientAsync(new DescribeUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientId = cid,
        });
        Assert.Equal("UpdatedName", desc.UserPoolClient.ClientName);
    }

    [Fact]
    public async Task ClientSecretGenerated()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "SecretClientPool" })).UserPool.Id;
        var client = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "SecretApp",
            GenerateSecret = true,
        })).UserPoolClient;
        Assert.NotNull(client.ClientSecret);
        Assert.True(client.ClientSecret.Length > 20);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // USER MANAGEMENT
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AdminCreateAndGetUser()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "AdminUserPool" })).UserPool.Id;
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest
        {
            UserPoolId = pid,
            Username = "alice",
            UserAttributes = [new AttributeType { Name = "email", Value = "alice@example.com" }],
        });

        var user = await _idp.AdminGetUserAsync(new AdminGetUserRequest
        {
            UserPoolId = pid,
            Username = "alice",
        });
        Assert.Equal("alice", user.Username);
        var attrs = user.UserAttributes.ToDictionary(a => a.Name, a => a.Value);
        Assert.Equal("alice@example.com", attrs["email"]);
    }

    [Fact]
    public async Task ListUsers()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "ListUsersPool" })).UserPool.Id;
        foreach (var name in new[] { "user1", "user2", "user3" })
        {
            await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = name });
        }

        var users = await _idp.ListUsersAsync(new ListUsersRequest { UserPoolId = pid });
        var usernames = users.Users.Select(u => u.Username).ToList();
        Assert.Contains("user1", usernames);
        Assert.Contains("user2", usernames);
        Assert.Contains("user3", usernames);
    }

    [Fact]
    public async Task ListUsersFilter()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "FilterUsersPool" })).UserPool.Id;
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest
        {
            UserPoolId = pid,
            Username = "bob",
            UserAttributes = [new AttributeType { Name = "email", Value = "bob@example.com" }],
        });
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest
        {
            UserPoolId = pid,
            Username = "charlie",
            UserAttributes = [new AttributeType { Name = "email", Value = "charlie@example.com" }],
        });

        var resp = await _idp.ListUsersAsync(new ListUsersRequest
        {
            UserPoolId = pid,
            Filter = "username = \"bob\"",
        });
        Assert.Single(resp.Users);
        Assert.Equal("bob", resp.Users[0].Username);
    }

    [Fact]
    public async Task AdminDeleteUser()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "DeleteUserPool" })).UserPool.Id;
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "kate" });
        await _idp.AdminDeleteUserAsync(new AdminDeleteUserRequest { UserPoolId = pid, Username = "kate" });

        var ex = await Assert.ThrowsAsync<UserNotFoundException>(() =>
            _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "kate" }));
        Assert.Equal("UserNotFoundException", ex.ErrorCode);
    }

    [Fact]
    public async Task AdminUpdateUserAttributes()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "UpdateAttrPool" })).UserPool.Id;
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest
        {
            UserPoolId = pid,
            Username = "irene",
            UserAttributes = [new AttributeType { Name = "email", Value = "irene@example.com" }],
        });
        await _idp.AdminUpdateUserAttributesAsync(new AdminUpdateUserAttributesRequest
        {
            UserPoolId = pid,
            Username = "irene",
            UserAttributes = [new AttributeType { Name = "email", Value = "irene@updated.com" }],
        });

        var user = await _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "irene" });
        var attrs = user.UserAttributes.ToDictionary(a => a.Name, a => a.Value);
        Assert.Equal("irene@updated.com", attrs["email"]);
    }

    [Fact]
    public async Task AdminDisableEnableUser()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "DisablePool" })).UserPool.Id;
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "jack" });

        await _idp.AdminDisableUserAsync(new AdminDisableUserRequest { UserPoolId = pid, Username = "jack" });
        var user1 = await _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "jack" });
        Assert.False(user1.Enabled);

        await _idp.AdminEnableUserAsync(new AdminEnableUserRequest { UserPoolId = pid, Username = "jack" });
        var user2 = await _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "jack" });
        Assert.True(user2.Enabled);
    }

    [Fact]
    public async Task DuplicateUsernameError()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "DupUserPool" })).UserPool.Id;
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "dup" });
        var ex = await Assert.ThrowsAsync<UsernameExistsException>(() =>
            _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "dup" }));
        Assert.Equal("UsernameExistsException", ex.ErrorCode);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // AUTH FLOWS
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task AdminSetUserPasswordAndAuth()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "PwdPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "PwdApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "dave" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "dave",
            Password = "NewPass123!",
            Permanent = true,
        });

        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "dave",
                ["PASSWORD"] = "NewPass123!",
            },
        });
        Assert.NotNull(auth.AuthenticationResult);
        Assert.NotEmpty(auth.AuthenticationResult.AccessToken);
    }

    [Fact]
    public async Task AdminInitiateAuthWrongPassword()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "AuthFailPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "AuthFailApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "eve" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "eve",
            Password = "Correct1!",
            Permanent = true,
        });

        var ex = await Assert.ThrowsAsync<NotAuthorizedException>(() =>
            _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
            {
                UserPoolId = pid,
                ClientId = cid,
                AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
                AuthParameters = new Dictionary<string, string>
                {
                    ["USERNAME"] = "eve",
                    ["PASSWORD"] = "Wrong1!",
                },
            }));
        Assert.Equal("NotAuthorizedException", ex.ErrorCode);
    }

    [Fact]
    public async Task InitiateAuthUserPassword()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "InitiateAuthPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "InitiateApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "frank" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "frank",
            Password = "FrankPass1!",
            Permanent = true,
        });

        var auth = await _idp.InitiateAuthAsync(new InitiateAuthRequest
        {
            ClientId = cid,
            AuthFlow = AuthFlowType.USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "frank",
                ["PASSWORD"] = "FrankPass1!",
            },
        });
        Assert.NotNull(auth.AuthenticationResult);
        Assert.NotEmpty(auth.AuthenticationResult.AccessToken);
        Assert.NotEmpty(auth.AuthenticationResult.IdToken);
        Assert.NotEmpty(auth.AuthenticationResult.RefreshToken);
    }

    [Fact]
    public async Task ForceChangePasswordChallenge()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "ForceChangePool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "ForceApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest
        {
            UserPoolId = pid,
            Username = "forceuser",
            TemporaryPassword = "TempPwd1!",
        });

        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "forceuser",
                ["PASSWORD"] = "TempPwd1!",
            },
        });
        Assert.Equal(ChallengeNameType.NEW_PASSWORD_REQUIRED, auth.ChallengeName);
        Assert.NotEmpty(auth.Session);
    }

    [Fact]
    public async Task RespondToAuthChallengeNewPassword()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "ChallengePool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "ChallengeApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "newpwduser" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "newpwduser",
            Password = "TempPass1!",
            Permanent = false,
        });

        var auth = await _idp.InitiateAuthAsync(new InitiateAuthRequest
        {
            ClientId = cid,
            AuthFlow = AuthFlowType.USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "newpwduser",
                ["PASSWORD"] = "TempPass1!",
            },
        });
        Assert.Equal(ChallengeNameType.NEW_PASSWORD_REQUIRED, auth.ChallengeName);

        var result = await _idp.RespondToAuthChallengeAsync(new RespondToAuthChallengeRequest
        {
            ClientId = cid,
            ChallengeName = ChallengeNameType.NEW_PASSWORD_REQUIRED,
            Session = auth.Session,
            ChallengeResponses = new Dictionary<string, string>
            {
                ["USERNAME"] = "newpwduser",
                ["NEW_PASSWORD"] = "FinalPass1!",
            },
        });
        Assert.NotNull(result.AuthenticationResult);

        var user = await _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "newpwduser" });
        Assert.Equal("CONFIRMED", user.UserStatus);
    }

    [Fact]
    public async Task RefreshTokenAuthCorrectUser()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "RefreshPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "RefreshApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        foreach (var (name, pw) in new[] { ("first", "First1!"), ("second", "Second1!") })
        {
            await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = name });
            await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
            {
                UserPoolId = pid,
                Username = name,
                Password = pw,
                Permanent = true,
            });
        }

        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "second",
                ["PASSWORD"] = "Second1!",
            },
        });

        var refresh = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.REFRESH_TOKEN_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["REFRESH_TOKEN"] = auth.AuthenticationResult.RefreshToken,
            },
        });
        Assert.NotNull(refresh.AuthenticationResult);

        var user = await _idp.GetUserAsync(new GetUserRequest
        {
            AccessToken = refresh.AuthenticationResult.AccessToken,
        });
        Assert.Equal("second", user.Username);
    }

    [Fact]
    public async Task RefreshTokenAlias()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "RefreshAliasPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "RefreshAliasApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "aliasuser" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "aliasuser",
            Password = "AliasPass1!",
            Permanent = true,
        });

        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "aliasuser",
                ["PASSWORD"] = "AliasPass1!",
            },
        });

        var refresh = await _idp.InitiateAuthAsync(new InitiateAuthRequest
        {
            ClientId = cid,
            AuthFlow = AuthFlowType.REFRESH_TOKEN,
            AuthParameters = new Dictionary<string, string>
            {
                ["REFRESH_TOKEN"] = auth.AuthenticationResult.RefreshToken,
            },
        });
        Assert.NotNull(refresh.AuthenticationResult);
        Assert.NotEmpty(refresh.AuthenticationResult.AccessToken);
        // AWS doesn't return a new refresh token
        Assert.Null(refresh.AuthenticationResult.RefreshToken);
    }

    [Fact]
    public async Task DisabledUserAuthFails()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "DisabledAuthPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "DisabledApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "disabled" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "disabled",
            Password = "Dis1!",
            Permanent = true,
        });
        await _idp.AdminDisableUserAsync(new AdminDisableUserRequest { UserPoolId = pid, Username = "disabled" });

        await Assert.ThrowsAsync<NotAuthorizedException>(() =>
            _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
            {
                UserPoolId = pid,
                ClientId = cid,
                AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
                AuthParameters = new Dictionary<string, string>
                {
                    ["USERNAME"] = "disabled",
                    ["PASSWORD"] = "Dis1!",
                },
            }));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SELF-SERVICE
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SignUpAndConfirm()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "SignupPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "SignupApp",
        })).UserPoolClient.ClientId;

        var resp = await _idp.SignUpAsync(new SignUpRequest
        {
            ClientId = cid,
            Username = "grace",
            Password = "GracePass1!",
            UserAttributes = [new AttributeType { Name = "email", Value = "grace@example.com" }],
        });
        Assert.NotEmpty(resp.UserSub);

        await _idp.ConfirmSignUpAsync(new ConfirmSignUpRequest
        {
            ClientId = cid,
            Username = "grace",
            ConfirmationCode = "123456",
        });

        var user = await _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "grace" });
        Assert.Equal("CONFIRMED", user.UserStatus);
    }

    [Fact]
    public async Task SignUpAlwaysUnconfirmed()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest
        {
            PoolName = "AutoVerifyPool",
            AutoVerifiedAttributes = ["email"],
        })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "AutoVerifyApp",
        })).UserPoolClient.ClientId;

        var resp = await _idp.SignUpAsync(new SignUpRequest
        {
            ClientId = cid,
            Username = "testuser",
            Password = "TestPass1!",
            UserAttributes = [new AttributeType { Name = "email", Value = "test@example.com" }],
        });
        Assert.False(resp.UserConfirmed);

        var user = await _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "testuser" });
        Assert.Equal("UNCONFIRMED", user.UserStatus);
    }

    [Fact]
    public async Task AdminConfirmSignUp()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "AdminConfirmPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "AdminConfirmApp",
        })).UserPoolClient.ClientId;

        await _idp.SignUpAsync(new SignUpRequest
        {
            ClientId = cid,
            Username = "olivia",
            Password = "OliviaPass1!",
        });
        await _idp.AdminConfirmSignUpAsync(new AdminConfirmSignUpRequest
        {
            UserPoolId = pid,
            Username = "olivia",
        });

        var user = await _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "olivia" });
        Assert.Equal("CONFIRMED", user.UserStatus);
    }

    [Fact]
    public async Task ForgotPasswordAndConfirm()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "ForgotPwdPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "ForgotApp",
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "henry" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "henry",
            Password = "OldPass1!",
            Permanent = true,
        });

        await _idp.ForgotPasswordAsync(new ForgotPasswordRequest
        {
            ClientId = cid,
            Username = "henry",
        });

        await _idp.ConfirmForgotPasswordAsync(new ConfirmForgotPasswordRequest
        {
            ClientId = cid,
            Username = "henry",
            ConfirmationCode = "654321",
            Password = "NewPass2!",
        });
    }

    [Fact]
    public async Task ChangePassword()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "ChangePwdPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "ChangePwdApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "pwduser" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "pwduser",
            Password = "OldPass1!",
            Permanent = true,
        });

        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "pwduser",
                ["PASSWORD"] = "OldPass1!",
            },
        });

        await _idp.ChangePasswordAsync(new ChangePasswordRequest
        {
            AccessToken = auth.AuthenticationResult.AccessToken,
            PreviousPassword = "OldPass1!",
            ProposedPassword = "NewPass2!",
        });

        // New password works
        var auth2 = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "pwduser",
                ["PASSWORD"] = "NewPass2!",
            },
        });
        Assert.NotNull(auth2.AuthenticationResult);

        // Old password fails
        await Assert.ThrowsAsync<NotAuthorizedException>(() =>
            _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
            {
                UserPoolId = pid,
                ClientId = cid,
                AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
                AuthParameters = new Dictionary<string, string>
                {
                    ["USERNAME"] = "pwduser",
                    ["PASSWORD"] = "OldPass1!",
                },
            }));
    }

    [Fact]
    public async Task GetUserFromToken()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "GetUserPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "GetUserApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest
        {
            UserPoolId = pid,
            Username = "maya",
            UserAttributes = [new AttributeType { Name = "email", Value = "maya@example.com" }],
        });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "maya",
            Password = "MayaPass1!",
            Permanent = true,
        });

        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "maya",
                ["PASSWORD"] = "MayaPass1!",
            },
        });

        var user = await _idp.GetUserAsync(new GetUserRequest
        {
            AccessToken = auth.AuthenticationResult.AccessToken,
        });
        Assert.Equal("maya", user.Username);
    }

    [Fact]
    public async Task UpdateUserAttributesViaToken()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "UpdateAttrTokenPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "UpdateAttrApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest
        {
            UserPoolId = pid,
            Username = "attrupdate",
            UserAttributes = [new AttributeType { Name = "email", Value = "old@example.com" }],
        });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "attrupdate",
            Password = "AttrPass1!",
            Permanent = true,
        });

        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "attrupdate",
                ["PASSWORD"] = "AttrPass1!",
            },
        });

        await _idp.UpdateUserAttributesAsync(new UpdateUserAttributesRequest
        {
            AccessToken = auth.AuthenticationResult.AccessToken,
            UserAttributes = [new AttributeType { Name = "email", Value = "new@example.com" }],
        });

        var user = await _idp.AdminGetUserAsync(new AdminGetUserRequest
        {
            UserPoolId = pid,
            Username = "attrupdate",
        });
        var attrs = user.UserAttributes.ToDictionary(a => a.Name, a => a.Value);
        Assert.Equal("new@example.com", attrs["email"]);
    }

    [Fact]
    public async Task DeleteUserViaToken()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "DeleteSelfPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "DeleteSelfApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "selfdelete" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "selfdelete",
            Password = "DelPass1!",
            Permanent = true,
        });

        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "selfdelete",
                ["PASSWORD"] = "DelPass1!",
            },
        });

        await _idp.DeleteUserAsync(new DeleteUserRequest
        {
            AccessToken = auth.AuthenticationResult.AccessToken,
        });

        await Assert.ThrowsAsync<UserNotFoundException>(() =>
            _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "selfdelete" }));
    }

    [Fact]
    public async Task GlobalSignOut()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "SignOutPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "SignOutApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "noah" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "noah",
            Password = "NoahPass1!",
            Permanent = true,
        });

        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "noah",
                ["PASSWORD"] = "NoahPass1!",
            },
        });

        // Must not throw
        await _idp.GlobalSignOutAsync(new GlobalSignOutRequest
        {
            AccessToken = auth.AuthenticationResult.AccessToken,
        });
    }

    [Fact]
    public async Task RevokeToken()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "RevokePool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "RevokeApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "revokeuser" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "revokeuser",
            Password = "RevokePass1!",
            Permanent = true,
        });

        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "revokeuser",
                ["PASSWORD"] = "RevokePass1!",
            },
        });

        // Must not throw
        await _idp.RevokeTokenAsync(new RevokeTokenRequest
        {
            Token = auth.AuthenticationResult.RefreshToken,
            ClientId = cid,
        });
    }

    [Fact]
    public async Task AdminResetUserPassword()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "ResetPwdPool" })).UserPool.Id;
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "resetuser" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "resetuser",
            Password = "Pass1!",
            Permanent = true,
        });

        await _idp.AdminResetUserPasswordAsync(new AdminResetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "resetuser",
        });

        var user = await _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "resetuser" });
        Assert.Equal("RESET_REQUIRED", user.UserStatus);
    }

    [Fact]
    public async Task AdminUserGlobalSignOut()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "AdminSignOutPool" })).UserPool.Id;
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "signoutuser" });

        // Must not throw
        await _idp.AdminUserGlobalSignOutAsync(new AdminUserGlobalSignOutRequest
        {
            UserPoolId = pid,
            Username = "signoutuser",
        });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // GROUPS
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GroupsCrud()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "GroupPool" })).UserPool.Id;

        var createResp = await _idp.CreateGroupAsync(new CreateGroupRequest
        {
            UserPoolId = pid,
            GroupName = "admins",
            Description = "Admins",
        });
        Assert.Equal("admins", createResp.Group.GroupName);

        var group = (await _idp.GetGroupAsync(new GetGroupRequest { UserPoolId = pid, GroupName = "admins" })).Group;
        Assert.Equal("Admins", group.Description);

        var groups = (await _idp.ListGroupsAsync(new ListGroupsRequest { UserPoolId = pid })).Groups;
        Assert.Contains(groups, g => g.GroupName == "admins");

        await _idp.DeleteGroupAsync(new DeleteGroupRequest { UserPoolId = pid, GroupName = "admins" });
        var groups2 = (await _idp.ListGroupsAsync(new ListGroupsRequest { UserPoolId = pid })).Groups;
        Assert.DoesNotContain(groups2, g => g.GroupName == "admins");
    }

    [Fact]
    public async Task AdminAddRemoveUserFromGroup()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "GroupMemberPool" })).UserPool.Id;
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "liam" });
        await _idp.CreateGroupAsync(new CreateGroupRequest { UserPoolId = pid, GroupName = "editors" });

        await _idp.AdminAddUserToGroupAsync(new AdminAddUserToGroupRequest
        {
            UserPoolId = pid,
            Username = "liam",
            GroupName = "editors",
        });

        var members = (await _idp.ListUsersInGroupAsync(new ListUsersInGroupRequest
        {
            UserPoolId = pid,
            GroupName = "editors",
        })).Users;
        Assert.Contains(members, u => u.Username == "liam");

        var groupsForUser = (await _idp.AdminListGroupsForUserAsync(new AdminListGroupsForUserRequest
        {
            UserPoolId = pid,
            Username = "liam",
        })).Groups;
        Assert.Contains(groupsForUser, g => g.GroupName == "editors");

        await _idp.AdminRemoveUserFromGroupAsync(new AdminRemoveUserFromGroupRequest
        {
            UserPoolId = pid,
            Username = "liam",
            GroupName = "editors",
        });

        var members2 = (await _idp.ListUsersInGroupAsync(new ListUsersInGroupRequest
        {
            UserPoolId = pid,
            GroupName = "editors",
        })).Users;
        Assert.DoesNotContain(members2, u => u.Username == "liam");
    }

    [Fact]
    public async Task ListUsersInGroup()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "GroupMembersPool" })).UserPool.Id;
        await _idp.CreateGroupAsync(new CreateGroupRequest { UserPoolId = pid, GroupName = "grp" });

        foreach (var u in new[] { "u1", "u2", "u3" })
        {
            await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = u });
            await _idp.AdminAddUserToGroupAsync(new AdminAddUserToGroupRequest
            {
                UserPoolId = pid,
                Username = u,
                GroupName = "grp",
            });
        }

        var members = (await _idp.ListUsersInGroupAsync(new ListUsersInGroupRequest
        {
            UserPoolId = pid,
            GroupName = "grp",
        })).Users;
        var names = members.Select(u => u.Username).ToHashSet();
        Assert.Equal(new HashSet<string> { "u1", "u2", "u3" }, names);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DOMAIN
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DomainCrud()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "DomainPool" })).UserPool.Id;

        var createResp = await _idp.CreateUserPoolDomainAsync(new CreateUserPoolDomainRequest
        {
            UserPoolId = pid,
            Domain = "my-test-domain",
        });
        Assert.NotNull(createResp.CloudFrontDomain);

        var desc = await _idp.DescribeUserPoolDomainAsync(new DescribeUserPoolDomainRequest
        {
            Domain = "my-test-domain",
        });
        Assert.Equal(pid, desc.DomainDescription.UserPoolId);
        Assert.Equal("ACTIVE", desc.DomainDescription.Status.Value);

        await _idp.DeleteUserPoolDomainAsync(new DeleteUserPoolDomainRequest
        {
            UserPoolId = pid,
            Domain = "my-test-domain",
        });

        var desc2 = await _idp.DescribeUserPoolDomainAsync(new DescribeUserPoolDomainRequest
        {
            Domain = "my-test-domain",
        });
        Assert.Null(desc2.DomainDescription.UserPoolId);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // MFA
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MfaConfig()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "MfaPool" })).UserPool.Id;

        var resp = await _idp.GetUserPoolMfaConfigAsync(new GetUserPoolMfaConfigRequest { UserPoolId = pid });
        Assert.Equal(UserPoolMfaType.OFF, resp.MfaConfiguration);

        await _idp.SetUserPoolMfaConfigAsync(new SetUserPoolMfaConfigRequest
        {
            UserPoolId = pid,
            SoftwareTokenMfaConfiguration = new SoftwareTokenMfaConfigType { Enabled = true },
            MfaConfiguration = UserPoolMfaType.OPTIONAL,
        });

        var resp2 = await _idp.GetUserPoolMfaConfigAsync(new GetUserPoolMfaConfigRequest { UserPoolId = pid });
        Assert.Equal(UserPoolMfaType.OPTIONAL, resp2.MfaConfiguration);
        Assert.True(resp2.SoftwareTokenMfaConfiguration.Enabled);
    }

    [Fact]
    public async Task TotpFullFlow()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "TotpFullPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "TotpApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.SetUserPoolMfaConfigAsync(new SetUserPoolMfaConfigRequest
        {
            UserPoolId = pid,
            SoftwareTokenMfaConfiguration = new SoftwareTokenMfaConfigType { Enabled = true },
            MfaConfiguration = UserPoolMfaType.ON,
        });

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest
        {
            UserPoolId = pid,
            Username = "totp-user",
            TemporaryPassword = "Tmp1!",
        });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "totp-user",
            Password = "Perm1!",
            Permanent = true,
        });

        // Pool ON with no enrollment → auth succeeds
        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "totp-user",
                ["PASSWORD"] = "Perm1!",
            },
        });
        Assert.NotNull(auth.AuthenticationResult);
        var accessToken = auth.AuthenticationResult.AccessToken;

        // Associate software token
        var assoc = await _idp.AssociateSoftwareTokenAsync(new AssociateSoftwareTokenRequest
        {
            AccessToken = accessToken,
        });
        Assert.NotEmpty(assoc.SecretCode);

        // Verify software token
        var verify = await _idp.VerifySoftwareTokenAsync(new VerifySoftwareTokenRequest
        {
            AccessToken = accessToken,
            UserCode = "123456",
        });
        Assert.Equal(VerifySoftwareTokenResponseType.SUCCESS, verify.Status);

        // Now auth should return SOFTWARE_TOKEN_MFA challenge
        var auth2 = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string>
            {
                ["USERNAME"] = "totp-user",
                ["PASSWORD"] = "Perm1!",
            },
        });
        Assert.Equal(ChallengeNameType.SOFTWARE_TOKEN_MFA, auth2.ChallengeName);
        Assert.NotEmpty(auth2.Session);

        // Respond with TOTP code
        var result = await _idp.AdminRespondToAuthChallengeAsync(new AdminRespondToAuthChallengeRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            ChallengeName = ChallengeNameType.SOFTWARE_TOKEN_MFA,
            ChallengeResponses = new Dictionary<string, string>
            {
                ["USERNAME"] = "totp-user",
                ["SOFTWARE_TOKEN_MFA_CODE"] = "123456",
            },
        });
        Assert.NotNull(result.AuthenticationResult);
        Assert.NotEmpty(result.AuthenticationResult.AccessToken);
    }

    [Fact]
    public async Task TotpOptionalMfa()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "TotpOptPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "TotpOptApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.SetUserPoolMfaConfigAsync(new SetUserPoolMfaConfigRequest
        {
            UserPoolId = pid,
            SoftwareTokenMfaConfiguration = new SoftwareTokenMfaConfigType { Enabled = true },
            MfaConfiguration = UserPoolMfaType.OPTIONAL,
        });

        // User without MFA enrolled
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "no-mfa-user", TemporaryPassword = "Tmp1!" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest { UserPoolId = pid, Username = "no-mfa-user", Password = "Perm1!", Permanent = true });
        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string> { ["USERNAME"] = "no-mfa-user", ["PASSWORD"] = "Perm1!" },
        });
        Assert.NotNull(auth.AuthenticationResult); // no challenge

        // User with MFA enrolled
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "mfa-user", TemporaryPassword = "Tmp1!" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest { UserPoolId = pid, Username = "mfa-user", Password = "Perm1!", Permanent = true });
        await _idp.AdminSetUserMFAPreferenceAsync(new AdminSetUserMFAPreferenceRequest
        {
            UserPoolId = pid,
            Username = "mfa-user",
            SoftwareTokenMfaSettings = new SoftwareTokenMfaSettingsType { Enabled = true, PreferredMfa = true },
        });

        var auth2 = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string> { ["USERNAME"] = "mfa-user", ["PASSWORD"] = "Perm1!" },
        });
        Assert.Equal(ChallengeNameType.SOFTWARE_TOKEN_MFA, auth2.ChallengeName);
    }

    [Fact]
    public async Task AdminGetUserMfaFields()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "MfaFieldsPool" })).UserPool.Id;
        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest
        {
            UserPoolId = pid,
            Username = "mfa-check",
            TemporaryPassword = "Tmp1!",
        });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
        {
            UserPoolId = pid,
            Username = "mfa-check",
            Password = "Perm1!",
            Permanent = true,
        });

        var u = await _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "mfa-check" });
        Assert.Empty(u.UserMFASettingList ?? []);
        Assert.True(string.IsNullOrEmpty(u.PreferredMfaSetting));

        await _idp.AdminSetUserMFAPreferenceAsync(new AdminSetUserMFAPreferenceRequest
        {
            UserPoolId = pid,
            Username = "mfa-check",
            SoftwareTokenMfaSettings = new SoftwareTokenMfaSettingsType { Enabled = true, PreferredMfa = true },
        });

        var u2 = await _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "mfa-check" });
        Assert.Contains("SOFTWARE_TOKEN_MFA", u2.UserMFASettingList);
        Assert.Equal("SOFTWARE_TOKEN_MFA", u2.PreferredMfaSetting);
    }

    [Fact]
    public async Task SetUserMfaPreferenceViaToken()
    {
        var pid = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "MfaSelfEnrollPool" })).UserPool.Id;
        var cid = (await _idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
        {
            UserPoolId = pid,
            ClientName = "MfaSelfApp",
            ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
        })).UserPoolClient.ClientId;

        await _idp.AdminCreateUserAsync(new AdminCreateUserRequest { UserPoolId = pid, Username = "self-enroll", TemporaryPassword = "Tmp1!" });
        await _idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest { UserPoolId = pid, Username = "self-enroll", Password = "Perm1!", Permanent = true });

        var auth = await _idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
        {
            UserPoolId = pid,
            ClientId = cid,
            AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
            AuthParameters = new Dictionary<string, string> { ["USERNAME"] = "self-enroll", ["PASSWORD"] = "Perm1!" },
        });

        await _idp.SetUserMFAPreferenceAsync(new SetUserMFAPreferenceRequest
        {
            AccessToken = auth.AuthenticationResult.AccessToken,
            SoftwareTokenMfaSettings = new SoftwareTokenMfaSettingsType { Enabled = true, PreferredMfa = true },
        });

        var u = await _idp.AdminGetUserAsync(new AdminGetUserRequest { UserPoolId = pid, Username = "self-enroll" });
        Assert.Contains("SOFTWARE_TOKEN_MFA", u.UserMFASettingList);
        Assert.Equal("SOFTWARE_TOKEN_MFA", u.PreferredMfaSetting);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // TAGS
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Tags()
    {
        var createResp = await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "TagPool" });
        var arn = createResp.UserPool.Arn;

        await _idp.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = arn,
            Tags = new Dictionary<string, string> { ["project"] = "microstack" },
        });

        var tags = (await _idp.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceArn = arn })).Tags;
        Assert.Equal("microstack", tags["project"]);

        await _idp.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceArn = arn,
            TagKeys = ["project"],
        });

        var tags2 = (await _idp.ListTagsForResourceAsync(new ListTagsForResourceRequest { ResourceArn = arn })).Tags;
        Assert.DoesNotContain("project", tags2.Keys);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // COGNITO IDENTITY (Identity Pools)
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task IdentityPoolCrud()
    {
        var resp = await _identity.CreateIdentityPoolAsync(new CreateIdentityPoolRequest
        {
            IdentityPoolName = "TestIdPool",
            AllowUnauthenticatedIdentities = false,
        });
        var iid = resp.IdentityPoolId;
        Assert.Equal("TestIdPool", resp.IdentityPoolName);
        Assert.StartsWith("us-east-1:", iid);

        var desc = await _identity.DescribeIdentityPoolAsync(new DescribeIdentityPoolRequest { IdentityPoolId = iid });
        Assert.Equal(iid, desc.IdentityPoolId);
        Assert.Equal("TestIdPool", desc.IdentityPoolName);

        var pools = (await _identity.ListIdentityPoolsAsync(new ListIdentityPoolsRequest { MaxResults = 60 })).IdentityPools;
        Assert.Contains(pools, p => p.IdentityPoolId == iid);

        await _identity.UpdateIdentityPoolAsync(new UpdateIdentityPoolRequest
        {
            IdentityPoolId = iid,
            IdentityPoolName = "TestIdPool",
            AllowUnauthenticatedIdentities = true,
        });
        var desc2 = await _identity.DescribeIdentityPoolAsync(new DescribeIdentityPoolRequest { IdentityPoolId = iid });
        Assert.True(desc2.AllowUnauthenticatedIdentities);

        await _identity.DeleteIdentityPoolAsync(new DeleteIdentityPoolRequest { IdentityPoolId = iid });
        var pools2 = (await _identity.ListIdentityPoolsAsync(new ListIdentityPoolsRequest { MaxResults = 60 })).IdentityPools;
        Assert.DoesNotContain(pools2, p => p.IdentityPoolId == iid);
    }

    [Fact]
    public async Task GetIdAndCredentials()
    {
        var resp = await _identity.CreateIdentityPoolAsync(new CreateIdentityPoolRequest
        {
            IdentityPoolName = "CredsPool",
            AllowUnauthenticatedIdentities = true,
        });
        var iid = resp.IdentityPoolId;

        var idResp = await _identity.GetIdAsync(new GetIdRequest
        {
            IdentityPoolId = iid,
            AccountId = "000000000000",
        });
        var identityId = idResp.IdentityId;
        Assert.NotEmpty(identityId);

        var creds = await _identity.GetCredentialsForIdentityAsync(new GetCredentialsForIdentityRequest
        {
            IdentityId = identityId,
        });
        Assert.Equal(identityId, creds.IdentityId);
        Assert.StartsWith("ASIA", creds.Credentials.AccessKeyId);
        Assert.NotEmpty(creds.Credentials.SecretKey);
        Assert.NotEmpty(creds.Credentials.SessionToken);
    }

    [Fact]
    public async Task IdentityPoolRoles()
    {
        var resp = await _identity.CreateIdentityPoolAsync(new CreateIdentityPoolRequest
        {
            IdentityPoolName = "RolesPool",
            AllowUnauthenticatedIdentities = true,
        });
        var iid = resp.IdentityPoolId;

        await _identity.SetIdentityPoolRolesAsync(new SetIdentityPoolRolesRequest
        {
            IdentityPoolId = iid,
            Roles = new Dictionary<string, string>
            {
                ["authenticated"] = "arn:aws:iam::000000000000:role/AuthRole",
                ["unauthenticated"] = "arn:aws:iam::000000000000:role/UnauthRole",
            },
        });

        var roles = await _identity.GetIdentityPoolRolesAsync(new GetIdentityPoolRolesRequest { IdentityPoolId = iid });
        Assert.Equal("arn:aws:iam::000000000000:role/AuthRole", roles.Roles["authenticated"]);
        Assert.Equal("arn:aws:iam::000000000000:role/UnauthRole", roles.Roles["unauthenticated"]);
    }

    [Fact]
    public async Task ListIdentities()
    {
        var resp = await _identity.CreateIdentityPoolAsync(new CreateIdentityPoolRequest
        {
            IdentityPoolName = "ListIdPool",
            AllowUnauthenticatedIdentities = true,
        });
        var iid = resp.IdentityPoolId;

        var id1 = (await _identity.GetIdAsync(new GetIdRequest { IdentityPoolId = iid, AccountId = "000000000000" })).IdentityId;
        var id2 = (await _identity.GetIdAsync(new GetIdRequest { IdentityPoolId = iid, AccountId = "000000000000" })).IdentityId;

        var identities = (await _identity.ListIdentitiesAsync(new ListIdentitiesRequest
        {
            IdentityPoolId = iid,
            MaxResults = 60,
        })).Identities;
        var ids = identities.Select(i => i.IdentityId).ToList();
        Assert.Contains(id1, ids);
        Assert.Contains(id2, ids);
    }

    [Fact]
    public async Task GetOpenIdToken()
    {
        var resp = await _identity.CreateIdentityPoolAsync(new CreateIdentityPoolRequest
        {
            IdentityPoolName = "OidcPool",
            AllowUnauthenticatedIdentities = true,
        });
        var iid = resp.IdentityPoolId;
        var identityId = (await _identity.GetIdAsync(new GetIdRequest { IdentityPoolId = iid, AccountId = "000000000000" })).IdentityId;

        var tokenResp = await _identity.GetOpenIdTokenAsync(new GetOpenIdTokenRequest { IdentityId = identityId });
        Assert.Equal(identityId, tokenResp.IdentityId);
        var parts = tokenResp.Token.Split('.');
        Assert.Equal(3, parts.Length);
    }

    [Fact]
    public async Task DescribeIdentity()
    {
        var resp = await _identity.CreateIdentityPoolAsync(new CreateIdentityPoolRequest
        {
            IdentityPoolName = "DescribeIdPool",
            AllowUnauthenticatedIdentities = true,
        });
        var iid = resp.IdentityPoolId;
        var identityId = (await _identity.GetIdAsync(new GetIdRequest { IdentityPoolId = iid, AccountId = "000000000000" })).IdentityId;

        var desc = await _identity.DescribeIdentityAsync(new DescribeIdentityRequest { IdentityId = identityId });
        Assert.Equal(identityId, desc.IdentityId);
    }

    [Fact]
    public async Task MergeDeveloperIdentities()
    {
        var resp = await _identity.CreateIdentityPoolAsync(new CreateIdentityPoolRequest
        {
            IdentityPoolName = "MergePool",
            AllowUnauthenticatedIdentities = true,
            DeveloperProviderName = "login.myapp",
        });
        var iid = resp.IdentityPoolId;

        var result = await _identity.MergeDeveloperIdentitiesAsync(new MergeDeveloperIdentitiesRequest
        {
            SourceUserIdentifier = "user-a",
            DestinationUserIdentifier = "user-b",
            DeveloperProviderName = "login.myapp",
            IdentityPoolId = iid,
        });
        Assert.NotEmpty(result.IdentityId);
    }

    [Fact]
    public async Task CredentialsSecretAccessKey()
    {
        var iid = (await _identity.CreateIdentityPoolAsync(new CreateIdentityPoolRequest
        {
            IdentityPoolName = "CredsKeyPool",
            AllowUnauthenticatedIdentities = true,
        })).IdentityPoolId;

        var identityId = (await _identity.GetIdAsync(new GetIdRequest
        {
            IdentityPoolId = iid,
            AccountId = "000000000000",
        })).IdentityId;

        var creds = await _identity.GetCredentialsForIdentityAsync(new GetCredentialsForIdentityRequest
        {
            IdentityId = identityId,
        });
        var c = creds.Credentials;
        Assert.NotEmpty(c.SecretKey);
        Assert.StartsWith("ASIA", c.AccessKeyId);
        Assert.NotEmpty(c.SessionToken);
        Assert.True(c.Expiration > DateTime.UtcNow);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // WELL-KNOWN ENDPOINTS
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task JwksEndpoint()
    {
        var pool = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "JwksPool" })).UserPool;
        var poolId = pool.Id;

        var resp = await _fixture.HttpClient.GetAsync($"/{poolId}/.well-known/jwks.json");
        Assert.Equal(System.Net.HttpStatusCode.OK, resp.StatusCode);

        var json = await resp.Content.ReadAsStringAsync();
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("keys", out var keys));
        Assert.True(keys.GetArrayLength() >= 1);
        var key = keys[0];
        Assert.Equal("RSA", key.GetProperty("kty").GetString());
        Assert.Equal("RS256", key.GetProperty("alg").GetString());
    }

    [Fact]
    public async Task OpenIdConfigurationEndpoint()
    {
        var pool = (await _idp.CreateUserPoolAsync(new CreateUserPoolRequest { PoolName = "OidcConfigPool" })).UserPool;
        var poolId = pool.Id;

        var resp = await _fixture.HttpClient.GetAsync($"/{poolId}/.well-known/openid-configuration");
        Assert.Equal(System.Net.HttpStatusCode.OK, resp.StatusCode);

        var json = await resp.Content.ReadAsStringAsync();
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("issuer", out var issuer));
        Assert.Contains(poolId, issuer.GetString());
        Assert.True(doc.RootElement.TryGetProperty("jwks_uri", out _));
        Assert.True(doc.RootElement.TryGetProperty("token_endpoint", out _));
    }
}
