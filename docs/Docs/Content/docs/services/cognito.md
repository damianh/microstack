---
title: Cognito
description: Cognito emulation — User Pools with JWT auth, and Identity Pools with federated credentials.
order: 11
section: Services
---

# Cognito

MicroStack emulates both Cognito services: **Cognito User Pools** (identity provider — users, auth flows, JWT tokens, groups, MFA) and **Cognito Identity** (identity pools — federated identities, credential vending).

## Cognito User Pools

### Supported Operations

- CreateUserPool, DeleteUserPool, DescribeUserPool, ListUserPools, UpdateUserPool
- CreateUserPoolClient, DeleteUserPoolClient, DescribeUserPoolClient, ListUserPoolClients, UpdateUserPoolClient
- AdminCreateUser, AdminDeleteUser, AdminGetUser, ListUsers, AdminSetUserPassword
- AdminUpdateUserAttributes, AdminConfirmSignUp, AdminDisableUser, AdminEnableUser
- AdminResetUserPassword, AdminUserGlobalSignOut, AdminListGroupsForUser, AdminListUserAuthEvents
- AdminAddUserToGroup, AdminRemoveUserFromGroup
- AdminInitiateAuth, AdminRespondToAuthChallenge, InitiateAuth, RespondToAuthChallenge
- GlobalSignOut, RevokeToken, SignUp, ConfirmSignUp, ForgotPassword, ConfirmForgotPassword
- ChangePassword, GetUser, UpdateUserAttributes, DeleteUser
- CreateGroup, DeleteGroup, GetGroup, ListGroups, ListUsersInGroup
- CreateUserPoolDomain, DeleteUserPoolDomain, DescribeUserPoolDomain
- GetUserPoolMfaConfig, SetUserPoolMfaConfig
- AssociateSoftwareToken, VerifySoftwareToken, AdminSetUserMFAPreference, SetUserMFAPreference
- TagResource, UntagResource, ListTagsForResource

### Usage

```csharp
var idp = new AmazonCognitoIdentityProviderClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonCognitoIdentityProviderConfig { ServiceURL = "http://localhost:4566" });

// Create a user pool
var pool = await idp.CreateUserPoolAsync(new CreateUserPoolRequest
{
    PoolName = "MyAppPool",
});
var poolId = pool.UserPool.Id;

// Create an app client
var client = await idp.CreateUserPoolClientAsync(new CreateUserPoolClientRequest
{
    UserPoolId = poolId,
    ClientName = "MyApp",
    ExplicitAuthFlows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
});
var clientId = client.UserPoolClient.ClientId;

// Create a user and set password
await idp.AdminCreateUserAsync(new AdminCreateUserRequest
{
    UserPoolId = poolId,
    Username = "alice",
    UserAttributes = [new AttributeType { Name = "email", Value = "alice@example.com" }],
});

await idp.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
{
    UserPoolId = poolId,
    Username = "alice",
    Password = "SecurePass123!",
    Permanent = true,
});
```

### Authentication Flow

```csharp
// Authenticate and get JWT tokens
var auth = await idp.AdminInitiateAuthAsync(new AdminInitiateAuthRequest
{
    UserPoolId = poolId,
    ClientId = clientId,
    AuthFlow = AuthFlowType.ADMIN_USER_PASSWORD_AUTH,
    AuthParameters = new Dictionary<string, string>
    {
        ["USERNAME"] = "alice",
        ["PASSWORD"] = "SecurePass123!",
    },
});

var accessToken = auth.AuthenticationResult.AccessToken;
var idToken = auth.AuthenticationResult.IdToken;
var refreshToken = auth.AuthenticationResult.RefreshToken;
```

:::aside{type="note" title="JWT tokens"}
Access and ID tokens are structurally valid JWTs signed with a local RSA key. They can be decoded and verified, but will not pass signature validation against real AWS Cognito JWKS endpoints.
:::

## Cognito Identity Pools

### Supported Operations

- CreateIdentityPool, DeleteIdentityPool, DescribeIdentityPool, ListIdentityPools, UpdateIdentityPool
- GetId, GetCredentialsForIdentity, GetOpenIdToken
- SetIdentityPoolRoles, GetIdentityPoolRoles
- ListIdentities, DescribeIdentity, MergeDeveloperIdentities, UnlinkDeveloperIdentity, UnlinkIdentity
- TagResource, UntagResource, ListTagsForResource

### Usage

```csharp
var identity = new AmazonCognitoIdentityClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonCognitoIdentityConfig { ServiceURL = "http://localhost:4566" });

// Create identity pool
var pool = await identity.CreateIdentityPoolAsync(new CreateIdentityPoolRequest
{
    IdentityPoolName = "MyIdentityPool",
    AllowUnauthenticatedIdentities = true,
});

// Get a federated identity ID
var idResp = await identity.GetIdAsync(new GetIdRequest
{
    IdentityPoolId = pool.IdentityPoolId,
    AccountId = "000000000000",
});

// Exchange for temporary AWS credentials
var creds = await identity.GetCredentialsForIdentityAsync(
    new GetCredentialsForIdentityRequest { IdentityId = idResp.IdentityId });

Console.WriteLine(creds.Credentials.AccessKeyId);   // ASIA...
Console.WriteLine(creds.Credentials.SecretKey);
Console.WriteLine(creds.Credentials.SessionToken);
```
