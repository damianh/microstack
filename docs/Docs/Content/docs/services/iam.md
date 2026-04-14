---
title: IAM
description: IAM emulation — users, roles, policies, access keys, groups, instance profiles, OIDC providers.
order: 9
section: Services
---

# IAM

MicroStack's IAM handler supports users, roles, managed and inline policies, access keys, groups, instance profiles, service-linked roles, and OIDC providers.

## Supported Operations

**Roles**
- CreateRole, GetRole, ListRoles, DeleteRole, UpdateRole, UpdateAssumeRolePolicy
- AttachRolePolicy, DetachRolePolicy, ListAttachedRolePolicies
- PutRolePolicy, GetRolePolicy, DeleteRolePolicy, ListRolePolicies
- TagRole, UntagRole, ListRoleTags

**Users**
- CreateUser, GetUser, ListUsers, DeleteUser
- AttachUserPolicy, DetachUserPolicy, ListAttachedUserPolicies
- PutUserPolicy, GetUserPolicy, DeleteUserPolicy, ListUserPolicies
- CreateAccessKey, ListAccessKeys, DeleteAccessKey
- TagUser, UntagUser, ListUserTags

**Policies**
- CreatePolicy, GetPolicy, GetPolicyVersion, ListPolicyVersions, CreatePolicyVersion, DeletePolicyVersion
- ListPolicies, DeletePolicy, ListEntitiesForPolicy
- TagPolicy, UntagPolicy, ListPolicyTags
- SimulatePrincipalPolicy, SimulateCustomPolicy

**Groups**
- CreateGroup, GetGroup, DeleteGroup, ListGroups
- AddUserToGroup, RemoveUserFromGroup, ListGroupsForUser

**Instance Profiles**
- CreateInstanceProfile, DeleteInstanceProfile, GetInstanceProfile
- AddRoleToInstanceProfile, RemoveRoleFromInstanceProfile
- ListInstanceProfiles, ListInstanceProfilesForRole

**Other**
- CreateServiceLinkedRole, DeleteServiceLinkedRole, GetServiceLinkedRoleDeletionStatus
- CreateOpenIDConnectProvider, GetOpenIDConnectProvider, DeleteOpenIDConnectProvider

## Usage

```csharp
var iam = new AmazonIdentityManagementServiceClient(
    new BasicAWSCredentials("test", "test"),
    new AmazonIdentityManagementServiceConfig { ServiceURL = "http://localhost:4566" });

var assumeRolePolicy = """
    {
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": { "Service": "lambda.amazonaws.com" },
        "Action": "sts:AssumeRole"
      }]
    }
    """;

// Create a role
var role = await iam.CreateRoleAsync(new CreateRoleRequest
{
    RoleName = "my-lambda-role",
    AssumeRolePolicyDocument = assumeRolePolicy,
    Description = "Role for Lambda execution",
});

Console.WriteLine(role.Role.Arn);

// Attach a managed policy
await iam.AttachRolePolicyAsync(new AttachRolePolicyRequest
{
    RoleName = "my-lambda-role",
    PolicyArn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
});
```

## Users and Access Keys

```csharp
// Create a user
await iam.CreateUserAsync(new CreateUserRequest { UserName = "deploy-bot" });

// Create access key for the user
var key = await iam.CreateAccessKeyAsync(new CreateAccessKeyRequest
{
    UserName = "deploy-bot",
});

Console.WriteLine(key.AccessKey.AccessKeyId);       // AKIA...
Console.WriteLine(key.AccessKey.SecretAccessKey);   // secret key value
```

## Instance Profiles

```csharp
// Create role and instance profile, then attach
await iam.CreateRoleAsync(new CreateRoleRequest
{
    RoleName = "ec2-role",
    AssumeRolePolicyDocument = """{"Version":"2012-10-17","Statement":[]}""",
});

var profile = await iam.CreateInstanceProfileAsync(
    new CreateInstanceProfileRequest { InstanceProfileName = "ec2-profile" });

await iam.AddRoleToInstanceProfileAsync(new AddRoleToInstanceProfileRequest
{
    InstanceProfileName = "ec2-profile",
    RoleName = "ec2-role",
});

var describe = await iam.GetInstanceProfileAsync(
    new GetInstanceProfileRequest { InstanceProfileName = "ec2-profile" });

Console.WriteLine(describe.InstanceProfile.Roles[0].RoleName); // ec2-role
```
