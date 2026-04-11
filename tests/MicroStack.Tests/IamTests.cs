using System.Text.Json;
using Amazon;
using Amazon.IdentityManagement;
using Amazon.IdentityManagement.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the IAM service handler.
/// Uses the AWS SDK for .NET v4 pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_iam.py.
/// </summary>
public sealed class IamTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonIdentityManagementServiceClient _iam;

    public IamTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _iam = CreateIamClient(fixture);
    }

    private static AmazonIdentityManagementServiceClient CreateIamClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonIdentityManagementServiceConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonIdentityManagementServiceClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _iam.Dispose();
        return Task.CompletedTask;
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private static string EmptyPolicyDocument => JsonSerializer.Serialize(
        new { Version = "2012-10-17", Statement = Array.Empty<object>() });

    private static string S3PolicyDocument => JsonSerializer.Serialize(
        new
        {
            Version = "2012-10-17",
            Statement = new[]
            {
                new { Effect = "Allow", Action = "s3:GetObject", Resource = "arn:aws:s3:::my-bucket/*" },
            },
        });

    private static string AssumeRolePolicyDocument => JsonSerializer.Serialize(
        new
        {
            Version = "2012-10-17",
            Statement = new[]
            {
                new
                {
                    Effect = "Allow",
                    Principal = new { Service = "lambda.amazonaws.com" },
                    Action = "sts:AssumeRole",
                },
            },
        });

    // ── Role & User CRUD ────────────────────────────────────────────────────────

    [Fact]
    public async Task RoleAndUserCrud()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "test-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });

        var roles = (await _iam.ListRolesAsync(new ListRolesRequest())).Roles ?? [];
        Assert.Contains(roles, r => r.RoleName == "test-role");

        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "test-user" });

        var users = (await _iam.ListUsersAsync(new ListUsersRequest())).Users ?? [];
        Assert.Contains(users, u => u.UserName == "test-user");
    }

    [Fact]
    public async Task CreateUser()
    {
        var resp = await _iam.CreateUserAsync(new CreateUserRequest { UserName = "iam-test-user" });
        var user = resp.User;
        Assert.Equal("iam-test-user", user.UserName);
        Assert.NotEmpty(user.Arn);
        Assert.NotEmpty(user.UserId);
    }

    [Fact]
    public async Task GetUser()
    {
        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "iam-test-user" });
        var resp = await _iam.GetUserAsync(new GetUserRequest { UserName = "iam-test-user" });
        Assert.Equal("iam-test-user", resp.User.UserName);
    }

    [Fact]
    public async Task GetUserNotFound()
    {
        var ex = await Assert.ThrowsAsync<NoSuchEntityException>(
            () => _iam.GetUserAsync(new GetUserRequest { UserName = "ghost-user-xyz" }));
        Assert.Equal("NoSuchEntity", ex.ErrorCode);
    }

    [Fact]
    public async Task ListUsers()
    {
        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "iam-test-user" });
        var resp = await _iam.ListUsersAsync(new ListUsersRequest());
        var names = (resp.Users ?? []).Select(u => u.UserName).ToList();
        Assert.Contains("iam-test-user", names);
    }

    [Fact]
    public async Task DeleteUser()
    {
        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "iam-del-user" });
        await _iam.DeleteUserAsync(new DeleteUserRequest { UserName = "iam-del-user" });

        var ex = await Assert.ThrowsAsync<NoSuchEntityException>(
            () => _iam.GetUserAsync(new GetUserRequest { UserName = "iam-del-user" }));
        Assert.Equal("NoSuchEntity", ex.ErrorCode);
    }

    // ── Role operations ─────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateRole()
    {
        var resp = await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "iam-test-role",
            AssumeRolePolicyDocument = AssumeRolePolicyDocument,
            Description = "integration test role",
        });

        var role = resp.Role;
        Assert.Equal("iam-test-role", role.RoleName);
        Assert.NotEmpty(role.Arn);
        Assert.NotEmpty(role.RoleId);
    }

    [Fact]
    public async Task GetRole()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "iam-test-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });

        var resp = await _iam.GetRoleAsync(new GetRoleRequest { RoleName = "iam-test-role" });
        Assert.Equal("iam-test-role", resp.Role.RoleName);
    }

    [Fact]
    public async Task ListRoles()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "iam-test-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });

        var resp = await _iam.ListRolesAsync(new ListRolesRequest());
        var names = (resp.Roles ?? []).Select(r => r.RoleName).ToList();
        Assert.Contains("iam-test-role", names);
    }

    [Fact]
    public async Task DeleteRole()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "iam-del-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });

        await _iam.DeleteRoleAsync(new DeleteRoleRequest { RoleName = "iam-del-role" });

        var ex = await Assert.ThrowsAsync<NoSuchEntityException>(
            () => _iam.GetRoleAsync(new GetRoleRequest { RoleName = "iam-del-role" }));
        Assert.Equal("NoSuchEntity", ex.ErrorCode);
    }

    // ── Policy CRUD ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task CreatePolicy()
    {
        var resp = await _iam.CreatePolicyAsync(new CreatePolicyRequest
        {
            PolicyName = "iam-test-policy",
            PolicyDocument = S3PolicyDocument,
        });

        var pol = resp.Policy;
        Assert.Equal("iam-test-policy", pol.PolicyName);
        Assert.NotEmpty(pol.Arn);
        Assert.Equal("v1", pol.DefaultVersionId);
    }

    [Fact]
    public async Task GetPolicy()
    {
        var createResp = await _iam.CreatePolicyAsync(new CreatePolicyRequest
        {
            PolicyName = "iam-test-policy",
            PolicyDocument = S3PolicyDocument,
        });
        var arn = createResp.Policy.Arn;

        var resp = await _iam.GetPolicyAsync(new GetPolicyRequest { PolicyArn = arn });
        Assert.Equal("iam-test-policy", resp.Policy.PolicyName);
    }

    // ── Attach / Detach role policy ─────────────────────────────────────────────

    [Fact]
    public async Task AttachRolePolicy()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "iam-test-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });
        var policyArn = (await _iam.CreatePolicyAsync(new CreatePolicyRequest
        {
            PolicyName = "iam-test-policy",
            PolicyDocument = S3PolicyDocument,
        })).Policy.Arn;

        await _iam.AttachRolePolicyAsync(new AttachRolePolicyRequest
        {
            RoleName = "iam-test-role",
            PolicyArn = policyArn,
        });
    }

    [Fact]
    public async Task ListAttachedRolePolicies()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "iam-test-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });
        var policyArn = (await _iam.CreatePolicyAsync(new CreatePolicyRequest
        {
            PolicyName = "iam-test-policy",
            PolicyDocument = S3PolicyDocument,
        })).Policy.Arn;

        await _iam.AttachRolePolicyAsync(new AttachRolePolicyRequest
        {
            RoleName = "iam-test-role",
            PolicyArn = policyArn,
        });

        var resp = await _iam.ListAttachedRolePoliciesAsync(
            new ListAttachedRolePoliciesRequest { RoleName = "iam-test-role" });
        var arns = (resp.AttachedPolicies ?? []).Select(p => p.PolicyArn).ToList();
        Assert.Contains(policyArn, arns);
    }

    [Fact]
    public async Task DetachRolePolicy()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "iam-test-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });
        var policyArn = (await _iam.CreatePolicyAsync(new CreatePolicyRequest
        {
            PolicyName = "iam-test-policy",
            PolicyDocument = S3PolicyDocument,
        })).Policy.Arn;

        await _iam.AttachRolePolicyAsync(new AttachRolePolicyRequest
        {
            RoleName = "iam-test-role",
            PolicyArn = policyArn,
        });

        await _iam.DetachRolePolicyAsync(new DetachRolePolicyRequest
        {
            RoleName = "iam-test-role",
            PolicyArn = policyArn,
        });

        var resp = await _iam.ListAttachedRolePoliciesAsync(
            new ListAttachedRolePoliciesRequest { RoleName = "iam-test-role" });
        var arns = (resp.AttachedPolicies ?? []).Select(p => p.PolicyArn).ToList();
        Assert.DoesNotContain(policyArn, arns);
    }

    // ── Inline role policy ──────────────────────────────────────────────────────

    [Fact]
    public async Task PutRolePolicy()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "iam-test-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });

        var inlineDoc = JsonSerializer.Serialize(new
        {
            Version = "2012-10-17",
            Statement = new[]
            {
                new { Effect = "Allow", Action = "logs:*", Resource = "*" },
            },
        });

        await _iam.PutRolePolicyAsync(new PutRolePolicyRequest
        {
            RoleName = "iam-test-role",
            PolicyName = "inline-logs",
            PolicyDocument = inlineDoc,
        });
    }

    [Fact]
    public async Task GetRolePolicy()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "iam-test-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });

        var inlineDoc = JsonSerializer.Serialize(new
        {
            Version = "2012-10-17",
            Statement = new[]
            {
                new { Effect = "Allow", Action = "logs:*", Resource = "*" },
            },
        });

        await _iam.PutRolePolicyAsync(new PutRolePolicyRequest
        {
            RoleName = "iam-test-role",
            PolicyName = "inline-logs",
            PolicyDocument = inlineDoc,
        });

        var resp = await _iam.GetRolePolicyAsync(new GetRolePolicyRequest
        {
            RoleName = "iam-test-role",
            PolicyName = "inline-logs",
        });

        Assert.Equal("iam-test-role", resp.RoleName);
        Assert.Equal("inline-logs", resp.PolicyName);

        var decoded = Uri.UnescapeDataString(resp.PolicyDocument);
        var doc = JsonDocument.Parse(decoded);
        var action = doc.RootElement.GetProperty("Statement")[0].GetProperty("Action").GetString();
        Assert.Equal("logs:*", action);
    }

    [Fact]
    public async Task ListRolePolicies()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "iam-test-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });

        await _iam.PutRolePolicyAsync(new PutRolePolicyRequest
        {
            RoleName = "iam-test-role",
            PolicyName = "inline-logs",
            PolicyDocument = EmptyPolicyDocument,
        });

        var resp = await _iam.ListRolePoliciesAsync(
            new ListRolePoliciesRequest { RoleName = "iam-test-role" });
        Assert.Contains("inline-logs", resp.PolicyNames ?? []);
    }

    // ── Access keys ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task CreateAccessKey()
    {
        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "iam-test-user" });

        var resp = await _iam.CreateAccessKeyAsync(new CreateAccessKeyRequest
        {
            UserName = "iam-test-user",
        });

        var key = resp.AccessKey;
        Assert.Equal("iam-test-user", key.UserName);
        Assert.StartsWith("AKIA", key.AccessKeyId);
        Assert.NotEmpty(key.SecretAccessKey);
        Assert.Equal(StatusType.Active, key.Status);
    }

    // ── Instance profiles ───────────────────────────────────────────────────────

    [Fact]
    public async Task InstanceProfile()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "ip-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });

        var createResp = await _iam.CreateInstanceProfileAsync(
            new CreateInstanceProfileRequest { InstanceProfileName = "test-ip" });
        Assert.Equal("test-ip", createResp.InstanceProfile.InstanceProfileName);
        Assert.NotEmpty(createResp.InstanceProfile.Arn);

        await _iam.AddRoleToInstanceProfileAsync(new AddRoleToInstanceProfileRequest
        {
            InstanceProfileName = "test-ip",
            RoleName = "ip-role",
        });

        var getResp = await _iam.GetInstanceProfileAsync(
            new GetInstanceProfileRequest { InstanceProfileName = "test-ip" });
        var roles = getResp.InstanceProfile.Roles ?? [];
        Assert.Contains(roles, r => r.RoleName == "ip-role");

        var listResp = await _iam.ListInstanceProfilesAsync(new ListInstanceProfilesRequest());
        var names = (listResp.InstanceProfiles ?? []).Select(p => p.InstanceProfileName).ToList();
        Assert.Contains("test-ip", names);

        await _iam.RemoveRoleFromInstanceProfileAsync(new RemoveRoleFromInstanceProfileRequest
        {
            InstanceProfileName = "test-ip",
            RoleName = "ip-role",
        });

        await _iam.DeleteInstanceProfileAsync(
            new DeleteInstanceProfileRequest { InstanceProfileName = "test-ip" });
    }

    // ── Groups ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Groups()
    {
        await _iam.CreateGroupAsync(new CreateGroupRequest { GroupName = "test-grp" });

        var getResp = await _iam.GetGroupAsync(new GetGroupRequest { GroupName = "test-grp" });
        Assert.Equal("test-grp", getResp.Group.GroupName);

        var listResp = await _iam.ListGroupsAsync(new ListGroupsRequest());
        Assert.Contains(listResp.Groups ?? [], g => g.GroupName == "test-grp");

        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "grp-usr" });
        await _iam.AddUserToGroupAsync(new AddUserToGroupRequest
        {
            GroupName = "test-grp",
            UserName = "grp-usr",
        });

        var members = await _iam.GetGroupAsync(new GetGroupRequest { GroupName = "test-grp" });
        Assert.Contains(members.Users ?? [], u => u.UserName == "grp-usr");

        var userGroups = await _iam.ListGroupsForUserAsync(
            new ListGroupsForUserRequest { UserName = "grp-usr" });
        Assert.Contains(userGroups.Groups ?? [], g => g.GroupName == "test-grp");

        await _iam.RemoveUserFromGroupAsync(new RemoveUserFromGroupRequest
        {
            GroupName = "test-grp",
            UserName = "grp-usr",
        });

        await _iam.DeleteGroupAsync(new DeleteGroupRequest { GroupName = "test-grp" });
    }

    // ── User inline policy ──────────────────────────────────────────────────────

    [Fact]
    public async Task UserInlinePolicy()
    {
        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "inl-pol-usr" });

        var doc = JsonSerializer.Serialize(new
        {
            Version = "2012-10-17",
            Statement = new[]
            {
                new { Effect = "Allow", Action = "s3:*", Resource = "*" },
            },
        });

        await _iam.PutUserPolicyAsync(new PutUserPolicyRequest
        {
            UserName = "inl-pol-usr",
            PolicyName = "s3-acc",
            PolicyDocument = doc,
        });

        var getResp = await _iam.GetUserPolicyAsync(new GetUserPolicyRequest
        {
            UserName = "inl-pol-usr",
            PolicyName = "s3-acc",
        });
        Assert.Equal("s3-acc", getResp.PolicyName);

        var listResp = await _iam.ListUserPoliciesAsync(
            new ListUserPoliciesRequest { UserName = "inl-pol-usr" });
        Assert.Contains("s3-acc", listResp.PolicyNames ?? []);

        await _iam.DeleteUserPolicyAsync(new DeleteUserPolicyRequest
        {
            UserName = "inl-pol-usr",
            PolicyName = "s3-acc",
        });
    }

    // ── Service-linked role ─────────────────────────────────────────────────────

    [Fact]
    public async Task ServiceLinkedRole()
    {
        var resp = await _iam.CreateServiceLinkedRoleAsync(new CreateServiceLinkedRoleRequest
        {
            AWSServiceName = "elasticloadbalancing.amazonaws.com",
        });

        var role = resp.Role;
        Assert.Contains("AWSServiceRoleFor", role.RoleName);
        Assert.StartsWith("/aws-service-role/", role.Path);

        var delResp = await _iam.DeleteServiceLinkedRoleAsync(
            new DeleteServiceLinkedRoleRequest { RoleName = role.RoleName });
        var taskId = delResp.DeletionTaskId;
        Assert.NotEmpty(taskId);

        var status = await _iam.GetServiceLinkedRoleDeletionStatusAsync(
            new GetServiceLinkedRoleDeletionStatusRequest { DeletionTaskId = taskId });
        Assert.Equal("SUCCEEDED", status.Status.Value);

        var ex = await Assert.ThrowsAsync<NoSuchEntityException>(
            () => _iam.GetRoleAsync(new GetRoleRequest { RoleName = role.RoleName }));
        Assert.Equal("NoSuchEntity", ex.ErrorCode);
    }

    // ── OIDC provider ───────────────────────────────────────────────────────────

    [Fact]
    public async Task OidcProvider()
    {
        var resp = await _iam.CreateOpenIDConnectProviderAsync(
            new CreateOpenIDConnectProviderRequest
            {
                Url = "https://oidc.example.com",
                ClientIDList = ["my-client"],
                ThumbprintList = [new string('a', 40)],
            });

        var arn = resp.OpenIDConnectProviderArn;
        Assert.Contains("oidc.example.com", arn);

        var desc = await _iam.GetOpenIDConnectProviderAsync(
            new GetOpenIDConnectProviderRequest { OpenIDConnectProviderArn = arn });
        Assert.Contains("my-client", desc.ClientIDList ?? []);

        await _iam.DeleteOpenIDConnectProviderAsync(
            new DeleteOpenIDConnectProviderRequest { OpenIDConnectProviderArn = arn });
    }

    // ── Policy tags ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task PolicyTags()
    {
        var policyArn = (await _iam.CreatePolicyAsync(new CreatePolicyRequest
        {
            PolicyName = "tagged-pol",
            PolicyDocument = JsonSerializer.Serialize(new
            {
                Version = "2012-10-17",
                Statement = new[]
                {
                    new { Effect = "Allow", Action = "*", Resource = "*" },
                },
            }),
        })).Policy.Arn;

        await _iam.TagPolicyAsync(new TagPolicyRequest
        {
            PolicyArn = policyArn,
            Tags = [new Tag { Key = "env", Value = "test" }],
        });

        var tags = await _iam.ListPolicyTagsAsync(
            new ListPolicyTagsRequest { PolicyArn = policyArn });
        Assert.Contains(tags.Tags ?? [], t => t.Key == "env");

        await _iam.UntagPolicyAsync(new UntagPolicyRequest
        {
            PolicyArn = policyArn,
            TagKeys = ["env"],
        });

        var tags2 = await _iam.ListPolicyTagsAsync(
            new ListPolicyTagsRequest { PolicyArn = policyArn });
        Assert.DoesNotContain(tags2.Tags ?? [], t => t.Key == "env");
    }

    // ── Update role ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateRole()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "test-update-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });

        await _iam.UpdateRoleAsync(new UpdateRoleRequest
        {
            RoleName = "test-update-role",
            Description = "updated desc",
            MaxSessionDuration = 7200,
        });

        var resp = await _iam.GetRoleAsync(new GetRoleRequest { RoleName = "test-update-role" });
        Assert.Equal("updated desc", resp.Role.Description);
        Assert.Equal(7200, resp.Role.MaxSessionDuration);
    }

    // ── Policy version CRUD ─────────────────────────────────────────────────────

    [Fact]
    public async Task PolicyVersionCrud()
    {
        var doc1 = JsonSerializer.Serialize(new
        {
            Version = "2012-10-17",
            Statement = new[]
            {
                new { Effect = "Allow", Action = "s3:*", Resource = "*" },
            },
        });

        var doc2 = JsonSerializer.Serialize(new
        {
            Version = "2012-10-17",
            Statement = new[]
            {
                new { Effect = "Allow", Action = "sqs:*", Resource = "*" },
            },
        });

        var arn = (await _iam.CreatePolicyAsync(new CreatePolicyRequest
        {
            PolicyName = "qa-iam-versions",
            PolicyDocument = doc1,
        })).Policy.Arn;

        await _iam.CreatePolicyVersionAsync(new CreatePolicyVersionRequest
        {
            PolicyArn = arn,
            PolicyDocument = doc2,
            SetAsDefault = true,
        });

        var versions = (await _iam.ListPolicyVersionsAsync(
            new ListPolicyVersionsRequest { PolicyArn = arn })).Versions ?? [];
        Assert.Equal(2, versions.Count);

        var defaultVersion = versions.First(v => v.IsDefaultVersion == true);
        Assert.Equal("v2", defaultVersion.VersionId);

        var v1 = (await _iam.GetPolicyVersionAsync(new GetPolicyVersionRequest
        {
            PolicyArn = arn,
            VersionId = "v1",
        })).PolicyVersion;
        Assert.False(v1.IsDefaultVersion);

        await _iam.DeletePolicyVersionAsync(new DeletePolicyVersionRequest
        {
            PolicyArn = arn,
            VersionId = "v1",
        });

        var versions2 = (await _iam.ListPolicyVersionsAsync(
            new ListPolicyVersionsRequest { PolicyArn = arn })).Versions ?? [];
        Assert.Single(versions2);
    }

    // ── Inline user policy (full CRUD) ──────────────────────────────────────────

    [Fact]
    public async Task InlineUserPolicy()
    {
        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "qa-iam-inline-user" });

        var doc = JsonSerializer.Serialize(new
        {
            Version = "2012-10-17",
            Statement = new[]
            {
                new { Effect = "Allow", Action = "s3:GetObject", Resource = "*" },
            },
        });

        await _iam.PutUserPolicyAsync(new PutUserPolicyRequest
        {
            UserName = "qa-iam-inline-user",
            PolicyName = "qa-inline",
            PolicyDocument = doc,
        });

        var policies = (await _iam.ListUserPoliciesAsync(
            new ListUserPoliciesRequest { UserName = "qa-iam-inline-user" })).PolicyNames ?? [];
        Assert.Contains("qa-inline", policies);

        var got = await _iam.GetUserPolicyAsync(new GetUserPolicyRequest
        {
            UserName = "qa-iam-inline-user",
            PolicyName = "qa-inline",
        });

        var decoded = Uri.UnescapeDataString(got.PolicyDocument);
        Assert.Contains("s3:GetObject", decoded);

        await _iam.DeleteUserPolicyAsync(new DeleteUserPolicyRequest
        {
            UserName = "qa-iam-inline-user",
            PolicyName = "qa-inline",
        });

        var policies2 = (await _iam.ListUserPoliciesAsync(
            new ListUserPoliciesRequest { UserName = "qa-iam-inline-user" })).PolicyNames ?? [];
        Assert.DoesNotContain("qa-inline", policies2);
    }

    // ── Instance profile CRUD ───────────────────────────────────────────────────

    [Fact]
    public async Task InstanceProfileCrud()
    {
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "qa-iam-ip-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });

        await _iam.CreateInstanceProfileAsync(
            new CreateInstanceProfileRequest { InstanceProfileName = "qa-iam-ip" });

        await _iam.AddRoleToInstanceProfileAsync(new AddRoleToInstanceProfileRequest
        {
            InstanceProfileName = "qa-iam-ip",
            RoleName = "qa-iam-ip-role",
        });

        var ip = (await _iam.GetInstanceProfileAsync(
            new GetInstanceProfileRequest { InstanceProfileName = "qa-iam-ip" })).InstanceProfile;
        Assert.Equal("qa-iam-ip", ip.InstanceProfileName);
        Assert.Contains(ip.Roles ?? [], r => r.RoleName == "qa-iam-ip-role");

        var profiles = (await _iam.ListInstanceProfilesAsync(
            new ListInstanceProfilesRequest())).InstanceProfiles ?? [];
        Assert.Contains(profiles, p => p.InstanceProfileName == "qa-iam-ip");

        await _iam.RemoveRoleFromInstanceProfileAsync(new RemoveRoleFromInstanceProfileRequest
        {
            InstanceProfileName = "qa-iam-ip",
            RoleName = "qa-iam-ip-role",
        });

        await _iam.DeleteInstanceProfileAsync(
            new DeleteInstanceProfileRequest { InstanceProfileName = "qa-iam-ip" });
    }

    // ── Attach / Detach user policy ─────────────────────────────────────────────

    [Fact]
    public async Task AttachDetachUserPolicy()
    {
        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "qa-iam-attach-user" });

        var policyArn = (await _iam.CreatePolicyAsync(new CreatePolicyRequest
        {
            PolicyName = "qa-iam-attach-pol",
            PolicyDocument = EmptyPolicyDocument,
        })).Policy.Arn;

        await _iam.AttachUserPolicyAsync(new AttachUserPolicyRequest
        {
            UserName = "qa-iam-attach-user",
            PolicyArn = policyArn,
        });

        var attached = (await _iam.ListAttachedUserPoliciesAsync(
            new ListAttachedUserPoliciesRequest { UserName = "qa-iam-attach-user" }))
            .AttachedPolicies ?? [];
        Assert.Contains(attached, p => p.PolicyArn == policyArn);

        await _iam.DetachUserPolicyAsync(new DetachUserPolicyRequest
        {
            UserName = "qa-iam-attach-user",
            PolicyArn = policyArn,
        });

        var attached2 = (await _iam.ListAttachedUserPoliciesAsync(
            new ListAttachedUserPoliciesRequest { UserName = "qa-iam-attach-user" }))
            .AttachedPolicies ?? [];
        Assert.DoesNotContain(attached2, p => p.PolicyArn == policyArn);
    }

    // ── List entities for policy ────────────────────────────────────────────────

    [Fact]
    public async Task ListEntitiesForPolicy()
    {
        var policyArn = (await _iam.CreatePolicyAsync(new CreatePolicyRequest
        {
            PolicyName = "qa-entities-pol",
            PolicyDocument = EmptyPolicyDocument,
        })).Policy.Arn;

        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "qa-entities-user" });
        await _iam.CreateRoleAsync(new CreateRoleRequest
        {
            RoleName = "qa-entities-role",
            AssumeRolePolicyDocument = EmptyPolicyDocument,
        });

        await _iam.AttachUserPolicyAsync(new AttachUserPolicyRequest
        {
            UserName = "qa-entities-user",
            PolicyArn = policyArn,
        });
        await _iam.AttachRolePolicyAsync(new AttachRolePolicyRequest
        {
            RoleName = "qa-entities-role",
            PolicyArn = policyArn,
        });

        var resp = await _iam.ListEntitiesForPolicyAsync(
            new ListEntitiesForPolicyRequest { PolicyArn = policyArn });
        var userNames = (resp.PolicyUsers ?? []).Select(u => u.UserName).ToList();
        var roleNames = (resp.PolicyRoles ?? []).Select(r => r.RoleName).ToList();
        Assert.Contains("qa-entities-user", userNames);
        Assert.Contains("qa-entities-role", roleNames);

        // Detach user and verify it's removed
        await _iam.DetachUserPolicyAsync(new DetachUserPolicyRequest
        {
            UserName = "qa-entities-user",
            PolicyArn = policyArn,
        });

        var resp2 = await _iam.ListEntitiesForPolicyAsync(
            new ListEntitiesForPolicyRequest { PolicyArn = policyArn });
        var userNames2 = (resp2.PolicyUsers ?? []).Select(u => u.UserName).ToList();
        Assert.DoesNotContain("qa-entities-user", userNames2);
        Assert.Contains("qa-entities-role",
            (resp2.PolicyRoles ?? []).Select(r => r.RoleName).ToList());

        // Test EntityFilter
        var resp3 = await _iam.ListEntitiesForPolicyAsync(
            new ListEntitiesForPolicyRequest
            {
                PolicyArn = policyArn,
                EntityFilter = EntityType.Role,
            });
        Assert.True((resp3.PolicyRoles ?? []).Count >= 1);
        Assert.Empty(resp3.PolicyUsers ?? []);
    }
}
