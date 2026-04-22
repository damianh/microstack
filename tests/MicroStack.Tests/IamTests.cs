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
public sealed class IamTests(MicroStackFixture fixture) : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly AmazonIdentityManagementServiceClient _iam = CreateIamClient(fixture);

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

    public async ValueTask InitializeAsync()
    {
        await fixture.HttpClient.PostAsync("/_microstack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _iam.Dispose();
        return ValueTask.CompletedTask;
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
        roles.ShouldContain(r => r.RoleName == "test-role");

        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "test-user" });

        var users = (await _iam.ListUsersAsync(new ListUsersRequest())).Users ?? [];
        users.ShouldContain(u => u.UserName == "test-user");
    }

    [Fact]
    public async Task CreateUser()
    {
        var resp = await _iam.CreateUserAsync(new CreateUserRequest { UserName = "iam-test-user" });
        var user = resp.User;
        user.UserName.ShouldBe("iam-test-user");
        user.Arn.ShouldNotBeEmpty();
        user.UserId.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task GetUser()
    {
        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "iam-test-user" });
        var resp = await _iam.GetUserAsync(new GetUserRequest { UserName = "iam-test-user" });
        resp.User.UserName.ShouldBe("iam-test-user");
    }

    [Fact]
    public async Task GetUserNotFound()
    {
        var ex = await Should.ThrowAsync<NoSuchEntityException>(
            () => _iam.GetUserAsync(new GetUserRequest { UserName = "ghost-user-xyz" }));
        ex.ErrorCode.ShouldBe("NoSuchEntity");
    }

    [Fact]
    public async Task ListUsers()
    {
        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "iam-test-user" });
        var resp = await _iam.ListUsersAsync(new ListUsersRequest());
        var names = (resp.Users ?? []).Select(u => u.UserName).ToList();
        names.ShouldContain("iam-test-user");
    }

    [Fact]
    public async Task DeleteUser()
    {
        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "iam-del-user" });
        await _iam.DeleteUserAsync(new DeleteUserRequest { UserName = "iam-del-user" });

        var ex = await Should.ThrowAsync<NoSuchEntityException>(
            () => _iam.GetUserAsync(new GetUserRequest { UserName = "iam-del-user" }));
        ex.ErrorCode.ShouldBe("NoSuchEntity");
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
        role.RoleName.ShouldBe("iam-test-role");
        role.Arn.ShouldNotBeEmpty();
        role.RoleId.ShouldNotBeEmpty();
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
        resp.Role.RoleName.ShouldBe("iam-test-role");
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
        names.ShouldContain("iam-test-role");
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

        var ex = await Should.ThrowAsync<NoSuchEntityException>(
            () => _iam.GetRoleAsync(new GetRoleRequest { RoleName = "iam-del-role" }));
        ex.ErrorCode.ShouldBe("NoSuchEntity");
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
        pol.PolicyName.ShouldBe("iam-test-policy");
        pol.Arn.ShouldNotBeEmpty();
        pol.DefaultVersionId.ShouldBe("v1");
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
        resp.Policy.PolicyName.ShouldBe("iam-test-policy");
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
        arns.ShouldContain(policyArn);
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
        arns.ShouldNotContain(policyArn);
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

        resp.RoleName.ShouldBe("iam-test-role");
        resp.PolicyName.ShouldBe("inline-logs");

        var decoded = Uri.UnescapeDataString(resp.PolicyDocument);
        var doc = JsonDocument.Parse(decoded);
        var action = doc.RootElement.GetProperty("Statement")[0].GetProperty("Action").GetString();
        action.ShouldBe("logs:*");
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
        resp.PolicyNames.ShouldContain("inline-logs");
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
        key.UserName.ShouldBe("iam-test-user");
        key.AccessKeyId.ShouldStartWith("AKIA");
        key.SecretAccessKey.ShouldNotBeEmpty();
        key.Status.ShouldBe(StatusType.Active);
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
        createResp.InstanceProfile.InstanceProfileName.ShouldBe("test-ip");
        createResp.InstanceProfile.Arn.ShouldNotBeEmpty();

        await _iam.AddRoleToInstanceProfileAsync(new AddRoleToInstanceProfileRequest
        {
            InstanceProfileName = "test-ip",
            RoleName = "ip-role",
        });

        var getResp = await _iam.GetInstanceProfileAsync(
            new GetInstanceProfileRequest { InstanceProfileName = "test-ip" });
        var roles = getResp.InstanceProfile.Roles ?? [];
        roles.ShouldContain(r => r.RoleName == "ip-role");

        var listResp = await _iam.ListInstanceProfilesAsync(new ListInstanceProfilesRequest());
        var names = (listResp.InstanceProfiles ?? []).Select(p => p.InstanceProfileName).ToList();
        names.ShouldContain("test-ip");

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
        getResp.Group.GroupName.ShouldBe("test-grp");

        var listResp = await _iam.ListGroupsAsync(new ListGroupsRequest());
        listResp.Groups.ShouldContain(g => g.GroupName == "test-grp");

        await _iam.CreateUserAsync(new CreateUserRequest { UserName = "grp-usr" });
        await _iam.AddUserToGroupAsync(new AddUserToGroupRequest
        {
            GroupName = "test-grp",
            UserName = "grp-usr",
        });

        var members = await _iam.GetGroupAsync(new GetGroupRequest { GroupName = "test-grp" });
        members.Users.ShouldContain(u => u.UserName == "grp-usr");

        var userGroups = await _iam.ListGroupsForUserAsync(
            new ListGroupsForUserRequest { UserName = "grp-usr" });
        userGroups.Groups.ShouldContain(g => g.GroupName == "test-grp");

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
        getResp.PolicyName.ShouldBe("s3-acc");

        var listResp = await _iam.ListUserPoliciesAsync(
            new ListUserPoliciesRequest { UserName = "inl-pol-usr" });
        listResp.PolicyNames.ShouldContain("s3-acc");

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
        role.RoleName.ShouldContain("AWSServiceRoleFor");
        role.Path.ShouldStartWith("/aws-service-role/");

        var delResp = await _iam.DeleteServiceLinkedRoleAsync(
            new DeleteServiceLinkedRoleRequest { RoleName = role.RoleName });
        var taskId = delResp.DeletionTaskId;
        taskId.ShouldNotBeEmpty();

        var status = await _iam.GetServiceLinkedRoleDeletionStatusAsync(
            new GetServiceLinkedRoleDeletionStatusRequest { DeletionTaskId = taskId });
        status.Status.Value.ShouldBe("SUCCEEDED");

        var ex = await Should.ThrowAsync<NoSuchEntityException>(
            () => _iam.GetRoleAsync(new GetRoleRequest { RoleName = role.RoleName }));
        ex.ErrorCode.ShouldBe("NoSuchEntity");
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
        arn.ShouldContain("oidc.example.com");

        var desc = await _iam.GetOpenIDConnectProviderAsync(
            new GetOpenIDConnectProviderRequest { OpenIDConnectProviderArn = arn });
        desc.ClientIDList.ShouldContain("my-client");

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
        tags.Tags.ShouldContain(t => t.Key == "env");

        await _iam.UntagPolicyAsync(new UntagPolicyRequest
        {
            PolicyArn = policyArn,
            TagKeys = ["env"],
        });

        var tags2 = await _iam.ListPolicyTagsAsync(
            new ListPolicyTagsRequest { PolicyArn = policyArn });
        (tags2.Tags ?? []).ShouldNotContain(t => t.Key == "env");
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
        resp.Role.Description.ShouldBe("updated desc");
        resp.Role.MaxSessionDuration.ShouldBe(7200);
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
        versions.Count.ShouldBe(2);

        var defaultVersion = versions.First(v => v.IsDefaultVersion == true);
        defaultVersion.VersionId.ShouldBe("v2");

        var v1 = (await _iam.GetPolicyVersionAsync(new GetPolicyVersionRequest
        {
            PolicyArn = arn,
            VersionId = "v1",
        })).PolicyVersion;
        v1.IsDefaultVersion.ShouldBe(false);

        await _iam.DeletePolicyVersionAsync(new DeletePolicyVersionRequest
        {
            PolicyArn = arn,
            VersionId = "v1",
        });

        var versions2 = (await _iam.ListPolicyVersionsAsync(
            new ListPolicyVersionsRequest { PolicyArn = arn })).Versions ?? [];
        versions2.ShouldHaveSingleItem();
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
        policies.ShouldContain("qa-inline");

        var got = await _iam.GetUserPolicyAsync(new GetUserPolicyRequest
        {
            UserName = "qa-iam-inline-user",
            PolicyName = "qa-inline",
        });

        var decoded = Uri.UnescapeDataString(got.PolicyDocument);
        decoded.ShouldContain("s3:GetObject");

        await _iam.DeleteUserPolicyAsync(new DeleteUserPolicyRequest
        {
            UserName = "qa-iam-inline-user",
            PolicyName = "qa-inline",
        });

        var policies2 = (await _iam.ListUserPoliciesAsync(
            new ListUserPoliciesRequest { UserName = "qa-iam-inline-user" })).PolicyNames ?? [];
        policies2.ShouldNotContain("qa-inline");
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
        ip.InstanceProfileName.ShouldBe("qa-iam-ip");
        ip.Roles.ShouldContain(r => r.RoleName == "qa-iam-ip-role");

        var profiles = (await _iam.ListInstanceProfilesAsync(
            new ListInstanceProfilesRequest())).InstanceProfiles ?? [];
        profiles.ShouldContain(p => p.InstanceProfileName == "qa-iam-ip");

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
        attached.ShouldContain(p => p.PolicyArn == policyArn);

        await _iam.DetachUserPolicyAsync(new DetachUserPolicyRequest
        {
            UserName = "qa-iam-attach-user",
            PolicyArn = policyArn,
        });

        var attached2 = (await _iam.ListAttachedUserPoliciesAsync(
            new ListAttachedUserPoliciesRequest { UserName = "qa-iam-attach-user" }))
            .AttachedPolicies ?? [];
        attached2.ShouldNotContain(p => p.PolicyArn == policyArn);
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
        userNames.ShouldContain("qa-entities-user");
        roleNames.ShouldContain("qa-entities-role");

        // Detach user and verify it's removed
        await _iam.DetachUserPolicyAsync(new DetachUserPolicyRequest
        {
            UserName = "qa-entities-user",
            PolicyArn = policyArn,
        });

        var resp2 = await _iam.ListEntitiesForPolicyAsync(
            new ListEntitiesForPolicyRequest { PolicyArn = policyArn });
        var userNames2 = (resp2.PolicyUsers ?? []).Select(u => u.UserName).ToList();
        userNames2.ShouldNotContain("qa-entities-user");
        (resp2.PolicyRoles ?? []).Select(r => r.RoleName).ToList().ShouldContain("qa-entities-role");

        // Test EntityFilter
        var resp3 = await _iam.ListEntitiesForPolicyAsync(
            new ListEntitiesForPolicyRequest
            {
                PolicyArn = policyArn,
                EntityFilter = EntityType.Role,
            });
        (((resp3.PolicyRoles ?? []).Count >= 1)).ShouldBe(true);
        (resp3.PolicyUsers ?? []).ShouldBeEmpty();
    }
}
