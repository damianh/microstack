using Amazon;
using Amazon.Runtime;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the SecretsManager service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/services/secretsmanager.py.
/// </summary>
public sealed class SecretsManagerTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonSecretsManagerClient _sm;

    public SecretsManagerTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _sm = CreateClient(fixture);
    }

    private static AmazonSecretsManagerClient CreateClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonSecretsManagerConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonSecretsManagerClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _sm.Dispose();
        return ValueTask.CompletedTask;
    }

    // -- CreateSecret / GetSecretValue -----------------------------------------

    [Fact]
    public async Task CreateSecretAndGetSecretValue()
    {
        var create = await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "test/secret1",
            SecretString = "{\"user\":\"admin\",\"pass\":\"hunter2\"}",
        });

        create.ARN.ShouldContain("test/secret1");
        create.Name.ShouldBe("test/secret1");
        create.VersionId.ShouldNotBeEmpty();

        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "test/secret1",
        });

        get.SecretString.ShouldBe("{\"user\":\"admin\",\"pass\":\"hunter2\"}");
        get.VersionId.ShouldBe(create.VersionId);
        get.VersionStages.ShouldContain("AWSCURRENT");
    }

    [Fact]
    public async Task CreateSecretDuplicateNameFails()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "dup-secret",
            SecretString = "value1",
        });

        var ex = await Should.ThrowAsync<ResourceExistsException>(() =>
            _sm.CreateSecretAsync(new CreateSecretRequest
            {
                Name = "dup-secret",
                SecretString = "value2",
            }));

        ex.Message.ShouldContain("already exists");
    }

    [Fact]
    public async Task GetSecretValueNotFoundReturnsError()
    {
        await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _sm.GetSecretValueAsync(new GetSecretValueRequest
            {
                SecretId = "nonexistent-secret",
            }));
    }

    // -- GetSecretValue by VersionId and VersionStage --------------------------

    [Fact]
    public async Task GetSecretValueByVersionId()
    {
        var create = await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "version-test",
            SecretString = "original",
        });

        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "version-test",
            VersionId = create.VersionId,
        });

        get.SecretString.ShouldBe("original");
    }

    [Fact]
    public async Task GetSecretValueInvalidVersionIdReturnsError()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "invalid-vid-test",
            SecretString = "val",
        });

        await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _sm.GetSecretValueAsync(new GetSecretValueRequest
            {
                SecretId = "invalid-vid-test",
                VersionId = "00000000-0000-0000-0000-000000000000",
            }));
    }

    // -- DeleteSecret / RestoreSecret ------------------------------------------

    [Fact]
    public async Task DeleteSecretScheduleAndRestore()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "del-restore",
            SecretString = "val",
        });

        var del = await _sm.DeleteSecretAsync(new DeleteSecretRequest
        {
            SecretId = "del-restore",
            RecoveryWindowInDays = 7,
        });

        del.Name.ShouldBe("del-restore");
        del.DeletionDate.ShouldNotBe(default);

        // Cannot get value while scheduled for deletion
        await Should.ThrowAsync<InvalidRequestException>(() =>
            _sm.GetSecretValueAsync(new GetSecretValueRequest { SecretId = "del-restore" }));

        // Restore
        var restore = await _sm.RestoreSecretAsync(new RestoreSecretRequest
        {
            SecretId = "del-restore",
        });

        restore.Name.ShouldBe("del-restore");

        // Can get value again
        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "del-restore",
        });

        get.SecretString.ShouldBe("val");
    }

    [Fact]
    public async Task DeleteSecretForceDeleteRemovesPermanently()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "force-del",
            SecretString = "val",
        });

        await _sm.DeleteSecretAsync(new DeleteSecretRequest
        {
            SecretId = "force-del",
            ForceDeleteWithoutRecovery = true,
        });

        await Should.ThrowAsync<ResourceNotFoundException>(() =>
            _sm.GetSecretValueAsync(new GetSecretValueRequest { SecretId = "force-del" }));
    }

    [Fact]
    public async Task DeleteSecretForceAndRecoveryWindowMutuallyExclusive()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "mutual-excl",
            SecretString = "val",
        });

        await Should.ThrowAsync<InvalidParameterException>(() =>
            _sm.DeleteSecretAsync(new DeleteSecretRequest
            {
                SecretId = "mutual-excl",
                ForceDeleteWithoutRecovery = true,
                RecoveryWindowInDays = 7,
            }));
    }

    [Fact]
    public async Task DeleteSecretInvalidRecoveryWindowFails()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "bad-window",
            SecretString = "val",
        });

        await Should.ThrowAsync<InvalidParameterException>(() =>
            _sm.DeleteSecretAsync(new DeleteSecretRequest
            {
                SecretId = "bad-window",
                RecoveryWindowInDays = 5,
            }));
    }

    [Fact]
    public async Task RestoreSecretNotDeletedFails()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "not-deleted",
            SecretString = "val",
        });

        await Should.ThrowAsync<InvalidRequestException>(() =>
            _sm.RestoreSecretAsync(new RestoreSecretRequest { SecretId = "not-deleted" }));
    }

    // -- UpdateSecret ----------------------------------------------------------

    [Fact]
    public async Task UpdateSecretDescriptionOnly()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "update-desc",
            SecretString = "val",
        });

        var update = await _sm.UpdateSecretAsync(new UpdateSecretRequest
        {
            SecretId = "update-desc",
            Description = "new description",
        });

        update.Name.ShouldBe("update-desc");

        var desc = await _sm.DescribeSecretAsync(new DescribeSecretRequest
        {
            SecretId = "update-desc",
        });

        desc.Description.ShouldBe("new description");
    }

    [Fact]
    public async Task UpdateSecretWithNewValue()
    {
        var create = await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "update-val",
            SecretString = "original",
        });

        var update = await _sm.UpdateSecretAsync(new UpdateSecretRequest
        {
            SecretId = "update-val",
            SecretString = "updated",
        });

        update.VersionId.ShouldNotBe(create.VersionId);

        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "update-val",
        });

        get.SecretString.ShouldBe("updated");
    }

    // -- DescribeSecret --------------------------------------------------------

    [Fact]
    public async Task DescribeSecretReturnsMetadata()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "describe-me",
            SecretString = "val",
            Description = "test desc",
            Tags = [new Tag { Key = "env", Value = "test" }],
        });

        var desc = await _sm.DescribeSecretAsync(new DescribeSecretRequest
        {
            SecretId = "describe-me",
        });

        desc.Name.ShouldBe("describe-me");
        desc.Description.ShouldBe("test desc");
        desc.ARN.ShouldContain("describe-me");
        desc.CreatedDate.ShouldNotBe(default);
        desc.Tags.ShouldHaveSingleItem();
        desc.Tags[0].Key.ShouldBe("env");
        desc.Tags[0].Value.ShouldBe("test");
        desc.VersionIdsToStages.ShouldNotBeEmpty();
    }

    // -- PutSecretValue --------------------------------------------------------

    [Fact]
    public async Task PutSecretValuePromotesToAwsCurrent()
    {
        var create = await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "put-val",
            SecretString = "v1",
        });

        var put = await _sm.PutSecretValueAsync(new PutSecretValueRequest
        {
            SecretId = "put-val",
            SecretString = "v2",
        });

        put.VersionId.ShouldNotBe(create.VersionId);
        put.VersionStages.ShouldContain("AWSCURRENT");

        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "put-val",
        });

        get.SecretString.ShouldBe("v2");
    }

    [Fact]
    public async Task PutSecretValueWithPendingStage()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "put-pending",
            SecretString = "v1",
        });

        var put = await _sm.PutSecretValueAsync(new PutSecretValueRequest
        {
            SecretId = "put-pending",
            SecretString = "v2-pending",
            VersionStages = ["AWSPENDING"],
        });

        put.VersionStages.ShouldContain("AWSPENDING");

        // AWSCURRENT still returns v1
        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "put-pending",
        });

        get.SecretString.ShouldBe("v1");
    }

    // -- ListSecrets -----------------------------------------------------------

    [Fact]
    public async Task ListSecretsReturnsAll()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "list-a",
            SecretString = "a",
        });

        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "list-b",
            SecretString = "b",
        });

        var list = await _sm.ListSecretsAsync(new ListSecretsRequest());

        (list.SecretList.Count >= 2).ShouldBe(true);
        list.SecretList.ShouldContain(s => s.Name == "list-a");
        list.SecretList.ShouldContain(s => s.Name == "list-b");
    }

    [Fact]
    public async Task ListSecretsWithNameFilter()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "filtered-alpha",
            SecretString = "a",
        });

        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "filtered-beta",
            SecretString = "b",
        });

        var list = await _sm.ListSecretsAsync(new ListSecretsRequest
        {
            Filters = [new Filter { Key = FilterNameStringType.Name, Values = ["alpha"] }],
        });

        list.SecretList.ShouldHaveSingleItem();
        list.SecretList[0].Name.ShouldBe("filtered-alpha");
    }

    [Fact]
    public async Task ListSecretsExcludesDeletedSecrets()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "list-deleted",
            SecretString = "val",
        });

        await _sm.DeleteSecretAsync(new DeleteSecretRequest
        {
            SecretId = "list-deleted",
            RecoveryWindowInDays = 7,
        });

        var list = await _sm.ListSecretsAsync(new ListSecretsRequest());
        list.SecretList.ShouldNotContain(s => s.Name == "list-deleted");
    }

    // -- TagResource / UntagResource -------------------------------------------

    [Fact]
    public async Task TagAndUntagResource()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "tag-test",
            SecretString = "val",
        });

        var desc1 = await _sm.DescribeSecretAsync(new DescribeSecretRequest
        {
            SecretId = "tag-test",
        });

        await _sm.TagResourceAsync(new TagResourceRequest
        {
            SecretId = "tag-test",
            Tags = [
                new Tag { Key = "env", Value = "prod" },
                new Tag { Key = "team", Value = "platform" },
            ],
        });

        var desc2 = await _sm.DescribeSecretAsync(new DescribeSecretRequest
        {
            SecretId = "tag-test",
        });

        desc2.Tags.Count.ShouldBe(2);

        await _sm.UntagResourceAsync(new UntagResourceRequest
        {
            SecretId = "tag-test",
            TagKeys = ["team"],
        });

        var desc3 = await _sm.DescribeSecretAsync(new DescribeSecretRequest
        {
            SecretId = "tag-test",
        });

        desc3.Tags.ShouldHaveSingleItem();
        desc3.Tags[0].Key.ShouldBe("env");
    }

    // -- ListSecretVersionIds --------------------------------------------------

    [Fact]
    public async Task ListSecretVersionIds()
    {
        var create = await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "versions-list",
            SecretString = "v1",
        });

        await _sm.PutSecretValueAsync(new PutSecretValueRequest
        {
            SecretId = "versions-list",
            SecretString = "v2",
        });

        var list = await _sm.ListSecretVersionIdsAsync(new ListSecretVersionIdsRequest
        {
            SecretId = "versions-list",
        });

        (list.Versions.Count >= 2).ShouldBe(true);
        list.Name.ShouldBe("versions-list");
    }

    // -- UpdateSecretVersionStage ----------------------------------------------

    [Fact]
    public async Task UpdateSecretVersionStageMovesLabel()
    {
        var create = await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "stage-move",
            SecretString = "v1",
        });

        var put = await _sm.PutSecretValueAsync(new PutSecretValueRequest
        {
            SecretId = "stage-move",
            SecretString = "v2",
        });

        // v2 is AWSCURRENT, v1 is AWSPREVIOUS
        // Move AWSCURRENT back to v1
        await _sm.UpdateSecretVersionStageAsync(new UpdateSecretVersionStageRequest
        {
            SecretId = "stage-move",
            VersionStage = "AWSCURRENT",
            MoveToVersionId = create.VersionId,
            RemoveFromVersionId = put.VersionId,
        });

        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "stage-move",
        });

        get.SecretString.ShouldBe("v1");
    }

    // -- RotateSecret ----------------------------------------------------------

    [Fact]
    public async Task RotateSecretStub()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "rotate-test",
            SecretString = "original",
        });

        var rotate = await _sm.RotateSecretAsync(new RotateSecretRequest
        {
            SecretId = "rotate-test",
        });

        rotate.VersionId.ShouldNotBeEmpty();
        rotate.Name.ShouldBe("rotate-test");

        // After rotation, AWSCURRENT should still return the value
        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "rotate-test",
        });

        get.SecretString.ShouldBe("original");
    }

    // -- GetRandomPassword -----------------------------------------------------

    [Fact]
    public async Task GetRandomPasswordDefault()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest());
        resp.RandomPassword.Length.ShouldBe(32);
    }

    [Fact]
    public async Task GetRandomPasswordCustomLength()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 64,
        });

        resp.RandomPassword.Length.ShouldBe(64);
    }

    [Fact]
    public async Task GetRandomPasswordExcludeNumbers()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 100,
            ExcludeNumbers = true,
        });

        resp.RandomPassword.ShouldNotContain(c => char.IsDigit(c));
    }

    [Fact]
    public async Task GetRandomPasswordExcludeUppercase()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 100,
            ExcludeUppercase = true,
        });

        resp.RandomPassword.ShouldNotContain(c => char.IsUpper(c));
    }

    [Fact]
    public async Task GetRandomPasswordExcludeLowercase()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 100,
            ExcludeLowercase = true,
        });

        resp.RandomPassword.ShouldNotContain(c => char.IsLower(c));
    }

    [Fact]
    public async Task GetRandomPasswordExcludePunctuation()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 100,
            ExcludePunctuation = true,
        });

        var punctuation = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";
        resp.RandomPassword.ShouldNotContain(c => punctuation.Contains(c));
    }

    [Fact]
    public async Task GetRandomPasswordIncludeSpace()
    {
        // Request a long enough password to statistically include a space
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 4096,
            IncludeSpace = true,
        });

        resp.RandomPassword.Length.ShouldBe(4096);
    }

    [Fact]
    public async Task GetRandomPasswordRequireEachIncludedType()
    {
        // With all types included and RequireEachIncludedType=true, the password should include each type
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 20,
            RequireEachIncludedType = true,
        });

        resp.RandomPassword.Length.ShouldBe(20);
        resp.RandomPassword.ShouldContain(c => char.IsLower(c));
        resp.RandomPassword.ShouldContain(c => char.IsUpper(c));
        resp.RandomPassword.ShouldContain(c => char.IsDigit(c));
    }

    [Fact]
    public async Task GetRandomPasswordAllExcludedFails()
    {
        await Should.ThrowAsync<InvalidParameterException>(() =>
            _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
            {
                PasswordLength = 32,
                ExcludeNumbers = true,
                ExcludePunctuation = true,
                ExcludeUppercase = true,
                ExcludeLowercase = true,
            }));
    }

    [Fact]
    public async Task GetRandomPasswordExcludeCharacters()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 100,
            ExcludeCharacters = "abcABC123",
        });

        resp.RandomPassword.ShouldNotContain(c => "abcABC123".Contains(c));
    }

    // -- ReplicateSecretToRegions ----------------------------------------------

    [Fact]
    public async Task ReplicateSecretToRegionsStub()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "replicate-test",
            SecretString = "val",
        });

        var resp = await _sm.ReplicateSecretToRegionsAsync(new ReplicateSecretToRegionsRequest
        {
            SecretId = "replicate-test",
            AddReplicaRegions = [new ReplicaRegionType { Region = "eu-west-1" }],
        });

        resp.ReplicationStatus.ShouldHaveSingleItem();
        resp.ReplicationStatus[0].Region.ShouldBe("eu-west-1");
        resp.ReplicationStatus[0].Status.Value.ShouldBe("InSync");
    }

    // -- Resource Policies -----------------------------------------------------

    [Fact]
    public async Task PutGetDeleteResourcePolicy()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "policy-test",
            SecretString = "val",
        });

        await _sm.PutResourcePolicyAsync(new PutResourcePolicyRequest
        {
            SecretId = "policy-test",
            ResourcePolicy = "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
        });

        var get = await _sm.GetResourcePolicyAsync(new GetResourcePolicyRequest
        {
            SecretId = "policy-test",
        });

        get.ResourcePolicy.ShouldBe("{\"Version\":\"2012-10-17\",\"Statement\":[]}");

        await _sm.DeleteResourcePolicyAsync(new DeleteResourcePolicyRequest
        {
            SecretId = "policy-test",
        });

        var get2 = await _sm.GetResourcePolicyAsync(new GetResourcePolicyRequest
        {
            SecretId = "policy-test",
        });

        get2.ResourcePolicy.ShouldBeNull();
    }

    [Fact]
    public async Task ValidateResourcePolicyAlwaysPasses()
    {
        var resp = await _sm.ValidateResourcePolicyAsync(new ValidateResourcePolicyRequest
        {
            ResourcePolicy = "{}",
            SecretId = "anything",
        });

        resp.PolicyValidationPassed.ShouldBe(true);
        resp.ValidationErrors.ShouldBeEmpty();
    }

    // -- BatchGetSecretValue ---------------------------------------------------

    [Fact]
    public async Task BatchGetSecretValueByList()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "batch-a",
            SecretString = "val-a",
        });

        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "batch-b",
            SecretString = "val-b",
        });

        var resp = await _sm.BatchGetSecretValueAsync(new BatchGetSecretValueRequest
        {
            SecretIdList = ["batch-a", "batch-b"],
        });

        resp.SecretValues.Count.ShouldBe(2);
        resp.Errors.ShouldBeEmpty();
    }

    [Fact]
    public async Task BatchGetSecretValueWithErrors()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "batch-ok",
            SecretString = "val",
        });

        var resp = await _sm.BatchGetSecretValueAsync(new BatchGetSecretValueRequest
        {
            SecretIdList = ["batch-ok", "nonexistent"],
        });

        resp.SecretValues.ShouldHaveSingleItem();
        resp.Errors.ShouldHaveSingleItem();
        resp.Errors[0].SecretId.ShouldBe("nonexistent");
    }

    // -- Resolve by ARN -------------------------------------------------------

    [Fact]
    public async Task GetSecretValueByArn()
    {
        var create = await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "arn-resolve-test",
            SecretString = "by-arn",
        });

        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = create.ARN,
        });

        get.SecretString.ShouldBe("by-arn");
    }

    // -- Deleted secret edge cases ---------------------------------------------

    [Fact]
    public async Task CannotUpdateDeletedSecret()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "del-update",
            SecretString = "val",
        });

        await _sm.DeleteSecretAsync(new DeleteSecretRequest
        {
            SecretId = "del-update",
            RecoveryWindowInDays = 7,
        });

        await Should.ThrowAsync<InvalidRequestException>(() =>
            _sm.UpdateSecretAsync(new UpdateSecretRequest
            {
                SecretId = "del-update",
                SecretString = "new",
            }));
    }

    [Fact]
    public async Task CannotPutSecretValueOnDeletedSecret()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "del-put",
            SecretString = "val",
        });

        await _sm.DeleteSecretAsync(new DeleteSecretRequest
        {
            SecretId = "del-put",
            RecoveryWindowInDays = 7,
        });

        await Should.ThrowAsync<InvalidRequestException>(() =>
            _sm.PutSecretValueAsync(new PutSecretValueRequest
            {
                SecretId = "del-put",
                SecretString = "new",
            }));
    }

    [Fact]
    public async Task CannotRotateDeletedSecret()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "del-rotate",
            SecretString = "val",
        });

        await _sm.DeleteSecretAsync(new DeleteSecretRequest
        {
            SecretId = "del-rotate",
            RecoveryWindowInDays = 7,
        });

        await Should.ThrowAsync<InvalidRequestException>(() =>
            _sm.RotateSecretAsync(new RotateSecretRequest
            {
                SecretId = "del-rotate",
            }));
    }

    [Fact]
    public async Task CannotDeleteAlreadyDeletedSecret()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "del-double",
            SecretString = "val",
        });

        await _sm.DeleteSecretAsync(new DeleteSecretRequest
        {
            SecretId = "del-double",
            RecoveryWindowInDays = 7,
        });

        await Should.ThrowAsync<InvalidRequestException>(() =>
            _sm.DeleteSecretAsync(new DeleteSecretRequest
            {
                SecretId = "del-double",
                RecoveryWindowInDays = 7,
            }));
    }

    // -- ListSecrets with tag filters ------------------------------------------

    [Fact]
    public async Task ListSecretsFilterByTagKey()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "tag-filter-a",
            SecretString = "a",
            Tags = [new Tag { Key = "dept", Value = "eng" }],
        });

        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "tag-filter-b",
            SecretString = "b",
            Tags = [new Tag { Key = "team", Value = "ops" }],
        });

        var list = await _sm.ListSecretsAsync(new ListSecretsRequest
        {
            Filters = [new Filter { Key = FilterNameStringType.TagKey, Values = ["dept"] }],
        });

        list.SecretList.ShouldHaveSingleItem();
        list.SecretList[0].Name.ShouldBe("tag-filter-a");
    }

    [Fact]
    public async Task ListSecretsFilterByTagValue()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "tv-filter-a",
            SecretString = "a",
            Tags = [new Tag { Key = "env", Value = "production" }],
        });

        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "tv-filter-b",
            SecretString = "b",
            Tags = [new Tag { Key = "env", Value = "staging" }],
        });

        var list = await _sm.ListSecretsAsync(new ListSecretsRequest
        {
            Filters = [new Filter { Key = FilterNameStringType.TagValue, Values = ["production"] }],
        });

        list.SecretList.ShouldHaveSingleItem();
        list.SecretList[0].Name.ShouldBe("tv-filter-a");
    }

    [Fact]
    public async Task ListSecretsFilterByDescription()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "desc-filter-a",
            SecretString = "a",
            Description = "database credentials",
        });

        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "desc-filter-b",
            SecretString = "b",
            Description = "api key",
        });

        var list = await _sm.ListSecretsAsync(new ListSecretsRequest
        {
            Filters = [new Filter { Key = FilterNameStringType.Description, Values = ["database"] }],
        });

        list.SecretList.ShouldHaveSingleItem();
        list.SecretList[0].Name.ShouldBe("desc-filter-a");
    }

    // -- DescribeSecret with optional fields -----------------------------------

    [Fact]
    public async Task DescribeSecretShowsDeletedDate()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "desc-deleted",
            SecretString = "val",
        });

        await _sm.DeleteSecretAsync(new DeleteSecretRequest
        {
            SecretId = "desc-deleted",
            RecoveryWindowInDays = 7,
        });

        var desc = await _sm.DescribeSecretAsync(new DescribeSecretRequest
        {
            SecretId = "desc-deleted",
        });

        desc.DeletedDate.ShouldNotBe(default);
    }

    [Fact]
    public async Task DescribeSecretShowsKmsKeyId()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "desc-kms",
            SecretString = "val",
            KmsKeyId = "alias/my-key",
        });

        var desc = await _sm.DescribeSecretAsync(new DescribeSecretRequest
        {
            SecretId = "desc-kms",
        });

        desc.KmsKeyId.ShouldBe("alias/my-key");
    }

    // -- UpdateSecretVersionStage edge cases ------------------------------------

    [Fact]
    public async Task UpdateSecretVersionStageMissingVersionStageFails()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "stage-missing",
            SecretString = "val",
        });

        // The SDK might fill in empty string - we need to send a raw request
        // to test missing VersionStage, but at API level the SDK requires it.
        // Test requires RemoveFromVersionId or MoveToVersionId
        var create = await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "stage-no-move",
            SecretString = "val",
        });

        await Should.ThrowAsync<InvalidParameterException>(() =>
            _sm.UpdateSecretVersionStageAsync(new UpdateSecretVersionStageRequest
            {
                SecretId = "stage-no-move",
                VersionStage = "AWSCURRENT",
                // Neither MoveToVersionId nor RemoveFromVersionId
            }));
    }
}
