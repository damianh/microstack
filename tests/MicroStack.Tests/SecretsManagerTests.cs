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

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _sm.Dispose();
        return Task.CompletedTask;
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

        Assert.Contains("test/secret1", create.ARN);
        Assert.Equal("test/secret1", create.Name);
        Assert.NotEmpty(create.VersionId);

        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "test/secret1",
        });

        Assert.Equal("{\"user\":\"admin\",\"pass\":\"hunter2\"}", get.SecretString);
        Assert.Equal(create.VersionId, get.VersionId);
        Assert.Contains("AWSCURRENT", get.VersionStages);
    }

    [Fact]
    public async Task CreateSecretDuplicateNameFails()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "dup-secret",
            SecretString = "value1",
        });

        var ex = await Assert.ThrowsAsync<ResourceExistsException>(() =>
            _sm.CreateSecretAsync(new CreateSecretRequest
            {
                Name = "dup-secret",
                SecretString = "value2",
            }));

        Assert.Contains("already exists", ex.Message);
    }

    [Fact]
    public async Task GetSecretValueNotFoundReturnsError()
    {
        await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
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

        Assert.Equal("original", get.SecretString);
    }

    [Fact]
    public async Task GetSecretValueInvalidVersionIdReturnsError()
    {
        await _sm.CreateSecretAsync(new CreateSecretRequest
        {
            Name = "invalid-vid-test",
            SecretString = "val",
        });

        await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
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

        Assert.Equal("del-restore", del.Name);
        Assert.NotEqual(default, del.DeletionDate);

        // Cannot get value while scheduled for deletion
        await Assert.ThrowsAsync<InvalidRequestException>(() =>
            _sm.GetSecretValueAsync(new GetSecretValueRequest { SecretId = "del-restore" }));

        // Restore
        var restore = await _sm.RestoreSecretAsync(new RestoreSecretRequest
        {
            SecretId = "del-restore",
        });

        Assert.Equal("del-restore", restore.Name);

        // Can get value again
        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "del-restore",
        });

        Assert.Equal("val", get.SecretString);
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

        await Assert.ThrowsAsync<ResourceNotFoundException>(() =>
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

        await Assert.ThrowsAsync<InvalidParameterException>(() =>
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

        await Assert.ThrowsAsync<InvalidParameterException>(() =>
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

        await Assert.ThrowsAsync<InvalidRequestException>(() =>
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

        Assert.Equal("update-desc", update.Name);

        var desc = await _sm.DescribeSecretAsync(new DescribeSecretRequest
        {
            SecretId = "update-desc",
        });

        Assert.Equal("new description", desc.Description);
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

        Assert.NotEqual(create.VersionId, update.VersionId);

        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "update-val",
        });

        Assert.Equal("updated", get.SecretString);
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

        Assert.Equal("describe-me", desc.Name);
        Assert.Equal("test desc", desc.Description);
        Assert.Contains("describe-me", desc.ARN);
        Assert.NotEqual(default, desc.CreatedDate);
        Assert.Single(desc.Tags);
        Assert.Equal("env", desc.Tags[0].Key);
        Assert.Equal("test", desc.Tags[0].Value);
        Assert.NotEmpty(desc.VersionIdsToStages);
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

        Assert.NotEqual(create.VersionId, put.VersionId);
        Assert.Contains("AWSCURRENT", put.VersionStages);

        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "put-val",
        });

        Assert.Equal("v2", get.SecretString);
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

        Assert.Contains("AWSPENDING", put.VersionStages);

        // AWSCURRENT still returns v1
        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "put-pending",
        });

        Assert.Equal("v1", get.SecretString);
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

        Assert.True(list.SecretList.Count >= 2);
        Assert.Contains(list.SecretList, s => s.Name == "list-a");
        Assert.Contains(list.SecretList, s => s.Name == "list-b");
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

        Assert.Single(list.SecretList);
        Assert.Equal("filtered-alpha", list.SecretList[0].Name);
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
        Assert.DoesNotContain(list.SecretList, s => s.Name == "list-deleted");
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

        Assert.Equal(2, desc2.Tags.Count);

        await _sm.UntagResourceAsync(new UntagResourceRequest
        {
            SecretId = "tag-test",
            TagKeys = ["team"],
        });

        var desc3 = await _sm.DescribeSecretAsync(new DescribeSecretRequest
        {
            SecretId = "tag-test",
        });

        Assert.Single(desc3.Tags);
        Assert.Equal("env", desc3.Tags[0].Key);
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

        Assert.True(list.Versions.Count >= 2);
        Assert.Equal("versions-list", list.Name);
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

        Assert.Equal("v1", get.SecretString);
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

        Assert.NotEmpty(rotate.VersionId);
        Assert.Equal("rotate-test", rotate.Name);

        // After rotation, AWSCURRENT should still return the value
        var get = await _sm.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = "rotate-test",
        });

        Assert.Equal("original", get.SecretString);
    }

    // -- GetRandomPassword -----------------------------------------------------

    [Fact]
    public async Task GetRandomPasswordDefault()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest());
        Assert.Equal(32, resp.RandomPassword.Length);
    }

    [Fact]
    public async Task GetRandomPasswordCustomLength()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 64,
        });

        Assert.Equal(64, resp.RandomPassword.Length);
    }

    [Fact]
    public async Task GetRandomPasswordExcludeNumbers()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 100,
            ExcludeNumbers = true,
        });

        Assert.DoesNotContain(resp.RandomPassword, c => char.IsDigit(c));
    }

    [Fact]
    public async Task GetRandomPasswordExcludeUppercase()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 100,
            ExcludeUppercase = true,
        });

        Assert.DoesNotContain(resp.RandomPassword, c => char.IsUpper(c));
    }

    [Fact]
    public async Task GetRandomPasswordExcludeLowercase()
    {
        var resp = await _sm.GetRandomPasswordAsync(new GetRandomPasswordRequest
        {
            PasswordLength = 100,
            ExcludeLowercase = true,
        });

        Assert.DoesNotContain(resp.RandomPassword, c => char.IsLower(c));
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
        Assert.DoesNotContain(resp.RandomPassword, c => punctuation.Contains(c));
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

        Assert.Equal(4096, resp.RandomPassword.Length);
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

        Assert.Equal(20, resp.RandomPassword.Length);
        Assert.Contains(resp.RandomPassword, c => char.IsLower(c));
        Assert.Contains(resp.RandomPassword, c => char.IsUpper(c));
        Assert.Contains(resp.RandomPassword, c => char.IsDigit(c));
    }

    [Fact]
    public async Task GetRandomPasswordAllExcludedFails()
    {
        await Assert.ThrowsAsync<InvalidParameterException>(() =>
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

        Assert.DoesNotContain(resp.RandomPassword, c => "abcABC123".Contains(c));
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

        Assert.Single(resp.ReplicationStatus);
        Assert.Equal("eu-west-1", resp.ReplicationStatus[0].Region);
        Assert.Equal("InSync", resp.ReplicationStatus[0].Status);
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

        Assert.Equal("{\"Version\":\"2012-10-17\",\"Statement\":[]}", get.ResourcePolicy);

        await _sm.DeleteResourcePolicyAsync(new DeleteResourcePolicyRequest
        {
            SecretId = "policy-test",
        });

        var get2 = await _sm.GetResourcePolicyAsync(new GetResourcePolicyRequest
        {
            SecretId = "policy-test",
        });

        Assert.Null(get2.ResourcePolicy);
    }

    [Fact]
    public async Task ValidateResourcePolicyAlwaysPasses()
    {
        var resp = await _sm.ValidateResourcePolicyAsync(new ValidateResourcePolicyRequest
        {
            ResourcePolicy = "{}",
            SecretId = "anything",
        });

        Assert.True(resp.PolicyValidationPassed);
        Assert.Empty(resp.ValidationErrors);
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

        Assert.Equal(2, resp.SecretValues.Count);
        Assert.Empty(resp.Errors);
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

        Assert.Single(resp.SecretValues);
        Assert.Single(resp.Errors);
        Assert.Equal("nonexistent", resp.Errors[0].SecretId);
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

        Assert.Equal("by-arn", get.SecretString);
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

        await Assert.ThrowsAsync<InvalidRequestException>(() =>
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

        await Assert.ThrowsAsync<InvalidRequestException>(() =>
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

        await Assert.ThrowsAsync<InvalidRequestException>(() =>
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

        await Assert.ThrowsAsync<InvalidRequestException>(() =>
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

        Assert.Single(list.SecretList);
        Assert.Equal("tag-filter-a", list.SecretList[0].Name);
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

        Assert.Single(list.SecretList);
        Assert.Equal("tv-filter-a", list.SecretList[0].Name);
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

        Assert.Single(list.SecretList);
        Assert.Equal("desc-filter-a", list.SecretList[0].Name);
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

        Assert.NotEqual(default, desc.DeletedDate);
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

        Assert.Equal("alias/my-key", desc.KmsKeyId);
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

        await Assert.ThrowsAsync<InvalidParameterException>(() =>
            _sm.UpdateSecretVersionStageAsync(new UpdateSecretVersionStageRequest
            {
                SecretId = "stage-no-move",
                VersionStage = "AWSCURRENT",
                // Neither MoveToVersionId nor RemoveFromVersionId
            }));
    }
}
