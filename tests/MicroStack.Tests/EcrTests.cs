using Amazon;
using Amazon.ECR;
using Amazon.ECR.Model;
using Amazon.Runtime;
using Task = System.Threading.Tasks.Task;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the ECR service handler.
/// Mirrors coverage from ministack/tests/test_ecr.py.
/// </summary>
public sealed class EcrTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonECRClient _ecr;

    public EcrTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _ecr = CreateClient(fixture);
    }

    private static AmazonECRClient CreateClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        var config = new AmazonECRConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };
        return new AmazonECRClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _ecr.Dispose();
        return Task.CompletedTask;
    }

    // ── Repository CRUD ───────────────────────────────────────────────────────

    [Fact]
    public async Task CreateRepository()
    {
        var resp = await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "test-app",
        });

        Assert.Equal("test-app", resp.Repository.RepositoryName);
        Assert.NotNull(resp.Repository.RepositoryUri);
        Assert.NotNull(resp.Repository.RepositoryArn);
        Assert.Equal("MUTABLE", resp.Repository.ImageTagMutability.Value);
    }

    [Fact]
    public async Task CreateDuplicateRepository()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "dup-repo",
        });

        await Assert.ThrowsAsync<RepositoryAlreadyExistsException>(() =>
            _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
            {
                RepositoryName = "dup-repo",
            }));
    }

    [Fact]
    public async Task DescribeRepositories()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "desc-repo",
        });

        var resp = await _ecr.DescribeRepositoriesAsync(new DescribeRepositoriesRequest());
        var names = resp.Repositories.Select(r => r.RepositoryName).ToList();
        Assert.Contains("desc-repo", names);
    }

    [Fact]
    public async Task DescribeRepositoriesByName()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "by-name-repo",
        });

        var resp = await _ecr.DescribeRepositoriesAsync(new DescribeRepositoriesRequest
        {
            RepositoryNames = ["by-name-repo"],
        });
        Assert.Single(resp.Repositories);
        Assert.Equal("by-name-repo", resp.Repositories[0].RepositoryName);
    }

    [Fact]
    public async Task DescribeNonexistentRepository()
    {
        await Assert.ThrowsAsync<RepositoryNotFoundException>(() =>
            _ecr.DescribeRepositoriesAsync(new DescribeRepositoriesRequest
            {
                RepositoryNames = ["nonexistent"],
            }));
    }

    // ── Image operations ──────────────────────────────────────────────────────

    [Fact]
    public async Task PutImage()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "img-repo",
        });

        var manifest = "{\"schemaVersion\": 2, \"config\": {\"digest\": \"sha256:abc123\"}}";
        var resp = await _ecr.PutImageAsync(new PutImageRequest
        {
            RepositoryName = "img-repo",
            ImageManifest = manifest,
            ImageTag = "v1.0.0",
        });

        Assert.Equal("img-repo", resp.Image.RepositoryName);
        Assert.Equal("v1.0.0", resp.Image.ImageId.ImageTag);
        Assert.NotNull(resp.Image.ImageId.ImageDigest);
    }

    [Fact]
    public async Task ListImages()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "list-repo",
        });
        await _ecr.PutImageAsync(new PutImageRequest
        {
            RepositoryName = "list-repo",
            ImageManifest = "{\"schemaVersion\": 2}",
            ImageTag = "v1.0.0",
        });

        var resp = await _ecr.ListImagesAsync(new ListImagesRequest
        {
            RepositoryName = "list-repo",
        });
        Assert.True(resp.ImageIds.Count >= 1);
        var tags = resp.ImageIds.Select(i => i.ImageTag).ToList();
        Assert.Contains("v1.0.0", tags);
    }

    [Fact]
    public async Task DescribeImages()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "desc-img-repo",
        });
        await _ecr.PutImageAsync(new PutImageRequest
        {
            RepositoryName = "desc-img-repo",
            ImageManifest = "{\"schemaVersion\": 2}",
            ImageTag = "v1.0.0",
        });

        var resp = await _ecr.DescribeImagesAsync(new DescribeImagesRequest
        {
            RepositoryName = "desc-img-repo",
        });
        Assert.True(resp.ImageDetails.Count >= 1);
        Assert.NotNull(resp.ImageDetails[0].ImageDigest);
        Assert.Contains("v1.0.0", resp.ImageDetails[0].ImageTags);
    }

    [Fact]
    public async Task BatchGetImage()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "batch-get-repo",
        });
        await _ecr.PutImageAsync(new PutImageRequest
        {
            RepositoryName = "batch-get-repo",
            ImageManifest = "{\"schemaVersion\": 2}",
            ImageTag = "v1.0.0",
        });

        var resp = await _ecr.BatchGetImageAsync(new BatchGetImageRequest
        {
            RepositoryName = "batch-get-repo",
            ImageIds = [new ImageIdentifier { ImageTag = "v1.0.0" }],
        });
        Assert.Single(resp.Images);
        Assert.Equal("v1.0.0", resp.Images[0].ImageId.ImageTag);
        Assert.Empty(resp.Failures);
    }

    [Fact]
    public async Task BatchGetImageNotFound()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "batch-nf-repo",
        });

        var resp = await _ecr.BatchGetImageAsync(new BatchGetImageRequest
        {
            RepositoryName = "batch-nf-repo",
            ImageIds = [new ImageIdentifier { ImageTag = "nonexistent" }],
        });
        Assert.Empty(resp.Images);
        Assert.Single(resp.Failures);
    }

    [Fact]
    public async Task BatchDeleteImage()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "batch-del-repo",
        });
        await _ecr.PutImageAsync(new PutImageRequest
        {
            RepositoryName = "batch-del-repo",
            ImageManifest = "{\"schemaVersion\": 2, \"delete\": \"me\"}",
            ImageTag = "to-delete",
        });

        var resp = await _ecr.BatchDeleteImageAsync(new BatchDeleteImageRequest
        {
            RepositoryName = "batch-del-repo",
            ImageIds = [new ImageIdentifier { ImageTag = "to-delete" }],
        });
        Assert.Single(resp.ImageIds);
        Assert.Empty(resp.Failures);
    }

    // ── Authorization ─────────────────────────────────────────────────────────

    [Fact]
    public async Task GetAuthorizationToken()
    {
        var resp = await _ecr.GetAuthorizationTokenAsync(new GetAuthorizationTokenRequest());
        Assert.Single(resp.AuthorizationData);
        Assert.NotNull(resp.AuthorizationData[0].AuthorizationToken);
        Assert.NotNull(resp.AuthorizationData[0].ProxyEndpoint);
    }

    // ── Lifecycle Policy ──────────────────────────────────────────────────────

    [Fact]
    public async Task LifecyclePolicy()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "lc-repo",
        });

        var policy = "{\"rules\": [{\"rulePriority\": 1, \"selection\": {\"tagStatus\": \"untagged\", \"countType\": \"sinceImagePushed\", \"countUnit\": \"days\", \"countNumber\": 14}, \"action\": {\"type\": \"expire\"}}]}";
        await _ecr.PutLifecyclePolicyAsync(new PutLifecyclePolicyRequest
        {
            RepositoryName = "lc-repo",
            LifecyclePolicyText = policy,
        });

        var getResp = await _ecr.GetLifecyclePolicyAsync(new GetLifecyclePolicyRequest
        {
            RepositoryName = "lc-repo",
        });
        Assert.Equal(policy, getResp.LifecyclePolicyText);

        await _ecr.DeleteLifecyclePolicyAsync(new DeleteLifecyclePolicyRequest
        {
            RepositoryName = "lc-repo",
        });

        await Assert.ThrowsAsync<LifecyclePolicyNotFoundException>(() =>
            _ecr.GetLifecyclePolicyAsync(new GetLifecyclePolicyRequest
            {
                RepositoryName = "lc-repo",
            }));
    }

    // ── Repository Policy ─────────────────────────────────────────────────────

    [Fact]
    public async Task RepositoryPolicy()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "rp-repo",
        });

        var policy = "{\"Version\": \"2012-10-17\", \"Statement\": [{\"Effect\": \"Allow\", \"Principal\": \"*\", \"Action\": \"ecr:GetDownloadUrlForLayer\"}]}";
        await _ecr.SetRepositoryPolicyAsync(new SetRepositoryPolicyRequest
        {
            RepositoryName = "rp-repo",
            PolicyText = policy,
        });

        var getResp = await _ecr.GetRepositoryPolicyAsync(new GetRepositoryPolicyRequest
        {
            RepositoryName = "rp-repo",
        });
        Assert.Equal(policy, getResp.PolicyText);

        await _ecr.DeleteRepositoryPolicyAsync(new DeleteRepositoryPolicyRequest
        {
            RepositoryName = "rp-repo",
        });

        await Assert.ThrowsAsync<RepositoryPolicyNotFoundException>(() =>
            _ecr.GetRepositoryPolicyAsync(new GetRepositoryPolicyRequest
            {
                RepositoryName = "rp-repo",
            }));
    }

    // ── Tag mutability ────────────────────────────────────────────────────────

    [Fact]
    public async Task PutImageTagMutability()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "mut-repo",
        });

        await _ecr.PutImageTagMutabilityAsync(new PutImageTagMutabilityRequest
        {
            RepositoryName = "mut-repo",
            ImageTagMutability = ImageTagMutability.IMMUTABLE,
        });

        var desc = await _ecr.DescribeRepositoriesAsync(new DescribeRepositoriesRequest
        {
            RepositoryNames = ["mut-repo"],
        });
        Assert.Equal("IMMUTABLE", desc.Repositories[0].ImageTagMutability.Value);

        await _ecr.PutImageTagMutabilityAsync(new PutImageTagMutabilityRequest
        {
            RepositoryName = "mut-repo",
            ImageTagMutability = ImageTagMutability.MUTABLE,
        });
    }

    // ── Image scanning configuration ──────────────────────────────────────────

    [Fact]
    public async Task PutImageScanningConfiguration()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "scan-repo",
        });

        await _ecr.PutImageScanningConfigurationAsync(new PutImageScanningConfigurationRequest
        {
            RepositoryName = "scan-repo",
            ImageScanningConfiguration = new ImageScanningConfiguration { ScanOnPush = true },
        });

        var desc = await _ecr.DescribeRepositoriesAsync(new DescribeRepositoriesRequest
        {
            RepositoryNames = ["scan-repo"],
        });
        Assert.True(desc.Repositories[0].ImageScanningConfiguration.ScanOnPush);
    }

    // ── Tags ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TagAndUntagResource()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "tag-repo",
        });

        var descResp = await _ecr.DescribeRepositoriesAsync(new DescribeRepositoriesRequest
        {
            RepositoryNames = ["tag-repo"],
        });
        var arn = descResp.Repositories[0].RepositoryArn;

        await _ecr.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = arn,
            Tags = [new Tag { Key = "env", Value = "dev" }],
        });

        var tagsResp = await _ecr.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arn,
        });
        var tagKeys = tagsResp.Tags.Select(t => t.Key).ToList();
        Assert.Contains("env", tagKeys);

        await _ecr.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceArn = arn,
            TagKeys = ["env"],
        });

        var tagsResp2 = await _ecr.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arn,
        });
        var tagKeys2 = tagsResp2.Tags.Select(t => t.Key).ToList();
        Assert.DoesNotContain("env", tagKeys2);
    }

    // ── Delete repository ─────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteRepositoryNotEmpty()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "notempty-repo",
        });
        await _ecr.PutImageAsync(new PutImageRequest
        {
            RepositoryName = "notempty-repo",
            ImageManifest = "{\"schemaVersion\": 2}",
            ImageTag = "latest",
        });

        await Assert.ThrowsAsync<RepositoryNotEmptyException>(() =>
            _ecr.DeleteRepositoryAsync(new DeleteRepositoryRequest
            {
                RepositoryName = "notempty-repo",
            }));
    }

    [Fact]
    public async Task DeleteRepositoryForce()
    {
        await _ecr.CreateRepositoryAsync(new CreateRepositoryRequest
        {
            RepositoryName = "force-del-repo",
        });
        await _ecr.PutImageAsync(new PutImageRequest
        {
            RepositoryName = "force-del-repo",
            ImageManifest = "{\"schemaVersion\": 2}",
            ImageTag = "latest",
        });

        var resp = await _ecr.DeleteRepositoryAsync(new DeleteRepositoryRequest
        {
            RepositoryName = "force-del-repo",
            Force = true,
        });
        Assert.Equal("force-del-repo", resp.Repository.RepositoryName);
    }

    // ── Describe Registry ─────────────────────────────────────────────────────

    [Fact]
    public async Task DescribeRegistry()
    {
        var resp = await _ecr.DescribeRegistryAsync(new DescribeRegistryRequest());
        Assert.NotNull(resp.RegistryId);
        Assert.NotNull(resp.ReplicationConfiguration);
    }
}
