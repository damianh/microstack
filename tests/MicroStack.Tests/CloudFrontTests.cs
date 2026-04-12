using Amazon;
using Amazon.CloudFront;
using Amazon.CloudFront.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the CloudFront service handler.
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors coverage from ministack/tests/test_cloudfront.py.
/// </summary>
public sealed class CloudFrontTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonCloudFrontClient _cf;

    public CloudFrontTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _cf = CreateCloudFrontClient(fixture);
    }

    private static AmazonCloudFrontClient CreateCloudFrontClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonCloudFrontConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonCloudFrontClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async Task InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public Task DisposeAsync()
    {
        _cf.Dispose();
        return Task.CompletedTask;
    }

    private static DistributionConfig MakeDistConfig(string callerRef, string comment, bool enabled = true)
    {
        return new DistributionConfig
        {
            CallerReference = callerRef,
            Comment = comment,
            Enabled = enabled,
            Origins = new Origins
            {
                Quantity = 1,
                Items =
                [
                    new Origin
                    {
                        Id = "myS3Origin",
                        DomainName = "mybucket.s3.amazonaws.com",
                        S3OriginConfig = new S3OriginConfig
                        {
                            OriginAccessIdentity = "",
                        },
                    },
                ],
            },
            DefaultCacheBehavior = new DefaultCacheBehavior
            {
                TargetOriginId = "myS3Origin",
                ViewerProtocolPolicy = ViewerProtocolPolicy.RedirectToHttps,
                CachePolicyId = "658327ea-f89d-4fab-a63d-7e88639e58f6",
            },
        };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CreateDistribution
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateDistribution()
    {
        var resp = await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-test-ref-1", "test distribution"),
        });

        var dist = resp.Distribution;
        Assert.NotEmpty(dist.Id);
        Assert.EndsWith(".cloudfront.net", dist.DomainName);
        Assert.Equal("Deployed", dist.Status);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // ListDistributions
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ListDistributions()
    {
        await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-list-a", "list-a"),
        });
        await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-list-b", "list-b"),
        });

        var resp = await _cf.ListDistributionsAsync(new ListDistributionsRequest());
        Assert.True(resp.DistributionList.Items.Count >= 2);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // GetDistribution
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GetDistribution()
    {
        var createResp = await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-get-1", "get-test"),
        });
        var distId = createResp.Distribution.Id;

        var getResp = await _cf.GetDistributionAsync(new GetDistributionRequest { Id = distId });
        Assert.Equal(distId, getResp.Distribution.Id);
        Assert.Equal($"{distId}.cloudfront.net", getResp.Distribution.DomainName);
        Assert.Equal("Deployed", getResp.Distribution.Status);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // GetDistributionConfig
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GetDistributionConfig()
    {
        var createResp = await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-getcfg-1", "getcfg-test"),
        });
        var distId = createResp.Distribution.Id;
        var etag = createResp.ETag;

        var resp = await _cf.GetDistributionConfigAsync(new GetDistributionConfigRequest { Id = distId });
        Assert.Equal(etag, resp.ETag);
        Assert.Equal("getcfg-test", resp.DistributionConfig.Comment);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // UpdateDistribution
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task UpdateDistribution()
    {
        var createResp = await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-upd-1", "before-update"),
        });
        var distId = createResp.Distribution.Id;
        var etag = createResp.ETag;

        var updatedConfig = MakeDistConfig("cf-upd-1", "after-update");
        var updResp = await _cf.UpdateDistributionAsync(new UpdateDistributionRequest
        {
            DistributionConfig = updatedConfig,
            Id = distId,
            IfMatch = etag,
        });

        Assert.Equal(distId, updResp.Distribution.Id);
        Assert.NotEqual(etag, updResp.ETag);

        var getResp = await _cf.GetDistributionConfigAsync(new GetDistributionConfigRequest { Id = distId });
        Assert.Equal("after-update", getResp.DistributionConfig.Comment);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // UpdateDistribution ETag Mismatch
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task UpdateDistributionEtagMismatch()
    {
        var createResp = await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-etag-mismatch", "mismatch-test"),
        });
        var distId = createResp.Distribution.Id;

        var ex = await Assert.ThrowsAsync<PreconditionFailedException>(() =>
            _cf.UpdateDistributionAsync(new UpdateDistributionRequest
            {
                DistributionConfig = MakeDistConfig("cf-etag-mismatch", "mismatch-test"),
                Id = distId,
                IfMatch = "wrong-etag-value",
            }));
        Assert.Equal("PreconditionFailed", ex.ErrorCode);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DeleteDistribution
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DeleteDistribution()
    {
        var createResp = await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-del-1", "delete-test", enabled: true),
        });
        var distId = createResp.Distribution.Id;
        var etag = createResp.ETag;

        // Disable first
        var disabledConfig = MakeDistConfig("cf-del-1", "delete-test", enabled: false);
        var updResp = await _cf.UpdateDistributionAsync(new UpdateDistributionRequest
        {
            DistributionConfig = disabledConfig,
            Id = distId,
            IfMatch = etag,
        });
        var newEtag = updResp.ETag;

        await _cf.DeleteDistributionAsync(new DeleteDistributionRequest
        {
            Id = distId,
            IfMatch = newEtag,
        });

        var ex = await Assert.ThrowsAsync<NoSuchDistributionException>(() =>
            _cf.GetDistributionAsync(new GetDistributionRequest { Id = distId }));
        Assert.Equal("NoSuchDistribution", ex.ErrorCode);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DeleteEnabledDistribution
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DeleteEnabledDistribution()
    {
        var createResp = await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-del-enabled", "del-enabled-test", enabled: true),
        });
        var distId = createResp.Distribution.Id;
        var etag = createResp.ETag;

        var ex = await Assert.ThrowsAsync<DistributionNotDisabledException>(() =>
            _cf.DeleteDistributionAsync(new DeleteDistributionRequest
            {
                Id = distId,
                IfMatch = etag,
            }));
        Assert.Equal("DistributionNotDisabled", ex.ErrorCode);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // GetNonexistent
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GetNonexistent()
    {
        var ex = await Assert.ThrowsAsync<NoSuchDistributionException>(() =>
            _cf.GetDistributionAsync(new GetDistributionRequest { Id = "ENONEXISTENT1234" }));
        Assert.Equal("NoSuchDistribution", ex.ErrorCode);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CreateInvalidation
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateInvalidation()
    {
        var createResp = await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-inv-1", "inv-test"),
        });
        var distId = createResp.Distribution.Id;

        var invResp = await _cf.CreateInvalidationAsync(new CreateInvalidationRequest
        {
            DistributionId = distId,
            InvalidationBatch = new InvalidationBatch
            {
                CallerReference = "inv-ref-1",
                Paths = new Paths
                {
                    Quantity = 2,
                    Items = ["/index.html", "/static/*"],
                },
            },
        });

        Assert.NotEmpty(invResp.Invalidation.Id);
        Assert.Equal("Completed", invResp.Invalidation.Status);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // ListInvalidations
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ListInvalidations()
    {
        var createResp = await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-listinv-1", "listinv-test"),
        });
        var distId = createResp.Distribution.Id;

        await _cf.CreateInvalidationAsync(new CreateInvalidationRequest
        {
            DistributionId = distId,
            InvalidationBatch = new InvalidationBatch
            {
                CallerReference = "inv-list-a",
                Paths = new Paths { Quantity = 1, Items = ["/a"] },
            },
        });

        await _cf.CreateInvalidationAsync(new CreateInvalidationRequest
        {
            DistributionId = distId,
            InvalidationBatch = new InvalidationBatch
            {
                CallerReference = "inv-list-b",
                Paths = new Paths { Quantity = 1, Items = ["/b"] },
            },
        });

        var resp = await _cf.ListInvalidationsAsync(new ListInvalidationsRequest
        {
            DistributionId = distId,
        });
        Assert.Equal(2, resp.InvalidationList.Quantity);
        Assert.Equal(2, resp.InvalidationList.Items.Count);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // GetInvalidation
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GetInvalidation()
    {
        var createResp = await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("cf-getinv-1", "getinv-test"),
        });
        var distId = createResp.Distribution.Id;

        var invResp = await _cf.CreateInvalidationAsync(new CreateInvalidationRequest
        {
            DistributionId = distId,
            InvalidationBatch = new InvalidationBatch
            {
                CallerReference = "inv-get-ref",
                Paths = new Paths { Quantity = 1, Items = ["/getinv-path"] },
            },
        });
        var invId = invResp.Invalidation.Id;

        var getResp = await _cf.GetInvalidationAsync(new GetInvalidationRequest
        {
            DistributionId = distId,
            Id = invId,
        });

        Assert.Equal(invId, getResp.Invalidation.Id);
        Assert.Equal("Completed", getResp.Invalidation.Status);
        Assert.Contains("/getinv-path", getResp.Invalidation.InvalidationBatch.Paths.Items);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tags
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TagOperations()
    {
        var createResp = await _cf.CreateDistributionAsync(new CreateDistributionRequest
        {
            DistributionConfig = MakeDistConfig("tag-test-v42", "tag test"),
        });
        var distArn = createResp.Distribution.ARN;

        await _cf.TagResourceAsync(new TagResourceRequest
        {
            Resource = distArn,
            Tags = new Tags
            {
                Items =
                [
                    new Tag { Key = "env", Value = "test" },
                    new Tag { Key = "team", Value = "platform" },
                ],
            },
        });

        var tagsResp = await _cf.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            Resource = distArn,
        });
        var tagMap = tagsResp.Tags.Items.ToDictionary(t => t.Key, t => t.Value);
        Assert.Equal("test", tagMap["env"]);
        Assert.Equal("platform", tagMap["team"]);

        await _cf.UntagResourceAsync(new UntagResourceRequest
        {
            Resource = distArn,
            TagKeys = new TagKeys { Items = ["team"] },
        });

        var tags2 = await _cf.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            Resource = distArn,
        });
        var keys = tags2.Tags.Items.Select(t => t.Key).ToList();
        Assert.Contains("env", keys);
        Assert.DoesNotContain("team", keys);
    }
}
