using Amazon;
using Amazon.AppSync;
using Amazon.AppSync.Model;
using Amazon.Runtime;

namespace MicroStack.Tests;

/// <summary>
/// Integration tests for the AppSync service handler (management plane).
/// Uses the AWS SDK for .NET pointed at the in-process MicroStack server.
///
/// Mirrors management-plane coverage from ministack/tests/test_appsync.py.
/// GraphQL data-plane tests are excluded as they use raw HTTP against localhost:4566.
/// </summary>
public sealed class AppSyncTests : IClassFixture<MicroStackFixture>, IAsyncLifetime
{
    private readonly MicroStackFixture _fixture;
    private readonly AmazonAppSyncClient _appsync;

    public AppSyncTests(MicroStackFixture fixture)
    {
        _fixture = fixture;
        _appsync = CreateAppSyncClient(fixture);
    }

    private static AmazonAppSyncClient CreateAppSyncClient(MicroStackFixture fixture)
    {
        var innerHandler = fixture.Factory.Server.CreateHandler();
        var httpClient = new HttpClient(new CanonicalizeUriHandler(innerHandler))
        {
            BaseAddress = new Uri("http://localhost/"),
        };

        var config = new AmazonAppSyncConfig
        {
            RegionEndpoint = RegionEndpoint.USEast1,
            ServiceURL = "http://localhost/",
            HttpClientFactory = new FixedHttpClientFactory(httpClient),
        };

        return new AmazonAppSyncClient(
            new BasicAWSCredentials("test", "test"), config);
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.HttpClient.PostAsync("/_ministack/reset", null);
    }

    public ValueTask DisposeAsync()
    {
        _appsync.Dispose();
        return ValueTask.CompletedTask;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CreateGraphqlApi + ListGraphqlApis
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateAndListGraphqlApi()
    {
        var resp = await _appsync.CreateGraphqlApiAsync(new CreateGraphqlApiRequest
        {
            Name = "test-api",
            AuthenticationType = AuthenticationType.API_KEY,
        });

        var api = resp.GraphqlApi;
        api.Name.ShouldBe("test-api");
        api.ApiId.ShouldNotBeEmpty();
        api.AuthenticationType.ShouldBe(AuthenticationType.API_KEY);

        var listResp = await _appsync.ListGraphqlApisAsync(new ListGraphqlApisRequest());
        listResp.GraphqlApis.ShouldContain(a => a.ApiId == api.ApiId);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // GetGraphqlApi + DeleteGraphqlApi
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GetAndDeleteGraphqlApi()
    {
        var resp = await _appsync.CreateGraphqlApiAsync(new CreateGraphqlApiRequest
        {
            Name = "del-api",
            AuthenticationType = AuthenticationType.API_KEY,
        });
        var apiId = resp.GraphqlApi.ApiId;

        var getResp = await _appsync.GetGraphqlApiAsync(new GetGraphqlApiRequest
        {
            ApiId = apiId,
        });
        getResp.GraphqlApi.Name.ShouldBe("del-api");

        await _appsync.DeleteGraphqlApiAsync(new DeleteGraphqlApiRequest
        {
            ApiId = apiId,
        });

        await Should.ThrowAsync<NotFoundException>(() =>
            _appsync.GetGraphqlApiAsync(new GetGraphqlApiRequest { ApiId = apiId }));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // UpdateGraphqlApi
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task UpdateGraphqlApi()
    {
        var resp = await _appsync.CreateGraphqlApiAsync(new CreateGraphqlApiRequest
        {
            Name = "upd-api",
            AuthenticationType = AuthenticationType.API_KEY,
        });
        var apiId = resp.GraphqlApi.ApiId;

        await _appsync.UpdateGraphqlApiAsync(new UpdateGraphqlApiRequest
        {
            ApiId = apiId,
            Name = "updated-api",
            AuthenticationType = AuthenticationType.AWS_IAM,
        });

        var getResp = await _appsync.GetGraphqlApiAsync(new GetGraphqlApiRequest
        {
            ApiId = apiId,
        });

        getResp.GraphqlApi.Name.ShouldBe("updated-api");
        getResp.GraphqlApi.AuthenticationType.ShouldBe(AuthenticationType.AWS_IAM);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // API Key CRUD
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ApiKeyCrud()
    {
        var api = (await _appsync.CreateGraphqlApiAsync(new CreateGraphqlApiRequest
        {
            Name = "key-api",
            AuthenticationType = AuthenticationType.API_KEY,
        })).GraphqlApi;

        var keyResp = await _appsync.CreateApiKeyAsync(new CreateApiKeyRequest
        {
            ApiId = api.ApiId,
        });
        keyResp.ApiKey.Id.ShouldNotBeEmpty();

        var listResp = await _appsync.ListApiKeysAsync(new ListApiKeysRequest
        {
            ApiId = api.ApiId,
        });
        listResp.ApiKeys.ShouldContain(k => k.Id == keyResp.ApiKey.Id);

        await _appsync.DeleteApiKeyAsync(new DeleteApiKeyRequest
        {
            ApiId = api.ApiId,
            Id = keyResp.ApiKey.Id,
        });

        var listResp2 = await _appsync.ListApiKeysAsync(new ListApiKeysRequest
        {
            ApiId = api.ApiId,
        });
        listResp2.ApiKeys.ShouldNotContain(k => k.Id == keyResp.ApiKey.Id);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DataSource CRUD
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DataSourceCrud()
    {
        var api = (await _appsync.CreateGraphqlApiAsync(new CreateGraphqlApiRequest
        {
            Name = "ds-api",
            AuthenticationType = AuthenticationType.API_KEY,
        })).GraphqlApi;

        var dsResp = await _appsync.CreateDataSourceAsync(new CreateDataSourceRequest
        {
            ApiId = api.ApiId,
            Name = "myds",
            Type = DataSourceType.AMAZON_DYNAMODB,
            DynamodbConfig = new DynamodbDataSourceConfig
            {
                TableName = "test-table",
                AwsRegion = "us-east-1",
            },
        });
        dsResp.DataSource.Name.ShouldBe("myds");

        var getResp = await _appsync.GetDataSourceAsync(new GetDataSourceRequest
        {
            ApiId = api.ApiId,
            Name = "myds",
        });
        getResp.DataSource.Name.ShouldBe("myds");

        var listResp = await _appsync.ListDataSourcesAsync(new ListDataSourcesRequest
        {
            ApiId = api.ApiId,
        });
        listResp.DataSources.ShouldContain(d => d.Name == "myds");

        await _appsync.DeleteDataSourceAsync(new DeleteDataSourceRequest
        {
            ApiId = api.ApiId,
            Name = "myds",
        });

        var listResp2 = await _appsync.ListDataSourcesAsync(new ListDataSourcesRequest
        {
            ApiId = api.ApiId,
        });
        listResp2.DataSources.ShouldNotContain(d => d.Name == "myds");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Resolver CRUD
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ResolverCrud()
    {
        var api = (await _appsync.CreateGraphqlApiAsync(new CreateGraphqlApiRequest
        {
            Name = "res-api",
            AuthenticationType = AuthenticationType.API_KEY,
        })).GraphqlApi;

        await _appsync.CreateDataSourceAsync(new CreateDataSourceRequest
        {
            ApiId = api.ApiId,
            Name = "ds",
            Type = DataSourceType.AMAZON_DYNAMODB,
        });

        var resResp = await _appsync.CreateResolverAsync(new CreateResolverRequest
        {
            ApiId = api.ApiId,
            TypeName = "Query",
            FieldName = "getItem",
            DataSourceName = "ds",
        });
        resResp.Resolver.FieldName.ShouldBe("getItem");

        var getResp = await _appsync.GetResolverAsync(new GetResolverRequest
        {
            ApiId = api.ApiId,
            TypeName = "Query",
            FieldName = "getItem",
        });
        getResp.Resolver.FieldName.ShouldBe("getItem");

        var listResp = await _appsync.ListResolversAsync(new ListResolversRequest
        {
            ApiId = api.ApiId,
            TypeName = "Query",
        });
        listResp.Resolvers.ShouldContain(r => r.FieldName == "getItem");

        await _appsync.DeleteResolverAsync(new DeleteResolverRequest
        {
            ApiId = api.ApiId,
            TypeName = "Query",
            FieldName = "getItem",
        });

        var listResp2 = await _appsync.ListResolversAsync(new ListResolversRequest
        {
            ApiId = api.ApiId,
            TypeName = "Query",
        });
        listResp2.Resolvers.ShouldNotContain(r => r.FieldName == "getItem");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Type CRUD
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TypeCrud()
    {
        var api = (await _appsync.CreateGraphqlApiAsync(new CreateGraphqlApiRequest
        {
            Name = "type-api",
            AuthenticationType = AuthenticationType.API_KEY,
        })).GraphqlApi;

        var typeResp = await _appsync.CreateTypeAsync(new CreateTypeRequest
        {
            ApiId = api.ApiId,
            Definition = "type User { id: ID! name: String }",
            Format = TypeDefinitionFormat.SDL,
        });
        typeResp.Type.Name.ShouldNotBeEmpty();

        var listResp = await _appsync.ListTypesAsync(new ListTypesRequest
        {
            ApiId = api.ApiId,
            Format = TypeDefinitionFormat.SDL,
        });
        (listResp.Types.Count >= 1).ShouldBe(true);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tags
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TagOperations()
    {
        var api = (await _appsync.CreateGraphqlApiAsync(new CreateGraphqlApiRequest
        {
            Name = "tag-api",
            AuthenticationType = AuthenticationType.API_KEY,
            Tags = new Dictionary<string, string> { ["env"] = "test" },
        })).GraphqlApi;

        var arn = api.Arn;
        var listResp = await _appsync.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arn,
        });
        listResp.Tags.ContainsKey("env").ShouldBe(true);

        await _appsync.TagResourceAsync(new TagResourceRequest
        {
            ResourceArn = arn,
            Tags = new Dictionary<string, string> { ["team"] = "data" },
        });

        var listResp2 = await _appsync.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arn,
        });
        listResp2.Tags.Count.ShouldBe(2);

        await _appsync.UntagResourceAsync(new UntagResourceRequest
        {
            ResourceArn = arn,
            TagKeys = ["env"],
        });

        var listResp3 = await _appsync.ListTagsForResourceAsync(new ListTagsForResourceRequest
        {
            ResourceArn = arn,
        });
        listResp3.Tags.ShouldHaveSingleItem();
        listResp3.Tags.ContainsKey("team").ShouldBe(true);
    }
}
